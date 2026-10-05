# Perguntas e respostas para estudo

Material pessoal de preparação. Cada resposta está no tamanho de uma fala de entrevista e
aponta o arquivo onde a prova está. Antes de cada bloco, tente responder sem olhar.

---

## 1. Visão geral e escopo

**Explique o projeto em um minuto.**
Um serviço que recebe operações de provedores de jogos — aposta, ganho, perda, reembolso e
rollback — e movimenta a carteira do jogador. As operações entram por HTTP ou por uma fila
SQS e passam pelo mesmo processador. Três garantias: o saldo nunca fica negativo sob
concorrência, a mesma operação nunca é aplicada duas vezes, e todo movimento fica num
ledger que não pode ser alterado.

**Por que você não entregou tudo que o enunciado pede?**
O enunciado é de nível pleno/sênior e o prazo era de três dias. Preferi entregar o núcleo
financeiro bem testado e documentar o resto do que entregar tudo pela metade. O que ficou de
fora está na seção 10 do `ARCHITECTURE.md`: publisher da outbox, worker de referências
pendentes, reconciliação, ledger paginado e métricas. A seção 11
do mesmo arquivo compara o que fiz com cada critério de avaliação.

**Se tivesse mais um dia, o que faria primeiro?**
Escrever o publisher da outbox, porque é a maior lacuna em relação ao enunciado. Depois,
as métricas e a referência do `WIN`.

**Qual a parte de que você mais se orgulha? E a mais fraca?**
Mais forte: a idempotência atômica com o movimento financeiro, testada com 50 requisições
simultâneas e cruzando HTTP com SQS, e com o saldo conferido contra o ledger ao final. Mais fraca: o caso de uso mora no adapter Postgres, em
vez de uma camada de aplicação com portas.

---

## 2. Dinheiro

**Por que não usar `float64`?**
`0.1 + 0.2` não dá `0.3` em ponto flutuante. Em dinheiro, o erro acumula e o saldo deixa de
bater com o ledger. Uso `int64` em centavos.

**Por que o valor é string no JSON?**
Um número JSON vira `float` na maioria dos clientes. Como string decimal, o valor chega
exato e eu controlo o parsing (`ParseExternalAmount`, em `internal/domain/money.go`).

**O que acontece com `"25.001"`, `"-5"` e `""`?**
Todos são rejeitados com `ErrInvalidAmount`, que vira 400. A expressão regular aceita só
dígitos e até duas casas decimais.

**Como você trata overflow?**
`Add`, `Sub` e a conversão do parsing verificam o limite antes da operação e devolvem
`ErrMoneyOverflow`. O valor nunca "dá a volta".

**E moedas com três casas ou nenhuma, como BHD ou JPY?**
Não são suportadas: a escala é fixa em duas casas. É uma limitação documentada. A correção
seria guardar a escala por moeda.

**Qual a diferença entre `NewMoney` e `ParseExternalAmount`?**
`ParseExternalAmount` é a porta de entrada: recusa negativo e formato inválido. `NewMoney` é
interno e aceita negativo, porque um cálculo intermediário (como a diferença numa
reconciliação) pode ser negativo.

---

## 3. Concorrência e locks

**Saldo 100, duas apostas de 80 ao mesmo tempo. O que acontece?**
Cada uma abre uma transação e faz `SELECT ... FOR UPDATE` na carteira. A segunda fica
bloqueada até a primeira dar commit. Quando é liberada, lê o saldo 20 e é rejeitada com
`insufficient_funds`. Resultado: uma processada, uma rejeitada, saldo 20. Teste:
`TestConcurrentBets_OneWinsOneLoses`, rodado 100 vezes com `-race`.

**Por que lock pessimista e não otimista?**
Com otimista, a perdedora teria que repetir a tentativa, e em carteira disputada isso vira
uma tempestade de retries. Com `FOR UPDATE`, o banco enfileira. O custo é segurar uma
conexão durante a espera.

**Então para que serve a coluna `version`?**
É uma segunda barreira: `UPDATE ... WHERE version = $anterior`. Com o lock ela nunca deveria
falhar. Se falhar, alguém leu a carteira fora de um lock — um bug que eu quero ver.

**Pode dar deadlock?**
Não entre operações: cada operação trava uma única carteira. Deadlock exige dois recursos
travados em ordens diferentes.

**Como você prova que não há lock global?**
Um teste abre uma transação que segura o lock da carteira A e, enquanto ela está aberta,
processa uma operação na carteira B, que precisa terminar em até 3 segundos
(`TestProcess_ABusyWalletDoesNotBlockAnotherOne`). Outro teste processa 20 carteiras ao
mesmo tempo. E o teste de carga mostra a diferença: 2.523 RPS com 50 carteiras contra 462
com uma só.

**Isso funciona com três instâncias da aplicação?**
Sim, e está testado com processos de verdade. O pacote `internal/e2e` compila o binário,
sobe três processos independentes (memória e pool próprios) contra o mesmo Postgres e
dispara as apostas pelo HTTP. O lock é do banco, não um mutex em memória, então os três
processos se coordenam sem se conhecer. O que o teste não exercita é o consumidor SQS entre
instâncias.

**Um `sync.Mutex` não resolveria?**
Só dentro de um processo. Com duas instâncias, cada uma teria o seu mutex e as duas
debitariam.

**E se o processo morrer segurando o lock?**
A conexão cai, o Postgres aborta a transação e solta o lock. Nada foi gravado.

**Qual o gargalo desse desenho?**
Uma carteira muito disputada serializa tudo, e cada espera ocupa uma conexão do pool. Com o
pool esgotado, `DB_ACQUIRE_TIMEOUT` devolve erro em vez de pendurar. No teste de carga, uma
carteira só entregou 462 RPS, cerca de 2,2 ms por operação serializada.

---

## 4. Idempotência

**O que é idempotência aqui?**
Repetir a mesma requisição produz o mesmo resultado e um único efeito. O provedor pode
reenviar depois de um timeout sem medo de debitar duas vezes.

**Como funciona, passo a passo?**
Dentro da transação: `INSERT` da chave com `ON CONFLICT DO NOTHING`. Se inseriu, sou o
primeiro: processo e gravo a resposta na mesma linha. Se não inseriu, leio a linha: hash
igual devolve a resposta gravada; hash diferente é 422; `in_flight` é 409.

**Duas requisições idênticas chegam no mesmo milissegundo. E aí?**
As duas tentam inserir a mesma chave primária. A segunda bloqueia no índice até a primeira
terminar. Se a primeira deu commit, a segunda não insere e cai no replay. Se deu rollback, a
segunda insere e processa. Teste: `TestProcess_ConcurrentSameKeyCreatesOneOperation`.

**Por que a chave fica na mesma transação do débito?**
Se fossem separadas, haveria uma janela: débito gravado e chave não, ou o contrário. Na
mesma transação, ou existem os dois ou nenhum.

**Então quando o 409 `in_flight` acontece?**
Na prática, quase nunca, justamente porque a concorrente bloqueia. O ramo existe para o
contrato e é testado com uma linha semeada. Sou honesto sobre isso no documento.

**O que entra no hash e por quê?**
Só os campos de negócio. A chave e os metadados de transporte ficam de fora para que a
mesma operação tenha o mesmo hash por HTTP e por SQS.

**O que é "JSON canônico" na sua implementação?**
O `json.Marshal` de uma struct: a ordem dos campos é fixa pelo tipo, não há espaços, e o
valor passa pelo `MarshalJSON` do `Money`, então `25` e `25.00` dão o mesmo hash.

**Por que `TEXT` e não `JSONB` para a resposta?**
O `JSONB` normaliza o documento e reordena chaves. Descobri isso com um teste de replay
falhando.

**Saldo insuficiente: você grava ou faz rollback?**
Gravo. É uma decisão de negócio: vira uma operação `REJECTED` com código, e o replay devolve
a mesma rejeição. Já entrada inválida faz rollback e não consome a chave.

**O jogador informado precisa ser o dono da carteira?**
Sim. Depois do lock, o processador compara o dono da carteira com o `playerId` da operação.
Se forem diferentes, devolve `ErrWalletOwnerMismatch`: nada se move, a chave não é
consumida, e o HTTP responde 404, igual a carteira inexistente, para não revelar que ela
existe. Por SQS é erro permanente e a mensagem vai para a DLQ. Antes disso, um provedor
autenticado podia movimentar qualquer carteira informando outro jogador; descobri relendo o
enunciado contra o código.

**Mesma operação, chave diferente. O que acontece?**
A constraint `UNIQUE (provider_id, external_transaction_id)` impede o segundo efeito. A
violação vira `ErrDuplicateExternalTransaction`, que responde 409 `duplicate_operation`. A
transação inteira é desfeita, então o saldo não muda e a chave nova fica livre. Reconheço a
constraint pelo **nome**, e não só pelo código `23505`, porque a tabela tem outro índice único
(o de uma reversão por operação) que não pode ser confundido com esse. Teste:
`TestProcess_SameExternalIDUnderAnotherKeyIsAConflictAndMovesNothing`. Antes da correção
saía um 500 genérico.

**As chaves expiram?**
Não. A tabela cresce sem limite. Em produção haveria uma política de retenção.

**Abrir carteira é idempotente?**
Parcialmente: a unicidade de jogador e moeda impede a segunda carteira, mas a repetição
recebe 409 em vez de um replay.

---

## 5. Domínio

**Por que os campos são privados?**
Para que o único jeito de mudar o saldo seja `Debit` e `Credit`, que também produzem o
lançamento do ledger. Saldo e ledger mudam juntos.

**`NewWallet` e `RehydrateWallet`: qual a diferença?**
`New` valida e cria. `Rehydrate` reconstrói a partir do banco sem validar e sem reaplicar
movimentos — reaplicar contaria o histórico duas vezes.

**Quais os estados de uma operação?**
`PENDING`, `PENDING_REFERENCE`, `PROCESSED`, `REJECTED`, `FAILED`. Os três últimos são
terminais e imutáveis.

**O que `LOSS` faz?**
Nada no saldo. Valor zero, sem lançamento, sem mudar a versão. Só registra que a rodada foi
perdida. Como não há movimento, `Debit` e `Credit` não conferem a moeda; por isso o
processador confere a moeda da carteira antes de registrar o `LOSS`.

**Diferença entre `REFUND` e `ROLLBACK`?**
`REFUND` só devolve uma aposta. `ROLLBACK` desfaz aposta, ganho ou reembolso; desfazer um
ganho é um débito, e pode ser rejeitado por falta de saldo.

**Como você impede duas reversões da mesma aposta?**
Três camadas: o domínio valida, a consulta de "já revertida" roda depois do lock da
carteira, e um índice único parcial no banco permite só uma reversão `PROCESSED` por
referência.

**Por que a consulta precisa vir depois do lock?**
Sem o lock, duas reversões simultâneas consultariam, veriam "nenhuma", e as duas passariam.

**E se o reembolso chegar antes da aposta?**
É rejeitado com `reference_not_found`. O enunciado pede estacionar em `PENDING_REFERENCE` e
tentar de novo; o estado existe no domínio, mas o worker não foi feito.

**Por que `OPENING` não passa pelo construtor normal?**
`NewWagerTransaction` é a porta de entrada externa e recusa `OPENING`. A abertura usa um
construtor interno. Assim é impossível um provedor criar saldo do nada.

---

## 6. Banco de dados

**O que o banco garante sozinho, mesmo com bug no código?**
Saldo não negativo (`CHECK`), mesma transação não aplicada duas vezes na carteira
(`UNIQUE`), operação única por provedor, no máximo uma reversão bem-sucedida, e ledger
imutável por trigger.

**Como o ledger é append-only?**
Triggers recusam `UPDATE`, `DELETE` e `TRUNCATE`. Correção se faz com um novo lançamento.

**Um superusuário não pode desligar o trigger?**
Pode. O trigger protege contra bug da aplicação, não contra um administrador mal
intencionado. Para isso seriam necessários permissões separadas e auditoria.

**Por que guardar o saldo se dá para somar o ledger?**
Leitura e checagem de saldo em tempo constante. O ledger serve para auditoria e para a
reconciliação. O endpoint de reconciliação não foi implementado, mas os testes de integração
conferem o saldo guardado contra o ledger (`assertLedgerMatchesBalance`) e o teste de carga
confere cada carteira contra as operações confirmadas.

**Por que pgx e não GORM?**
O enunciado pede transações, locks e constraints explícitos. Com SQL à vista, dá para
apontar a linha do `FOR UPDATE`.

**Como rodam as migrations?**
`golang-migrate`. No Compose, um serviço dedicado aplica e termina antes do app subir. Há
um teste que faz up, down e up de novo.

---

## 7. HTTP

**Por que `net/http` puro?**
Desde o Go 1.22 o `ServeMux` aceita método e parâmetro na rota. Uma dependência a menos.

**Como você mapeia erro para status?**
Os handlers devolvem `error`; uma única função, `statusFor`, traduz. Erro sem mapeamento
vira 500 com mensagem genérica, e a causa vai só para o log.

**Dois 422 diferentes: como o cliente distingue?**
Rejeição de negócio traz `status: REJECTED` e `failureCode`. Chave reutilizada traz o
envelope `error` com `idempotency_key_reused`.

**Por que 404 para operação de outro provedor, e não 403?**
403 confirmaria que a operação existe.

**Por que `DisallowUnknownFields`?**
Um campo com erro de digitação seria ignorado e a operação seguiria incompleta. Prefiro
falhar com 400.

---

## 8. Autenticação

**Como a autenticação funciona?**
O provedor obtém um token no Keycloak com `client_credentials`. A API verifica assinatura,
emissor e validade, e lê o `azp`, que é o provedor.

**O que é JWKS?**
O endereço onde o IdP publica as chaves públicas. A API baixa e guarda em cache; não precisa
chamar o Keycloak a cada requisição.

**Por que restringir a `RS256`?**
Para barrar `alg: none` e o ataque em que alguém assina com HMAC usando a chave pública como
segredo. Há um teste com token HS256.

**Por que você não valida o `aud`?**
No `client_credentials` o Keycloak emite `account`. A identidade está no `azp`. O mais
rigoroso seria configurar uma audiência própria e exigi-la.

**Como separar provedor de serviço interno?**
Uma realm role, `wallet-internal`. Rotas de carteira exigem; rotas de operação recusam.

**E a revogação de um token?**
Não há. O token vale até expirar, cinco minutos.

**A fila é autenticada?**
Não por mensagem. O `providerId` vem do corpo, e a confiança é em quem tem permissão de
escrita na fila. É uma diferença real em relação ao HTTP.

**Por que duas variáveis, issuer e JWKS?**
No Docker o token diz `localhost:8081`, mas de dentro do container o Keycloak é
`keycloak:8080`.

---

## 9. SQS, inbox e outbox

**Por que fila FIFO?**
Ordem por grupo. Com `MessageGroupId` igual à carteira, as operações de uma carteira chegam
em ordem.

**A SQS já deduplica. Para que a inbox?**
A deduplicação da SQS dura cinco minutos e não cobre reentrega após falha do consumidor. A
inbox é durável e fica na mesma transação da operação.

**Por que uma mensagem por vez?**
Em lote, se a primeira falha e vai para retry, a segunda da mesma carteira seria aplicada
antes.

**Quando a mensagem é apagada?**
Só depois do commit. Se o processo morrer entre o commit e o delete, a mensagem volta, a
inbox e a chave reconhecem, e o resultado é um replay.

**Erro permanente e transitório: como você decide?**
Lista explícita em `isPermanent`. Fora dela, é transitório — o lado seguro, porque a
mensagem acaba na DLQ depois das tentativas.

**Como é o backoff?**
Mudando o visibility timeout: 2, 4, 8 segundos, até 60. Sem `sleep` no processo.

**O que é outbox e o que você implementou?**
Gravar o evento na mesma transação do estado, e publicar depois por um worker; evita
publicar algo que sofreu rollback. Implementei a tabela e a gravação dos eventos
`WagerTransactionProcessed`, `WagerTransactionRejected` e `WalletBalanceChanged`, na mesma
transação da operação e da abertura de carteira, com o envelope completo. Não implementei o
publisher, então os eventos são gravados e ainda não publicados.

**Como os eventos saem na ordem certa?**
Pela coluna `seq`, uma sequência da migration 000004. Eu primeiro ordenava por `occurred_at`
e o teste me mostrou que não serve: o Postgres guarda microssegundos, e os dois eventos da
mesma transação nascem no mesmo microssegundo, então o empate era desfeito por um UUID
aleatório e `WalletBalanceChanged` saía antes de `WagerTransactionProcessed`. Por carteira, a
ordem do `seq` é a do commit, porque a carteira fica travada do insert até o commit.

**Por que o `eventId` é o id da linha da outbox?**
Para uma republicação carregar o mesmo `eventId`. A publicação é "pelo menos uma vez"; o
consumidor reconhece a duplicata por esse id.

**Por que os eventos são gravados antes da linha da operação?**
É no insert da operação que uma duplicata de `(provider, externalTransactionId)` é recusada.
Com os eventos antes, a recusa precisa desfazê-los, e isso prova que estão na mesma
transação. Conferi com uma mutação: gravando os eventos numa transação separada, o teste
falha (as linhas da outbox foram de 2 para 4).

**Exactly-once existe?**
Não na entrega. O que existe é entrega at-least-once com processamento idempotente, que dá
efeito de uma vez só.

---

## 10. Fx e ciclo de vida

**O que é o Fx?**
Injeção de dependência: registro construtores, ele monta o grafo e chama na ordem certa.
Também gerencia início e parada.

**`Provide` e `Invoke`: diferença?**
`Provide` registra como construir; só roda se alguém precisar. `Invoke` é executado sempre e
força a construção do que ele pede.

**Como é o encerramento?**
Na ordem inversa da construção: HTTP para de aceitar e drena, o consumidor para e espera o
loop, e por último o pool fecha.

**Como você testou?**
`fx.ValidateApp` para o grafo, e um teste que sobe com Postgres real, faz uma requisição,
para, e verifica que o pool está fechado.

**O que acontece no `SIGTERM` com uma mensagem em andamento?**
O contexto é cancelado, a transação sofre rollback, e a visibilidade da mensagem é zerada
para outra instância pegar.

**Liveness e readiness?**
Liveness: o processo responde. Readiness: banco e fila respondem. Se o banco cai, o processo
não deve ser reiniciado, só tirado do balanceamento.

---

## 11. Testes

**Por que containers reais e não mocks?**
As garantias estão no banco: lock, constraint, trigger. Um mock não bloqueia nem viola
constraint.

**Como testar concorrência sem teste instável?**
Não dependo de tempo. Disparo goroutines, espero todas e verifico o estado final: um
sucesso, uma rejeição, saldo 20. Rodei 100 vezes com `-race`. A exceção é o teste da carteira
ocupada, que tem um limite de 3 segundos; uma falha repetida dele significaria lock global.

**O que o `-race` detecta?**
Acesso concorrente à memória sem sincronização, dentro do processo. Não detecta condição de
corrida no banco — para isso servem os testes de integração.

**O que não está testado?**
A interrupção do consumidor entre o commit e a remoção da mensagem, publishers concorrentes,
reversão antes da referência (não há worker), o consumidor SQS entre várias instâncias,
indisponibilidade do banco e da fila (só à mão, documentada no README) e a expiração real de
token no Keycloak (só com token assinado no teste).

**Como você testa vários processos?**
O pacote `internal/e2e` compila o binário real e o inicia três vezes com `os/exec`, em portas
livres, contra um Postgres e um Keycloak de testcontainers. Três testes: 100/80 em 30
carteiras ao mesmo tempo, a mesma chave 50 vezes espalhada nos três, e replay depois de
`Process.Kill()` seguido de um processo novo. O `Kill` é o equivalente prático do `kill -9`.

**O seu teste do 100/80 pegaria a falta do lock?**
A primeira versão não pegava, e eu só descobri porque testei o teste. Com uma carteira só, ele
passava mesmo sem o `FOR UPDATE`, três vezes seguidas: a disputa dura milissegundos e as três
requisições quase nunca se sobrepõem. Removi o lock numa cópia do projeto para conferir. A
versão final usa 30 carteiras liberadas no mesmo instante por um canal, e aí passa 3 de 3 com
o lock e falha 3 de 3 sem ele. Sem o lock, a versão (`WHERE version = ...`) ainda impediu o
dinheiro perdido: a requisição perdedora recebeu 409 e o teste acusou. É a defesa em
profundidade funcionando.

**Como você conferiu que o saldo bate?**
Dois níveis. Nos testes de integração, `assertLedgerMatchesBalance` verifica que o saldo
guardado é o saldo antes do primeiro lançamento mais créditos menos débitos. No teste de
carga, cada carteira precisa ter 1.000.000,00 menos o número de apostas que a API respondeu
`201`; em todas as rodadas bateu.

---

## 12. Observabilidade

**O que você entregou de observabilidade?**
Logs em JSON (um objeto por linha), com um `correlationId` em cada requisição e mensagem: o
`X-Correlation-Id` do cliente, se for válido, ou um UUID, devolvido no cabeçalho da resposta;
no consumidor, o `messageId`. Cada linha traz também `clientId`, `providerId`, `walletId`,
`kind` e `transactionId`. Há um log de acesso por requisição (método, rota, status, duração) e
uma linha por operação. E `/health/live` e `/health/ready`, que checa banco e fila e devolve
503 com o nome da dependência que caiu.

**Como você segue uma requisição nos logs?**
Pelo `correlationId`: `docker compose logs app --no-log-prefix | grep <id>`. Dá a linha da
operação e a de acesso, e o mesmo id vai para o envelope dos eventos da outbox.

**O que o log não pode conter?**
Valores monetários, corpos, cabeçalhos como `Authorization`, e o caminho cru da requisição
(que carrega ids). O log de acesso usa o padrão da rota. Há teste para isso, e eu conferi com
uma mutação: vazando o caminho e o cabeçalho, o teste falha.

**Como funciona o contexto de log?**
Um pacote, `logctx`, guarda uma sacola de atributos no `context`, compartilhada por ponteiro.
Quem está fundo na pilha (o handler que descobre o `transactionId`) acrescenta à sacola, e o
log de acesso do middleware mais externo, escrito depois, já vê o que foi acrescentado. Um
`slog.Handler` próprio embrulha o `JSONHandler` e junta esses atributos a cada registro.

**O que falta?**
Métricas (o enunciado pede resultados por status, duplicatas, retries, DLQ, conflitos, atraso
da outbox, latência e divergência de reconciliação) e um log próprio do processador Postgres:
hoje o log da operação sai do handler e do consumidor.

**Por que o IdP não está no readiness?**
O enunciado pede só Postgres e SQS. Além disso, as chaves do JWKS são buscadas sob demanda,
então o app sobe e atende mesmo com o Keycloak fora; só os tokens novos falham.

---

## 13. Teste de carga

**O que você mediu e como?**
Um programa, `cmd/loadtest`, abre carteiras e dispara `BET` de 1,00 com token real do
Keycloak e chave única, por um tempo fixo, em dois cenários: 50 carteiras e uma carteira só.
Ele reporta RPS, p50, p95, p99, status e erros, e confere o saldo de cada carteira. Método e
números em `docs/loadtest.md`.

**Quais foram os resultados?**
Com 32 clientes por 60 segundos: 50 carteiras deram 2.523 RPS, p50 de 12 ms e p99 de 23 ms;
uma carteira deu 462 RPS, p50 de 62 ms e p99 de 146 ms. Zero erros de transporte e saldos
batendo.

**Por que a carteira única é 5,5 vezes mais lenta?**
O `FOR UPDATE` serializa: cada transação segura o lock até o commit, então as operações
formam fila. 462 RPS equivale a cerca de 2,2 ms por operação. Não é defeito; é o preço da
garantia de não haver saldo negativo.

**Como você explica a latência?**
Pela lei de Little: com 32 clientes sempre pendentes, a latência média é 32 dividido pelo RPS.
Dá 12,7 ms no cenário de várias carteiras e 69 ms no de uma só, perto dos p50 medidos. O que
cresce na carteira única é a espera, não o trabalho de cada operação.

**Por que 128 clientes deram resultado pior que 32?**
A vazão caiu de 2.523 para 2.282 RPS e o p50 foi de 12 para 49,5 ms. O sistema já estava
saturado com 32; mais clientes só alongam a fila. Minha hipótese é o pool de 10 conexões e a
CPU dividida entre gerador, app, Postgres e Keycloak. Não medi qual dos dois limita, então
digo que é hipótese.

**O teste de carga é confiável?**
Para comparar cenários, sim. Para números absolutos, não: gerador e sistema dividem a máquina,
cada configuração rodou poucas vezes e não há intervalo de confiança. O `many` com 32 clientes
rodou três vezes e variou de 2.523 a 2.792 RPS.

**O que ficou sem medir?**
O atraso da outbox, porque não há publisher; conflitos forçados, porque as repetições viram
replay e as demais têm chave única, então 409/422 ficam em zero por construção; várias
instâncias; e falhas durante a carga.

**Replay é mais barato que operação nova?**
Quase igual: 2.792 RPS sem replays contra 2.584 com 10%. Um replay ainda abre transação,
reivindica a chave e lê a resposta gravada. A diferença é pequena e, com tão poucas rodadas,
não distingo de ruído.

---

## 14. Perguntas difíceis

**O Postgres caiu. E agora?**
Tudo para: escrita, idempotência e lock dependem dele. Não há réplica. É o ponto único de
falha, e está documentado.

**Quais defeitos você conhece?**
Estão no `ARCHITECTURE.md`, seção 10: a referência de um `WIN` não é validada; o ledger não
tem chave estrangeira para a operação; a mensagem de `idempotency_key_reused` expõe o prefixo
`postgres:`; e a outbox grava os eventos mas ainda não há publisher. Prefiro listar eu mesmo a
deixar o avaliador achar.

**Achei um bug: a mesma operação com outra chave devolve 500.**
Era verdade e foi corrigido: agora responde 409 `duplicate_operation`. Eu mesmo achei ao
reler o enunciado contra o código, e o teste cobre. O dinheiro nunca se movia duas vezes; o
defeito era só o contrato da resposta.

**Seu caso de uso está no adapter. Isso não fere a arquitetura hexagonal?**
Fere. Fiz assim porque a transação é o coração da regra e eu quis ela visível. O preço é que
trocar o banco exigiria reescrever o processador. O passo seguinte é extrair uma porta.

**Você usou IA?**
Sim, como par de programação: para gerar rascunhos e revisar. Eu revisei, rodei os testes,
corrigi erros e sei explicar cada decisão — inclusive as que deram errado, como o `JSONB`.

**Como escalaria para muito mais volume?**
Primeiro medir. Depois: réplicas de leitura para consultas, particionar o ledger por tempo,
mais consumidores (FIFO paraleliza por grupo), e só então pensar em dividir carteiras por
shard.

**O que te surpreendeu durante o projeto?**
Três coisas concretas: `JSONB` reordenar chaves; `for i := range string` devolver índice de
byte e não o caractere; e a contagem de operações incluir a linha de `OPENING`.

---

## Roteiro de demonstração (5 minutos)

1. `docker compose up --build` e `curl /health/ready`.
2. Token do serviço interno, abrir carteira com 1000.
3. Token do provedor, aposta de 25 → saldo 975.
4. Repetir → `idempotentReplay: true`, saldo igual.
5. Mesma chave com outro valor → 422.
6. Token de `provider-b` consultando a operação → 404.
7. Mesma operação pela fila → sem novo débito.
8. `docker compose stop localstack` → readiness 503.
9. Mesma aposta com um `playerId` que não é o dono da carteira → 404, saldo intacto.
10. `go run ./cmd/loadtest -scenario single -duration 20s` e depois `-scenario many`: mostrar
    a diferença de RPS e o `balance check: OK`.
11. Abrir o `ARCHITECTURE.md` na seção 10 e dizer o que falta.
