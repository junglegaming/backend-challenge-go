# Arquitetura e decisões

Este documento registra o que foi decidido, por quê, e o que **não** foi feito. O escopo
foi reduzido para caber no prazo; a seção 10 lista cada lacuna de forma explícita.

## 1. Visão geral

```
cmd/api                  main: carrega .env e roda o grafo Fx
internal/
  domain/                Money, Wallet, WalletLedgerEntry, WagerTransaction, regras
  app/                   Fingerprint: hash canônico do conteúdo de uma operação
  config/                leitura e validação das variáveis de ambiente
  composition/           módulos Fx e ciclo de vida
  adapters/
    postgres/            pool, repositórios, WagerProcessor (a transação financeira)
    httpapi/             rotas, DTOs, autenticação, mapeamento de erro
    sqs/                 consumidor, contrato da mensagem, classificação de erro
  testutil/              containers de teste (Postgres, Keycloak, LocalStack)
migrations/              schema versionado
deploy/                  realm do Keycloak e script das filas
```

`internal/domain` não importa Fx, HTTP, SQS nem pgx. Os testes dele rodam sem Docker.

**Desvio conhecido:** o caso de uso (`WagerProcessor`) mora no adapter Postgres, e não em
uma camada `app` com portas. HTTP e SQS compartilham esse mesmo processador, então a
garantia é uma só, mas a regra de orquestração está acoplada ao pgx. Extrair uma porta é o
próximo passo de arquitetura.

## 2. Dinheiro

- `Money` guarda `int64` em unidades menores (centavos) e o código da moeda. Não existe
  `float` em nenhuma etapa: parsing, cálculo, serialização ou persistência.
- Entrada externa passa por `ParseExternalAmount`: string decimal com no máximo duas casas,
  sem sinal. `"25"`, `"25.0"` e `"25.00"` são o mesmo valor; `"25.001"` e negativos são
  rejeitados. No JSON o valor é sempre string (`"amount": "25.00"`), porque um número JSON
  passaria por ponto flutuante no cliente.
- `Add`, `Sub` e `Negate` checam overflow antes de acontecer e devolvem erro.
- Operar moedas diferentes devolve `ErrCurrencyMismatch`.
- No banco: `BIGINT` (`*_minor`) mais `CHAR(3)`. Escala fixa de 2 casas — moedas com outra
  escala (JPY, BHD) não são suportadas.

## 3. Transação SQL e locks

Biblioteca: `pgx/v5` com SQL explícito, sem ORM.

Toda escrita financeira acontece dentro de **uma** transação, aberta por
`WagerProcessor.Process`:

1. reivindica a linha da inbox (só quando a origem é SQS);
2. reivindica a chave de idempotência (`INSERT ... ON CONFLICT DO NOTHING`);
3. `SELECT ... FOR UPDATE` na carteira;
4. o domínio decide (débito, crédito, rejeição);
5. grava carteira, lançamento do ledger, operação e a resposta armazenada na chave;
6. `COMMIT`.

Qualquer erro desfaz tudo, inclusive as reivindicações dos passos 1 e 2.

- **Lock pessimista** (`FOR UPDATE`) é o mecanismo que impede lost update e saldo negativo.
  O `UPDATE ... WHERE version = $anterior` é uma segunda barreira, que não deveria disparar.
- Uma operação trava **uma** carteira, então não há ordem de locks a respeitar e não há
  deadlock entre operações. Carteiras diferentes não se bloqueiam.
- Os repositórios recebem a `pgx.Tx` de quem chama; nenhum deles abre transação por conta
  própria no caminho financeiro.
- O banco impõe as invariantes mesmo com bug na aplicação: `CHECK (balance_minor >= 0)`,
  `UNIQUE (wallet_id, transaction_id)` no ledger, `UNIQUE (provider_id,
  external_transaction_id)`, índice único parcial que permite no máximo uma reversão
  `PROCESSED` por operação, e triggers que recusam `UPDATE`, `DELETE` e `TRUNCATE` no ledger.

## 4. Idempotência

- Tabela `idempotency_keys`, chave primária `(scope, endpoint, key)`. `scope` é o provedor.
- Guarda `request_hash`, `state` (`in_flight` / `completed`), `status_code` e
  `response_body`.
- **Hash:** SHA-256 em hexadecimal do JSON de `app.WagerBody` (`providerId`,
  `externalTransactionId`, `playerId`, `walletId`, `roundId`, `gameId`, `kind`, `money`,
  `referenceExternalTransactionId`). A ordem dos campos é fixa pelo tipo, sem espaços, e o
  valor é normalizado por `Money`. A chave de idempotência e os metadados de transporte
  ficam de fora. HTTP e SQS usam a mesma função, e um teste compara os dois hashes.

| Situação | Resultado |
|---|---|
| Chave nova | processa, grava a resposta, devolve |
| Mesma chave, mesmo hash | devolve a resposta gravada, `idempotentReplay: true` |
| Mesma chave, hash diferente | 422 `idempotency_key_reused` |
| Chave `in_flight` | 409 `request_in_flight` |

- A reivindicação acontece **dentro** da transação. Uma segunda requisição com a mesma
  chave bloqueia no índice até a primeira terminar e então cai no replay. Por isso
  `in_flight` quase nunca é visível; o ramo existe e é testado com uma linha semeada.
- **Mesma operação, outra chave.** `(providerId, externalTransactionId)` tem `UNIQUE` no
  banco, então uma operação não é reaplicada trocando a chave. A violação dessa constraint
  (identificada pelo nome, para não confundir com o índice de reversão) vira
  `ErrDuplicateExternalTransaction`: HTTP 409 `duplicate_operation`, e erro permanente no
  SQS. A transação inteira é desfeita, então o saldo não muda e a chave usada fica livre.
- A resposta é gravada como `TEXT`, não `JSONB`: o `JSONB` reordena chaves, e o replay deve
  devolver o conteúdo original.
- O replay devolve o saldo observado no processamento original.
- **Rejeição de negócio é gravada e reproduzida.** Saldo insuficiente gera uma operação
  `REJECTED` com `failureCode`, auditável. Entrada inválida faz rollback e não consome a chave.

## 5. Regras das operações e reversões

| Tipo | Efeito |
|---|---|
| `BET` | débito; sem saldo vira `REJECTED` / `insufficient_funds` |
| `WIN` | crédito |
| `LOSS` | valor zero, sem lançamento e sem mudar a versão da carteira |
| `REFUND` | desfaz um `BET` (crédito) |
| `ROLLBACK` | desfaz `BET` (crédito), `WIN` ou `REFUND` (débito) |
| `OPENING` | interno; recusado se vier por HTTP ou SQS |

Uma reversão exige referência do mesmo provedor, jogador, carteira, rodada e moeda, em
estado `PROCESSED`, com o mesmo valor. A checagem de "já revertida" acontece **depois** do
lock da carteira, então duas reversões simultâneas da mesma aposta são serializadas; o
índice único parcial é a garantia final. Reversão que deixaria o saldo negativo é rejeitada
com `reversal_insufficient_funds`, distinta de `insufficient_funds`.

**Referências pendentes:** o estado `PENDING_REFERENCE` e suas transições existem no
domínio, mas não há worker que o resolva. Uma reversão cuja referência ainda não chegou é
rejeitada na hora com `reference_not_found`. Isso é uma simplificação em relação ao enunciado.

## 6. Abertura de carteira

`POST /wallets` com saldo positivo grava, na mesma transação: a carteira (versão 1), uma
operação `OPENING` em `PROCESSED`, o lançamento de crédito e dois eventos na outbox. Saldo
zero grava só a carteira. Jogador e moeda repetidos devolvem 409 e nada fica para trás.

## 7. Inbox, SQS e outbox

**Consumidor**
- Uma mensagem por vez, para preservar a ordem dentro do grupo.
- `MessageGroupId` recomendado: `walletId` (ordem por carteira, paralelismo entre carteiras).
  `MessageDeduplicationId` recomendado: `messageId` do envelope. A deduplicação da SQS dura
  5 minutos; a garantia durável é a inbox.
- **Inbox:** uma linha por `(consumidor, messageId)` com o hash, gravada na mesma transação
  da operação. Reentrega com o mesmo corpo vira replay. Mesmo `messageId` com corpo
  diferente é erro permanente.
- A chave usada é `data.idempotencyKey`, no **mesmo espaço** do HTTP. A mesma operação
  enviada pelos dois canais é aplicada uma vez (há teste).
- A mensagem só é apagada depois do commit.

| Resultado | Ação |
|---|---|
| processada ou rejeitada pelo negócio | apaga |
| erro permanente (mensagem inválida, chave ou `messageId` reutilizados, carteira inexistente) | não apaga; o redrive move para a DLQ |
| erro transitório | adia com backoff 2 s, 4 s, 8 s… até 60 s, via visibility timeout |

- Visibility timeout: 30 s. Long polling: 10 s. `maxReceiveCount`: 3.
- No `SIGTERM` o consumidor para de buscar. Se uma mensagem estava em processamento, a
  transação é desfeita e a visibilidade é zerada para outra instância assumir.

**Outbox — parcial: os eventos são gravados, mas ainda não publicados.**

- Toda operação decidida grava seus eventos na outbox **na mesma transação** da operação:

  | Resultado | Eventos |
  |---|---|
  | `PROCESSED` com movimento de saldo | `WagerTransactionProcessed` e `WalletBalanceChanged` |
  | `LOSS` processada | só `WagerTransactionProcessed` (não há alteração de saldo) |
  | `REJECTED` | `WagerTransactionRejected` |
  | replay, ou operação recusada e desfeita | nenhum |

  A abertura de carteira grava os mesmos dois eventos de um `OPENING`.
- **Envelope** (`domain.Event`): `eventId`, `eventType`, `aggregateId`, `correlationId`,
  `causationId` (opcional), `occurredAt` em UTC, `version` e `data` tipado. O construtor
  define tipo e versão. O `eventId` é o id da própria linha da outbox, então qualquer
  republicação o preserva. O `correlationId` vem do contexto: o `X-Correlation-Id` da
  requisição ou o `messageId` da mensagem.
- **Ordem.** A coluna `seq` (migration 000004) dá uma ordem estritamente crescente. O
  `occurred_at` não serve: o Postgres guarda microssegundos, e dois eventos criados na mesma
  transação podem empatar. Por carteira, a ordem do `seq` é a do commit, porque a carteira
  fica travada do insert até o commit.
- **Escrita antes da linha da operação, de propósito.** É no insert da operação que uma
  duplicata `(providerId, externalTransactionId)` é recusada; escrever os eventos antes faz o
  rollback desfazer também os eventos, e um teste (com mutação) depende disso.
- **Falta:** o publisher (disputa entre vários, backoff, recuperação), a fila de destino, e
  `WagerTransactionPendingReference`, que depende do worker de `PENDING_REFERENCE`. Nenhum
  evento é publicado antes do commit porque nenhum evento é publicado ainda.

## 8. Autenticação e autorização

- **IdP:** Keycloak, `client_credentials`. Escolhido por ser o recomendado no enunciado e
  por permitir provisionar tudo com um arquivo de realm.
- **Validação:** `go-oidc` verifica assinatura (JWKS), `iss` e `exp`. Só `RS256` é aceito,
  o que barra `alg: none` e tokens HMAC. O `aud` não é verificado, porque o Keycloak emite
  `account` nesse fluxo.
- **Identidade:** o claim `azp` é o `providerId`. Ele vai para o contexto da requisição; os
  handlers nunca leem identidade de header ou corpo.
- **Permissões:** um client por provedor. O serviço interno é o client que tem a realm role
  `wallet-internal`. Rotas de carteira exigem a role; rotas de operação a recusam.
- **Isolamento:** corpo com `providerId` diferente do token, ou consulta a operação de outro
  provedor, respondem 404 — igual a recurso inexistente, para não revelar existência. A
  chave de idempotência é escopada por provedor, então um replay não cruza provedores.
- `AUTH_ISSUER` e `AUTH_JWKS_URL` são separados porque, no Compose, o token traz
  `localhost:8081` no `iss` e o app alcança o Keycloak por `keycloak:8080`.

Limitações: não há revogação antes do `exp` (5 min); todo client do realm sem a role
interna é tratado como provedor; a entrada por SQS **não** é autenticada por mensagem — o
`providerId` vem do corpo, e a confiança é em quem pode escrever na fila.

## 9. Fx e shutdown

Módulos: `config`, `persistence`, `httpapi`, `sqs`. Construtores via `fx.Provide`,
inicialização via `fx.Invoke`, recursos via `fx.Lifecycle`.

- Configuração inválida, banco inacessível ou porta ocupada falham na montagem do grafo.
- O JWKS e a fila são acessados sob demanda; o app sobe mesmo sem IdP ou SQS no ar, e
  `/health/ready` informa `database` e `queue`. O IdP não entra no readiness.
- Encerramento na ordem inversa da construção: servidor HTTP (`Shutdown`, drena o que está
  em andamento) → consumidor SQS (cancela e espera o loop terminar) → pool. A ordem vem do
  grafo de dependências, não de uma lista manual.
- Testes: `fx.ValidateApp` e um teste que inicia, atende uma requisição, encerra e verifica
  que o pool foi fechado.

Limitação: o Fx corta o encerramento em 15 s (`fx.StopTimeout` não foi ligado a
`SHUTDOWN_TIMEOUT`); valores maiores não têm efeito.

## 10. Trabalho não concluído

| Item do enunciado | Situação |
|---|---|
| Publisher da outbox, disputa entre publishers, recuperação | não feito |
| Eventos de outbox para operações de provedor | feito: `Processed`, `Rejected` e `BalanceChanged`, com envelope; `WagerTransactionPendingReference` depende do worker e não existe |
| Worker de `PENDING_REFERENCE` com backoff | não feito; reversão sem referência é rejeitada |
| `POST /wallets/:id/reconciliation` | não feito |
| `GET /wallets/:id/ledger` paginado por cursor | não feito |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId` | não feito |
| Prova com três instâncias e `kill -9` | feita em teste automatizado (`internal/e2e`, três processos reais); a indisponibilidade de Postgres e de SQS só à mão (README) |
| Métricas, tracing | não feito |
| Idempotência de `POST /wallets` | só pela unicidade `(player_id, currency)`; repetição devolve 409, não replay |

Outras exigências do enunciado não atendidas:

| Item do enunciado | Situação |
|---|---|
| Identificadores de correlação em todos os logs (§12) | feito nos logs de acesso, de operação e do consumidor; o processador Postgres não escreve log próprio |
| Destino dos eventos de saída provisionado (§11) | não feito |
| Controle de acesso à fila por credenciais e políticas do broker (§2) | não feito; o LocalStack aceita qualquer credencial |
| `OPENING` sem provedor, ID externo, chave e hash (§6.3) | desvio: a linha usa o provedor `internal`, o identificador `opening:{walletId}` e chave e hash vazios. A unicidade `(provider_id, external_transaction_id)` impede crédito inicial duplicado |
| Estado `FAILED` para falha permanente de infraestrutura (§6.3) | existe no domínio, nunca é gravado |
| Moeda validada contra a lista ISO 4217 (§6.1) | só o formato (três letras maiúsculas) é validado |
| Interrupção entre commit e remoção da mensagem (§13.5) | sem teste de interrupção; a reentrega da mesma mensagem é testada |
| Reinício da aplicação preservando idempotência (§13.8) | coberto por `internal/e2e`: um processo morto com `Kill` e um novo processo respondendo o replay |
| Credencial expirada contra o IdP real (§13) | testada só com token assinado no próprio teste |

Defeitos conhecidos:

- **`WIN` com referência** não é validado contra a aposta de origem.
- O ledger não tem chave estrangeira para `wager_transactions`.
- A mensagem de `idempotency_key_reused` expõe o prefixo interno `postgres:`.
- Readiness consulta as dependências a cada chamada, sem cache.

## 11. Situação por critério de avaliação

Autoavaliação contra a tabela do enunciado (§14). **Os percentuais e os pontos são uma
estimativa minha**, atribuída a partir do que tem evidência no repositório; o avaliador pode
pontuar diferente.

### 11.1. Pontuação estimada

| Critério | Peso | Atendido | Pontos | Por quê |
|---|---:|---:|---:|---|
| Integridade financeira | 20 | ~75% | 15 | Falta o endpoint de reconciliação, que o critério cita explicitamente, e a validação da referência de `WIN` |
| Concorrência | 20 | ~85% | 17 | Lock, 100/80/80 e carteiras em paralelo provados em teste, e três processos reais disputando o mesmo saldo em teste automatizado. Falta exercitar o consumidor SQS entre instâncias |
| Idempotência | 15 | ~95% | 14,25 | Replay depois de `kill -9` coberto por teste. Faltam a retenção das chaves e o replay da abertura de carteira |
| Mensageria e recuperação | 15 | ~50% | 7,5 | Inbox, retry, DLQ e shutdown prontos, e os eventos das operações gravados na outbox com envelope. Faltam o publisher, a fila de destino e `PENDING_REFERENCE` |
| Modelagem e arquitetura | 10 | ~80% | 8 | O caso de uso está no adapter, e não há controle de acesso à fila |
| Testes | 10 | ~80% | 8 | Infra real, isolamento entre provedores, três processos e reinício em teste. Faltam interrupção do consumidor, publishers concorrentes e reversão antes da referência |
| Observabilidade | 5 | ~60% | 3 | Health checks, logs JSON com `correlationId` e identificadores, log de acesso e uma linha por operação, sem segredos nem valores. Faltam as métricas |
| Documentação | 5 | ~95% | 4,75 | Execução reproduzível, múltiplas instâncias e falhas documentadas. Faltam só os cenários que dependem do que não foi implementado |
| **Total** | **100** | | **~77** | faixa plausível: 67 a 82 |

### 11.2. O que foi pedido, o que foi feito e o que falta

| Critério | O que foi pedido | O que foi feito | O que falta |
|---|---|---|---|
| **Integridade financeira (20)** | Dinheiro sem `float`, em todas as etapas<br>Invariantes impostas pelo banco<br>Ledger append-only, com lançamento validado<br>Reversões íntegras: valor igual, uma só por operação, código próprio para falta de saldo<br>Reconciliação confiável | `Money` em `int64` com moeda e overflow tratado<br>`CHECK` de saldo ≥ 0, `UNIQUE` no ledger e trigger contra `UPDATE`/`DELETE`/`TRUNCATE`<br>Lançamento confere `balanceAfter = balanceBefore ± valor`<br>Reversões com mesma rodada, mesma moeda e valor igual, uma só bem-sucedida, `reversal_insufficient_funds` distinto<br>Jogador conferido contra o dono da carteira; moeda conferida em `LOSS`<br>Saldo conferido contra o ledger nos testes | Endpoint `POST /wallets/:id/reconciliation`<br>Validação da referência de `WIN`<br>`OPENING` sem os metadados externos (usa provedor `internal`)<br>Estado `FAILED` nunca é gravado<br>Moeda validada só pelo formato, não pela lista ISO 4217 |
| **Concorrência (20)** | Coordenação por carteira, sem lock global<br>Sem atualização perdida<br>Teste obrigatório: saldo 100, duas apostas de 80<br>Carteiras diferentes em paralelo<br>Três processos independentes<br>A mesma aposta 50 vezes em paralelo | `SELECT ... FOR UPDATE` por carteira, mais a versão como segunda barreira<br>Teste 100/80/80 repetido 100 vezes com `-race`<br>Teste em que uma carteira ocupada não bloqueia outra<br>20 carteiras processadas ao mesmo tempo<br>50 envios simultâneos da mesma chave, com um único débito<br>Estado só no banco, nada em memória do processo<br>Três processos independentes disputando as mesmas carteiras e a mesma chave, em teste automatizado (`internal/e2e`) | O consumidor SQS entre instâncias (os testes `e2e` não o exercitam) |
| **Idempotência (15)** | Persistente, sobrevive ao reinício<br>Hash canônico igual entre HTTP e SQS<br>Mesma chave com corpo diferente: conflito<br>Replay devolve o resultado e o saldo originais<br>A mesma operação não é reaplicada com outra chave | Chave persistida na mesma transação do efeito<br>Hash determinístico de `app.Fingerprint`, com teste de equivalência HTTP × SQS<br>422 para corpo diferente, 409 para chave em processamento<br>Replay com o saldo do processamento original, inclusive de rejeições<br>Outra chave para a mesma operação: 409 `duplicate_operation`<br>Replay confirmado depois de `kill -9` e reinício, em teste automatizado (`internal/e2e`) | Política de retenção das chaves<br>Replay da abertura de carteira (hoje devolve 409) |
| **Mensageria e recuperação (15)** | Inbox atômica com a operação<br>Mensagem removida só após o commit<br>Retry com backoff e DLQ<br>Encerramento seguro<br>Outbox com publishers concorrentes e recuperação<br>`PENDING_REFERENCE` com worker | Inbox com hash na mesma transação<br>Remoção após o commit<br>Backoff de 2 s a 60 s pelo visibility timeout, redrive para a DLQ, ambos testados<br>`SIGTERM` libera a mensagem em andamento<br>Eventos `WagerTransactionProcessed`, `WagerTransactionRejected` e `WalletBalanceChanged` na outbox, no mesmo commit da operação e da abertura de carteira, com o envelope completo e `seq` para a ordem<br>Replay e operação recusada não deixam eventos (testado, inclusive com mutação) | Publisher da outbox, com disputa e recuperação<br>Destino dos eventos provisionado<br>`WagerTransactionPendingReference` (depende do worker)<br>Worker de `PENDING_REFERENCE`<br>Testes de interrupção entre commit e remoção |
| **Modelagem e arquitetura (10)** | Entidades encapsuladas, construtores, criação separada de reidratação<br>Erros classificáveis<br>Fx com módulos e ciclo de vida<br>Domínio independente de infraestrutura<br>Identidade que define o provedor, rotas de carteira só internas | Estado privado, construtores validando, `Rehydrate` separado<br>Erros com `errors.Is`<br>Fx com `Module`, `Provide`, `Invoke` e `Lifecycle`, e encerramento na ordem inversa<br>Domínio sem importar Fx, HTTP, SQS nem pgx<br>OIDC com Keycloak: `azp` é o provedor, rotas internas por role | Caso de uso dentro do adapter Postgres, sem porta<br>Controle de acesso à fila por credenciais e políticas |
| **Testes (10)** | Integração com Postgres, IdP e LocalStack reais<br>Isolamento entre provedores<br>Paralelismo e interrupção<br>Os oito cenários de concorrência e recuperação<br>`go test -race` | Containers reais dos três<br>Isolamento no envio e na consulta<br>Cenários 1, 2 e 3 (50 envios, 100/80/80, carteiras distintas), mais HTTP × SQS e a composição do Fx<br>Cenários 4 e 8 com três processos reais e `kill` (`internal/e2e`)<br>Token ausente, inválido, de outro emissor e com algoritmo errado | Cenário 5 (interrupção do consumidor)<br>Cenário 6 (dois publishers)<br>Cenário 7 (reversão antes da referência)<br>Token expirado contra o IdP real |
| **Observabilidade (5)** | Logs JSON com identificadores de correlação<br>Métricas<br>Health checks | Logs em JSON com `correlationId` (aceita `X-Correlation-Id`), `clientId`, `providerId`, `walletId`, `transactionId` e `kind`<br>Log de acesso: método, rota, status e duração, sem caminho cru, cabeçalhos nem valores<br>Uma linha por operação<br>`/health/live` e `/health/ready` com banco e fila<br>`messageId` como `correlationId` no consumidor | Métricas<br>Log próprio do processador Postgres (o log da operação sai do handler e do consumidor) |
| **Documentação (5)** | Execução reproduzível a partir de um checkout limpo<br>IdP provisionado<br>`.env.example`<br>Decisões e limitações<br>Como rodar múltiplas instâncias e falhas | `docker compose up --build` com migrations, realm e filas provisionados<br>`README.md` com exemplos autenticados<br>`.env.example`<br>Este arquivo<br>Teste de carga em `docs/loadtest.md`<br>Múltiplas instâncias e simulação de falhas no README | Cenários 5, 6 e 7 do enunciado sem instrução, porque dependem do que não foi implementado |

Condições eliminatórias (§14):

| Condição | Situação |
|---|---|
| Autenticação efetiva nos endpoints de negócio | atendida |
| Acesso não autorizado a operações ou transações | atendida entre provedores, e o jogador informado precisa ser o dono da carteira |
| Cálculo monetário em ponto flutuante | não ocorre |
| Saldo negativo por concorrência | impedido por lock e por `CHECK` |
| Movimentação duplicada | impedida por chave, inbox e constraints |
| Idempotência só em memória | não ocorre; está no banco |
| Dependência de uma única instância | o desenho não depende, e há teste automatizado com três processos reais (`internal/e2e`) |
| Publicação anterior ao commit | não ocorre; nada é publicado |
| Ledger auditável | atendida |
| Infraestrutura toda em mocks | não ocorre |

## 12. Pontos de falha da implantação

- **Postgres é ponto único de falha.** Sem réplica nem failover. É também o único
  coordenador: lock, idempotência e inbox dependem dele. Não há consenso distribuído.
- **Postgres fora:** `/health/ready` passa a 503; as apostas respondem 503
  `temporarily_unavailable`, sem detalhe interno, e nada é aplicado, então a mesma chave
  pode ser reenviada. O pool reconecta sozinho quando o banco volta, sem reiniciar o app
  (verificado à mão). Antes da correção desta entrega esse caso respondia 500.
- **Morte do processo (`kill -9`):** a idempotência sobrevive, porque as chaves estão no
  banco. Uma requisição que já tinha commitado, mas cuja resposta se perdeu, pode ser
  reenviada com a mesma chave e recebe o resultado gravado. Em carga, o saldo só pode
  divergir para **menos** que o contado pelo cliente, nunca para mais.
- **Pool:** uma transação por operação segura uma conexão durante o lock; carteira muito
  disputada enfileira no banco. `DB_ACQUIRE_TIMEOUT` transforma espera em erro rápido.
- **SQS / LocalStack fora:** o HTTP continua funcionando; o consumidor tenta de novo a cada
  segundo e o readiness passa a 503.
- **Keycloak fora:** tokens já validados continuam aceitos enquanto as chaves estiverem em
  cache; chaves novas não são obtidas.

## 13. Teste de carga

Diferencial opcional (§14). Método, ambiente e resultados completos em
[docs/loadtest.md](docs/loadtest.md); relatórios JSON em `docs/bench/`. Resumo, com 32
clientes por 60 s na máquina de desenvolvimento, gerador e serviços juntos:

| Cenário | RPS | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| 50 carteiras | 2.523 | 12 ms | 17,9 ms | 23 ms |
| 1 carteira | 462 | 62,4 ms | 114,2 ms | 145,6 ms |

Carteiras independentes escalam; a mesma carteira serializa pelo `FOR UPDATE`. O saldo bateu
com as operações confirmadas em todas as rodadas e não houve erro de transporte. Não foram
medidos: atraso da outbox (sem publisher), conflitos forçados e
variação estatística entre rodadas. Com três instâncias a vazão agregada foi cerca do
dobro de uma só (ver `docs/loadtest.md`).
