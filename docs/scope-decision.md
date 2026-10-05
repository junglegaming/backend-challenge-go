# Decisão de escopo — prazo de 3 dias

> Documento de referência. Registra o que será entregue, o que fica parcial, o que fica
> fora, e por quê — para consulta durante a implementação e para citar no `ARCHITECTURE.md`
> final.

---

## 1. Contexto que motiva o corte

O `README.md` descreve um desafio de nível pleno/sênior: Uber Fx obrigatório, OIDC/Keycloak
obrigatório, SQS/LocalStack obrigatório, inbox/outbox com DLQ, máquina de estados assíncrona,
prova com três processos independentes.

A vaga a que esse desafio se refere é **júnior** (faixa R$ 2.500–5.000, "experiência
profissional anterior não é obrigatória"), e lista explicitamente **Uber Fx** e
**concorrência em Go (goroutines/channels)** em "Diferenciais — não obrigatórios". A vaga
valoriza nomeadamente: "reconhecer quando pedir ajuda antes que um risco cresça" e
"investigar problemas... aplicar feedback".

O prazo de entrega é de **3 dias, fixado pela empresa, não negociável**.

Conclusão: completar o README inteiro no padrão que ele descreve não é viável no prazo.
A estratégia é entregar um subconjunto bem feito e documentar o resto com julgamento — que é
exatamente a competência que a própria vaga diz procurar.

---

## 2. Camada 0 — nunca pular (condições eliminatórias do §14)

Custam pouco tempo e anulam a nota inteira se ausentes:

- Dinheiro nunca passa por `float32`/`float64`, em nenhuma etapa.
- Nenhuma escrita financeira fora de uma transação SQL explícita.
- Idempotência persistida (nunca só em memória).
- Pelo menos um teste de integração real contra Postgres e um contra LocalStack — a
  suíte não pode ser 100% mockada.
- Autenticação real (validação de JWT do Keycloak), mesmo sem políticas de autorização
  ricas. Ausência total de auth é eliminatória; auth simples não é.
- Publicação de evento só depois do commit que a originou.
- Ledger auditável e append-only.
- Não depender de uma única instância para funcionar corretamente (mesmo que a prova
  formal com 3 processos fique parcial — ver seção 3).

---

## 3. Escopo por seção do README

| § | Item | Decisão |
|---|---|---|
| 2 | Autenticação OIDC/Keycloak | **Sim**, mínimo: valida JWT real (assinatura + claims), sem RBAC rico |
| 3 | Cenários de falha: duplicata, concorrência na mesma carteira | **Sim** |
| 3 | Cenários de falha: `kill -9`, indisponibilidade Postgres/SQS | **Documentado**, sem prova extensiva |
| 4 | Go + Fx + Postgres/pgx + migrations + testing | **Sim** |
| 4 | SQS via LocalStack | **Sim**, mínimo (um consumidor funcionando) |
| 5 | As 8 garantias obrigatórias (sem float, idempotência persistida, invariantes no banco, ledger append-only, carteiras paralelas, sem lost update) | **Sim** — é o núcleo |
| 6.1 | `Money` (decimal, moeda, overflow, serialização) | **Sim** |
| 6.2 | `Wallet` (saldo + versão armazenados, débito/crédito) | **Sim** |
| 6.3 | `WagerTransaction` + estados `PENDING/PROCESSED/REJECTED/FAILED` | **Sim** |
| 6.3 | `PENDING_REFERENCE` com worker de backoff sobrevivendo a restart | **Parcial**: uma tentativa de resolução, documentado o que falta |
| 6.4 | `WalletLedgerEntry` imutável | **Sim** |
| 6.5 | Inbox / Outbox | **Sim**, versão simples (um publisher, sem disputa) |
| 7 | Regras dos 5 tipos (`BET/WIN/LOSS/REFUND/ROLLBACK`) + proteção contra reversão duplicada | **Sim** — núcleo de domínio |
| 8 | Concorrência numa carteira (teste do saldo 100 / duas apostas de 80) | **Sim** |
| 8 | Prova com 3 processos independentes | **Parcial**: `docker compose --scale`, teste básico cross-instância |
| 9 | Contratos HTTP (criar carteira, leituras, enviar operação, reconciliação, health checks) | **Sim**, todos |
| 10 | Consumidor SQS (fila, redrive, dedup, shutdown) | **Sim**, mínimo funcional; tuning de redrive documentado |
| 11 | Outbox com múltiplos publishers disputando + recuperação de interrupção nos dois pontos | **Não** — gap consciente, documentado |
| 12 | Logs estruturados + health checks | **Sim** |
| 12 | Métricas ricas, tracing, dashboards | **Não** (diferencial explicitamente opcional) |
| 13 | Testes unitários (`Money`, `Wallet`, transições, regras dos 5 tipos) | **Sim** |
| 13 | Integração real (Postgres + IdP + LocalStack, pelo menos smoke) | **Sim** — evita o eliminatório de "tudo mockado" |
| 13 | Cenários de concorrência #1, #2, #3 (50x mesma aposta, disputa 80/80, carteiras paralelas) | **Sim** |
| 13 | Cenários #5, #7, #8 (interrupção de consumidor, `REFUND` antes da referência, restart) | **Parcial**, se der tempo no fim |
| 13 | Cenário #6 (dois publishers disputando outbox) | **Não** |
| 15 | `ARCHITECTURE.md` nomeando o que não foi feito | **Sim** — parte da entrega, não opcional |

---

## 4. Projeção de cobertura por critério de avaliação (§14)

| Critério | Pontos | Cobre | Fica de fora | Expectativa |
|---|---:|---|---|---|
| Integridade financeira | 20 | `Money`, `Wallet` versionada, `WalletLedgerEntry`, as 8 garantias, regras dos 5 tipos, reconciliação | Resolução robusta de reversão sem referência (parcial) | Forte |
| Concorrência | 20 | `Wallet` versionada, teste do §8, cenários #1/#2/#3 | Prova formal com 3 processos (parcial) | Forte, com ressalva |
| Idempotência | 15 | Persistência + hash, conflito de corpo, replay, dedup de inbox | — | Forte |
| Mensageria e recuperação | 15 | Consumidor SQS mínimo, inbox/outbox simples | Outbox multi-publisher, DLQ/redrive fino, `PENDING_REFERENCE` completo, cenário #6 | Fraco/parcial — maior corte |
| Modelagem e arquitetura | 10 | Fx básico, domínio encapsulado, auth real mínima | Políticas de autorização mais ricas | Boa |
| Testes | 10 | Unitários, integração real (smoke), cenários #1/#2/#3 | Cenários #4/#5/#6 | Boa, com buracos pontuais |
| Observabilidade | 5 | Logs estruturados, health checks | Métricas, tracing | Parcial |
| Documentação | 5 | `ARCHITECTURE.md` com limitações nomeadas, README reproduzível | — | Forte |

**Estimativa ilustrativa (não é garantia):** ~75-80 de 100, assumindo que a Camada 0
(seção 2) permanece intacta. Qualquer item eliminatório que falhar anula essa conta.

---

## 5. Ordem de execução recomendada

1. `Money` → `Wallet` (saldo + versão) → débito/crédito com lock pessimista
2. Teste obrigatório do §8 (saldo 100, duas apostas de 80 simultâneas)
3. Idempotência (chave + hash + replay), reaproveitando o padrão já validado no projeto
   `wallet-go` (claim/execute/complete na mesma transação)
4. Contratos HTTP dos §9 (carteira, operação, leituras, reconciliação, health checks)
5. Autenticação OIDC mínima
6. Consumidor SQS mínimo + inbox/outbox simples
7. Testes dos cenários #1/#2/#3; se houver tempo, #5/#7/#8
8. `ARCHITECTURE.md` e `README.md` da solução, nomeando tudo que ficou na Camada "Não"
   ou "Parcial"

---

## 6. Revisão desta decisão

Se o tempo permitir mais do que o previsto, a ordem de prioridade para "subir de camada"
é: `PENDING_REFERENCE` completo → cenário #6 → outbox multi-publisher → métricas. Não
inverter essa ordem — ela segue o peso dos critérios do §14.

---

## 7. O que foi aproveitado do `wallet-go`

O `wallet-go` é um projeto de estudo anterior (Go + Postgres, ledger append-only,
transferências idempotentes). Os domínios são diferentes: lá o saldo é derivado do ledger e
uma transferência liga duas contas; aqui o saldo é guardado com versão e cada operação toca
uma carteira. Por isso nada foi copiado inteiro. Esta seção separa o que veio quase igual, o
que foi adaptado, o que foi só conceito, e o que **não** foi trazido.

### 7.1. Veio quase igual

| Peça | Origem → destino | O que mudou |
|---|---|---|
| Pool com `AcquireTimeout` | `internal/adapters/postgres/pool.go` → mesmo caminho | só comentários e o nome de uma constante. O wrapper `Pool` faz toda operação respeitar o prazo, para um banco lento falhar rápido em vez de pendurar |

### 7.2. Adaptado

| Peça | Origem | Como ficou aqui |
|---|---|---|
| `Money` | `internal/domain/money.go`: `int64` em centavos, `Add`/`Sub` com overflow | ganhou moeda, parsing de string decimal, `Negate`, serialização `{"amount","currency"}`, e a regra de moeda incompatível |
| Idempotência | `internal/app/idempotency.go`: chave `(scope, endpoint, key)`, estados `in_flight` / `completed`, `ErrIdempotencyKeyReuse` (422) e `ErrRequestInFlight` (409), claim, execução e conclusão na mesma transação | mesma estrutura. Mudou: a resposta é guardada como `TEXT` (o `JSONB` reordena chaves), a rejeição de negócio também é gravada e reproduzida, e a chave convive com a inbox do SQS |
| `Fingerprint` | recebe bytes de um JSON qualquer e o canonicaliza por mapa de chaves ordenadas, depois SHA-256 | aqui recebe uma struct tipada (`WagerBody`), porque o hash precisa ser idêntico por HTTP e SQS e normalizar o valor (`25` = `25.00`) |
| Lock pessimista | `transfer_executor.go`: `SELECT ... FOR UPDATE` dentro de uma transação que pertence a quem chama; o repositório nunca abre transação por conta própria | mesmo princípio. Lá são duas contas travadas em ordem de `id` para evitar deadlock; aqui é uma carteira só, então não há ordem a respeitar |
| Mapeamento de erro | `internal/adapters/http/errors.go`: uma função `statusFor` com `errors.Is`, erro desconhecido vira 500 sem vazar detalhe | mesmo formato e testes em tabela. Os códigos e o envelope são outros, e a rejeição de negócio deixou de ser erro HTTP |
| Infra de teste | `testhelper_test.go`: Postgres 16 por `testcontainers-go` e migrations pela biblioteca `golang-migrate` com `file://` | virou o pacote `internal/testutil`, para o teste do Fx e do HTTP usarem o mesmo código, e ganhou Keycloak e LocalStack |
| Harness de carga | `cmd/loadtest/main.go`: cálculo de percentis, classificação de erro de transporte, gerador com semente, relatório JSON em `docs/bench/` | reescrito menor: carga fixa em dois cenários, em vez de "rampa até quebrar". Ganhou token do Keycloak renovado e a conferência de saldo ao final |
| Encerramento | `cmd/wallet/main.go`: parar de aceitar, drenar o servidor, só depois fechar o pool | a mesma ordem, agora pelo ciclo de vida do Fx em vez de `signal.NotifyContext` manual |
| Liveness e readiness | `/health/live` e `/health/ready` separados | o readiness passou a checar banco e fila |

### 7.3. Só o conceito

- **Domínio com campos privados**, construtores que validam e métodos explícitos de
  transição, em vez de structs abertas.
- **Erros de domínio como valores comparáveis** com `errors.Is`.
- **Disciplina de transação:** nenhum efeito financeiro fora de uma transação explícita, com
  `defer Rollback` depois de um `Commit` que o torna inofensivo.
- **A forma de trabalho:** cada fase com plano, teste que prova a invariante, e documento do
  que ficou de fora.

### 7.4. Lições que valeram dinheiro

Problemas que o `wallet-go` já tinha custado e que não precisaram ser redescobertos:

- `.gitattributes` com `eol=lf`: o `core.autocrlf` do Windows quebrava o `gofmt`.
- `-race` no Windows exige um `gcc` de 64 bits (o do PATH era de 32).
- O PowerShell não expande `*` para ferramentas nativas; JSON com aspas funciona melhor no
  Git Bash.
- `for i := range string` devolve o índice do byte, não o caractere (aparecia de novo na
  validação de moeda).
- O Docker Desktop precisa estar aberto antes dos testes com containers.

### 7.5. Existe no `wallet-go` e **não** foi trazido

Eram reaproveitáveis e ficaram de fora por prazo. Os dois primeiros fecham lacunas que o
`ARCHITECTURE.md` declara:

- **Middleware de log de acesso, `requestID` e `recoverer`** (`internal/adapters/http/middleware.go`).
  Fechariam o log de acesso HTTP e parte dos identificadores de correlação (§12 do
  enunciado). É o reaproveitamento mais barato que sobrou.
- **Paginação por cursor** (`entry_repository.go`, `queryAfter`/`queryLimit`). Serviria ao
  `GET /wallets/:id/ledger`, que não foi feito.
- **CI no GitHub Actions** (build, vet, `-race` e `golangci-lint`), **`.golangci.yml`** e
  **Makefile**. A solução só documenta os comandos manuais.

### 7.6. Não se aplica

- **Saldo derivado do ledger.** O `wallet-go` calcula o saldo por `SUM` sobre as entradas; o
  enunciado pede saldo guardado com versão, conferido contra o ledger.
- **Partidas dobradas, conta de sistema e transferência entre duas contas.** O enunciado
  trata de uma carteira por operação, e partidas dobradas são opcionais.
- **Autenticação por chave de API** (`apiKeyAuth`). Aqui é OIDC com Keycloak, escrito do zero.

