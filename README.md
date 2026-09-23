# Wallet Service — processamento distribuído de apostas em Go

Serviço em Go 1.27 + Uber Fx que movimenta carteiras de jogadores a partir de uma API HTTP e de um consumidor SQS, com as mesmas garantias nas duas entradas: dinheiro exato, ledger append-only, idempotência persistente, coordenação por carteira entre várias instâncias, inbox/outbox transacionais e recuperação após falhas.

- Enunciado original: [`docs/CHALLENGE.md`](docs/CHALLENGE.md)
- Decisões técnicas, limitações e interpretações: [`ARCHITECTURE.md`](ARCHITECTURE.md)
- Broker policy examples and verification limits: [`docs/BROKER_SECURITY.md`](docs/BROKER_SECURITY.md)
- Outbox retry and quarantine recovery: [`docs/OUTBOX_RECOVERY.md`](docs/OUTBOX_RECOVERY.md)

## Pré-requisitos

| Ferramenta | Versão usada |
| --- | --- |
| Go | 1.27.1 (declarada em `go.mod` e no `Dockerfile`) |
| Docker + Docker Compose v2 | Docker 29 |
| golangci-lint (opcional, para `make lint`) | v2 |

As portas locais usadas são `5432` (PostgreSQL), `4566` (LocalStack), `8180` (Keycloak) e `8080`–`8082` (três instâncias do serviço).

## Subir tudo

```sh
docker compose up --build
```

O compose sobe, nesta ordem:

1. `postgres` (que, com o volume vazio, executa `deploy/postgres/init/01-runtime-user.sql` e cria o login de runtime), `localstack` (SQS) e `keycloak` (realm `wallet` importado de `deploy/keycloak/realm-wallet.json`);
2. `migrate`: executa `wallet migrate up` como o dono do schema (`wallet`);
3. `provision-queues` (em paralelo com `migrate`): executa `wallet provision-queues`, que cria `wager-transactions.fifo`, `wager-transactions-dlq.fifo` (redrive com `maxReceiveCount` = `SQS_MAX_RECEIVE_COUNT` + 20, ou seja, 25 no padrão; ver [Filas](#filas)) e `wallet-events.fifo` (destino da outbox);
4. `app-1`, `app-2` e `app-3`: três processos independentes do mesmo serviço, em `localhost:8080`, `:8081` e `:8082`, conectados como `wallet_service` (menor privilégio).

`make up`, `make down` (para os containers e preserva os volumes), `make clean` (remove também os volumes) e `make logs` são atalhos.

### Migrations

As migrations ficam versionadas em `internal/adapter/postgres/migrations` (golang-migrate, embutidas no binário):

```sh
wallet migrate up          # aplica todas
wallet migrate down [n]    # reverte n (padrão 1)
wallet migrate version     # versão atual
```

Com o compose: `make migrate-up` / `make migrate-down`. Fora do Docker: `go run ./cmd/wallet migrate up`, com `DATABASE_URL` apontando para o banco. Os comandos `migrate` e `provision-queues` validam apenas as variáveis que usam (banco e SQS, respectivamente); só `serve` exige a configuração completa.

Há uma única migration inicial (`000001_init`) com o schema completo. Revertê-la remove o schema inteiro, então `migrate down` só deve ser usado num banco descartável. Depois de ir para produção, ela fica imutável e mudanças futuras entram em novas migrations versionadas.

#### Papéis do banco

- As migrations rodam como o dono do schema (`wallet`). O padrão de `DATABASE_URL` é esse usuário, por conveniência para o `migrate` local.
- A `000001` cria o grupo `NOLOGIN` `wallet_app`, com `SELECT, INSERT` em `ledger_entries` e `SELECT, INSERT, UPDATE` em `wallets`, `wager_transactions`, `inbox_messages` e `outbox_events`.
- O login `wallet_service` (membro de `wallet_app`, senha só local) é criado por `deploy/postgres/init/01-runtime-user.sql`, no compose e nos testcontainers. `app-1..3` conectam com ele; só o serviço `migrate` usa o dono. O `.env.example` também aponta para `wallet_service`.
- O script de init só roda com o volume vazio: num volume antigo, recrie com `docker compose down -v` (`make clean`).
- Em produção, o migrador precisa de `CREATEROLE` (ou um DBA cria `wallet_app` antes). O login é criado por um passo de provisionamento, fora das migrations, com a senha vinda de um cofre de segredos: `CREATE ROLE wallet_service LOGIN PASSWORD '…' IN ROLE wallet_app;` (ou `GRANT wallet_app TO wallet_service;` para um login existente).

O binário devolve `0` em sucesso, `2` para comando ou argumentos inválidos (imprime o uso) e `1` para qualquer outra falha; erros vão para `stderr`, os logs JSON para `stdout`.

### Filas

`wallet provision-queues` cria as filas de forma idempotente (`make provision-queues`). Rodar de novo depois de mudar `SQS_MAX_RECEIVE_COUNT` ou `SQS_VISIBILITY_TIMEOUT` reconcilia as filas existentes. No compose isso roda automaticamente.

Quem manda para a DLQ um erro transitório é o próprio consumidor, ao atingir `SQS_MAX_RECEIVE_COUNT`, com o atributo `failureReason`. O redrive da fila é provisionado com `maxReceiveCount` = `SQS_MAX_RECEIVE_COUNT` + 20 (`sqs.RedriveMaxReceiveCount`) porque, num FIFO, as mensagens atrás de uma cabeça em retry no mesmo `MessageGroupId` são recebidas (e liberadas) junto com ela, e o contador delas sobe sem que tenham sido tentadas. A folga garante que elas nunca sejam levadas pelo broker por falhas alheias. O custo: uma seguidora que esperou atrás de uma cabeça com falha começa com contador alto, então, se ela própria falhar de forma transitória, pode ir para a DLQ antes (com o seu próprio `failureReason`).

## Variáveis de ambiente

Todas têm padrão local, exceto `AWS_ENDPOINT_URL`, que vazio significa a AWS real (a lista completa está em [`.env.example`](.env.example), que o compose carrega). As principais:

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `INSTANCE_ID` | hostname | identifica o processo (dono dos leases da outbox e atributo `instance` dos logs) |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` ou `error` |
| `DATABASE_URL` | `postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable` | PostgreSQL. O padrão é o dono (para o `migrate`); o serviço deve usar `wallet_service` (ver [Papéis do banco](#papéis-do-banco)) |
| `DB_MAX_CONNS` | `20` | máximo de conexões do pool (1 a 2147483647, o limite do `int32` do pgx) |
| `DB_LOCK_TIMEOUT` / `DB_STATEMENT_TIMEOUT` | `5s` / `10s` | `lock_timeout` e `statement_timeout` de cada conexão |
| `OIDC_ISSUER` | `http://localhost:8180/realms/wallet` | `iss` esperado nos tokens |
| `OIDC_JWKS_URL` | `…/protocol/openid-connect/certs` | onde buscar as chaves (no compose, `http://keycloak:8080/…`) |
| `OIDC_AUDIENCE` | `wallet-api` | `aud` exigido |
| `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | vazio (AWS real; o `.env.example` aponta para o LocalStack), `us-east-1` | cliente SQS |
| `SQS_INPUT_QUEUE`, `SQS_DLQ`, `SQS_EVENTS_QUEUE` | nomes acima | filas |
| `SQS_SENDER_PROVIDERS` | `000000000000=*` | vínculo `SenderId` do SQS → provedores permitidos (`id=provider-a\|provider-b;outroId=*`) |
| `SQS_CONSUMERS`, `SQS_MAX_MESSAGES`, `SQS_WAIT_TIME`, `SQS_VISIBILITY_TIMEOUT`, `SQS_PROCESS_TIMEOUT`, `SQS_ACK_TIMEOUT`, `SQS_RETRY_BASE/MAX` | `2`, `10`, `10s`, `5m`, `20s`, `5s`, `2s/60s` | consumidor: goroutines, tamanho do lote, long polling, visibilidade, prazo por mensagem, prazo do delete/retry/cópia para a DLQ e backoff de retry |
| `SQS_MAX_RECEIVE_COUNT` | `5` | limite do próprio consumidor: um erro transitório nesse recebimento vai para a DLQ com `failureReason`. O redrive da fila fica em `SQS_MAX_RECEIVE_COUNT` + 20, só como rede de segurança |
| `PENDING_INTERVAL`, `PENDING_BASE_DELAY`, `PENDING_MAX_DELAY`, `PENDING_MAX_ATTEMPTS`, `PENDING_BATCH` | `1s`, `1s`, `60s`, `10`, `50` | referências pendentes |
| `OUTBOX_INTERVAL`, `OUTBOX_BATCH`, `OUTBOX_LEASE`, `OUTBOX_RETRY_BASE/MAX`, `OUTBOX_PUBLISH_TIMEOUT`, `OUTBOX_FINALIZE_TIMEOUT`, `OUTBOX_MAX_ATTEMPTS` | `500ms`, `50`, `30s`, `1s/60s`, `10s`, `5s`, `20` | relay da outbox; `OUTBOX_FINALIZE_TIMEOUT` cobre as escritas no banco em volta da publicação |
| `CONFLICT_RETRIES` | `5` | novas tentativas de uma transação SQL que perdeu uma disputa |
| `STARTUP_TIMEOUT` | `25s` | prazo para construir e iniciar a aplicação |
| `SHUTDOWN_TIMEOUT` | `61s` | prazo total do encerramento gracioso (consumidores param de buscar, HTTP drena, workers terminam, pool fecha) |

A configuração é validada no início (`internal/config`) e todos os erros são reportados juntos: parse, `LOG_LEVEL`, valores não positivos, limites do SQS (`SQS_MAX_MESSAGES` 1–10, `SQS_WAIT_TIME` 0–20 s, visibilidade, `SQS_PROCESS_TIMEOUT`, `SQS_ACK_TIMEOUT` e `SQS_RETRY_MAX` ≤ 12 h), durações do SQS em segundos inteiros e timeouts do PostgreSQL em milissegundos inteiros, de 1 a 2147483647 ms (os adaptadores truncariam o resto; `0` é rejeitado porque desligaria o timeout), bases de backoff ≤ máximos e `SQS_SENDER_PROVIDERS`. Duas relações evitam processamento duplicado: `SQS_VISIBILITY_TIMEOUT > SQS_MAX_MESSAGES × (SQS_PROCESS_TIMEOUT + SQS_ACK_TIMEOUT)` e `OUTBOX_PUBLISH_TIMEOUT + OUTBOX_FINALIZE_TIMEOUT < OUTBOX_LEASE`.

## Autenticação

O Keycloak é provisionado automaticamente com clients `client_credentials`:

| Client | Secret | Papel | `provider_id` |
| --- | --- | --- | --- |
| `wallet-service` | `wallet-service-secret` | `wallet-admin` (serviço interno) | — |
| `provider-a` | `provider-a-secret` | `wager-provider` | `provider-a` |
| `provider-b` | `provider-b-secret` | `wager-provider` | `provider-b` |
| `provider-a-short-lived` | `provider-a-short-lived-secret` | `wager-provider` (token de 3 s, para testes de expiração) | `provider-a` |
| `no-role-client` | `no-role-client-secret` | nenhum | — (sem claim `provider_id`) |

```sh
token() {
  curl -s -d grant_type=client_credentials -d client_id=$1 -d client_secret=$1-secret \
    http://localhost:8180/realms/wallet/protocol/openid-connect/token | jq -r .access_token
}
ADMIN=$(token wallet-service)
PROVIDER_A=$(token provider-a)
```

## Exemplos de chamadas

```sh
# Abrir carteira (somente o serviço interno)
curl -s -X POST localhost:8080/wallets -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}'

WALLET=<id retornado>

# Aposta (provedor), enviada a outra instância
curl -s -X POST localhost:8081/wagering/transactions -H "Authorization: Bearer $PROVIDER_A" \
  -H 'Idempotency-Key: provider-a:transaction-123' -H 'Content-Type: application/json' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"'$WALLET'","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
# => 201 {"transactionId":"…","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false}
# Repetir a mesma chamada => 200 com "idempotentReplay":true e o mesmo saldo original.

# Reversão: acrescente "referenceExternalTransactionId" e use kind REFUND ou ROLLBACK.

# Consultas
curl -s localhost:8082/wallets/$WALLET -H "Authorization: Bearer $ADMIN"
curl -s "localhost:8082/wallets/$WALLET/ledger?limit=50" -H "Authorization: Bearer $ADMIN"
curl -s localhost:8082/providers/provider-a/wagering/transactions/transaction-123 -H "Authorization: Bearer $PROVIDER_A"
curl -s -X POST localhost:8082/wallets/$WALLET/reconciliation -H "Authorization: Bearer $ADMIN"

# Health e métricas (públicos)
curl -s localhost:8080/health/live
curl -s localhost:8080/health/ready
curl -s localhost:8080/metrics

# Operação via SQS
awslocal sqs send-message --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id $WALLET --message-deduplication-id msg-123 \
  --message-body '{"messageId":"msg-123","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z","data":{"providerId":"provider-a","externalTransactionId":"transaction-124","idempotencyKey":"provider-a:transaction-124","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"'$WALLET'","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}}'
```

Para a ordem FIFO, `MessageGroupId` precisa ser o UUID canônico (minúsculo, com hífens) do `walletId`, e `MessageDeduplicationId` precisa ser igual ao `messageId`. O `walletId` do JSON pode ter maiúsculas, mas o grupo usa a grafia canônica; caso contrário a mensagem vai para a DLQ.

Os códigos HTTP, os corpos de erro e os `failureCode` estão em [`ARCHITECTURE.md`](ARCHITECTURE.md#13-contrato-http).

## Testes

### How to test

See [the complete testing walkthrough](docs/TESTING.md) for prerequisites, manual commands, expected balances, and the full regression sequence. Run `make test-walkthrough` to execute every business step with real dependencies and a separate result per step.

```sh
go test ./...          # unitários (sem Docker)
go test -race ./...
make vet               # go vet, também com as tags integration e e2e
make lint              # gofmt/goimports, gocyclo ≤ 6, gocognit ≤ 10 (code and tests)
make                   # vet + lint + testes unitários com -race
```

O CI (`.github/workflows/ci.yml`) roda tidy, gofmt, vet com todas as tags, build, testes unitários com `-race`, `golangci-lint` v2 fixado e `govulncheck` em todo push/PR. Os testes com Docker (`make coverage` e `make test-e2e`) rodam num job à parte, disparado manualmente (`workflow_dispatch`) ou toda noite.

### Integração e e2e

As dependências dos testes sobem sozinhas via **testcontainers** (é preciso Docker rodando; as imagens são as mesmas do compose). As suítes são separadas por build tags:

```sh
# PostgreSQL, LocalStack e Keycloak reais: schema/constraints/imutabilidade, migrations up/down,
# concorrência (50 apostas iguais, 80+80 sobre 100, carteiras independentes, reversões concorrentes),
# lock timeout, inbox/reentrega após crash, DLQ/redrive, outbox com publishers concorrentes e
# recuperação, autenticação real (inválida, adulterada, expirada), isolamento entre provedores,
# papel de runtime sem privilégio sobre o ledger, composição Fx (start/stop, ordem de
# encerramento, falhas de dependência) e CLI.
make test-integration   # go test -race -count=1 -tags integration ./test/integration/...

# O binário compilado roda como 3 processos independentes: duplicidade e disputa entre instâncias,
# HTTP + SQS para a mesma operação, REFUND antes da BET, SIGKILL das três instâncias com reinício
# (idempotência e pendências preservadas) e reconciliação de todas as carteiras ao final.
make test-e2e           # go test -count=1 -timeout 15m -tags e2e ./test/e2e/...

# Cobertura de unidade + integração (com -race) de cmd/ e internal/; imprime o total.
make coverage
```

`make coverage` grava o perfil em `coverage.out` e imprime o percentual total. Não há gate por pacote, e o e2e fica de fora (roda o binário compilado, sem instrumentação).

### Simulações de falha cobertas

| Situação | Onde |
| --- | --- |
| Consumidor morre depois do commit e antes do `DeleteMessage` | `TestConsumerCrashAfterCommitIsRedeliveredAndDeduplicated` |
| Publisher morre entre publicar e confirmar na outbox | `TestCrashBetweenPublishAndConfirmIsRecovered` |
| Commit feito, publicação nunca feita (outbox pendente) | `TestTwoPublishersShareTheOutbox` |
| PostgreSQL indisponível no consumo (retry e, no limite, DLQ com `failureReason`) | `TestTransientFailuresAreRetriedThenDeadLetteredWithTheirReason` |
| Mensagem atrás de uma cabeça com falha no mesmo grupo não vai para a DLQ | `TestFollowerOfAFailingHeadIsNotDeadLettered` |
| Redrive da fila como rede de segurança | `TestQueueRedriveRemainsASafetyNet` |
| Lock de carteira preso (lock timeout → retry/503) | `TestLockTimeoutIsTransient` |
| Encerramento abrupto de todas as instâncias | `TestCrashAndRestart` (e2e) |
| `SIGTERM`: para de buscar trabalho, conclui ou libera o em andamento | `TestConsumerReleasesUnstartedMessagesOnShutdown`, `TestApplicationStopsWorkersOnShutdown`, `TestFxStopsServerThenWorkersThenPool` |
| Runtime tentando reescrever o ledger ou desligar seus triggers | `TestRuntimeRoleCannotRewriteOrUnguardTheLedger` |
| Lançamento de ledger que não confere com a sua transação | `TestLedgerEntryMustMatchItsTransaction` |
| Provedor reutilizando chave e `externalTransactionId` de outro | `TestProviderIsolationOnReplays` |
