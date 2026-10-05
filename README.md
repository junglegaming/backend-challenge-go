# Wallet & Wagering — desafio backend Go

Serviço em Go que processa operações financeiras de provedores de jogos (`BET`, `WIN`,
`LOSS`, `REFUND`, `ROLLBACK`) sobre carteiras de jogadores, por HTTP e por SQS, com
idempotência persistida e ledger append-only.

- Enunciado original: [docs/README-desafio.md](docs/README-desafio.md)
- Decisões, limitações e o que não foi feito: [ARCHITECTURE.md](ARCHITECTURE.md)
- Feito e não feito por critério de avaliação: [ARCHITECTURE.md § 11](ARCHITECTURE.md#11-situação-por-critério-de-avaliação)
- Decisão de escopo para o prazo de 3 dias: [docs/scope-decision.md](docs/scope-decision.md)

> O escopo foi reduzido de forma consciente. O que ficou de fora está listado em
> [ARCHITECTURE.md § Trabalho não concluído](ARCHITECTURE.md#10-trabalho-não-concluído).

## Pré-requisitos

- Docker com Docker Compose
- Go 1.25 (só para rodar os testes ou a aplicação fora do container)

## Subir tudo

```sh
docker compose up --build
```

| Serviço | Endereço | O que é |
|---|---|---|
| `app` | http://localhost:8080 | a API e o consumidor SQS |
| `postgres` | localhost:5432 | banco (`wallet` / `wallet` / `wallet`) |
| `keycloak` | http://localhost:8081 | IdP, realm `wallet` importado na subida (admin `admin` / `admin`) |
| `localstack` | http://localhost:4566 | SQS |
| `migrate` | — | aplica as migrations e termina com código 0 |

Nada precisa ser configurado à mão: o realm do Keycloak, as filas e as migrations são
provisionados na subida. Para zerar o estado: `docker compose down -v`.

## Identidades de teste

Todas usam `client_credentials`. Os segredos são de desenvolvimento e estão versionados em
`deploy/keycloak/wallet-realm.json`.

| Client | Segredo | Papel |
|---|---|---|
| `provider-a` | `provider-a-dev-secret` | provedor |
| `provider-b` | `provider-b-dev-secret` | provedor |
| `wallet-service` | `wallet-service-dev-secret` | serviço interno (role `wallet-internal`) |

Obter um token (bash):

```sh
token() {
  curl -s -X POST http://localhost:8081/realms/wallet/protocol/openid-connect/token \
    -d grant_type=client_credentials -d client_id="$1" -d client_secret="$2" \
    | grep -o '"access_token":"[^"]*"' | cut -d'"' -f4
}
SVC=$(token wallet-service wallet-service-dev-secret)
PA=$(token provider-a provider-a-dev-secret)
```

O token vale 5 minutos.

## Exemplos de chamadas

Abrir uma carteira (serviço interno):

```sh
curl -s -X POST http://localhost:8080/wallets \
  -H "Authorization: Bearer $SVC" -H "Content-Type: application/json" \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}'
```

Enviar uma aposta (provedor). Troque `WALLET_ID` pelo `id` devolvido acima:

```sh
curl -s -X POST http://localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $PA" -H "Content-Type: application/json" \
  -H "Idempotency-Key: provider-a:transaction-123" \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"WALLET_ID","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
```

Repetir o mesmo comando devolve o mesmo resultado com `"idempotentReplay": true`, sem
segundo débito. Para uma reversão, acrescente `"referenceExternalTransactionId"` ao corpo.

Consultas:

```sh
curl -s http://localhost:8080/wallets/WALLET_ID -H "Authorization: Bearer $SVC"
curl -s http://localhost:8080/wagering/transactions/TRANSACTION_ID -H "Authorization: Bearer $PA"
curl -s http://localhost:8080/health/live
curl -s http://localhost:8080/health/ready
```

Enviar a mesma operação pela fila:

```sh
docker compose exec localstack awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id WALLET_ID --message-deduplication-id msg-1 \
  --message-body '{"messageId":"msg-1","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z","data":{"providerId":"provider-a","externalTransactionId":"transaction-124","idempotencyKey":"provider-a:transaction-124","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"WALLET_ID","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"10.00","currency":"BRL"}}}'
```

## Contrato HTTP

| Rota | Quem pode chamar |
|---|---|
| `POST /wallets` | serviço interno |
| `GET /wallets/{walletId}` | serviço interno |
| `POST /wagering/transactions` (exige `Idempotency-Key`) | provedor |
| `GET /wagering/transactions/{transactionId}` | provedor dono da operação |
| `GET /health/live`, `GET /health/ready` | público |

Respostas e como distingui-las:

| Situação | Status | Corpo |
|---|---|---|
| Operação processada | 201 | `status: PROCESSED`, `balance`, `idempotentReplay` |
| Rejeição de negócio (ex.: saldo insuficiente) | 422 | `status: REJECTED`, `failureCode`, **sem** envelope `error` |
| Mesma chave, corpo diferente | 422 | `error.code: idempotency_key_reused` |
| Chave ainda em processamento | 409 | `error.code: request_in_flight` |
| Mesma operação (provedor + `externalTransactionId`) com outra chave | 409 | `error.code: duplicate_operation` |
| Carteira já existe para jogador e moeda | 409 | `error.code: conflict` |
| Entrada inválida | 400 | `error.code: invalid_request` |
| Token ausente, inválido ou expirado | 401 | `error.code: unauthenticated` |
| Token válido sem permissão para a rota | 403 | `error.code: forbidden` |
| Recurso inexistente ou de outro provedor | 404 | `error.code: not_found` |
| Banco inacessível ou sem resposta no prazo (nada foi aplicado; reenvie com a mesma chave) | 503 | `error.code: temporarily_unavailable` |
| Dependência fora (`/health/ready`) | 503 | `checks: {database, queue}` |

Códigos de rejeição (`failureCode`): `insufficient_funds`, `reversal_insufficient_funds`,
`reference_not_found`, `reference_not_processed`, `reference_mismatch`,
`reference_amount_mismatch`, `duplicate_reversal`.

## Variáveis de ambiente

No Compose elas já estão definidas em `docker-compose.yml`. Para rodar fora do container,
copie `.env.example` para `.env` (o binário lê o `.env` do diretório atual; uma variável já
definida no ambiente tem precedência).

| Variável | Obrigatória | Padrão | Significado |
|---|---|---|---|
| `DATABASE_URL` | sim | — | DSN do Postgres |
| `AUTH_ISSUER` | sim | — | `iss` exigido nos tokens |
| `AUTH_JWKS_URL` | sim | — | de onde as chaves públicas são baixadas |
| `SQS_QUEUE_URL` | sim | — | fila FIFO consumida |
| `SQS_ENDPOINT` | não | vazio (AWS) | endpoint do SQS; use o do LocalStack localmente |
| `AWS_REGION` | não | `us-east-1` | região |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | sim para o SDK | — | no LocalStack, qualquer valor |
| `HTTP_ADDR` | não | `:8080` | endereço do servidor |
| `DB_MAX_CONNS` | não | `10` | tamanho do pool |
| `DB_ACQUIRE_TIMEOUT` | não | `5s` | prazo de cada operação no banco |
| `SHUTDOWN_TIMEOUT` | não | `10s` | prazo para drenar requisições no encerramento |

## Migrations

No Compose, o serviço `migrate` aplica tudo antes do `app` subir. Manualmente:

```sh
# aplicar
docker compose run --rm migrate -path /migrations \
  -database "postgres://wallet:wallet@postgres:5432/wallet?sslmode=disable" up
# reverter a última
docker compose run --rm migrate -path /migrations \
  -database "postgres://wallet:wallet@postgres:5432/wallet?sslmode=disable" down 1
```

## Filas

`deploy/localstack/init/ready.d/10-queues.sh` cria, a cada subida do LocalStack,
`wager-transactions.fifo` e `wager-transactions-dlq.fifo`, com redrive após 3 recebimentos.

## Observabilidade

Os logs saem em **JSON, uma linha por registro**, no `stdout`. Cada requisição HTTP e cada
mensagem SQS tem um `correlationId`:

- no HTTP, o valor do cabeçalho `X-Correlation-Id`, se for uma string curta e imprimível
  (até 128 caracteres, sem espaço nem caractere de controle); senão, um UUID gerado. O id
  volta no cabeçalho `X-Correlation-Id` da resposta;
- no consumidor SQS, o `messageId` do envelope.

Toda linha escrita para aquela requisição ou mensagem traz esse id, mais o que já se sabe
naquele ponto: `clientId`, `providerId`, `walletId`, `kind`, `transactionId` e, em erro,
`errorCode`. Exemplo real, de uma aposta:

```json
{"time":"2026-10-04T03:19:17.786108454Z","level":"INFO","msg":"wager operation","status":"PROCESSED","failureCode":"","replayed":false,"correlationId":"demo-1791083957","clientId":"provider-a","providerId":"provider-a","walletId":"181df830-893d-4572-9de8-f35de15af3f7","kind":"BET","transactionId":"79971434-ddf9-4d63-b548-a69e8a0056f7"}
{"time":"2026-10-04T03:19:17.786135437Z","level":"INFO","msg":"http request","method":"POST","route":"POST /wagering/transactions","status":201,"durationMs":7.741,"correlationId":"demo-1791083957","clientId":"provider-a","providerId":"provider-a","walletId":"181df830-893d-4572-9de8-f35de15af3f7","kind":"BET","transactionId":"79971434-ddf9-4d63-b548-a69e8a0056f7"}
```

Para seguir uma requisição:

```sh
docker compose logs app --no-log-prefix | grep demo-1791083957
```

O que **não** vai para o log, e há teste para isso: valores monetários, corpos, cabeçalhos
(`Authorization`) e o caminho cru da requisição. O log de acesso usa o **padrão da rota**
(`GET /wallets/{walletId}`), porque o caminho carrega ids.

Métricas **não foram implementadas**. Health checks: `/health/live` e `/health/ready`.

## Eventos de integração (outbox)

Toda operação decidida grava seus eventos na tabela `outbox`, na **mesma transação** da
operação. **Hoje os eventos são gravados, mas ainda não publicados**: não há publisher.

| Evento | Quando |
|---|---|
| `WagerTransactionProcessed` | operação `PROCESSED`, inclusive `LOSS` e a abertura de carteira |
| `WagerTransactionRejected` | rejeição de negócio |
| `WalletBalanceChanged` | alteração efetiva do saldo |

Envelope:

```json
{
  "eventId": "…",              "eventType": "WalletBalanceChanged",
  "aggregateId": "<walletId>", "correlationId": "demo-1791083957",
  "causationId": "<eventId do WagerTransactionProcessed>",
  "occurredAt": "2026-10-04T03:04:15.690Z", "version": 1,
  "data": {
    "walletId": "…", "transactionId": "…", "direction": "DEBIT",
    "money": {"amount": "25.00", "currency": "BRL"},
    "balanceBefore": {"amount": "1000.00", "currency": "BRL"},
    "balanceAfter": {"amount": "975.00", "currency": "BRL"},
    "walletVersion": 2
  }
}
```

O envelope fica na coluna `payload` (`JSONB`), e o Postgres reordena as chaves ao armazenar: a ordem acima é lógica, não a do que `psql` mostra. `eventId` é o id da linha da outbox: uma republicação o preserva, e o consumidor deve
deduplicar por ele. A ordem dos eventos é a coluna `seq`. Replay e operação recusada não
gravam eventos. Para ver o que foi gravado:

```sh
docker compose exec postgres psql -U wallet -d wallet -c "SELECT seq, event_type, aggregate_id FROM outbox ORDER BY seq DESC LIMIT 10;"
```

## Testes

Os testes de integração usam containers reais (Postgres, Keycloak e LocalStack) via
testcontainers. Só é preciso ter o Docker em execução; as imagens são baixadas na primeira
vez. Não há build tags.

```sh
go vet ./...
go test ./... -p 1
go test -race ./... -p 1
go test ./internal/domain/...     # só domínio: sem Docker, menos de 1 s
```

O pacote `internal/e2e` **compila o binário** (`go build ./cmd/api`), então exige o Go no PATH, e leva ~25 s. `-p 1` roda um pacote por vez. No Windows com Docker Desktop, pacotes em paralelo às vezes
falham ao conectar no Docker. `-race` exige um compilador C de 64 bits (`gcc`).

O que cada pacote prova:

| Pacote | Prova |
|---|---|
| `internal/domain` | `Money`, carteira, máquina de estados, regras dos cinco tipos, abertura, eventos e o formato do envelope |
| `internal/adapters/postgres` | constraints e triggers, ledger imutável, disputa de saldo (100 / duas apostas de 80), idempotência com 50 requisições simultâneas, reversões, uma carteira ocupada que não bloqueia outra, eventos na outbox (mesma transação, replay, recusa) |
| `internal/adapters/httpapi` | contratos HTTP com tokens reais do Keycloak, isolamento entre provedores, `correlationId` e log sem segredos |
| `internal/logctx` | o contexto de log: atributos compartilhados, `correlationId`, o handler do `slog` |
| `internal/adapters/sqs` | consumo real no LocalStack, reentrega, mesma operação por HTTP e SQS, DLQ |
| `internal/composition` | grafo Fx válido; início, atendimento e encerramento liberando o pool |
| `internal/e2e` | **três processos reais** do binário contra Postgres e Keycloak: 100/80 em 30 carteiras ao mesmo tempo, a mesma chave 50 vezes, e replay depois de `Kill` e novo processo |

## Múltiplas instâncias e simulação de falhas

Estes cenários rodam **à mão** contra o Compose. Foram executados na máquina de
desenvolvimento (Windows 11, Docker Desktop) e os resultados abaixo são os observados. Os
cenários 1, 2 e 5 também existem como **teste automatizado** em `internal/e2e`, com três
processos reais e `kill`; os cenários 3, 4 e 6 a 9 são só manuais. Os comandos de shell usam
o Git Bash.

### Preparação

```sh
docker compose up -d
docker compose run -d --name wallet-app-2 -p 8082:8080 app
docker compose run -d --name wallet-app-3 -p 8083:8080 app
```

O serviço `app` publica a porta 8080, então `--scale` colidiria. `docker compose run` não
herda as portas do serviço: as instâncias 2 e 3 usam as portas 8082 e 8083, com processo,
memória e pool próprios, e dividem Postgres, Keycloak e LocalStack. Os três `/health/ready`
devem responder `200`. Para remover as extras: `docker rm -f wallet-app-2 wallet-app-3`.

Funções usadas nos cenários (cole uma vez na janela do Git Bash):

```sh
B1=http://localhost:8080; B2=http://localhost:8082; B3=http://localhost:8083
K=http://localhost:8081/realms/wallet/protocol/openid-connect/token
tok(){ curl -s -X POST $K -d grant_type=client_credentials -d client_id=$1 -d client_secret=$2 | grep -o '"access_token":"[^"]*"' | cut -d'"' -f4; }
guid(){ powershell -NoProfile -Command "[guid]::NewGuid().ToString()" | tr -d '\r'; }
SVC=$(tok wallet-service wallet-service-dev-secret); PA=$(tok provider-a provider-a-dev-secret)
newwallet(){ # $1 saldo inicial; define P e WID
  P=$(guid)
  WID=$(curl -s -X POST $B1/wallets -H "Authorization: Bearer $SVC" -H "Content-Type: application/json" \
    -d "{\"playerId\":\"$P\",\"initialBalance\":{\"amount\":\"$1\",\"currency\":\"BRL\"}}" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4); }
bet(){ # $1 base  $2 id  $3 valor ; a chave é provider-a:$2-$T
  curl -s -X POST $1/wagering/transactions -H "Authorization: Bearer $PA" -H "Content-Type: application/json" \
    -H "Idempotency-Key: provider-a:$2-$T" \
    -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$2-$T\",\"playerId\":\"$P\",\"walletId\":\"$WID\",\"roundId\":\"r1\",\"gameId\":\"g1\",\"kind\":\"BET\",\"money\":{\"amount\":\"$3\",\"currency\":\"BRL\"}}"; echo; }
```

### 1. Três apostas de 80,00 sobre saldo 100,00, uma em cada instância

```sh
newwallet 100.00; T=$(date +%s)
bet $B1 a 80.00 & bet $B2 b 80.00 & bet $B3 c 80.00 & wait
curl -s $B1/wallets/$WID -H "Authorization: Bearer $SVC"
```

**Esperado e observado:** uma resposta `PROCESSED`, duas `REJECTED` com `insufficient_funds`,
saldo `20.00`, versão `2`. Quem decide a disputa entre processos é o `FOR UPDATE` no banco.

### 2. A mesma aposta 50 vezes, espalhada pelas três instâncias

```sh
newwallet 1000.00; T=$(date +%s); BASES=($B1 $B2 $B3)
for i in $(seq 0 49); do bet ${BASES[$((i%3))]} same 25.00 > /tmp/dup-$i.json & done; wait
echo "originais: $(grep -l '"idempotentReplay":false' /tmp/dup-*.json | wc -l)"
echo "replays:   $(grep -l '"idempotentReplay":true'  /tmp/dup-*.json | wc -l)"
curl -s $B1/wallets/$WID -H "Authorization: Bearer $SVC"; rm /tmp/dup-*.json
```

**Esperado e observado:** 1 original, 49 replays, saldo `975.00`.

### 3. Carga nas três instâncias ao mesmo tempo

Em três terminais, ao mesmo tempo:

```sh
go run ./cmd/loadtest -scenario many -base http://localhost:8080 -duration 60s
go run ./cmd/loadtest -scenario many -base http://localhost:8082 -duration 60s
go run ./cmd/loadtest -scenario many -base http://localhost:8083 -duration 60s
```

**Observado:** cerca de 1.650 RPS em cada, p99 de ~32 ms, zero erros e `balance check: OK`
nos três. Números e leitura em [docs/loadtest.md](docs/loadtest.md).

### 4. Integridade depois de qualquer cenário

```sh
docker compose exec postgres psql -U wallet -d wallet -c "SELECT w.id FROM wallets w LEFT JOIN wallet_ledger_entries e ON e.wallet_id = w.id GROUP BY w.id, w.balance_minor HAVING w.balance_minor <> COALESCE(SUM(CASE e.direction WHEN 'CREDIT' THEN e.amount_minor ELSE -e.amount_minor END), 0);"
```

**Esperado e observado:** `(0 rows)`. Cada linha seria uma carteira cujo saldo não bate com o
ledger.

### 5. Morte abrupta do processo e replay (`kill -9`)

```sh
newwallet 1000.00; T=$(date +%s)
bet $B1 crash 25.00                       # primeira vez
docker kill wallet-app-1                  # SIGKILL: sem encerramento gracioso
docker start wallet-app-1
until curl -s localhost:8080/health/ready | grep -q '"status":"ok"'; do sleep 1; done
bet $B1 crash 25.00                       # a mesma operação, a mesma chave
curl -s $B1/wallets/$WID -H "Authorization: Bearer $SVC"
```

Não redefina `T` entre as duas apostas: ele compõe a chave de idempotência.

**Esperado e observado:** a segunda resposta traz o mesmo `transactionId`,
`idempotentReplay: true` e saldo `975.00`. A chave está no banco, não na memória.

### 6. Morte do processo durante a carga

```sh
go run ./cmd/loadtest -scenario many -duration 60s      # terminal 1
docker kill wallet-app-1                                 # terminal 2, ~20 s depois
docker start wallet-app-1                                # ~10 s depois
```

**Observado:** 31 `connection_reset` (requisições em andamento na queda) e milhares de erros
de conexão durante o período fora. O `balance check` apontou divergência em 4 de 50
carteiras, **cada uma com exatamente 1,00 a menos** que o esperado: uma aposta já aplicada no
banco cuja resposta se perdeu na queda. O saldo só diverge para **menos**, e a diferença não
passa do número de requisições interrompidas; saldo **maior** que o esperado seria um bug
grave. O cliente que reenviasse a mesma chave receberia o resultado gravado. O `loadtest` não
reenvia, então isso não foi exercitado nesta carga, só no cenário 5. O RPS e os percentis
dessa rodada incluem as tentativas que falharam e **não devem ser lidos como vazão**.

### 7. Encerramento gracioso (`SIGTERM`)

```sh
docker stop wallet-app-1
docker compose logs app | grep "HOOK OnStop"
```

**Observado:** os hooks rodam na ordem servidor HTTP (17 ms), consumidor SQS, pool do banco.
O `docker stop` espera 10 s antes de enviar `SIGKILL`, o mesmo valor de `SHUTDOWN_TIMEOUT`;
um drenar mais lento que isso seria cortado pelo Docker. Se isso for um risco, acrescente
`stop_grace_period: 20s` ao serviço `app`.

### 8. Postgres indisponível

```sh
docker compose stop postgres
curl -s -w "\nHTTP %{http_code}\n" localhost:8080/health/ready
curl -s -w "\nHTTP %{http_code}\n" localhost:8080/health/live
# uma aposta (comando do cenário 5)
docker compose start postgres
```

**Observado:** `/health/ready` responde `503` com `database: down`; `/health/live` continua
`200`; a aposta responde **`503 temporarily_unavailable`** em milissegundos, sem expor detalhe
interno. Nada é aplicado. Quando o banco volta, **o mesmo processo** (zero reinícios, conferido)
se recupera sozinho, e reenviar a mesma chave processa a operação normalmente.

Antes da correção desta entrega, a aposta nesse cenário respondia `500`; o teste
`TestStatusFor` cobre agora a falha de conexão e o desligamento do servidor (`57P01`).

### 9. SQS indisponível

```sh
docker compose stop localstack
curl -s -w "\nHTTP %{http_code}\n" localhost:8080/health/ready
# uma aposta HTTP (comando do cenário 5)
docker compose start localstack
```

**Observado:** `/health/ready` responde `503` com `queue: down`; apostas por HTTP continuam
`201`; o consumidor registra um erro de `receive messages` por segundo. Quando o LocalStack
volta, o script de init recria as filas e o readiness volta a `200`. A imagem usada **não
persiste** mensagens entre reinícios, então o que estava na fila se perde (limitação do
emulador).

### O que não é reproduzível à mão

- **Interromper o consumidor depois do commit e antes de apagar a mensagem** (cenário 5 do
  enunciado): depende de uma janela de milissegundos. O comportamento está coberto por
  `TestConsumer_BetIsAppliedOnceEvenWhenTheMessageRepeats`, que reentrega a mensagem.
- **Dois publishers disputando a outbox** (cenário 6) e **reversão antes da referência**
  (cenário 7): não há publisher nem worker de `PENDING_REFERENCE`.

## Teste de carga

Com a pilha de pé:

```sh
go run ./cmd/loadtest -scenario many -wallets 50 -workers 32 -duration 60s
go run ./cmd/loadtest -scenario single -workers 32 -duration 60s
```

Metodologia, ambiente e resultados em [docs/loadtest.md](docs/loadtest.md). Os relatórios
JSON ficam em `docs/bench/`.

## Rodar fora do container

```sh
docker compose up -d postgres migrate keycloak localstack
cp .env.example .env    # ajuste DATABASE_URL para wallet:wallet@localhost:5432/wallet
go run ./cmd/api
```

`Ctrl+C` encerra na ordem: servidor HTTP, consumidor SQS, pool do banco.
