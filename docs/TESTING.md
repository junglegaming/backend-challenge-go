# Testing walkthrough

Run commands from the repository root with Go 1.27.1 and Docker running.
The manual commands also require `curl`, `jq` and `uuidgen`.

## 1. Check the environment and run fast checks

```sh
go version
docker info
make build
make test-race
make vet
make lint
```

Every command must exit with status 0. The race detector checks unit tests for
data races. Vet also checks integration/e2e code. Lint requires golangci-lint v2.
The existing lint configuration permits complexity 15/20; this walkthrough's
functions stay within cyclomatic complexity 6 and cognitive complexity 10.

## 2. Run the automated journey

```sh
make test-walkthrough
```

`TestWalkthrough` starts disposable PostgreSQL, LocalStack SQS and Keycloak
containers, applies migrations, provisions private queues and starts the real
application in process. No Compose stack is needed. It uses real HTTP and
Keycloak tokens, and stops at the first failed step. Cleanup stops the app and
removes the test resources without using the Compose database.

| Step | Action | Expected result |
| --- | --- | --- |
| 01 | Health; no token; provider accessing wallet administration | 200; 401; 403 |
| 02 | Open wallet | 201; BRL 1000.00 |
| 03 | BET 25; replay; same key with amount 26 | 201; 200 with same ID; 409; balance 975.00 |
| 03 | Read by internal/external ID; read as another provider | Same transaction; 404 for another provider |
| 04 | WIN 50 referencing BET | PROCESSED; balance 1025.00 |
| 04 | LOSS 0 | PROCESSED; balance unchanged |
| 04 | REFUND 25 referencing BET | PROCESSED; balance 1050.00 |
| 04 | ROLLBACK 50 referencing WIN | PROCESSED; balance 1000.00 |
| 04 | BET 1001 | 422, REJECTED / INSUFFICIENT_FUNDS; unchanged balance |
| 05 | REFUND 40 before BET; then submit BET | 202 PENDING_REFERENCE; eventually PROCESSED; balance 1000.00 |
| 06 | SQS BET 15; repeat through HTTP | Processed inbox; HTTP 200 replay; balance 985.00 |
| 07 | Ledger pages of 2; reconciliation | 8 distinct entries; pagination ends; consistent=true; calculated balance 985.00 |
| 08 | Outbox publication | Nonempty outbox; zero unpublished events |

Async checks poll with deadlines. Outbox assertions verify persisted publication
acknowledgements, not downstream business processing. This journey does not
replace the concurrency, infrastructure failure and crash/restart suites below.

## 3. Reproduce the operations manually

```sh
make up
docker compose ps -a
curl -fsS localhost:8080/health/live
curl -fsS localhost:8080/health/ready
```

Wait for readiness HTTP 200. Migration and provisioning jobs should exit 0.
Use `make logs` to diagnose startup errors. In the same shell, initialize unique
identifiers and fetch tokens using the development realm credentials:

```sh
token() {
  curl -fsS http://localhost:8180/realms/wallet/protocol/openid-connect/token \
    -d grant_type=client_credentials -d client_id="$1" \
    -d client_secret="$1-secret" | jq -er .access_token
}
ADMIN=$(token wallet-service)
PROVIDER_A=$(token provider-a)
PROVIDER_B=$(token provider-b)
PLAYER=$(uuidgen | tr '[:upper:]' '[:lower:]')
PREFIX=$(uuidgen | tr '[:upper:]' '[:lower:]')
WALLET=$(curl -fsS localhost:8080/wallets \
  -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"1000.00\",\"currency\":\"BRL\"}}" | jq -er .id)

submit() {
  body=$(jq -n --arg ext "$PREFIX-$1" --arg player "$PLAYER" \
    --arg wallet "$WALLET" --arg kind "$2" --arg amount "$3" \
    --arg ref "${4:+$PREFIX-$4}" \
    '{providerId:"provider-a",externalTransactionId:$ext,playerId:$player,
      walletId:$wallet,roundId:"round-1",gameId:"game-1",kind:$kind,
      money:{amount:$amount,currency:"BRL"}} +
      (if $ref == "" then {} else {referenceExternalTransactionId:$ref} end)')
  curl -sS -w '\nHTTP %{http_code}\n' localhost:8081/wagering/transactions \
    -H "Authorization: Bearer $PROVIDER_A" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: provider-a:$PREFIX-$1" -d "$body"
}

submit bet BET 25.00
submit bet BET 25.00
submit bet BET 26.00
submit win WIN 50.00 bet
submit loss LOSS 0.00
submit refund REFUND 25.00 bet
submit rollback ROLLBACK 50.00 win
submit rejected BET 1001.00
submit early-refund REFUND 40.00 late-bet
submit late-bet BET 40.00
curl -fsS "localhost:8082/providers/provider-a/wagering/transactions/$PREFIX-early-refund" \
  -H "Authorization: Bearer $PROVIDER_A" | jq
```

Compare each response with the table above. Repeat the last GET until the refund
is `PROCESSED`; the wallet returns to 1000.00. Re-fetch expired tokens if needed.
Check access control and read the BET as its owning provider. Capture the
transaction ID before attempting the same read as another provider:

```sh
curl -sS -w '\nHTTP %{http_code}\n' "localhost:8082/wallets/$WALLET"
# Expected: HTTP 401.
curl -sS -w '\nHTTP %{http_code}\n' "localhost:8082/wallets/$WALLET" \
  -H "Authorization: Bearer $PROVIDER_A"
# Expected: HTTP 403.
TRANSACTION_ID=$(curl -fsS "localhost:8082/providers/provider-a/wagering/transactions/$PREFIX-bet" \
  -H "Authorization: Bearer $PROVIDER_A" | jq -er .transactionId)
curl -sS -w '\nHTTP %{http_code}\n' "localhost:8082/wagering/transactions/$TRANSACTION_ID" \
  -H "Authorization: Bearer $PROVIDER_A"
# Expected: HTTP 200, with the same transactionId and status PROCESSED.
curl -sS -w '\nHTTP %{http_code}\n' "localhost:8082/wagering/transactions/$TRANSACTION_ID" \
  -H "Authorization: Bearer $PROVIDER_B"
# Expected: HTTP 404; another provider cannot read this transaction.
```

The negative checks omit curl's `-f` option so the response body and expected
HTTP error status remain visible.

Submit the final bet through SQS:

```sh
MESSAGE=$(jq -n --arg id "$PREFIX-message" --arg ext "$PREFIX-sqs" \
  --arg player "$PLAYER" --arg wallet "$WALLET" \
  --arg now "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  '{messageId:$id,type:"WagerTransactionRequested",occurredAt:$now,
    data:{providerId:"provider-a",externalTransactionId:$ext,
      idempotencyKey:("provider-a:"+$ext),playerId:$player,walletId:$wallet,
      roundId:"round-1",gameId:"game-1",kind:"BET",
      money:{amount:"15.00",currency:"BRL"}}}')
QUEUE=$(docker compose exec -T localstack awslocal sqs get-queue-url \
  --queue-name wager-transactions.fifo --query QueueUrl --output text)
docker compose exec -T localstack awslocal sqs send-message \
  --queue-url "$QUEUE" --message-group-id "$WALLET" \
  --message-deduplication-id "$PREFIX-message" --message-body "$MESSAGE"
curl -fsS "localhost:8082/providers/provider-a/wagering/transactions/$PREFIX-sqs" \
  -H "Authorization: Bearer $PROVIDER_A" | jq
```

Wait for that GET to return `PROCESSED`, then check the replay and final state:

```sh
submit sqs BET 15.00
curl -fsS "localhost:8082/wallets/$WALLET" -H "Authorization: Bearer $ADMIN" | jq
curl -fsS "localhost:8082/wallets/$WALLET/ledger?limit=2" -H "Authorization: Bearer $ADMIN" | jq
curl -fsS -X POST "localhost:8082/wallets/$WALLET/reconciliation" \
  -H "Authorization: Bearer $ADMIN" | jq
docker compose exec -T postgres psql -U wallet -d wallet -c \
  "SELECT count(*) AS total, count(*) FILTER (WHERE published_at IS NULL) AS pending FROM outbox_events WHERE partition_key='$WALLET';"
```

Expect HTTP 200 replay, balance 985.00, reconciliation `consistent: true` and
`checkedEntries: 8`. Pass each nonempty `nextCursor` as the ledger's URL-encoded
`cursor` parameter to read all 8 distinct entries. Outbox total must be positive;
poll until pending is zero. Opening plus seven movements create ledger entries;
LOSS, rejection and replays create no additional financial entries.

## 4. Run all regression suites

```sh
make test-integration
make test-e2e
make coverage
```

Integration includes this journey plus schema protections, migrations,
authentication failures, concurrency, retries, DLQ, inbox/outbox recovery and
shutdown. E2E runs the compiled binary as three processes, including concurrent
requests and crash/restart. Coverage combines unit and integration tests into
`coverage.out`; it does not instrument e2e processes.

## 5. Stop the manual environment

```sh
make down
```

This preserves Compose volumes. `make clean` deletes those volumes: use it only
when you intend to discard the local database and queue data. Automated tests
clean up their own resources independently.
