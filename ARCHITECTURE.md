# Decisões de Arquitetura

Este documento explica as soluções implementadas para garantir exatidão financeira, concorrência e tolerância a falhas na manipulação distribuída de carteiras.

## 1. Zero-Float e Exatidão Monetária
Todo valor que entra na aplicação (`"100.00"`) é imediatamente convertido para centavos de moeda fiduciária num `int64` fixado em escala 2 (`10000`). Utiliza-se a estrutura `Money` no domínio para garantir o isolamento sem risco de erros de ponto flutuante, realizando validações estritas (ex: `LOSS` exige valor 0.00). 

## 2. PostgreSQL e Concorrência Segura (Pessimistic Locking)
O PostgreSQL foi utilizado como fonte da verdade transacional utilizando `pgxpool`.
Todas as alterações em uma mesma carteira ocorrem dentro de um `pgx.Tx` travando a respectiva linha de balance da carteira via `SELECT ... FOR UPDATE`. Isso blinda a transação contra o fenômeno "Lost Update" ao receber requests paralelos das três instâncias do *wallet-app*.

## 3. Idempotência Persistente Forte
Para suportar o *at-least-once* (entrega múltipla pela web ou SQS), empregamos uma tabela de transações. Antes da alteração, há uma busca via `external_transaction_id` + `provider_id`.
- Se existir uma tentativa prévia e for do exato mesmo hash/payload: Retornamos `HTTP 200 OK` (Replay) sem gerar duplo débito.
- Se a tentativa prévia possuir hash diferente: `HTTP 409 Conflict`.

## 4. Pending References e Assincronicidade
Um `ROLLBACK` (estorno) pode chegar *antes* da transação alvo (`BET`) devido à latência de rede. Nesses cenários, gravamos o ROLLBACK com o status `PENDING_REFERENCE`. O worker secundário (`PendingReferenceWorker`) com *exponential backoff* reavalia ciclicamente até o `BET` surgir (resolvendo o saldo), ou até atingir timeout e abortar o rollback como falho (`FailureReferenceNotFound`).

## 5. Event Driven & Transactional Outbox
Eventos de domínio (`WagerTransactionProcessed`, `WalletBalanceChanged`, etc) jamais são disparados de maneira "chutável". Utilizamos o padrão *Transactional Outbox*: os eventos são gravados na tabela `outbox_events` na MESMA transação ACID que debita a carteira.
Um worker assíncrono consome essa tabela travando a linha com `SELECT ... FOR UPDATE SKIP LOCKED` e, com exclusividade, pública no SQS (`outbox-events.fifo`) com deduplicação de SQS ativada (DLQ suportada).

## 6. Autenticação e Autorização via OIDC (Keycloak)
Para controlar permissões, utilizamos o Keycloak como IdP (`client_credentials`). Através do token JWT decodificado no middleware (`azp` (Authorized Party)), mapeamos o real `providerId` que executa a transação, impedindo que Provedor A tente processar pagamentos na transação do Provedor B.

## 7. Graceful Shutdown & Contextos
Implementamos encerramentos elegantes (SIGINT/SIGTERM) utilizando o Lifecycle Hook do Uber Fx. Todo processamento de consumidores (SQS Long Polling) ou workers escutam canais contextuais para evitar a interrupção abrupta no meio de um processamento, protegendo os registros da transação local.
