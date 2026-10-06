# Architecture & Design Decisions

## 1. Domain-Driven Design & Hexagonal Architecture
A aplicação foi estruturada seguindo os princípios de DDD e Arquitetura Hexagonal (Ports and Adapters):
- **Core Domain (`internal/domain`)**: Contém as regras de negócio puras (`Money`, `WagerTransaction`, `Wallet`). Não possui nenhuma dependência externa.
- **Use Cases (`internal/app/usecases`)**: Orquestra as regras de negócio usando as portas (interfaces) do banco de dados e gerenciadores de transação.
- **Infrastructure (`internal/infra`)**: Implementa os adaptadores (PostgreSQL com `pgxpool`, SQS, HTTP Handlers com Uber Fx e Autenticação OIDC).

## 2. Composição e Ciclo de Vida (Uber Fx)
Utilizamos **Uber Fx** para injeção de dependências e controle de ciclo de vida:
- Todas as dependências (configurações, conexões com o banco de dados, SQS, repositórios, handlers e background workers) são fornecidas via `fx.Provide`.
- O servidor HTTP, os consumidores SQS e os Workers (Outbox e Pending Reference) são inicializados de forma concorrente e atrelados aos hooks `OnStart` e `OnStop` do `fx.Lifecycle`.
- Durante um shutdown gracioso, o Fx garante que as conexões sejam liberadas de forma segura e que nenhum processamento fique pela metade.

## 3. Precisão Financeira
- **Zero Float Rule**: Valores monetários NUNCA passam por `float32` ou `float64`.
- A entrada `"100.00"` é interpretada diretamente via manipulação de string para `int64`, com checagem rígida de regex.
- Os cálculos são realizados nativamente com inteiros na escala de centavos, prevenidos contra *overflows* (`math.MaxInt64`), garantindo exatidão na persistência.

## 4. Persistência de Dados e Idempotência Persistente
Para garantir consistência e evitar ataques de repetição:
- **Ledger Append-Only**: A tabela `wallet_ledger` conta com um gatilho nativo no PostgreSQL que impede `UPDATE` e `DELETE`.
- **Prevenção de Lost Updates**: Utilizou-se lock pessimista no PostgreSQL (`SELECT ... FOR UPDATE`) na carteira do jogador, serializando qualquer alteração simultânea de saldo.
- **Idempotência Forte**: A tabela `wager_transactions` possui a constraint `UNIQUE(provider_id, external_transaction_id)`. Se um hash de payload diferente chegar para a mesma transação, a operação falha (HTTP 409). Se o payload for o mesmo, o processamento devolve a resposta que estava persistida em banco.

## 5. Garantia At-Least-Once e Transações Distribuídas
Em um ambiente distribuído com SQS:
- **Deduplicação Atômica**: A tabela `inbox_messages` previne a entrega duplicada pela mesma fila no padrão *Transactional Inbox*, e tudo é executado dentro da mesma transação SQL (`pgx.Tx`).
- **Transactional Outbox**: Todo evento disparado ("WagerTransactionProcessed") não é enviado diretamente ao SQS, mas salvo atomicamente na tabela `outbox_events`. Um `OutboxWorker` pesquisa e envia as mensagens usando `FOR UPDATE SKIP LOCKED`, garantindo que não existam concorrências de leitura nos nós do Outbox e previnindo a perda de mensagens se o processo principal falhar.

## 6. Worker de Referências Pendentes
Quando uma referência ausente ou pendente é informada, o sistema usa retries distribuídos (em vez de bloquear threads esperando).
A operação é salva como `PENDING_REFERENCE`. O `PendingReferenceWorker` consome em lote a cada poucos segundos, permitindo que a referência chegue, e rejeita as pendências quando excedem o prazo de 10 minutos.

## 7. Autenticação e Multitenancy
- A integração com **Keycloak** via OIDC permite que clientes do tipo `client_credentials` emitam tokens seguros.
- O Middleware HTTP valida os tokens, insere o `provider_id` no contexto, e barra provedores de consultarem transações ou movimentarem o saldo de terceiros, garantindo o isolamento da arquitetura Multitenant.
