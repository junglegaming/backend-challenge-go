# Wallet & Wagering Backend Challenge (Go)

Este repositório contém a implementação do serviço distribuído de carteiras e apostas. Ele provê alta consistência (ACID), isolamento de concorrência, idempotência persistente (com zero float handling), comunicação outbox/SQS para tolerância a falhas, e Autenticação OIDC via Keycloak.

## Tecnologias e Padrões Utilizados
- **Go 1.22+**, Go Modules, Uber Fx
- **PostgreSQL 16** via `pgxpool` (uso de `pgx.Tx` explícito)
- **AWS SQS** (LocalStack) 
- **Keycloak** (OIDC - `client_credentials`)
- **Docker & Docker Compose** (Ecossistema local rodando 3 instâncias)
- **Prometheus** (Métricas customizadas exportadas em `/metrics`)

## Executando o Projeto

1. Inicie o ecossistema (PostgreSQL, LocalStack, Keycloak e a aplicação):
   ```bash
   docker-compose up --build -d
   ```
   > Esse comando irá subir o banco de dados, aplicar as migrations (via o container `migrate`), instanciar filas no SQS (via `tools/sqs-setup.sh`), importar as configurações do Keycloak, e inicializar 3 réplicas concorrentes da aplicação (`wallet-app-1`, `wallet-app-2`, `wallet-app-3` nas portas 8081, 8082, 8083).

2. Acesse a rota de métricas de uma das aplicações para garantir que está de pé:
   ```bash
   curl http://localhost:8081/metrics
   ```

3. Geração do Token OIDC (Keycloak admin):
   ```bash
   curl -X POST http://localhost:8180/realms/wallet/protocol/openid-connect/token \
     -d "client_id=wallet-api" \
     -d "client_secret=secret" \
     -d "grant_type=client_credentials"
   ```

4. Executando os Testes Unitários e de Integração (simula concorrência, deadlocks, lost updates):
   ```bash
   go test ./... -v
   go test ./test/integration/... -tags=integration -v
   ```

## Variáveis de Ambiente
Renomeie ou utilize o `.env.example` local se for rodar o binário `go run cmd/wallet-service/main.go`.

Para mais detalhes sobre as decisões, consulte [ARCHITECTURE.md](ARCHITECTURE.md).
