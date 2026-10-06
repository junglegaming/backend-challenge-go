# Wallet Service (Distributed Betting System)

Serviço de processamento financeiro distribuído, desenvolvido em **Go 1.22+**, para provedores de jogos de azar e apostas esportivas, suportando alta simultaneidade sem _lost updates_ e escalando horizontalmente sem _floating point errors_ ou dados inconsistentes.

## Principais Funcionalidades
- **Concorrência Segura**: Lock pessimista no Postgres (`FOR UPDATE`).
- **Idempotência Durável**: Proteção contra replays no banco e filas SQS.
- **Precisão Financeira Total**: Representação em `int64` puro; ausência total de _Ponto Flutuante_.
- **Autenticação Segura (OIDC)**: Integração Multitenant usando Keycloak e `client_credentials`.
- **Mensageria com Outbox Pattern**: Transações salvam eventos de forma atômica para o `outbox_events`, que são processados por workers e publicados no **AWS SQS** em _at-least-once delivery_.
- **Uber Fx**: Inicialização elegante e graciosa do HTTP Server, SQS Consumer e Workers.

## Subindo o Projeto

A aplicação depende de PostgreSQL, LocalStack (SQS), e Keycloak, já mapeados no `docker-compose.yml`.

### Passo a Passo:

1. Subir os serviços de infraestrutura:
```bash
docker compose up -d
```
2. Inicializar o serviço:
```bash
go mod tidy
go run ./cmd/wallet-service
```
O serviço rodará na porta `8080`.

## Scripts Úteis

- Testes Unitários e de Concorrência:
```bash
go test -v -race ./...
```
