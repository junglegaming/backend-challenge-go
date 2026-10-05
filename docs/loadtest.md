# Teste de carga

Diferencial opcional do enunciado (§14). Este documento traz o comando, o ambiente, a
metodologia e os resultados de uma máquina de desenvolvimento. **Não é um benchmark de
produção**: gerador de carga e todos os serviços dividem a mesma máquina.

## Como reproduzir

Com a pilha de pé (`docker compose up -d`), na raiz do repositório:

```sh
go run ./cmd/loadtest -scenario many   -wallets 50 -workers 32 -duration 60s
go run ./cmd/loadtest -scenario single             -workers 32 -duration 60s
```

Cada execução imprime o resumo e grava um relatório JSON em `docs/bench/`
(`<cenário>-<timestamp>.json`) com ambiente, parâmetros e resultado.

| Flag | Padrão | Significado |
|---|---|---|
| `-scenario` | `many` | `many` espalha as operações por várias carteiras; `single` usa uma só |
| `-wallets` | 50 | carteiras no cenário `many` |
| `-workers` | 32 | clientes simultâneos, cada um em loop |
| `-duration` | 60s | duração da carga |
| `-replay-ratio` | 0,1 | fração das requisições que repete a anterior (replay idempotente) |
| `-seed` | 1 | semente: mesma sequência de carteiras sorteadas |
| `-out` | `docs/bench` | pasta do relatório |
| `-note` | vazio | texto livre gravado no relatório |

## Metodologia

- Cada cliente envia `BET` de 1,00 BRL em `POST /wagering/transactions`, com token real do
  Keycloak (renovado a cada minuto, porque vale 5) e chave de idempotência única.
- `-replay-ratio` faz parte das requisições repetir exatamente a anterior do mesmo cliente.
  Elas devem voltar como replay e não podem debitar de novo.
- Carteiras abertas antes da medição com saldo de 1.000.000,00, o bastante para nenhuma
  aposta ser rejeitada por saldo.
- A latência é medida no cliente, da escrita da requisição ao fim da leitura da resposta.
  Percentis por ordenação de todas as amostras; sem descarte de aquecimento.
- **Verificação ao final:** para cada carteira, o saldo lido pela API precisa ser
  1.000.000,00 menos o número de apostas únicas que a API respondeu `201`. Um replay não
  entra na conta. É a mesma verificação "saldo × operações confirmadas" que o enunciado pede.

## Ambiente

Windows 11, Docker Desktop. `app`, `postgres` (16), `keycloak` e `localstack` em containers,
**na mesma máquina do gerador de carga**. App com `DB_MAX_CONNS=10` (padrão). Nenhuma
otimização de Postgres além da configuração padrão da imagem.

## Resultados

| Cenário | Clientes | Duração | Requisições | RPS | p50 | p95 | p99 | máx | Erros |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `many`, 50 carteiras | 32 | 60 s | 151.406 | 2.523 | 12 ms | 17,9 ms | 23 ms | 69,8 ms | nenhum |
| `single`, 1 carteira | 32 | 60 s | 27.722 | 462 | 62,4 ms | 114,2 ms | 145,6 ms | 270,7 ms | nenhum |
| `many`, 50 carteiras | 128 | 60 s | 137.059 | 2.282 | 49,5 ms | 91,6 ms | 126,8 ms | 213,8 ms | nenhum |
| `many`, semente 7 | 32 | 30 s | 77.549 | 2.584 | 11,2 ms | 18,4 ms | 23,9 ms | 74,4 ms | nenhum |
| `many`, sem replays | 32 | 30 s | 83.791 | 2.792 | 10,9 ms | 15,5 ms | 20,2 ms | 57,7 ms | nenhum |
| `many`, instância 1 de 3 | 32 | 60 s | 99.498 | 1.658 | 19 ms | 26,4 ms | 32,2 ms | 93,4 ms | nenhum |
| `many`, instância 2 de 3 | 32 | 60 s | 99.001 | 1.650 | 19 ms | 26,2 ms | 32,2 ms | 111 ms | nenhum |
| `many`, instância 3 de 3 | 32 | 60 s | 99.590 | 1.659 | 19 ms | 26,2 ms | 32,1 ms | 93,5 ms | nenhum |

Em todas as rodadas: status `201` em 100% das respostas, **zero erros de transporte**, e a
verificação de saldo passou. Os relatórios completos estão em `docs/bench/`.

## Como ler os números

- **Carteiras independentes escalam; a mesma carteira serializa.** Com 32 clientes, 50
  carteiras entregam 2.523 RPS contra 462 RPS em uma só, 5,5 vezes menos. No `single`, o
  `SELECT ... FOR UPDATE` enfileira todas as operações: cada transação segura o lock até o
  commit. 462 RPS corresponde a cerca de 2,2 ms por operação serializada.
- **A latência acompanha a fila (lei de Little).** Com 32 clientes, a latência média é
  `32 / RPS`: 12,7 ms no `many` e 69 ms no `single`, perto dos p50 medidos (12 ms e 62 ms).
  O tempo de cada operação em si mudou pouco; o que cresce no `single` é a espera.
- **Quadruplicar os clientes não aumentou a vazão.** Com 128 clientes, o `many` caiu de
  2.523 para 2.282 RPS e o p50 subiu de 12 para 49,5 ms. O sistema já estava saturado com
  32: mais clientes só alongam a fila. O limite provável é o pool de 10 conexões e a CPU
  compartilhada entre gerador, app, Postgres e Keycloak. **Isso é uma hipótese; não medi
  qual dos dois limita.**
- **Três instâncias dobraram a vazão agregada.** As três cargas rodaram ao mesmo tempo,
  cada uma contra uma instância, e somaram cerca de 4.970 RPS contra 2.523 de uma instância
  só, com todos os saldos conferidos. Isso indica que o limite da rodada com 128 clientes
  estava **em cada instância** (CPU do app ou o pool de 10 conexões, que são 30 no total com
  três instâncias) e não no Postgres. **Não separa pool de CPU**, e as três cargas e os
  cinco containers dividiram a mesma máquina, então o ganho é uma indicação, não uma medida
  de escalabilidade.
- **Replays custam quase o mesmo que operações novas.** Sem replays, 2.792 RPS contra 2.584
  RPS com 10% de replays (rodadas curtas, sem repetição estatística): a diferença é pequena e
  não distingo ruído de efeito. Um replay ainda abre transação, reivindica a chave e lê a
  resposta gravada.

## O que não foi medido

- **Atraso da outbox:** não existe publisher, então não há atraso para medir.
- **Conflitos:** o enunciado pede contar conflitos. Aqui ficam em zero **por construção**: as
  repetições viram replay (`201`) e as demais têm chave única. Não há cenário que force
  `409`/`422`. O conflito real de concorrência (disputa 100 / 80 / 80) é coberto pelo teste
  de integração, não por este programa.
- **Variação entre rodadas:** cada configuração foi executada uma vez, exceto o `many` com
  32 clientes (três execuções, 2.523 a 2.792 RPS, com replays diferentes). Não há intervalo
  de confiança.
- **SQS sob carga.** (Várias instâncias e a morte do processo durante a carga estão no
  README, na seção "Múltiplas instâncias e simulação de falhas"; lá o RPS da rodada com falha
  não deve ser lido como vazão, porque este programa conta as tentativas que falharam em
  `requests` e nos percentis.)
- **Gerador e sistema na mesma máquina:** o gerador consome CPU que o sistema não tem. Os
  números absolutos são, portanto, pessimistas em relação a uma execução em máquinas
  separadas, e não comparáveis com outro ambiente.
