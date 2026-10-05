# Insights de um colega — avaliados contra o README e o `scope-decision.md`

> Registrado pra não esquecer, e pra deixar por escrito **por que** cada sugestão foi
> aceita, adaptada, ou descartada — isso é parte do que o `ARCHITECTURE.md` final vai
> cobrar: "decisões... limitações, interpretações adotadas".

---

## 1. Princípio geral: POC mínima primeiro, evolui em cima

> "Faz o que é core da descrição, cria uma arquitetura mínima viável e ideia geral e
> evolui em cima disso, faz a POCzinha primeiro."

**Avaliação:** já é exatamente a ordem definida em `scope-decision.md` §5 — `Money` →
`Wallet` → débito/crédito sob concorrência → idempotência → HTTP → auth → SQS → testes →
documentação. Day 1 (já implementado) é literalmente essa POC: domínio puro, sem banco,
sem Fx, sem HTTP. **Confirma o plano, não muda nada.**

---

## 2. Nomear pontos de falha conscientes, não fingir que não existem

> "Não tem como fazer algo perfeito a falhas... vão te perguntar."

Concordo, e isso é uma exigência explícita do próprio README (§15: "limitações,
interpretações adotadas e trabalho não concluído"). Dois pontos de falha concretos pra
já deixar nomeados no `ARCHITECTURE.md` final:

### 2.1. PostgreSQL como ponto único de falha

Não há réplica, não há failover automático, não há multi-AZ. Se o Postgres cair, a
aplicação cai com ele — o `/readyz` (se implementarmos, nos moldes do wallet-go) reporta
isso honestamente em vez de mascarar, mas não existe recuperação automática.

**O que seria necessário em produção real:** replicação em streaming + failover
automatizado (ex: Patroni, Postgres com `repmgr`) ou um serviço gerenciado com HA (RDS
Multi-AZ, Aurora). **Fora do escopo de 3 dias** — documentar como limitação conhecida,
não tentar simular.

### 2.2. "Consenso" entre múltiplas instâncias da aplicação

Cobrir todos os casos de disputa entre instâncias com um protocolo de consenso formal
(Raft/Paxos) em 3 dias não é viável, e — mais importante — **não é necessário pra esse
desafio**. Ver seção 3.1.

---

## 3. As três sugestões técnicas do colega, avaliadas uma a uma

### 3.1. "Consenso distribuído pra chegar num veredito" — **não vamos implementar**

A citação completa:

> "vai ter que fazer algo como um consenso distribuído, pra chegar num veredito (tem no
> matching engine do meu perfil). Mas talvez deve ter algo até mais simples"

**Por que não se aplica aqui:** consenso distribuído (Raft/Paxos, ou qualquer protocolo
caseiro) resolve o problema de **múltiplas fontes de verdade independentes** precisando
concordar entre si — por exemplo, várias réplicas de escrita decidindo sozinhas e
sincronizando depois, ou um exchange com múltiplos matching engines paralelos (daí a
referência ao projeto dele).

Não é esse o desenho aqui. O desafio (§8) tem **um único Postgres**, compartilhado por N
processos de aplicação sem estado próprio. Nesse desenho, "o veredito" já está resolvido
pelo próprio banco: `SELECT ... FOR UPDATE` na linha da carteira serializa o acesso, e
quem comita primeiro "venceu" — não existe ambiguidade a resolver por consenso de
aplicação, porque não existem duas fontes de verdade concorrentes, existe uma só.

**Decisão:** o Postgres único é a autoridade. A "resolução de disputa" é delegada ao lock
de linha do banco (mais o `version` como checagem extra), não a um protocolo de consenso
em nível de aplicação. Isso vira uma frase pronta pro `ARCHITECTURE.md` e pra defender na
entrevista: *"não precisei de consenso distribuído porque o desenho tem uma única fonte
de verdade; teria sido necessário só se houvesse múltiplos bancos decidindo de forma
independente."*

**Guardar para o futuro:** se um dia esse sistema precisasse de múltiplas regiões/
bancos escrevendo de forma independente (ex: latência geográfica), aí sim a ideia do
colega entraria em jogo. Não é o caso deste desafio.

### 3.2. RabbitMQ em vez de SQS — **não vamos trocar, é requisito obrigatório**

> "at_least_once com um dB aí tem que ter uma mensageria antes, recomendo rabbitmq pq é
> fácil."

**Por que não dá pra seguir:** o README é explícito e obrigatório — "Mensageria: AWS SQS,
executado localmente com LocalStack ou MiniStack" (§4, tabela de tecnologias
obrigatórias). Trocar por RabbitMQ descumpriria um requisito obrigatório explícito, por
mais que RabbitMQ também resolva at-least-once de forma válida em abstrato. Isso pesaria
contra a entrega, não a favor, mesmo sendo "mais fácil".

**Decisão:** SQS via LocalStack, sem alternativa. Boa notícia: não precisa reinventar —
LocalStack sobe no `docker-compose.yml` igual a qualquer outro serviço, e o SDK oficial
(`github.com/aws/aws-sdk-go-v2/service/sqs`) fala com ele normalmente, só mudando o
endpoint. Não é mais difícil que RabbitMQ, só menos familiar.

### 3.3. Idempotência via Postgres central vs Redis — **confirma Postgres, com motivo extra**

> "idempotencia dá pra usar um postgres central, mais fácil, mas pessoal costuma usar
> redis pra isso."

**Avaliação:** a opção mais simples (Postgres) já era a decisão do `scope-decision.md`,
reaproveitando o padrão já validado no wallet-go (claim/execute/complete na mesma
transação SQL). Mas tem um motivo além de "mais simples" que vale registrar: o próprio
README **exige** que a idempotência "seja persistente e sobreviva ao reinício de todos os
processos" (§5.2) e que, na entrada por SQS, o registro de idempotência "compartilhe a
transação SQL das alterações de domínio" (§6.5) — ou seja, o requisito já pede
**atomicidade transacional** entre o registro de idempotência e a mutação de saldo.

Isso só é natural se os dois estiverem no mesmo banco, na mesma transação. Usar Redis
exigiria coordenar uma transação distribuída (ou algum padrão de compensação) entre Redis
e Postgres — o que é **mais complexo**, não menos, justamente por causa desse requisito
específico. É o oposto do que normalmente se assume sobre "Redis é mais simples/rápido
pra idempotência" — aqui o requisito de atomicidade inverte essa vantagem.

**Decisão:** Postgres central, confirmado — e agora com uma justificativa mais forte que
"é mais fácil". Redis fica anotado como possível diferencial futuro (cache de leitura de
saldo, por exemplo), nunca como fonte de verdade da idempotência.

---

## 4. Resumo das decisões desta rodada

| Sugestão | Decisão | Motivo principal |
|---|---|---|
| POC mínima primeiro | **Confirma** o plano já em execução | Já é a ordem do `scope-decision.md` |
| Nomear pontos de falha (banco, consenso) | **Aceita** | Exigência explícita do §15 do README |
| Consenso distribuído pra veredito | **Rejeitada** | Não há múltiplas fontes de verdade — um Postgres só já resolve |
| RabbitMQ em vez de SQS | **Rejeitada** | SQS/LocalStack é requisito obrigatório (§4) |
| Idempotência em Postgres (vs Redis) | **Confirma**, com motivo mais forte | Atomicidade transacional exigida pelo §5.2/§6.5 favorece um único banco |
