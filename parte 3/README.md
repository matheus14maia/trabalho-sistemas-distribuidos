# Sistema de Base de Dados Replicada — Parte 3 (Ordenação Total)

Nesta parte, adicionamos um sequenciador para garantir **ordenação total** dos comandos antes de aplicá-los nas réplicas. O objetivo é eliminar divergências entre nós observadas na Parte 1 (sem ordem) e reduzir os casos de desalinhamento ainda possíveis na Parte 2 (ordem por chegada ao broker, mas não global por múltiplos publicadores).

## Ideia principal
- Todos os nós publicam comandos brutos em uma fila `kv3_to_sequencer`.
- O **sequenciador** consome essa fila, atribui um número de sequência global (`seq`) crescente e republica em um *exchange* `fanout` chamado `kv3_ordered`.
- Cada nó consome `kv3_ordered` e **aplica somente na ordem** (`seq` crescente), bufferizando mensagens fora de ordem até que o próximo `seq` esperado chegue.

Assim, todas as réplicas aplicam a mesma sequência de atualizações e convergem para o mesmo estado.

## Componentes
- `sequencer.py`: consome `kv3_to_sequencer`, gera `seq` e publica no `kv3_ordered`.
- `node_main.py`: nó que expõe RPC (gRPC/JSON), publica comandos brutos para o sequenciador e consome comandos já ordenados, aplicando na ordem.
- `messaging/rabbitmq.py`: publishers/consumers para fila do sequenciador e exchange ordenado.
- `rpc/json_grpc.py`, `rpc/kv_service.py`: RPC sem geração de código, usando JSON no gRPC.
- `app/kv_store.py`: estado do dicionário replicado.
- `client_cli.py`: cliente de linha de comando para enviar SET/GET/PRINT via RPC.

## Executando com Docker
Pré-requisitos: Docker e Docker Compose.

```bash
cd "parte 3"
docker compose up -d --build
```

Serviços:
- `rabbitmq` (porta 5672; console em 15672).
- `sequencer` (atribui seq e publica ordenado).
- `kv3_node1` (gRPC 50081), `kv3_node2` (50082), `kv3_node3` (50083).

### Usando o cliente CLI
Envie comandos via RPC para qualquer nó:
```bash
docker exec -it kv3_node1 python client_cli.py --target kv3_node1:50081
# exemplos dentro do CLI
SET x 1
SET x 2
GET x
PRINT
```
Repita em `kv3_node2` ou `kv3_node3` mudando a porta.

## Como a ordenação total garante consistência
1. Todos os comandos passam pelo sequenciador, que define `seq` global.
2. Consumidores aplicam somente o `seq` esperado; se receberem `seq` futuro, guardam em buffer.
3. Não há aplicação fora de ordem; assim, todas as réplicas percorrem a mesma sequência e convergem.

## Vantagens e desvantagens em relação às Partes 1 e 2
- **Consistência:** Parte 3 garante ordem total → estados alinhados. Parte 1 não tinha ordem; Parte 2 tinha entrega confiável, mas não ordem global entre múltiplos publicadores.
- **Simplicidade vs. Infra:** Parte 3 adiciona um sequenciador (possível gargalo/ponto único de falha). Parte 2 só exigia broker; Parte 1 não tinha broker.
- **Latência:** Parte 3 adiciona um salto extra (sequenciador) em relação à Parte 2; Parte 1 era a mais enxuta.
- **Observabilidade:** `seq` global facilita auditoria e replay. Não disponível na Parte 1; na Parte 2, a ordem era por chegada no broker, não total.

## Estrutura de pastas
```
.
├── app/
│   └── kv_store.py
├── messaging/
│   └── rabbitmq.py
├── models/
│   └── messages.py
├── rpc/
│   ├── json_grpc.py
│   └── kv_service.py
├── client_cli.py
├── docker-compose.yml
├── Dockerfile
├── node_main.py
├── sequencer.py
├── README.md
└── requirements.txt
```

## Notas de uso
- Variáveis: `AMQP_URL`, `TO_SEQ_QUEUE`, `ORDERED_EXCHANGE`, `RPC_PORT`, `NODE_ID`, `LOG_LEVEL`.
- O cliente RPC recebe feedback local, mas a consistência é garantida pela aplicação ordenada vinda do sequenciador.
- Para parar: `docker compose down -v` (remove filas/volumes do broker).

