# Sistema de Base de Dados Replicada — Parte 2 (RPC + Fila de Mensagens)

Esta parte substitui o multicast por uma combinação de RPC (gRPC com payload JSON) e fila de mensagens (RabbitMQ) para replicar os comandos do Key-Value Store entre os nós.

## O que mudou em relação à Parte 1
- **Comunicação entre nós**: sai o socket UDP multicast; entra um *exchange* `fanout` no RabbitMQ. Cada nó publica comandos e consome a mesma sequência, garantindo entrega a todos.
- **Interface de cliente**: comandos chegam via RPC (gRPC/JSON). O CLI (`client_cli.py`) atua como cliente RPC.
- **Consistência**: como todos os nós recebem o mesmo fluxo vindo do broker, a ordem tende a ser uniforme, reduzindo divergências observadas na Parte 1. Ainda não há consenso/ordenação forte entre publicadores diferentes, mas o broker oferece entrega confiável e reenvio em caso de queda do consumidor.

## Arquitetura
- `main.py`: inicia logger, conexões RabbitMQ (publisher/consumer) e servidor gRPC (JSON). Cada nó consome sua fila exclusiva ligada ao *exchange* `fanout`.
- `messaging/rabbitmq.py`: abstrações para publicar comandos e consumir a fila.
- `rpc/json_grpc.py`: utilitários para gRPC sem geração de código (serialização JSON).
- `rpc/kv_service.py`: implementação do serviço RPC; valida entrada, publica comandos na fila e responde ao cliente.
- `app/kv_store.py`: dicionário replicado e contagem de ordem local.
- `models/messages.py`: modelo Pydantic dos comandos.

Fluxo de um comando:
1. Cliente chama RPC `Set`/`Get`/`Print` em qualquer nó.
2. Nó publica `CommandMessage` no *exchange* `fanout`.
3. RabbitMQ entrega a mensagem às filas de todos os nós.
4. Cada consumidor aplica o comando ao seu `KeyValueStore` e registra log (ordem local).

## Executando com Docker
Pré-requisitos: Docker e Docker Compose.

```bash
cd "parte 2"
docker compose up -d --build
```

Serviços:
- `rabbitmq` (porta 5672; console em 15672).
- `kv2_node1` (gRPC em 50051), `kv2_node2` (50052), `kv2_node3` (50053).

### Usando o cliente CLI
Em outro terminal, envie comandos via RPC para qualquer nó:
```bash
docker exec -it kv2_node1 python client_cli.py --target kv2_node1:50051
# exemplos dentro do CLI
SET x 1
SET x 2
GET x
PRINT
```
Você pode repetir em `kv2_node2` ou `kv2_node3` mudando a porta.

## Vantagens e desvantagens (Parte 1 x Parte 2)
- **Confiabilidade**: RabbitMQ garante entrega/reenfileiramento; UDP multicast podia perder pacotes. (**vantagem da Parte 2**)
- **Ordem**: Parte 1 não tinha ordenação, produzindo divergência explícita. Parte 2 recebe a mesma sequência distribuída pelo broker, tendendo a estados alinhados (não há consenso forte, mas há uma ordenação por chegada no broker). (**vantagem da Parte 2 para consistência; Parte 1 expõe desordem**)
- **Complexidade operacional**: Parte 2 precisa de broker e servidor RPC (mais serviços, mais dependências). Parte 1 era leve (só processos com sockets). (**vantagem da Parte 1**)
- **Acoplamento do cliente**: RPC oferece interface clara e tipada; UDP exigia formatar JSON manualmente. (**vantagem da Parte 2**)
- **Escalabilidade**: broker facilita fanout e monitoração, mas pode ser gargalo/único ponto de falha; multicast era simples porém dependia da rede aceitar tráfego multicast. (**trade-off**)

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
├── main.py
├── README.md
└── requirements.txt
```

## Notas de uso
- Variáveis importantes: `AMQP_URL`, `EXCHANGE_NAME`, `RPC_PORT`, `NODE_ID`, `LOG_LEVEL`.
- O cliente RPC retorna também o valor local (em `GET`) e snapshot local (em `PRINT`) como feedback imediato; a mutação real acontece ao consumir a fila.
- Parar os serviços: `docker compose down -v` (remove filas/volumes do broker).

