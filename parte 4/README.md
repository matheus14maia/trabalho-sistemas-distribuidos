# Sistema de Base de Dados Replicada — Parte 4 (Líder único, menos mensagens)

Nesta parte, adotamos um modelo **primário-réplica** (líder único) para reduzir trocas de mensagens mantendo consistência. Apenas o líder recebe comandos de escrita; ele aplica localmente, atribui `seq` crescente e replica via RabbitMQ (fanout) para seguidores. Um único produtor garante ordem por fila, evitando o sequenciador adicional da Parte 3.

## Ideia principal
- Clientes enviam SET/GET/PRINT ao **líder** (gRPC/JSON).
- O líder aplica o comando, incrementa `seq` e publica no exchange `kv4_updates`.
- Seguidores consomem `kv4_updates` e aplicam somente se `seq` é o próximo esperado.
- GET/PRINT nos seguidores retornam o estado local (após replicação).

## Componentes
- `node_main.py`: inicia nó como líder ou seguidor (`ROLE`).
- `rpc/kv_service.py`: no líder, aceita SET/GET/PRINT; em seguidores, SET é rejeitado (somente leituras).
- `messaging/rabbitmq.py`: publisher (líder) e consumer (seguidores) para o exchange fanout.
- `app/kv_store.py`: estado do dicionário com controle de sequência.
- `models/messages.py`: comando com campo `seq` atribuído pelo líder.
- `client_cli.py`: cliente CLI; aponte para o líder para SET.

## Executando com Docker
Pré-requisitos: Docker e Docker Compose.

```bash
cd "parte 4"
docker compose up -d --build
```

Serviços:
- `rabbitmq` (5672; console 15672)
- `kv4_leader` (gRPC 50071)
- `kv4_follower1` (gRPC 50072)
- `kv4_follower2` (gRPC 50073)

### Usando o cliente CLI
Envie comandos via líder:
```bash
docker exec -it kv4_leader python client_cli.py --target kv4_leader:50071
# exemplos
SET x 1
SET x 2
GET x
PRINT
```
Nos seguidores (apenas leitura):
```bash
docker exec -it kv4_follower1 python client_cli.py --target kv4_follower1:50072
GET x
PRINT
```

## Vantagens x Partes anteriores
- Menos mensagens que a Parte 3: sem hop adicional do sequenciador; um único produtor ordena os updates.
- Ordem total garantida pelo líder (seq único) → réplicas convergem.
- Mais simples que consenso completo; porém há ponto único de falha (líder). Troca de líder não está automatizada.
- Menos latência que a Parte 3 (um hop a menos), mais confiável que Parte 1 (sem ordem) e Parte 2 (múltiplos publicadores sem total order).

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
├── README.md
└── requirements.txt
```

## Notas
- Variáveis: `AMQP_URL`, `UPDATES_EXCHANGE`, `RPC_PORT`, `ROLE`, `NODE_ID`, `LOG_LEVEL`.
- Para limpeza: `docker compose down -v`.

