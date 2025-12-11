# Sistema de Base de Dados Replicada (Key-Value Store) sobre Multicast

Este projeto implementa a Parte 1 do trabalho prático sobre sistemas distribuídos: uma aplicação de base de dados replicada (Key-Value Store) construída sobre um sistema de comunicação multicast sem protocolo de ordenação de mensagens. O objetivo é demonstrar, de forma clara e prática, que a ausência de ordenação leva a estados inconsistentes entre nós.

## Objetivo
- Cada nó mantém uma cópia completa de um dicionário (Key-Value Store).
- As mensagens trocadas no grupo multicast são comandos da aplicação (SET, GET, PRINT).
- Não há ordenação global: cada nó processa as mensagens na ordem em que as recebe, podendo divergir dos demais.

## Demonstração do Problema (Inconsistência)
Se o `node1` envia `SET x 1` e o `node2` envia `SET x 2` quase simultaneamente, então:
- Um `node3` pode receber e processar `SET x 1` antes de `SET x 2` (resultado local: `x=2`).
- Um `node4` pode receber `SET x 2` antes de `SET x 1` (resultado local: `x=1`).

Os logs mostram: ID do nó, UUID da mensagem, quem enviou e a ordem local de processamento.

## Arquitetura
- `main.py`: ponto de entrada. Inicializa logger, store, sender e receiver; roda o loop de entrada do usuário.
- `models/messages.py`: modelo Pydantic da mensagem de comando (`CommandMessage`).
- `network/sender.py`: envio multicast assíncrono.
- `network/receiver.py`: recepção multicast com `asyncio.DatagramProtocol` e `socket` configurado para grupo multicast.
- `app/kv_store.py`: lógica do dicionário replicado e contagem da ordem de processamento local.

Separação de camadas:
- Camada de rede (multicast): `network/`
- Camada de aplicação (Key-Value Store): `app/`
- Modelos (serialização/validação): `models/`

## Design e Decisões
- Mensagens validadas por Pydantic antes do processamento; mensagens inválidas são descartadas com log.
- Processamento imediato ao receber (sem ordenação). Cada operação incrementa um contador local de ordem.
- O nó envia o comando e não aplica localmente de imediato, confiando no recebimento multicast (loopback habilitado). Isso torna a ordem do próprio nó também dependente da rede.
- Logs extensivos incluem: `node`, `message_uuid`, `sender_id`, tipo de comando, parâmetros e `ordem_local`.

## Comandos Disponíveis
- `SET <chave> <valor>`: envia comando SET via multicast.
- `GET <chave>`: envia comando GET via multicast e exibe o valor local imediatamente para conveniência.
- `PRINT`: envia comando PRINT via multicast; cada nó loga seu snapshot local.
- `HELP`: ajuda.
- `EXIT`: encerra o nó.

## Executando com Docker
Pré-requisitos: Docker e Docker Compose instalados.

1) Build da imagem e subida de 3 nós:
```bash
docker compose up --build
```
Cada serviço (`node1`, `node2`, `node3`) abre um terminal interativo.

2) Teste rápido para observar inconsistência:
- No terminal do `kv_node1`:
```bash
SET x 1
```
- No terminal do `kv_node2` (logo em seguida):
```bash
SET x 2
```
- Observe os logs em `kv_node3`: a ordem local pode ser `SET x 1` depois `SET x 2` ou o inverso. O `PRINT` ajuda a ver o estado:
```bash
PRINT
```

Dica: repita o experimento várias vezes para observar diferentes ordens.

## Variáveis de Ambiente
- `MULTICAST_GROUP` (padrão: `239.0.0.1`)
- `MULTICAST_PORT` (padrão: `5007`)
- `MULTICAST_TTL` (padrão: `1`)
- `NODE_ID` (opcional; se não informado, é gerado um ID aleatório curto)

## Detalhes de Implementação
- `asyncio` para I/O não bloqueante (envio/recebimento + leitura de stdin).
- `socket` configurado com `IP_ADD_MEMBERSHIP` para ingressar no grupo multicast e `IP_MULTICAST_LOOP` para receber a própria mensagem.
- `pydantic` (v2) para validar/serializar mensagens JSON (`CommandMessage`).
- `logging` com `LoggerAdapter` injeta `node_id` no formato dos logs.

## Tratamento de Erros e Guard Clauses
- Desserialização/validação falhou → descarta a mensagem e loga `warning`.
- Campos essenciais ausentes (`command_type`, `sender_id`, `message_uuid`) → descarta e loga.
- `SET` sem `key` → descarta e loga.
- `GET` sem `key` → descarta e loga.
- A recepção usa `DatagramProtocol`; erros do transporte são logados e o processo segue.

## Observações sobre Multicast no Docker
- Usamos a rede bridge padrão do Docker; o multicast costuma funcionar entre containers no mesmo host.
- O TTL padrão é 1 (escopo local).
- Em ambientes mais restritivos, pode ser necessário ajustar configurações de rede do host ou do Docker.

## Estrutura de Pastas
```
.
├── app/
│   ├── __init__.py
│   └── kv_store.py
├── models/
│   ├── __init__.py
│   └── messages.py
├── network/
│   ├── __init__.py
│   ├── receiver.py
│   └── sender.py
├── Dockerfile
├── docker-compose.yml
├── main.py
├── requirements.txt
└── README.md
```

## Licença
Uso acadêmico/educacional.
