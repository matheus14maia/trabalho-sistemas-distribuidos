from __future__ import annotations

import argparse
import asyncio

from rpc.json_grpc import KvRpcClient


HELP_TEXT = """Comandos:
  SET <chave> <valor>    -> envia comando SET via RPC (gRPC JSON) (somente no líder)
  GET <chave>            -> consulta valor local do nó alvo
  PRINT                  -> snapshot local do nó alvo
  HELP                   -> mostra este texto
  EXIT                   -> encerra o cliente
"""


async def repl(target: str) -> None:
    async with KvRpcClient(target) as client:
        print(HELP_TEXT, flush=True)
        while True:
            line = input("> ").strip()
            if not line:
                continue
            parts = line.split()
            cmd = parts[0].upper()

            if cmd in ("EXIT", "QUIT"):
                print("Encerrando cliente...", flush=True)
                break
            if cmd == "HELP":
                print(HELP_TEXT, flush=True)
                continue
            if cmd == "SET":
                if len(parts) < 3:
                    print("Uso: SET <chave> <valor>")
                    continue
                key = parts[1]
                value = " ".join(parts[2:])
                resp = await client.set(key, value)
                print(resp)
                continue
            if cmd == "GET":
                if len(parts) != 2:
                    print("Uso: GET <chave>")
                    continue
                key = parts[1]
                resp = await client.get(key)
                print(resp)
                continue
            if cmd == "PRINT":
                resp = await client.print_state()
                print(resp)
                continue

            print("Comando desconhecido. Digite HELP para ajuda.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Cliente CLI para o serviço RPC do KV Store (parte 4).")
    parser.add_argument(
        "--target", default="localhost:50071", help="host:porta do serviço gRPC (ex.: líder:50071)"
    )
    args = parser.parse_args()
    asyncio.run(repl(args.target))


if __name__ == "__main__":
    main()

