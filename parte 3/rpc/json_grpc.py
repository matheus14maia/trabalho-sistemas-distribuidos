from __future__ import annotations

import json
from typing import Any, Awaitable, Callable, Dict

import grpc


Request = Dict[str, Any]
Response = Dict[str, Any]


def _serialize(obj: Response) -> bytes:
    return json.dumps(obj).encode("utf-8")


def _deserialize(data: bytes) -> Request:
    return json.loads(data.decode("utf-8"))


def build_unary_handler(method: Callable[[Request, grpc.aio.ServicerContext], Awaitable[Response]]) -> grpc.RpcMethodHandler:
    return grpc.unary_unary_rpc_method_handler(
        method,
        request_deserializer=_deserialize,
        response_serializer=_serialize,
    )


def create_grpc_server(
    service_name: str,
    methods: Dict[str, Callable[[Request, grpc.aio.ServicerContext], Awaitable[Response]]],
    port: int,
    logger,
) -> grpc.aio.Server:
    server = grpc.aio.server()
    handlers = {name: build_unary_handler(func) for name, func in methods.items()}
    generic_handler = grpc.method_handlers_generic_handler(service_name, handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_insecure_port(f"[::]:{port}")
    logger.info(f"gRPC (JSON) ouvindo em 0.0.0.0:{port} serviço={service_name}")
    return server


class KvRpcClient:
    def __init__(self, target: str) -> None:
        self._channel = grpc.aio.insecure_channel(target)
        self._set = self._channel.unary_unary(
            "/kv.KvService/Set", request_serializer=_serialize, response_deserializer=_deserialize
        )
        self._get = self._channel.unary_unary(
            "/kv.KvService/Get", request_serializer=_serialize, response_deserializer=_deserialize
        )
        self._print = self._channel.unary_unary(
            "/kv.KvService/Print", request_serializer=_serialize, response_deserializer=_deserialize
        )

    async def set(self, key: str, value: str) -> Response:
        return await self._set({"key": key, "value": value})

    async def get(self, key: str) -> Response:
        return await self._get({"key": key})

    async def print_state(self) -> Response:
        return await self._print({})

    async def close(self) -> None:
        await self._channel.close()

    async def __aenter__(self) -> "KvRpcClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        await self.close()

