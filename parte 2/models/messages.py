from __future__ import annotations

from typing import Optional, Literal, Any
from pydantic import BaseModel, Field
import time


class CommandMessage(BaseModel):
    command_type: Literal["SET", "GET", "PRINT"]
    key: Optional[str] = None
    value: Optional[Any] = None
    sender_id: str
    message_uuid: str
    timestamp: float = Field(default_factory=lambda: time.time())

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, data: str) -> "CommandMessage":
        return cls.model_validate_json(data)

