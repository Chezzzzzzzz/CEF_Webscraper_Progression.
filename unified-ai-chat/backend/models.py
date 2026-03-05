from pydantic import BaseModel, Field
from typing import Literal, Optional
from enum import Enum
import uuid
from datetime import datetime


class Provider(str, Enum):
    OPENAI = "openai"
    CLAUDE = "claude"


class Message(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    role: Literal["user", "assistant", "system"]
    content: str
    provider: Optional[Provider] = None  # which AI generated this (None = user)
    model: Optional[str] = None           # exact model used
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


class Conversation(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    messages: list[Message] = []
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    title: Optional[str] = None


class ChatRequest(BaseModel):
    conversation_id: Optional[str] = None  # None = start new conversation
    message: str
    provider: Provider
    model: Optional[str] = None            # specific model override
    system_prompt: Optional[str] = None    # optional system instructions
    stream: bool = True


class ChatResponse(BaseModel):
    conversation_id: str
    message: Message
    provider: Provider
    model_used: str
