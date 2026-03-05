"""
In-memory conversation store with a normalized message format.
Both OpenAI and Claude use role/content pairs, so the same history
can be fed to either provider without transformation.
"""

from models import Conversation, Message
from typing import Optional
import threading


class ConversationStore:
    """Thread-safe in-memory store. Swap out for Redis/DB in production."""

    def __init__(self):
        self._store: dict[str, Conversation] = {}
        self._lock = threading.Lock()

    def create(self) -> Conversation:
        conv = Conversation()
        with self._lock:
            self._store[conv.id] = conv
        return conv

    def get(self, conversation_id: str) -> Optional[Conversation]:
        with self._lock:
            return self._store.get(conversation_id)

    def get_or_create(self, conversation_id: Optional[str]) -> Conversation:
        if conversation_id:
            conv = self.get(conversation_id)
            if conv:
                return conv
        return self.create()

    def add_message(self, conversation_id: str, message: Message) -> None:
        with self._lock:
            conv = self._store.get(conversation_id)
            if conv:
                conv.messages.append(message)
                # Auto-title from first user message (first 60 chars)
                if conv.title is None and message.role == "user":
                    conv.title = message.content[:60] + ("…" if len(message.content) > 60 else "")

    def list_conversations(self) -> list[dict]:
        with self._lock:
            return [
                {"id": c.id, "title": c.title or "New conversation", "created_at": c.created_at}
                for c in sorted(self._store.values(), key=lambda x: x.created_at, reverse=True)
            ]

    def build_provider_messages(
        self, conversation_id: str, system_prompt: Optional[str] = None
    ) -> list[dict]:
        """
        Return the conversation as a plain list of {role, content} dicts —
        the format accepted by both OpenAI and Anthropic.
        System prompt is prepended only for OpenAI (Claude handles it separately).
        """
        conv = self.get(conversation_id)
        if not conv:
            return []

        messages = [
            {"role": m.role, "content": m.content}
            for m in conv.messages
            if m.role in ("user", "assistant")
        ]
        return messages


# Module-level singleton
store = ConversationStore()
