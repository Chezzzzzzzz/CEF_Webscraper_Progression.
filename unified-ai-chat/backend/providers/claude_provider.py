"""
Anthropic Claude provider — wraps the anthropic SDK and yields streamed chunks.
"""

import os
from typing import AsyncIterator, Optional
import anthropic

DEFAULT_MODEL = "claude-sonnet-4-6"
AVAILABLE_MODELS = [
    "claude-opus-4-6",
    "claude-sonnet-4-6",
    "claude-haiku-4-5-20251001",
]


class ClaudeProvider:
    def __init__(self):
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise EnvironmentError("ANTHROPIC_API_KEY environment variable not set")
        self.client = anthropic.AsyncAnthropic(api_key=api_key)

    async def stream_chat(
        self,
        messages: list[dict],
        model: Optional[str] = None,
        system_prompt: Optional[str] = None,
    ) -> AsyncIterator[str]:
        """
        Accepts the same normalized message list used by OpenAI.
        Claude requires the first message to be from 'user', so we
        filter out any leading assistant turns just in case.
        System prompt is passed via the top-level `system` parameter.
        Yields text chunks as they arrive.
        """
        target_model = model or DEFAULT_MODEL

        # Claude requires messages to start with a user turn
        filtered = _ensure_starts_with_user(messages)

        kwargs = dict(
            model=target_model,
            max_tokens=4096,
            messages=filtered,
            stream=True,
        )
        if system_prompt:
            kwargs["system"] = system_prompt

        async with self.client.messages.stream(**kwargs) as stream:
            async for text in stream.text_stream:
                yield text

    async def complete_chat(
        self,
        messages: list[dict],
        model: Optional[str] = None,
        system_prompt: Optional[str] = None,
    ) -> tuple[str, str]:
        """Non-streaming version. Returns (content, model_used)."""
        target_model = model or DEFAULT_MODEL
        filtered = _ensure_starts_with_user(messages)

        kwargs = dict(
            model=target_model,
            max_tokens=4096,
            messages=filtered,
        )
        if system_prompt:
            kwargs["system"] = system_prompt

        response = await self.client.messages.create(**kwargs)
        return response.content[0].text, response.model


def _ensure_starts_with_user(messages: list[dict]) -> list[dict]:
    """
    Drop leading assistant messages — Claude rejects histories that
    don't begin with a user turn. Also merge consecutive same-role
    messages (Claude requires alternating roles).
    """
    # Strip leading assistant turns
    start = 0
    for i, m in enumerate(messages):
        if m["role"] == "user":
            start = i
            break

    trimmed = messages[start:]

    # Merge consecutive same-role turns into a single message
    merged: list[dict] = []
    for msg in trimmed:
        if merged and merged[-1]["role"] == msg["role"]:
            merged[-1] = {
                "role": msg["role"],
                "content": merged[-1]["content"] + "\n\n" + msg["content"],
            }
        else:
            merged.append({"role": msg["role"], "content": msg["content"]})

    return merged
