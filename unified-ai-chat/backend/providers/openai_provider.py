"""
OpenAI provider — wraps the openai SDK and yields streamed text chunks.
"""

import os
from typing import AsyncIterator, Optional
from openai import AsyncOpenAI

DEFAULT_MODEL = "gpt-4o"
AVAILABLE_MODELS = [
    "gpt-4o",
    "gpt-4o-mini",
    "gpt-4-turbo",
    "gpt-3.5-turbo",
]


class OpenAIProvider:
    def __init__(self):
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise EnvironmentError("OPENAI_API_KEY environment variable not set")
        self.client = AsyncOpenAI(api_key=api_key)

    async def stream_chat(
        self,
        messages: list[dict],
        model: Optional[str] = None,
        system_prompt: Optional[str] = None,
    ) -> AsyncIterator[str]:
        """
        Accepts the normalized message list (role/content dicts).
        Optionally injects a system message at position 0.
        Yields text chunks as they arrive.
        """
        target_model = model or DEFAULT_MODEL
        full_messages = []

        if system_prompt:
            full_messages.append({"role": "system", "content": system_prompt})

        full_messages.extend(messages)

        stream = await self.client.chat.completions.create(
            model=target_model,
            messages=full_messages,
            stream=True,
        )

        async for chunk in stream:
            delta = chunk.choices[0].delta
            if delta and delta.content:
                yield delta.content

    async def complete_chat(
        self,
        messages: list[dict],
        model: Optional[str] = None,
        system_prompt: Optional[str] = None,
    ) -> tuple[str, str]:
        """Non-streaming version. Returns (content, model_used)."""
        target_model = model or DEFAULT_MODEL
        full_messages = []

        if system_prompt:
            full_messages.append({"role": "system", "content": system_prompt})

        full_messages.extend(messages)

        response = await self.client.chat.completions.create(
            model=target_model,
            messages=full_messages,
        )
        return response.choices[0].message.content, response.model
