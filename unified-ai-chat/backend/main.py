"""
Unified AI Chat — FastAPI backend
----------------------------------
Endpoints
  POST /api/chat          Non-streaming chat (JSON response)
  POST /api/chat/stream   Streaming chat (Server-Sent Events)
  GET  /api/conversations List all conversations
  GET  /api/conversations/{id}  Full conversation history
  DELETE /api/conversations/{id}
  GET  /api/models        Available models per provider
"""

import asyncio
import json
import os
import sys
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

from conversation import store
from models import ChatRequest, ChatResponse, Message, Provider
from providers import ClaudeProvider, OpenAIProvider

app = FastAPI(title="Unified AI Chat", version="1.0.0")

# Allow the frontend (any origin in dev) to hit the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Provider singletons (lazy-init so missing keys don't crash at startup) ──

_openai: OpenAIProvider | None = None
_claude: ClaudeProvider | None = None


def get_openai() -> OpenAIProvider:
    global _openai
    if _openai is None:
        _openai = OpenAIProvider()
    return _openai


def get_claude() -> ClaudeProvider:
    global _claude
    if _claude is None:
        _claude = ClaudeProvider()
    return _claude


def get_provider(provider: Provider):
    if provider == Provider.OPENAI:
        return get_openai()
    return get_claude()


# ── REST endpoints ──────────────────────────────────────────────────────────

@app.get("/api/models")
def list_models():
    from providers.openai_provider import AVAILABLE_MODELS as OAI_MODELS, DEFAULT_MODEL as OAI_DEFAULT
    from providers.claude_provider import AVAILABLE_MODELS as ANT_MODELS, DEFAULT_MODEL as ANT_DEFAULT
    return {
        "openai": {"models": OAI_MODELS, "default": OAI_DEFAULT},
        "claude": {"models": ANT_MODELS, "default": ANT_DEFAULT},
    }


@app.get("/api/conversations")
def list_conversations():
    return store.list_conversations()


@app.get("/api/conversations/{conversation_id}")
def get_conversation(conversation_id: str):
    conv = store.get(conversation_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return conv


@app.delete("/api/conversations/{conversation_id}")
def delete_conversation(conversation_id: str):
    conv = store.get(conversation_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")
    with store._lock:
        del store._store[conversation_id]
    return {"status": "deleted"}


@app.post("/api/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    """Non-streaming endpoint — returns the full assistant reply at once."""
    conv = store.get_or_create(req.conversation_id)

    # Save user message
    user_msg = Message(role="user", content=req.message)
    store.add_message(conv.id, user_msg)

    provider = get_provider(req.provider)
    history = store.build_provider_messages(conv.id)

    content, model_used = await provider.complete_chat(
        messages=history,
        model=req.model,
        system_prompt=req.system_prompt,
    )

    # Save assistant reply
    ai_msg = Message(
        role="assistant",
        content=content,
        provider=req.provider,
        model=model_used,
    )
    store.add_message(conv.id, ai_msg)

    return ChatResponse(
        conversation_id=conv.id,
        message=ai_msg,
        provider=req.provider,
        model_used=model_used,
    )


@app.post("/api/chat/stream")
async def chat_stream(req: ChatRequest):
    """
    Streaming endpoint using Server-Sent Events (SSE).

    Event format:
      data: {"type": "meta",   "conversation_id": "...", "message_id": "...", "model": "..."}
      data: {"type": "chunk",  "content": "..."}
      data: {"type": "done"}
      data: {"type": "error",  "detail": "..."}
    """
    conv = store.get_or_create(req.conversation_id)

    # Save user message immediately so history is consistent
    user_msg = Message(role="user", content=req.message)
    store.add_message(conv.id, user_msg)

    provider = get_provider(req.provider)
    history = store.build_provider_messages(conv.id)

    from providers.openai_provider import DEFAULT_MODEL as OAI_DEFAULT
    from providers.claude_provider import DEFAULT_MODEL as ANT_DEFAULT
    default_model = OAI_DEFAULT if req.provider == Provider.OPENAI else ANT_DEFAULT
    model_used = req.model or default_model

    # Placeholder message — will be filled as chunks arrive
    ai_msg = Message(
        role="assistant",
        content="",
        provider=req.provider,
        model=model_used,
    )
    store.add_message(conv.id, ai_msg)

    async def event_generator() -> AsyncIterator[str]:
        full_content = []
        try:
            # Send metadata first so the client knows the conversation/message IDs
            meta = json.dumps({
                "type": "meta",
                "conversation_id": conv.id,
                "message_id": ai_msg.id,
                "model": model_used,
            })
            yield f"data: {meta}\n\n"

            async for chunk in provider.stream_chat(
                messages=history,
                model=req.model,
                system_prompt=req.system_prompt,
            ):
                full_content.append(chunk)
                payload = json.dumps({"type": "chunk", "content": chunk})
                yield f"data: {payload}\n\n"

            # Persist completed assistant message
            ai_msg.content = "".join(full_content)
            # Update in-place (message already in list)
            conv_obj = store.get(conv.id)
            if conv_obj:
                for m in conv_obj.messages:
                    if m.id == ai_msg.id:
                        m.content = ai_msg.content
                        break

            yield f"data: {json.dumps({'type': 'done'})}\n\n"

        except Exception as exc:
            err = json.dumps({"type": "error", "detail": str(exc)})
            yield f"data: {err}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Serve frontend static files ─────────────────────────────────────────────

frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.isdir(frontend_path):
    app.mount("/", StaticFiles(directory=frontend_path, html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
