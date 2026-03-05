# Unified AI Chat

A single-thread chat interface that lets you switch between **OpenAI (ChatGPT)**
and **Anthropic (Claude)** mid-conversation — full history preserved at all times.

---

## Architecture

```
unified-ai-chat/
├── backend/
│   ├── main.py                  # FastAPI app — REST + SSE streaming endpoints
│   ├── models.py                # Pydantic data models
│   ├── conversation.py          # Thread-safe in-memory conversation store
│   ├── providers/
│   │   ├── openai_provider.py   # OpenAI async wrapper (streaming + non-streaming)
│   │   └── claude_provider.py   # Anthropic async wrapper (streaming + non-streaming)
│   └── requirements.txt
├── frontend/
│   ├── index.html               # Single-page UI
│   ├── style.css                # Dark-theme styles
│   └── app.js                   # Vanilla JS — SSE consumer, state, DOM
└── .env.example
```

### Key design decisions

| Concern | Approach |
|---|---|
| **Unified history** | Normalized `{role, content}` list — identical format for both APIs |
| **Provider switch** | Just pass the same history to the new provider — no transforms needed |
| **Claude quirks** | Auto-merge consecutive same-role turns; strip leading assistant messages |
| **Streaming** | Server-Sent Events (SSE) — works without WebSockets, easy to proxy |
| **State** | In-memory dict (dev). Swap `ConversationStore` for Redis/Postgres in prod |

---

## Quick start

### 1. Clone & enter directory
```bash
cd unified-ai-chat
```

### 2. Set API keys
```bash
cp .env.example .env
# Edit .env and add your OPENAI_API_KEY and ANTHROPIC_API_KEY
```

### 3. Install dependencies
```bash
pip install -r backend/requirements.txt
```

### 4. Run the server
```bash
cd backend
python -m dotenv run -- python main.py
# or: uvicorn main:app --reload --port 8000
```

### 5. Open the app
Visit [http://localhost:8000](http://localhost:8000) — the backend serves the frontend automatically.

---

## API reference

| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/api/models` | List available models per provider |
| `GET`  | `/api/conversations` | All conversations (id + title) |
| `GET`  | `/api/conversations/{id}` | Full message history |
| `DELETE` | `/api/conversations/{id}` | Delete a conversation |
| `POST` | `/api/chat` | Non-streaming single reply |
| `POST` | `/api/chat/stream` | **SSE streaming reply** |

### POST `/api/chat/stream` — request body
```json
{
  "conversation_id": "optional-uuid",
  "message": "Explain async generators in Python",
  "provider": "openai",
  "model": "gpt-4o",
  "system_prompt": "You are a concise assistant.",
  "stream": true
}
```

### SSE event types
```
data: {"type":"meta",  "conversation_id":"...", "message_id":"...", "model":"gpt-4o"}
data: {"type":"chunk", "content":"..."}
data: {"type":"done"}
data: {"type":"error", "detail":"..."}
```

---

## Tech stack

- **Backend**: Python 3.11+, FastAPI, Uvicorn, openai SDK, anthropic SDK
- **Frontend**: Vanilla HTML/CSS/JS (no build step, no framework dependency)
- **Streaming**: Server-Sent Events (EventSource compatible)

---

## Production notes

- Replace `ConversationStore` (in-memory) with Redis or a database
- Add authentication (JWT / API key header)
- Set `allow_origins` in CORS middleware to your actual domain
- Deploy the backend with `gunicorn -k uvicorn.workers.UvicornWorker main:app`
