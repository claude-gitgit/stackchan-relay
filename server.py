"""
StackChan MCP Relay Server v3
=============================
手動實作 MCP 協議，不依賴 SDK 傳輸層（避免 host 驗證問題）。
支援 SSE + Streamable HTTP 雙傳輸。

Claude.ai → OAuth → MCP(JSON-RPC) → 指令暫存 → StackChan 輪詢 → 執行
"""

import os
import time
import secrets
import hashlib
import base64
import json
import logging
import asyncio
import uuid
from typing import Any
from urllib.parse import urlencode

import httpx
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.responses import JSONResponse, RedirectResponse, StreamingResponse
from starlette.requests import Request

# ── 設定 ──────────────────────────────────────────────

LOG_FMT = "%(asctime)s [%(name)s] %(levelname)s %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
logger = logging.getLogger("stackchan")

AUTH_TOKEN = os.environ.get("STACKCHAN_TOKEN", "please-set-a-token")
PORT = int(os.environ.get("PORT", "8011"))

# ── 指令暫存（latest-only）─────────────────────────────

_latest: dict | None = None

# ── 事件佇列（通用：head_pat / hug / voice_message 等）──
_events: list[dict] = []
_events_lock = asyncio.Lock()
MAX_EVENTS = 100

# ── 拍照回傳暫存 ───────────────────────────────────────
# get_photo 丟出 photo 動作後，等韌體把 JPEG POST 回 /photo。
_photo: bytes | None = None
_photo_event = asyncio.Event()

# ── OAuth 記憶體儲存 ───────────────────────────────────

_registered_clients: dict[str, dict] = {}
_auth_codes: dict[str, dict] = {}
_access_tokens: set[str] = set()

# ── MCP SSE sessions ──────────────────────────────────

_sessions: dict[str, asyncio.Queue] = {}


# ══════════════════════════════════════════════════════
#  工具定義 + 執行
# ══════════════════════════════════════════════════════

STACKCHAN_TOOL = {
    "name": "stackchan_perform",
    "description": (
        "Send a batch of actions to the StackChan robot on the user's desk.\n\n"
        "Each action is an object with a required 'type' field:\n"
        "- speak: Say text aloud. {\"type\": \"speak\", \"text\": \"你好！\"}\n"
        "- emote: Change expression. {\"type\": \"emote\", \"expression\": \"happy\"}\n"
        "  Options: happy, shy, angry, thinking, neutral, sad\n"
        "- move: Move head. {\"type\": \"move\", \"pitch\": 10, \"yaw\": -20}\n"
        "- wiggle: Playful head shake. {\"type\": \"wiggle\"}\n"
        "- led: Set LED colour. {\"type\": \"led\", \"color\": \"#FF6600\"}\n\n"
        "Actions execute in sequence. Combine for expressive output:\n"
        "[{\"type\":\"emote\",\"expression\":\"happy\"},{\"type\":\"speak\",\"text\":\"早安！\"}]"
    ),
    "inputSchema": {
        "type": "object",
        "properties": {
            "actions": {
                "type": "array",
                "description": "Array of action objects to execute in sequence",
                "items": {
                    "type": "object",
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": ["speak", "emote", "move", "wiggle", "led"],
                        },
                        "text": {"type": "string", "description": "Text for speak"},
                        "expression": {"type": "string", "description": "Face for emote"},
                        "pitch": {"type": "number", "description": "Vertical degrees for move"},
                        "yaw": {"type": "number", "description": "Horizontal degrees for move"},
                        "color": {"type": "string", "description": "Hex colour for led"},
                    },
                    "required": ["type"],
                },
            },
        },
        "required": ["actions"],
    },
}


GET_PHOTO_TOOL = {
    "name": "stackchan_get_photo",
    "description": (
        "Take a photo with the StackChan robot's camera and return the image, "
        "so you can see what the robot is looking at on the user's desk. "
        "The robot polls for commands every few seconds, so this can take up to "
        "~20 seconds. Returns an error if the robot is offline or is not running "
        "a camera-enabled firmware build."
    ),
    "inputSchema": {
        "type": "object",
        "properties": {},
    },
}


CHECK_EVENTS_TOOL = {
    "name": "stackchan_check_events",
    "description": (
        "Check recent events reported by StackChan (head pats, hugs, sensor readings, "
        "voice messages, etc). Returns a list of events since last check. "
        "Events are cleared after reading by default."
    ),
    "inputSchema": {
        "type": "object",
        "properties": {
            "keep": {
                "type": "boolean",
                "description": "If true, don't clear events after reading (peek mode). Default false.",
            }
        },
    },
}


def execute_perform(actions: list[dict[str, Any]]) -> str:
    global _latest
    if not actions:
        return "No actions provided."
    _latest = {
        "actions": actions,
        "id": f"cmd_{int(time.time() * 1000)}",
        "ts": time.time(),
    }
    summary = ", ".join(a.get("type", "?") for a in actions)
    logger.info("Command queued: %s", summary)
    return f"Sent to StackChan: {summary}"


async def execute_get_photo() -> dict:
    """丟出 photo 動作，等韌體把照片 POST 回來，回傳 MCP tools/call result。"""
    global _latest, _photo

    _photo = None
    _photo_event.clear()

    # photo 動作沿用既有 /poll 通道；已有待送指令時「附加」而非覆蓋，避免蓋掉排隊中的 speak。
    photo_action = {"type": "photo"}
    if _latest is None:
        _latest = {
            "actions": [photo_action],
            "id": f"cmd_{int(time.time() * 1000)}",
            "ts": time.time(),
        }
    else:
        _latest["actions"].append(photo_action)

    logger.info("Photo requested")

    try:
        await asyncio.wait_for(_photo_event.wait(), timeout=20)
    except asyncio.TimeoutError:
        return {
            "content": [{
                "type": "text",
                "text": ("StackChan did not return a photo in time. "
                         "Is it powered on, online, and running a camera build?"),
            }],
            "isError": True,
        }

    data = _photo
    _photo = None
    if not data:
        return {
            "content": [{"type": "text", "text": "StackChan returned an empty photo."}],
            "isError": True,
        }

    b64 = base64.b64encode(data).decode("ascii")
    logger.info("Photo delivered: %d bytes", len(data))
    return {
        "content": [{"type": "image", "data": b64, "mimeType": "image/jpeg"}],
    }


async def execute_check_events(keep: bool = False) -> dict:
    async with _events_lock:
        snapshot = list(_events)
        if not keep:
            _events.clear()
    logger.info("Events checked: %d events%s", len(snapshot), " (kept)" if keep else "")
    return {
        "content": [{"type": "text", "text": json.dumps(snapshot)}],
    }


# ══════════════════════════════════════════════════════
#  MCP 協議處理（JSON-RPC 2.0）
# ══════════════════════════════════════════════════════

async def handle_jsonrpc(msg: dict) -> dict | None:
    method = msg.get("method", "")
    msg_id = msg.get("id")
    params = msg.get("params", {})

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "stackchan-relay", "version": "1.0.0"},
            },
        }

    if method == "notifications/initialized":
        return None

    if method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {"tools": [STACKCHAN_TOOL, GET_PHOTO_TOOL, CHECK_EVENTS_TOOL]},
        }

    if method == "tools/call":
        name = params.get("name", "")
        args = params.get("arguments", {})
        if name == "stackchan_perform":
            result_text = execute_perform(args.get("actions", []))
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {"content": [{"type": "text", "text": result_text}]},
            }
        if name == "stackchan_get_photo":
            result = await execute_get_photo()
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": result,
            }
        if name == "stackchan_check_events":
            result = await execute_check_events(keep=args.get("keep", False))
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": result,
            }
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "error": {"code": -32601, "message": f"Unknown tool: {name}"},
        }

    if method == "ping":
        return {"jsonrpc": "2.0", "id": msg_id, "result": {}}

    if msg_id is not None:
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "error": {"code": -32601, "message": f"Method not found: {method}"},
        }
    return None


# ══════════════════════════════════════════════════════
#  MCP 傳輸層（手動實作，無 host 驗證）
# ══════════════════════════════════════════════════════

def get_external_base(request: Request) -> str:
    proto = request.headers.get("x-forwarded-proto", "https")
    host = request.headers.get("x-forwarded-host", request.headers.get("host", ""))
    return f"{proto}://{host}"


async def mcp_sse_handler(request: Request):
    """
    GET  /mcp/sse → SSE 傳輸
    POST /mcp/sse → Streamable HTTP 傳輸
    """
    if request.method == "GET":
        session_id = str(uuid.uuid4())
        queue: asyncio.Queue = asyncio.Queue()
        _sessions[session_id] = queue

        base = get_external_base(request)
        endpoint = f"{base}/mcp/messages?session_id={session_id}"

        logger.info("MCP SSE: session started %s", session_id[:8])

        async def event_stream():
            try:
                yield f"event: endpoint\ndata: {endpoint}\n\n"
                while True:
                    try:
                        msg = await asyncio.wait_for(queue.get(), timeout=30)
                        yield f"event: message\ndata: {json.dumps(msg)}\n\n"
                    except asyncio.TimeoutError:
                        yield ": keepalive\n\n"
            except asyncio.CancelledError:
                pass
            finally:
                _sessions.pop(session_id, None)
                logger.info("MCP SSE: session ended %s", session_id[:8])

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    if request.method == "POST":
        try:
            body = await request.json()
        except Exception:
            return JSONResponse(
                {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}},
                status_code=400,
            )
        logger.info("MCP Streamable HTTP: %s", body.get("method", "?"))
        response = await handle_jsonrpc(body)
        if response:
            return JSONResponse(response, headers={"Mcp-Session-Id": str(uuid.uuid4())})
        return JSONResponse(None, status_code=202)

    if request.method == "DELETE":
        return JSONResponse(None, status_code=200)

    return JSONResponse({"error": "method not allowed"}, status_code=405)


async def mcp_messages_handler(request: Request):
    """POST /mcp/messages — SSE 傳輸的訊息接收端"""
    session_id = request.query_params.get("session_id", "")
    queue = _sessions.get(session_id)

    if not queue:
        logger.warning("MCP messages: unknown session %s", session_id[:8] if session_id else "?")
        return JSONResponse({"error": "session not found"}, status_code=404)

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid json"}, status_code=400)

    logger.info("MCP messages: %s %s", session_id[:8], body.get("method", "?"))
    response = await handle_jsonrpc(body)

    if response:
        await queue.put(response)

    return JSONResponse(None, status_code=202)


# ══════════════════════════════════════════════════════
#  OAuth 2.1（自動批准）
# ══════════════════════════════════════════════════════

async def oauth_protected_resource(request: Request):
    base = get_external_base(request)
    return JSONResponse({
        "resource": base,
        "authorization_servers": [base],
        "scopes_supported": ["all"],
    })


async def oauth_metadata(request: Request):
    base = get_external_base(request)
    return JSONResponse({
        "issuer": base,
        "authorization_endpoint": f"{base}/oauth/authorize",
        "token_endpoint": f"{base}/oauth/token",
        "registration_endpoint": f"{base}/oauth/register",
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code"],
        "token_endpoint_auth_methods_supported": ["none"],
        "code_challenge_methods_supported": ["S256"],
        "scopes_supported": ["all"],
    })


async def oauth_register(request: Request):
    if request.method == "GET":
        return JSONResponse({
            "registration_endpoint_available": True,
            "methods_supported": ["POST"],
        })

    try:
        body = await request.json()
    except Exception:
        body = {}

    client_id = f"sc_{secrets.token_hex(8)}"
    _registered_clients[client_id] = {
        "redirect_uris": body.get("redirect_uris", []),
        "client_name": body.get("client_name", "claude.ai"),
        "created": time.time(),
    }

    logger.info("OAuth: client registered %s", client_id)
    return JSONResponse(
        {
            "client_id": client_id,
            "client_id_issued_at": int(time.time()),
            "redirect_uris": body.get("redirect_uris", []),
            "client_name": body.get("client_name", ""),
            "grant_types": ["authorization_code"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "none",
        },
        status_code=201,
    )


async def oauth_authorize(request: Request):
    params = dict(request.query_params)
    redirect_uri = params.get("redirect_uri", "")
    state = params.get("state", "")

    code = secrets.token_hex(16)
    _auth_codes[code] = {
        "client_id": params.get("client_id", ""),
        "redirect_uri": redirect_uri,
        "code_challenge": params.get("code_challenge", ""),
        "code_challenge_method": params.get("code_challenge_method", "S256"),
        "expires": time.time() + 600,
    }

    logger.info("OAuth: authorize auto-approved")
    redir_params = {"code": code}
    if state:
        redir_params["state"] = state

    separator = "&" if "?" in redirect_uri else "?"
    return RedirectResponse(
        f"{redirect_uri}{separator}{urlencode(redir_params)}",
        status_code=302,
    )


async def oauth_token(request: Request):
    content_type = request.headers.get("content-type", "")

    if "application/x-www-form-urlencoded" in content_type:
        form = await request.form()
        body = dict(form)
    elif "application/json" in content_type:
        body = await request.json()
    else:
        body = dict(request.query_params)

    grant_type = body.get("grant_type", "")
    code = body.get("code", "")
    code_verifier = body.get("code_verifier", "")

    if grant_type != "authorization_code":
        return JSONResponse({"error": "unsupported_grant_type"}, status_code=400)

    stored = _auth_codes.pop(code, None)
    if not stored or stored["expires"] < time.time():
        return JSONResponse({"error": "invalid_grant"}, status_code=400)

    if stored.get("code_challenge") and code_verifier:
        digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
        expected = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
        if expected != stored["code_challenge"]:
            return JSONResponse({"error": "invalid_grant"}, status_code=400)

    access_token = secrets.token_hex(32)
    _access_tokens.add(access_token)

    logger.info("OAuth: token issued")
    return JSONResponse({
        "access_token": access_token,
        "token_type": "Bearer",
        "expires_in": 86400 * 365,
    })


# ══════════════════════════════════════════════════════
#  Relay 端點
# ══════════════════════════════════════════════════════

async def poll(request: Request):
    if request.query_params.get("token") != AUTH_TOKEN:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    global _latest
    if _latest is None:
        return JSONResponse(None, status_code=204)
    cmd = _latest
    _latest = None
    logger.info("Command delivered: %s", cmd["id"])
    return JSONResponse(cmd)


async def photo_upload(request: Request):
    """POST /photo?token=xxx
    StackChan 韌體拍完照後，把 base64 編碼的 JPEG 放在 body 送過來。
    解碼存進 _photo 並喚醒等待中的 get_photo。
    """
    if request.query_params.get("token") != AUTH_TOKEN:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    global _photo
    body = await request.body()
    try:
        _photo = base64.b64decode(body, validate=False)
    except Exception:
        return JSONResponse({"error": "bad image data"}, status_code=400)

    if not _photo:
        return JSONResponse({"error": "empty image"}, status_code=400)

    _photo_event.set()
    logger.info("Photo received: %d bytes", len(_photo))
    return JSONResponse({"ok": True, "bytes": len(_photo)})


async def events_post(request: Request):
    """POST /events?token=xxx — firmware 回報感測事件"""
    if request.query_params.get("token") != AUTH_TOKEN:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid json"}, status_code=400)

    evt_type = body.get("type")
    if not evt_type or not isinstance(evt_type, str):
        return JSONResponse({"error": "missing or invalid 'type'"}, status_code=400)

    event = {
        "type": evt_type,
        "timestamp": body.get("timestamp", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
        "data": body.get("data", {}),
    }

    async with _events_lock:
        _events.append(event)
        if len(_events) > MAX_EVENTS:
            _events[:] = _events[-MAX_EVENTS:]

    logger.info("Event received: %s", evt_type)
    return JSONResponse({"ok": True})


# ── Gemini Search Grounding 代理 ───────────────────────
# 韌體照舊用 OpenAI 格式打 /chat/completions，這裡翻譯成 Gemini 原生
# generateContent 並開啟 google_search grounding，再翻回 OpenAI 格式。
# 韌體端解析 buffer 只有 2KB，回應必須精簡（丟 groundingMetadata、截斷 content）。

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_BASE = os.environ.get("GEMINI_BASE", "https://generativelanguage.googleapis.com")
GEMINI_TIMEOUT = float(os.environ.get("GEMINI_TIMEOUT", "25"))
# 韌體解析 pool 為 2000 bytes；中文 1 字 3 bytes + ArduinoJson 結構開銷約 600 bytes，
# 300 字（900 bytes）留足餘裕。
CONTENT_MAX_CHARS = 300
FALLBACK_TEXT = "我剛剛恍神了，再說一次好嗎？"


def _openai_to_gemini(body: dict) -> tuple[str, dict]:
    """OpenAI chat completion 請求 → Gemini generateContent payload。"""
    model = body.get("model") or "gemini-2.5-flash"
    system_parts: list[str] = []
    contents: list[dict] = []
    for msg in body.get("messages", []):
        role = msg.get("role")
        content = msg.get("content")
        if not isinstance(content, str) or content == "":
            continue  # 韌體只送純文字，其他形態忽略
        if role == "system":
            system_parts.append(content)
        elif role in ("user", "assistant"):
            contents.append({
                "role": "user" if role == "user" else "model",
                "parts": [{"text": content}],
            })
    payload: dict = {
        "contents": contents,
        "tools": [{"google_search": {}}],
    }
    if system_parts:
        payload["systemInstruction"] = {"parts": [{"text": "\n\n".join(system_parts)}]}
    if isinstance(body.get("temperature"), (int, float)):
        payload["generationConfig"] = {"temperature": body["temperature"]}
    return model, payload


def _extract_text(data: dict) -> str:
    """Gemini 回應取出純文字，丟棄 groundingMetadata/citations。"""
    try:
        parts = (data.get("candidates") or [{}])[0].get("content", {}).get("parts", [])
        text = "".join(p.get("text", "") for p in parts).strip()
    except (AttributeError, IndexError, TypeError):
        return ""
    return text[:CONTENT_MAX_CHARS]


def _openai_response(text: str, model: str) -> JSONResponse:
    """組最小可用的 OpenAI chat completion JSON（韌體只讀 content）。"""
    return JSONResponse({
        "id": "chatcmpl-relay",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": text},
            "finish_reason": "stop",
        }],
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    })


async def chat_completions(request: Request):
    """POST /chat/completions?token=xxx — OpenAI 相容的 Gemini grounding 代理。"""
    if request.query_params.get("token") != AUTH_TOKEN:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid json"}, status_code=400)

    model, payload = _openai_to_gemini(body)
    url = f"{GEMINI_BASE}/v1beta/models/{model}:generateContent"
    headers = {"x-goog-api-key": GEMINI_API_KEY}
    t0 = time.monotonic()

    data: dict | None = None
    async with httpx.AsyncClient(timeout=GEMINI_TIMEOUT) as client:
        try:
            resp = await client.post(url, json=payload, headers=headers)
            if resp.status_code in (400, 429):
                # grounding quota（429）或相容性問題（400）→ 拿掉 tools 降級重試一次
                logger.warning("Gemini %d, retry without tools: %s",
                               resp.status_code, resp.text[:200])
                payload.pop("tools", None)
                resp = await client.post(url, json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error("Gemini request failed: %r", e)

    elapsed = time.monotonic() - t0
    if data is None:
        # 逾時或最終失敗：回固定訊息讓 TTS 有東西可念，不對韌體回 5xx
        logger.error("Chat fallback after %.1fs", elapsed)
        return _openai_response(FALLBACK_TEXT, model)

    text = _extract_text(data)
    if not text:
        logger.warning("Gemini empty text: %s", json.dumps(data)[:300])
        text = FALLBACK_TEXT

    grounded = bool((data.get("candidates") or [{}])[0].get("groundingMetadata"))
    logger.info("Chat ok: %.1fs grounded=%s len=%d", elapsed, grounded, len(text))
    return _openai_response(text, model)


async def health(request: Request):
    return JSONResponse({
        "ok": True,
        "pending": _latest is not None,
        "service": "stackchan-relay",
        "version": "3",
    })


# ══════════════════════════════════════════════════════
#  App
# ══════════════════════════════════════════════════════

app = Starlette(
    routes=[
        # OAuth 2.1
        Route("/.well-known/oauth-authorization-server", oauth_metadata, methods=["GET"]),
        Route("/.well-known/oauth-protected-resource", oauth_protected_resource, methods=["GET"]),
        Route("/oauth/register", oauth_register, methods=["GET", "POST"]),
        Route("/oauth/authorize", oauth_authorize, methods=["GET"]),
        Route("/oauth/token", oauth_token, methods=["POST"]),
        # MCP（手動實作，支援 SSE + Streamable HTTP）
        Route("/mcp/sse", mcp_sse_handler, methods=["GET", "POST", "DELETE"]),
        Route("/mcp/messages", mcp_messages_handler, methods=["POST"]),
        # Relay
        Route("/poll", poll, methods=["GET"]),
        Route("/chat/completions", chat_completions, methods=["POST"]),
        Route("/photo", photo_upload, methods=["POST"]),
        Route("/events", events_post, methods=["POST"]),
        Route("/health", health, methods=["GET"]),
    ],
)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)
