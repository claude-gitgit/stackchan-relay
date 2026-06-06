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


# ══════════════════════════════════════════════════════
#  MCP 協議處理（JSON-RPC 2.0）
# ══════════════════════════════════════════════════════

def handle_jsonrpc(msg: dict) -> dict | None:
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
            "result": {"tools": [STACKCHAN_TOOL]},
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
        response = handle_jsonrpc(body)
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
    response = handle_jsonrpc(body)

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
        Route("/health", health, methods=["GET"]),
    ],
)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)
