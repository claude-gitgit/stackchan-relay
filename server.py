"""
StackChan MCP Relay Server
==========================
Claude.ai → OAuth handshake → MCP tool call → 指令暫存 → StackChan 輪詢 → 執行

兩個角色：
  1. MCP Server：讓 Claude 看到 stackchan_perform 工具
  2. Relay：讓 StackChan 韌體透過 /poll 取走最新指令

OAuth 2.1 端點是給 claude.ai 的 MCP 連接器用的認證流程，
採用自動批准模式（不需要使用者手動操作）。
"""

import os
import time
import secrets
import hashlib
import base64
import logging
from typing import Any
from urllib.parse import urlencode

from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.routing import Route, Mount
from starlette.responses import JSONResponse, RedirectResponse
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


# ══════════════════════════════════════════════════════
#  MCP Server
# ══════════════════════════════════════════════════════

mcp = FastMCP(
    "stackchan-relay",
    instructions=(
        "You can control a physical StackChan robot on the user's desk. "
        "Use stackchan_perform to make it speak, emote, move, etc. "
        "Combine multiple actions for lively, expressive responses. "
        "Always respond in the language the user is using."
    ),
)


@mcp.tool()
async def stackchan_perform(actions: list[dict[str, Any]]) -> str:
    """Send a batch of actions to the StackChan robot.

    Each action is a dict with a required "type" field:

    • speak  → Say text aloud.
              {"type": "speak", "text": "你好！"}

    • emote  → Change facial expression.
              {"type": "emote", "expression": "happy"}
              Options: happy, shy, angry, thinking, neutral, sad

    • move   → Move head (degrees from center).
              {"type": "move", "pitch": 10, "yaw": -20}

    • wiggle → Playful head shake.
              {"type": "wiggle"}

    • led    → Set LED colour (hex).
              {"type": "led", "color": "#FF6600"}

    Actions execute in sequence. Combine for richer output:
    [
      {"type": "emote", "expression": "happy"},
      {"type": "speak", "text": "早安！今天也要加油喔"},
      {"type": "wiggle"}
    ]
    """
    global _latest

    if not actions:
        return "⚠ No actions provided."

    _latest = {
        "actions": actions,
        "id": f"cmd_{int(time.time() * 1000)}",
        "ts": time.time(),
    }

    summary = ", ".join(a.get("type", "?") for a in actions)
    logger.info("Command queued → %s", summary)
    return f"✓ Sent to StackChan: {summary}"


# ══════════════════════════════════════════════════════
#  OAuth 2.1（自動批准模式）
#  讓 claude.ai 的 MCP 連接器完成認證握手
# ══════════════════════════════════════════════════════

async def oauth_protected_resource(request: Request):
    """GET /.well-known/oauth-protected-resource"""
    base = str(request.base_url).rstrip("/")
    return JSONResponse({
        "resource": base,
        "authorization_servers": [base],
        "scopes_supported": ["all"],
    })


async def oauth_metadata(request: Request):
    """GET /.well-known/oauth-authorization-server"""
    base = str(request.base_url).rstrip("/")
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
    """POST /oauth/register — 動態客戶端註冊（自動批准）"""
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

    logger.info("OAuth: client registered → %s", client_id)
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
    """GET /oauth/authorize — 自動批准，立即重導向回 claude.ai"""
    params = dict(request.query_params)
    redirect_uri = params.get("redirect_uri", "")
    state = params.get("state", "")

    # 產生授權碼
    code = secrets.token_hex(16)
    _auth_codes[code] = {
        "client_id": params.get("client_id", ""),
        "redirect_uri": redirect_uri,
        "code_challenge": params.get("code_challenge", ""),
        "code_challenge_method": params.get("code_challenge_method", "S256"),
        "expires": time.time() + 600,
    }

    logger.info("OAuth: authorize auto-approved → %s", params.get("client_id", ""))

    # 組合重導向 URL
    redir_params = {"code": code}
    if state:
        redir_params["state"] = state

    separator = "&" if "?" in redirect_uri else "?"
    return RedirectResponse(
        f"{redirect_uri}{separator}{urlencode(redir_params)}",
        status_code=302,
    )


async def oauth_token(request: Request):
    """POST /oauth/token — 用授權碼換取 access token"""
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
        logger.warning("OAuth: invalid or expired code")
        return JSONResponse({"error": "invalid_grant"}, status_code=400)

    # 驗證 PKCE
    if stored.get("code_challenge") and code_verifier:
        digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
        expected = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
        if expected != stored["code_challenge"]:
            logger.warning("OAuth: PKCE verification failed")
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
#  Relay 端點（StackChan 韌體用）
# ══════════════════════════════════════════════════════

async def poll(request: Request):
    """GET /poll?token=xxx"""
    if request.query_params.get("token") != AUTH_TOKEN:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    global _latest
    if _latest is None:
        return JSONResponse(None, status_code=204)

    cmd = _latest
    _latest = None
    logger.info("Command delivered → %s", cmd["id"])
    return JSONResponse(cmd)


async def health(request: Request):
    """GET /health"""
    return JSONResponse({
        "ok": True,
        "pending": _latest is not None,
        "service": "stackchan-relay",
    })


# ══════════════════════════════════════════════════════
#  組合 App
# ══════════════════════════════════════════════════════

app = Starlette(
    routes=[
        # OAuth 2.1
        Route("/.well-known/oauth-authorization-server", oauth_metadata, methods=["GET"]),
        Route("/.well-known/oauth-protected-resource", oauth_protected_resource, methods=["GET"]),
        Route("/oauth/register", oauth_register, methods=["POST"]),
        Route("/oauth/authorize", oauth_authorize, methods=["GET"]),
        Route("/oauth/token", oauth_token, methods=["POST"]),
        # MCP
        Mount("/mcp", app=mcp.sse_app()),
        # Relay
        Route("/poll", poll, methods=["GET"]),
        Route("/health", health, methods=["GET"]),
    ],
)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)
