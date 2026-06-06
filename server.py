"""
StackChan MCP Relay Server
==========================
Claude.ai → MCP tool call → 這裡存指令 → StackChan 來拿 → 執行

兩個角色：
  1. MCP Server：讓 Claude 看到 stackchan_perform 工具
  2. Relay：讓 StackChan 韌體透過 /poll 取走最新指令
"""

import os
import time
import logging
from typing import Any

from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.routing import Route, Mount
from starlette.responses import JSONResponse
from starlette.requests import Request

# ── 設定 ──────────────────────────────────────────────

LOG_FMT = "%(asctime)s [%(name)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
logger = logging.getLogger("stackchan")

# StackChan 輪詢時需要帶的驗證 token，在 Zeabur 環境變數裡設定
AUTH_TOKEN = os.environ.get("STACKCHAN_TOKEN", "please-set-a-token")
PORT = int(os.environ.get("PORT", "8011"))

# ── 指令暫存（latest-only，不排隊）─────────────────────

_latest: dict | None = None


# ══════════════════════════════════════════════════════
#  MCP Server — Claude 看到的工具定義
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
#  Relay 端點 — StackChan 韌體呼叫的 HTTP API
# ══════════════════════════════════════════════════════

async def poll(request: Request):
    """GET /poll?token=xxx
    StackChan 韌體每隔幾秒呼叫一次。
    有新指令 → 回傳 JSON，同時清除（latest-only）。
    沒有 → 回傳 204 No Content。
    """
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
    """GET /health — 確認服務活著。"""
    return JSONResponse({
        "ok": True,
        "pending": _latest is not None,
        "service": "stackchan-relay",
    })


# ══════════════════════════════════════════════════════
#  組合：MCP + Relay 共用同一個 ASGI app
# ══════════════════════════════════════════════════════

app = Starlette(
    routes=[
        # MCP 協議掛在 /mcp 底下
        # Claude.ai 填入的 URL 會是 https://你的域名/mcp/sse
        Mount("/mcp", app=mcp.sse_app()),

        # StackChan 輪詢端點
        Route("/poll", poll, methods=["GET"]),

        # 健康檢查
        Route("/health", health, methods=["GET"]),
    ],
)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)
