"""Gemini grounding proxy smoke tests.

Run with: python3 test_grounding.py
(Uses a fake Gemini upstream, no real API key needed.)
"""
import asyncio
import json
import os
import sys

os.environ["STACKCHAN_TOKEN"] = "testtoken"
os.environ["GEMINI_API_KEY"] = "fake-key"
os.environ["GEMINI_BASE"] = "http://127.0.0.1:8899"
os.environ["GEMINI_TIMEOUT"] = "2"  # 逾時測試用短 timeout

import httpx
import uvicorn
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.requests import Request

from server import app as relay_app, FALLBACK_TEXT

# ── 假 Gemini 上游 ─────────────────────────────────────
calls: list[dict] = []          # 記錄收到的 payload 供斷言
quota_call_count = {"n": 0}

GROUNDED_RESPONSE = {
    "candidates": [{
        "content": {"parts": [{"text": "今天台北多雲時晴，氣溫 28 到 33 度。"}]},
        "groundingMetadata": {"webSearchQueries": ["台北天氣"]},
    }],
}

def make_response(text):
    return {"candidates": [{"content": {"parts": [{"text": text}]}}]}

async def fake_generate(request: Request):
    payload = await request.json()
    calls.append({"path": request.url.path, "payload": payload,
                  "key": request.headers.get("x-goog-api-key")})
    user_text = payload["contents"][-1]["parts"][0]["text"]
    if "天氣" in user_text:
        return JSONResponse(GROUNDED_RESPONSE)
    if "quota" in user_text:
        quota_call_count["n"] += 1
        if "tools" in payload:
            return JSONResponse({"error": {"code": 429, "message": "quota"}}, status_code=429)
        return JSONResponse(make_response("降級回答（無搜尋）"))
    if "slow" in user_text:
        await asyncio.sleep(5)
        return JSONResponse(make_response("太慢了不該收到"))
    if "long" in user_text:
        return JSONResponse(make_response("囉" * 800))
    return JSONResponse(make_response("你好呀！"))

fake_app = Starlette(routes=[
    Route("/v1beta/models/{model_action}", fake_generate, methods=["POST"]),
])


def openai_req(text, system="你是StackChan", model="gemini-2.5-flash"):
    return {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": "之前的話"},
            {"role": "assistant", "content": "之前的回答"},
            {"role": "user", "content": text},
        ],
        "temperature": 0.7,
    }


async def main():
    config = uvicorn.Config(fake_app, host="127.0.0.1", port=8899, log_level="warning")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.05)

    transport = httpx.ASGITransport(app=relay_app)
    passed = failed = 0

    def check(name, cond, detail=""):
        nonlocal passed, failed
        if cond:
            passed += 1
            print(f"  PASS  {name}")
        else:
            failed += 1
            print(f"  FAIL  {name}  {detail}")

    async with httpx.AsyncClient(transport=transport, base_url="http://relay") as c:
        # 1. 未授權
        r = await c.post("/chat/completions?token=wrong", json=openai_req("hi"))
        check("401 on bad token", r.status_code == 401, r.text)

        # 2. 一般對話透傳 + 翻譯正確性
        calls.clear()
        r = await c.post("/chat/completions?token=testtoken", json=openai_req("你好嗎"))
        body = r.json()
        check("normal chat 200", r.status_code == 200, r.text)
        check("openai shape", body["choices"][0]["message"]["content"] == "你好呀！", json.dumps(body, ensure_ascii=False))
        check("finish_reason stop", body["choices"][0]["finish_reason"] == "stop")
        p = calls[0]["payload"]
        check("system -> systemInstruction", "你是StackChan" in p["systemInstruction"]["parts"][0]["text"])
        check("roles mapped", [m["role"] for m in p["contents"]] == ["user", "model", "user"])
        check("google_search tool attached", p.get("tools") == [{"google_search": {}}])
        check("temperature passthrough", p["generationConfig"]["temperature"] == 0.7)
        check("api key header", calls[0]["key"] == "fake-key")
        check("model in url", "gemini-2.5-flash:generateContent" in calls[0]["path"])

        # 3. grounded 回應（丟棄 groundingMetadata）
        r = await c.post("/chat/completions?token=testtoken", json=openai_req("今天台北天氣如何"))
        body = r.json()
        check("grounded answer", "多雲時晴" in body["choices"][0]["message"]["content"], r.text)
        check("groundingMetadata stripped", "groundingMetadata" not in r.text)

        # 4. 429 降級重試（拿掉 tools）
        calls.clear()
        r = await c.post("/chat/completions?token=testtoken", json=openai_req("quota測試"))
        body = r.json()
        check("429 fallback 200", r.status_code == 200, r.text)
        check("degraded answer", body["choices"][0]["message"]["content"] == "降級回答（無搜尋）", r.text)
        check("retry without tools", "tools" in calls[0]["payload"] and "tools" not in calls[1]["payload"])

        # 5. 逾時 → 固定訊息（GEMINI_TIMEOUT=2s，假上游睡 5s）
        r = await c.post("/chat/completions?token=testtoken", json=openai_req("slow測試"))
        body = r.json()
        check("timeout fallback 200", r.status_code == 200, r.text)
        check("fallback text", body["choices"][0]["message"]["content"] == FALLBACK_TEXT, r.text)

        # 6. 長回應截斷（2KB 韌體 buffer 防呆）
        r = await c.post("/chat/completions?token=testtoken", json=openai_req("long測試"))
        body = r.json()
        check("content truncated to 300", len(body["choices"][0]["message"]["content"]) == 300)
        # 韌體 pool 2000 bytes ≈ content bytes + ArduinoJson 結構開銷 ~600
        check("response fits 2KB firmware buffer", len(r.content) < 1400, f"{len(r.content)} bytes")

        # 7. 既有端點 no-clobber
        r = await c.get("/health")
        check("health ok", r.status_code == 200 and r.json()["ok"] is True, r.text)
        r = await c.get("/poll?token=testtoken")
        check("poll 204 empty", r.status_code == 204, str(r.status_code))
        r = await c.get("/poll?token=wrong")
        check("poll 401 bad token", r.status_code == 401)

    server.should_exit = True
    await server_task
    print(f"\n{passed} passed, {failed} failed")
    return failed


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
