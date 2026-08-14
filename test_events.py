"""Event queue smoke tests — run with: python3 -m pytest test_events.py -v"""

import asyncio
import json
import os

import httpx
import pytest

os.environ.setdefault("STACKCHAN_TOKEN", "test-token")

from server import app  # noqa: E402

TOKEN = os.environ["STACKCHAN_TOKEN"]


@pytest.fixture()
def client():
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://testserver")


@pytest.fixture(autouse=True)
def _clear_events():
    from server import _events
    _events.clear()


@pytest.mark.anyio
async def test_post_event_ok(client):
    r = await client.post(
        f"/events?token={TOKEN}",
        json={"type": "head_pat", "timestamp": "2026-06-28T10:00:00Z", "data": {}},
    )
    assert r.status_code == 200
    assert r.json()["ok"] is True


@pytest.mark.anyio
async def test_post_event_no_token(client):
    r = await client.post(
        "/events",
        json={"type": "head_pat"},
    )
    assert r.status_code == 401


@pytest.mark.anyio
async def test_post_event_bad_token(client):
    r = await client.post(
        "/events?token=wrong",
        json={"type": "head_pat"},
    )
    assert r.status_code == 401


@pytest.mark.anyio
async def test_post_event_missing_type(client):
    r = await client.post(
        f"/events?token={TOKEN}",
        json={"data": {}},
    )
    assert r.status_code == 400


@pytest.mark.anyio
async def test_check_events_returns_all_in_order(client):
    for i in range(3):
        await client.post(
            f"/events?token={TOKEN}",
            json={"type": f"evt_{i}", "timestamp": f"2026-06-28T10:0{i}:00Z", "data": {"i": i}},
        )

    result = await _mcp_check_events(client)
    events = json.loads(result)
    assert len(events) == 3
    assert [e["type"] for e in events] == ["evt_0", "evt_1", "evt_2"]
    assert events[2]["data"] == {"i": 2}


@pytest.mark.anyio
async def test_check_events_clears_after_read(client):
    await client.post(
        f"/events?token={TOKEN}",
        json={"type": "head_pat"},
    )

    result1 = await _mcp_check_events(client)
    assert len(json.loads(result1)) == 1

    result2 = await _mcp_check_events(client)
    assert json.loads(result2) == []


@pytest.mark.anyio
async def test_check_events_keep_mode(client):
    await client.post(
        f"/events?token={TOKEN}",
        json={"type": "head_pat"},
    )

    result1 = await _mcp_check_events(client, keep=True)
    assert len(json.loads(result1)) == 1

    result2 = await _mcp_check_events(client, keep=True)
    assert len(json.loads(result2)) == 1


@pytest.mark.anyio
async def test_check_events_empty(client):
    result = await _mcp_check_events(client)
    assert json.loads(result) == []


@pytest.mark.anyio
async def test_event_cap_100(client):
    for i in range(110):
        await client.post(
            f"/events?token={TOKEN}",
            json={"type": "bulk", "data": {"i": i}},
        )

    result = await _mcp_check_events(client)
    events = json.loads(result)
    assert len(events) == 100
    assert events[0]["data"]["i"] == 10
    assert events[-1]["data"]["i"] == 109


@pytest.mark.anyio
async def test_server_adds_timestamp_if_missing(client):
    await client.post(
        f"/events?token={TOKEN}",
        json={"type": "head_pat"},
    )
    result = await _mcp_check_events(client)
    events = json.loads(result)
    assert "timestamp" in events[0]
    assert events[0]["timestamp"] != ""


@pytest.mark.anyio
async def test_tools_list_includes_check_events(client):
    r = await client.post(
        "/mcp/sse",
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
    )
    tools = r.json()["result"]["tools"]
    names = [t["name"] for t in tools]
    assert "stackchan_check_events" in names


# ── helper ──────────────────────────────────────────────

async def _mcp_check_events(client: httpx.AsyncClient, keep: bool = False) -> str:
    r = await client.post(
        "/mcp/sse",
        json={
            "jsonrpc": "2.0",
            "id": 99,
            "method": "tools/call",
            "params": {
                "name": "stackchan_check_events",
                "arguments": {"keep": keep},
            },
        },
    )
    assert r.status_code == 200
    return r.json()["result"]["content"][0]["text"]
