# StackChan MCP Relay

Claude.ai ↔ MCP ↔ StackChan 的中繼服務。

## 環境變數

| 變數 | 說明 | 預設值 |
|------|------|--------|
| `STACKCHAN_TOKEN` | StackChan 輪詢時的驗證 token | `please-set-a-token` |
| `PORT` | 伺服器埠號（Zeabur 自動設定） | `8011` |

## 端點

- `GET /mcp/sse` — MCP SSE 端點（Claude.ai 連這裡）
- `GET /poll?token=xxx` — StackChan 韌體輪詢指令
- `GET /health` — 健康檢查
