# StackChan MCP Relay Server

Claude.ai ↔ MCP ↔ StackChan 的中繼服務。

讓 claude.ai 透過 MCP 協議控制桌上的 Stack-chan 機器人：說話、表情、移動、拍照、接收感測事件。
同時提供 Gemini Search Grounding 代理，讓韌體用 OpenAI 格式就能取得即時搜尋結果。

## 架構

```
claude.ai ──OAuth 2.1──► Relay Server ◄──poll── StackChan 韌體
                MCP           │
          (SSE / Streamable)  ├── /poll         韌體輪詢指令
                              ├── /photo        韌體回傳照片
                              ├── /events       韌體回報事件
                              └── /chat/completions  Gemini grounding 代理
```

## 環境變數

| 變數 | 說明 | 預設值 |
|------|------|--------|
| `STACKCHAN_TOKEN` | 韌體輪詢 & Gemini 代理的驗證 token | `please-set-a-token` |
| `STACKCHAN_OAUTH_SECRET` | OAuth /authorize 的部署密鑰（防止任何人連上你的 MCP） | 未設定時 fall back 到 `STACKCHAN_TOKEN` |
| `GEMINI_API_KEY` | Gemini API 金鑰（grounding 代理用） | （空） |
| `PORT` | 伺服器埠號（Zeabur/Railway 自動設定） | `8011` |
| `GEMINI_BASE` | Gemini API base URL（測試用） | `https://generativelanguage.googleapis.com` |
| `GEMINI_TIMEOUT` | Gemini 請求逾時秒數 | `25` |

## 部署（Zeabur）

1. 在 Zeabur 建立新 service，指向這個資料夾或 git repo
2. 設定環境變數：
   - `STACKCHAN_TOKEN`：一組隨機字串，韌體 `SC_ExConfig.yaml` 裡的 `relay_token` 要填同一組
   - `STACKCHAN_OAUTH_SECRET`：另一組隨機字串，claude.ai 首次連線時會要求輸入
   - `GEMINI_API_KEY`：從 Google AI Studio 取得（若不用 grounding 可留空）
3. 部署完成後，在 claude.ai 設定 → MCP Servers → 加入你的 URL

## 本地開發

```bash
# 建立虛擬環境
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 啟動
STACKCHAN_TOKEN=dev-token STACKCHAN_OAUTH_SECRET=dev-secret python server.py
```

## 測試

```bash
# 事件佇列測試（需 pytest + anyio）
pip install pytest anyio
python3 -m pytest test_events.py -v

# Grounding 代理測試（自帶假 Gemini 上游，不需 API key）
python3 test_grounding.py
```

## MCP Tools

| 工具 | 說明 |
|------|------|
| `stackchan_perform` | 送出動作序列（speak / emote / move / wiggle / led） |
| `stackchan_get_photo` | 用機器人相機拍照並回傳影像 |
| `stackchan_check_events` | 檢查感測事件（摸頭、擁抱、語音訊息等） |

## 端點一覽

| 路徑 | 方法 | 說明 |
|------|------|------|
| `/mcp/sse` | GET/POST | MCP 傳輸（SSE + Streamable HTTP） |
| `/mcp/messages` | POST | MCP SSE 訊息接收 |
| `/poll` | GET | 韌體輪詢指令（需 token） |
| `/photo` | POST | 韌體回傳照片（需 token） |
| `/events` | POST | 韌體回報事件（需 token） |
| `/chat/completions` | POST | Gemini grounding 代理（OpenAI 格式，需 token） |
| `/health` | GET | 健康檢查 |
| `/oauth/*` | GET/POST | OAuth 2.1 流程 |

## 安全機制

- **OAuth 2.1 + PKCE**：claude.ai 連線需經過 OAuth 授權流程
- **部署密鑰**：`/oauth/authorize` 需輸入密鑰，不再自動批准
- **Bearer Token**：MCP 端點需帶 Authorization header
- **Fail-closed**：未設定密鑰時一律拒絕授權
- **韌體端點 token 驗證**：`/poll`、`/photo`、`/events`、`/chat/completions` 都需 token

## 注意事項

- 重新部署後記憶體中的 OAuth token 會失效，claude.ai 會自動觸發重新授權
- `python-multipart` 故意不裝，form 解析用自己的 `parse_urlencoded_form()`
- 韌體端解析 buffer 僅 2KB，Gemini 回應會自動截斷至 300 字
