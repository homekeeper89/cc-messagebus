# cc-messagebus

Claude Code 세션 간 메시지 큐 + 실시간 대시보드. 단일 Node daemon + SQLite 한 파일로 동작하며 외부 인프라(Redis/RabbitMQ 등) 의존성이 없습니다.

- **1:1 메시지 큐**: at-least-once, visibility timeout 30s, TTL 30일
- **MCP stdio adapter**: Claude Code에 `register / send / read / ack / list_peers / unregister` 도구 노출
- **SSE 알림**: `cc-messagebus tail <topicId>` 가 stdout으로 라인 푸시 → Claude `Monitor` 도구가 즉시 수신
- **대시보드**: `http://127.0.0.1:5959/dashboard` 에서 세션 + 메시지 흐름 라이브 관찰

## Requirements

- Node.js >= 20
- macOS / Linux (Windows 미검증)

## Install

```bash
# 전역 설치
npm install -g cc-messagebus

# 또는 npx (설치 없이 즉시 실행)
npx cc-messagebus serve
```

## Quick Start

### 1) MCP 등록 — `.claude/settings.json`

```json
{
  "mcpServers": {
    "cc-messagebus": {
      "command": "npx",
      "args": ["-y", "cc-messagebus", "mcp"]
    }
  }
}
```

이게 끝입니다. 첫 `register` 호출 시 broker daemon이 자동으로 spawn 되고, 응답의 `monitorCommand` 를 Claude가 `Monitor` 도구로 실행해 수신 알림 채널까지 자동 구성됩니다.

### 2) 세션 흐름 예시

세션 A:
```
register({ topicId: "alice" })
send({ from: "alice", to: "bob", subject: "ping", body: "안녕" })
```

세션 B:
```
register({ topicId: "bob" })
# Monitor 가 자동으로 알림 라인 수신
read({ topicId: "bob" })          # 메시지 fetch (visibility timeout 30s 시작)
ack({ topicId: "bob", messageId }) # 처리 완료 표시
```

### 3) 대시보드

```bash
cc-messagebus dashboard
# 브라우저: http://127.0.0.1:5959/dashboard
```

세션 목록과 메시지 라이프사이클(`sent / read / acked / redelivered / expired`)이 SSE로 실시간 push 됩니다.

## CLI Reference

| Command | Description |
|---|---|
| `cc-messagebus serve` | Broker daemon 시작 (HTTP RPC + SSE) |
| `cc-messagebus mcp` | MCP stdio adapter (Claude Code 가 spawn) |
| `cc-messagebus tail <topicId>` | 토픽 SSE 구독, 수신 메시지를 stdout 라인으로 push |
| `cc-messagebus dashboard` | Dashboard URL 출력 |

## MCP Tools

| Tool | Purpose |
|---|---|
| `register` | 토픽 등록. 응답에 `monitorCommand` 포함 (Claude가 자동 실행) |
| `unregister` | 토픽 해제. `purgeQueue` 옵션으로 큐 함께 비우기 |
| `send` | 다른 토픽에 메시지 전송 (1:1) |
| `read` | 자기 큐에서 메시지 pull (visibility timeout 시작) |
| `ack` | 메시지 처리 완료 마킹 (생략 시 timeout 후 재배달) |
| `list_peers` | 등록된 토픽 목록 조회 |

## Configuration

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `CC_MESSAGEBUS_DB` | `~/.cc-messagebus/data.db` | SQLite 파일 경로 |
| `CC_MESSAGEBUS_URL` | `http://127.0.0.1:5959` | `tail` / `dashboard` 가 접속할 broker base URL |

### Server defaults (`serve`)

| Knob | Default |
|---|---|
| Bind host | `127.0.0.1` |
| Port | `5959` |
| Visibility timeout | 30s |
| Message TTL | 30 days |
| Cleanup interval | 60s |

## Delivery Semantics

- **At-least-once**: `read()` 시 메시지가 `in_flight_until = now + 30s` 로 잠금. `ack()` 호출 시 `acked_at` 기록.
- **Redelivery**: visibility timeout 내 `ack()` 없으면 다음 `read()` 에서 다시 fetch 가능.
- **TTL**: 모든 메시지는 ack 여부와 무관하게 30일 보관 후 cleanup. 관찰성·감사 용도.
- **Disconnect 감지**: `tail` SSE close → 해당 토픽 `disconnected` 마킹 (큐는 보존).

## Architecture

```
┌─────────────┐    HTTP RPC      ┌──────────────────┐
│  MCP stdio  │ ───────────────▶ │  Broker daemon   │
│  (per Claude│                  │  Fastify + SQLite │
│   session)  │ ◀─────────────── │  127.0.0.1:5959  │
└─────────────┘    JSON          └──────────────────┘
                                   │           │
                          SSE      │           │  SSE
                  ┌────────────────┘           └──────────────┐
                  ▼                                           ▼
         ┌────────────────────┐                  ┌─────────────────────┐
         │ cc-messagebus tail │                  │  /dashboard (HTML)  │
         │  → stdout 라인     │                  │  /events SSE stream │
         │  → Claude Monitor  │                  │  세션·메시지 live   │
         └────────────────────┘                  └─────────────────────┘
```

- **Transport**: HTTP RPC + SSE (WebSocket 미사용)
- **Persistence**: SQLite 단일 파일 (`messages`, `sessions` 테이블)
- **Process model**: 단일 daemon, 단일 SQLite writer

## Not Building (v1)

- 인증 / 인가 / TLS — 로컬(`127.0.0.1`) 한정
- 메시지 암호화
- Broadcast / fan-out — 1:1 전송만
- 메시지 priority / scheduling — FIFO
- 파일 첨부 — 텍스트 payload만
- 클러스터링 / HA — 단일 노드

## Development

```bash
npm install
npm run typecheck   # tsc --noEmit
npm run lint        # biome lint
npm run check       # biome check --write (lint + format + organize imports)
npm test            # node --test --import tsx
npm run build       # tsc + dashboard html copy
```

자세한 PRD는 `prd.md` 참조.
