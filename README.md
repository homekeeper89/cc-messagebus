# cc-messagebus

Claude Code 세션 간 메시지 큐 + Pub-Sub 채널 + 실시간 대시보드. 단일 Node daemon + SQLite 한 파일로 동작하며 외부 인프라(Redis/RabbitMQ 등) 의존성이 없습니다.

- **1:1 메시지 큐**: at-least-once, visibility timeout 30s, TTL 30일
- **Pub-Sub 채널**: N:M fan-out, subscriber 별 독립 큐 + read/ack 라이프사이클, history 조회
- **MCP stdio adapter**: Claude Code 에 1:1 도구 (`register / send / read / ack / list_peers / unregister`) + 채널 도구 (`list_channels / channel_create / channel_subscribe / channel_send / channel_unsubscribe / channel_history / channel_detail`) 노출
- **SSE 알림**: `cc-messagebus tail <topicId>` 가 stdout 으로 라인 푸시 → Claude `Monitor` 도구가 즉시 수신
- **대시보드**: `http://127.0.0.1:5959/dashboard` 에서 4-카테고리 트리(ALL/토픽/DM/에이전트) + 채널 detail view + 메시지 흐름 라이브 관찰

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

### 1) MCP 등록

두 방식 중 하나를 선택하세요. 등록 후 첫 `register` 호출 시 broker daemon이 자동으로 spawn 되고, 응답의 `monitorCommand` 를 Claude가 `Monitor` 도구로 실행해 수신 알림 채널까지 자동 구성됩니다.

#### A. Plugin 설치 (권장)

```
/plugin install homekeeper89/cc-messagebus
```

`.claude-plugin/plugin.json` 이 자동 로드되어 MCP 서버가 활성화됩니다. settings.json 편집 불필요.

#### B. settings.json 수동 등록

`.claude/settings.json` 에 추가:

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

### 2) 1:1 세션 흐름 예시

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

### 3) 채널 (Pub-Sub) 흐름 예시

```
# 발행자
channel_create({ channelId: "demo" })
channel_send({ channelId: "demo", subject: "deploy", body: "v0.1.0 released" })

# 구독자 (다수 가능)
channel_subscribe({ channelId: "demo" })
# 이후 본인 토픽 큐로 fan-out 사본이 들어옴 → read / ack 로 소비
channel_history({ channelId: "demo", limit: 50 })   # backlog 조회
channel_unsubscribe({ channelId: "demo" })
```

채널은 1:1 큐와 동일한 visibility timeout / TTL 라이프사이클을 따르며, subscriber 별로 독립된 큐 사본을 받습니다.

### 4) 대시보드

```bash
cc-messagebus dashboard
# 브라우저: http://127.0.0.1:5959/dashboard
```

좌측 패널은 4-카테고리 트리(`[ALL ACTIVITY]` / 토픽 / DM / 에이전트)로 구성되며, 카테고리·항목별 카운트와 마지막 활동 시각이 SSE 로 실시간 갱신됩니다. 채널을 선택하면 우측이 detail view (헤더 + subscriber 카드 + history) 로 전환되어 `queueDepth` / `lastReadAt` / 신규 발행 이벤트가 즉시 반영됩니다. 그 외 선택 시에는 메시지 라이프사이클(`sent / read / acked / redelivered / expired`) 흐름이 push 됩니다.

## CLI Reference

| Command | Description |
|---|---|
| `cc-messagebus serve` | Broker daemon 시작 (HTTP RPC + SSE) |
| `cc-messagebus mcp` | MCP stdio adapter (Claude Code 가 spawn) |
| `cc-messagebus tail <topicId>` | 토픽 SSE 구독, 수신 메시지를 stdout 라인으로 push |
| `cc-messagebus dashboard` | Dashboard URL 출력 |

## MCP Tools

### 1:1 Direct

| Tool | Purpose |
|---|---|
| `register` | 토픽 등록. 응답에 `monitorCommand` 포함 (Claude 가 자동 실행) |
| `unregister` | 토픽 해제. `purgeQueue` 옵션으로 큐 함께 비우기 |
| `send` | 다른 토픽에 메시지 전송 (1:1) |
| `read` | 자기 큐에서 메시지 pull (visibility timeout 시작) |
| `ack` | 메시지 처리 완료 마킹 (생략 시 timeout 후 재배달) |
| `list_peers` | 등록된 토픽 목록 조회 (`lastActivityAt DESC NULLS LAST` 정렬) |

### Channels (Pub-Sub)

| Tool | Purpose |
|---|---|
| `list_channels` | 채널 메타 목록 + `subscriberCount` + `lastPublishedAt` |
| `channel_create` | 채널 생성 (이미 존재하면 idempotent) |
| `channel_subscribe` | 채널 구독. 이후 본인 큐로 fan-out 사본 수신 (replay 없음, history 는 `channel_history` 로) |
| `channel_send` | 채널에 발행 → 모든 subscriber 큐에 fan-out 사본 |
| `channel_unsubscribe` | 채널 구독 해제 |
| `channel_history` | 채널 backlog 조회 (`limit`, `beforeSentAt` paging) |
| `channel_detail` | 채널 헤더 + subscriber 별 `queueDepth` / `lastReadAt` |

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

## Not Building (v0.1)

- 인증 / 인가 / TLS — 로컬(`127.0.0.1`) 한정
- 메시지 암호화
- 메시지 priority / scheduling — FIFO
- 파일 첨부 — 텍스트 payload 만
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
