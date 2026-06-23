# cc-messagebus

Claude Code 세션 간 메시지 큐 + Pub-Sub 토픽 + 실시간 대시보드. 단일 Node daemon + SQLite 한 파일로 동작하며 외부 인프라(Redis/RabbitMQ 등) 의존성이 없습니다.

- **DM (1:1 메시지 큐)**: at-least-once, visibility timeout 30s, TTL 30일
- **Pub-Sub Topic**: N:M fan-out, subscriber 별 독립 큐 + read/ack 라이프사이클, history 조회
- **MCP stdio adapter**: Claude Code 에 DM 도구 (`register / send / read / ack / list_peers / unregister`) + 토픽 도구 (`list_topics / topic_create / topic_subscribe / topic_send / topic_unsubscribe / topic_history / topic_detail`) 노출
- **SSE 알림**: `cc-messagebus tail <peerId>` 가 stdout 으로 라인 푸시 → Claude `Monitor` 도구가 즉시 수신
- **대시보드**: `http://127.0.0.1:5959/dashboard` 에서 등록 peer 목록 + topic 목록 + 메시지 흐름 라이브 관찰

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

### 2) DM (1:1) 흐름 예시

세션 A:
```
register({ peerId: "alice" })
send({ to: "bob", subject: "ping", body: "안녕" })
```

세션 B:
```
register({ peerId: "bob" })
# Monitor 가 자동으로 알림 라인 수신
read({})            # 메시지 fetch (visibility timeout 30s 시작)
ack({ messageId })  # 처리 완료 표시
```

### 3) Topic (Pub-Sub) 흐름 예시

```
# 발행자
topic_create({ topicId: "demo" })
topic_send({ topicId: "demo", subject: "deploy", body: "v0.2.0 released" })

# 구독자 (다수 가능)
topic_subscribe({ topicId: "demo" })
# 이후 본인 peer inbox 로 fan-out 사본이 들어옴 → read / ack 로 소비
topic_history({ topicId: "demo", limit: 50 })   # backlog 조회
topic_unsubscribe({ topicId: "demo" })
```

Topic 은 DM 과 동일한 visibility timeout / TTL 라이프사이클을 따르며, subscriber 별로 독립된 inbox 사본을 받습니다. publisher 자신은 자기 inbox 에 사본을 받지 않습니다.

### 4) 대시보드

```bash
cc-messagebus dashboard
# 브라우저: http://127.0.0.1:5959/dashboard
```

좌측 사이드바는 두 섹션 — `PEERS` (등록 세션 + 마지막 활동 시각 + 상태) 와 `TOPICS` (생성 토픽 + subscriber 수 + 최근 publish 시각) — 가 SSE 로 실시간 갱신됩니다. topic 을 선택하면 우측이 detail view (헤더 + subscriber 카드 + history) 로 전환되어 `queueDepth` / `lastReadAt` / 신규 publish 이벤트가 즉시 반영됩니다. peer 를 선택하면 해당 peer 와 연결된 메시지 라이프사이클(`sent / read / acked / redelivered / expired`) 흐름이 push 됩니다.

## CLI Reference

| Command | Description |
|---|---|
| `cc-messagebus serve` | Broker daemon 시작 (HTTP RPC + SSE) |
| `cc-messagebus mcp` | MCP stdio adapter (Claude Code 가 spawn) |
| `cc-messagebus tail <peerId>` | peer SSE 구독, 수신 메시지를 stdout 라인으로 push |
| `cc-messagebus dashboard` | Dashboard URL 출력 |

## MCP Tools

`register` 호출 후 같은 세션의 모든 도구는 caller `peerId` / `from` 을 암묵적으로 사용하므로 args 에서 생략합니다. 성공 응답은 아래 "Returns" 의 JSON 객체가 그대로 반환되고, 실패 시 `{ ok: false, error: { code, message } }` 가 반환됩니다.

### DM (1:1)

| Tool | Args | Returns | Errors |
|---|---|---|---|
| `register` | `{ peerId }` | `{ peerId, monitorCommand, dashboardUrl }` | `PEER_ALREADY_REGISTERED` |
| `unregister` | `{ purgeQueue?: boolean }` | `{ purged: boolean }` | `PEER_NOT_FOUND` |
| `send` | `{ to, subject, body, threadId? }` | `{ messageId, sentAt }` | `PEER_NOT_FOUND` |
| `read` | `{ max?: number }` (default 50) | `{ messages: MessageDto[] }` | `PEER_NOT_FOUND` |
| `ack` | `{ messageId }` | `{ ackedAt }` | `PEER_NOT_FOUND`, `MESSAGE_NOT_FOUND`, `MESSAGE_NOT_IN_FLIGHT` |
| `list_peers` | `{}` | `{ peers: PeerDto[] }` (`lastActivityAt DESC NULLS LAST`) | — |

### Topics (Pub-Sub)

Topic 은 ACL 이 없습니다 — 누구나 `list_topics` / `topic_history` / `topic_detail` 호출 가능. `topic_subscribe` 는 그 시점 이후 publish 분만 본인 inbox 로 fan-out 합니다 (replay 없음 — backlog 는 `topic_history`).

| Tool | Args | Returns | Errors |
|---|---|---|---|
| `list_topics` | `{}` | `{ topics: TopicSummaryDto[] }` | — |
| `topic_create` | `{ topicId }` | `{ topic: TopicDto }` | `PEER_NOT_FOUND`, `TOPIC_ALREADY_EXISTS` |
| `topic_subscribe` | `{ topicId }` | `{ subscribedAt }` | `PEER_NOT_FOUND`, `TOPIC_NOT_FOUND`, `ALREADY_SUBSCRIBED` |
| `topic_send` | `{ topicId, subject, body }` | `{ topicMessageId, deliveredTo: PeerId[], sentAt }` | `PEER_NOT_FOUND`, `TOPIC_NOT_FOUND` |
| `topic_unsubscribe` | `{ topicId }` | `{ unsubscribedAt }` | `PEER_NOT_FOUND`, `TOPIC_NOT_FOUND`, `NOT_SUBSCRIBED` |
| `topic_history` | `{ topicId, limit?: 1..200 (default 50), beforeSentAt? }` | `{ messages: TopicMessageDto[], hasMore: boolean }` | `TOPIC_NOT_FOUND`, `VALIDATION_FAILED` |
| `topic_detail` | `{ topicId }` | `{ topic: TopicDetailDto }` | `TOPIC_NOT_FOUND` |

### Response shapes

```ts
type IsoTimestamp = string  // ISO-8601 UTC, 예: "2026-06-22T08:54:00.000Z"

interface MessageDto {
  id: string
  from: string
  to: string
  subject: string
  body: string
  threadId: string | null
  sentAt: IsoTimestamp
  inFlightUntil: IsoTimestamp | null  // read 후 visibility timeout 만료 시각
  ackedAt: IsoTimestamp | null
  expiresAt: IsoTimestamp              // TTL 기준 cleanup 시각
}

interface PeerDto {
  peerId: string
  status: "connected" | "disconnected"
  connectedAt: IsoTimestamp
  lastSeenAt: IsoTimestamp
  lastActivityAt: IsoTimestamp | null  // send/read/ack 마지막 시각
  queueLength: number                  // 미-ack 메시지 수
}

interface TopicDto {
  topicId: string
  createdBy: string
  createdAt: IsoTimestamp
}

interface TopicSummaryDto extends TopicDto {
  subscriberCount: number
  lastPublishedAt: IsoTimestamp | null
}

interface SubscriberDto {
  peerId: string
  subscribedAt: IsoTimestamp
  queueDepth: number
  lastReadAt: IsoTimestamp | null
}

interface TopicDetailDto extends TopicDto {
  subscribers: SubscriberDto[]
}

interface TopicMessageDto {
  topicMessageId: string
  topicId: string
  from: string
  subject: string
  body: string
  sentAt: IsoTimestamp
  expiresAt: IsoTimestamp
}
```

### Error codes

| Code | When | Recovery |
|---|---|---|
| `PEER_ALREADY_REGISTERED` | 같은 `peerId` 가 이미 connected | `unregister` 후 재시도 |
| `PEER_NOT_FOUND` | 호출 컨텍스트 peer (caller / from / `to` / subscriber) 미등록 | `register` 부터 |
| `MESSAGE_NOT_FOUND` | `ack` 대상이 본인 inbox 에 없음 (잘못된 id / TTL 만료) | id 검증 또는 무시 |
| `MESSAGE_NOT_IN_FLIGHT` | 이미 acked 또는 한 번도 read 되지 않은 메시지 | 무시 가능 |
| `TOPIC_ALREADY_EXISTS` | `topic_create` 중복 | 무시 가능 (idempotent) |
| `TOPIC_NOT_FOUND` | 토픽 미존재 | `topic_create` 또는 `list_topics` |
| `ALREADY_SUBSCRIBED` | 이미 구독 중 | 무시 가능 |
| `NOT_SUBSCRIBED` | unsubscribe 대상이 구독 상태 아님 | 무시 가능 |
| `VALIDATION_FAILED` | 잘못된 인자 (예: `topic_history.limit` 가 1..200 범위 외) | args 확인 |

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
- **Disconnect 감지**: `tail` SSE close → 해당 peer `disconnected` 마킹 (큐는 보존).
- **Topic fan-out**: `topic_send` 는 canonical 1행 + 구독자 N-1 inbox 복사본을 atomic 트랜잭션으로 INSERT. publisher 자신은 자기 inbox 에 사본을 받지 않음.

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
         │  → Claude Monitor  │                  │  peer·메시지 live   │
         └────────────────────┘                  └─────────────────────┘
```

- **Transport**: HTTP RPC + SSE (WebSocket 미사용)
- **Persistence**: SQLite 단일 파일 (`peers`, `messages`, `topics`, `topic_subscriptions`, `topic_messages` 테이블)
- **Process model**: 단일 daemon, 단일 SQLite writer

## Not Building (v0.2)

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

자세한 PRD는 `prd.md`, wire-level 프로토콜 사양은 `docs/protocol.md` 참조.
