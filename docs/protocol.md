# cc-messagebus Protocol Spec

> 본 문서는 `src/protocol/*.ts` 의 타입 정의를 사람이 빠르게 참조할 수 있도록 정리한 것이다.
> 의미론이 충돌하면 **`prd.md` 가 source of truth** 이고, 그 다음이 `src/protocol/*.ts`, 마지막이 본 문서.

---

## 1. Overview

| 항목 | 값 |
| --- | --- |
| 프로세스 | 단일 Node daemon (`cc-messagebus serve`) |
| 기본 bind | `127.0.0.1:5959` |
| 데이터 저장 | `~/.cc-messagebus/data.db` (SQLite, better-sqlite3) |
| Transports | HTTP RPC (`POST /api/*`), SSE 2종 (`/tail/:topicId`, `/events`), 정적 dashboard (`/dashboard`) |
| 인증 | v1 미적용. 외부 차단은 loopback bind 로 달성 |
| 직렬화 | JSON. 모든 시각은 ISO 8601 UTC 문자열 (`new Date().toISOString()`) |
| 메시지 ID | UUID v4 (`crypto.randomUUID()`) |

타입 import 진입점:

```ts
import { /* ... */ } from "../protocol/index.js";
```

---

## 2. HTTP RPC

모든 endpoint 는 `POST`. 본문은 JSON, 응답은 `ApiResponse<T>` envelope.

| Method | Path | Request type | Response type | 의미 |
| --- | --- | --- | --- | --- |
| POST | `/api/register` | `RegisterRequest` | `RegisterResponse` | topicId 신규 등록. 중복 시 `TOPIC_ALREADY_REGISTERED` (409) |
| POST | `/api/unregister` | `UnregisterRequest` | `UnregisterResponse` | 본인 topic 제거. `purgeQueue=true` 시 큐도 삭제 |
| POST | `/api/send` | `SendRequest` | `SendResponse` | 메시지 enqueue. 수신자 offline 이어도 큐에 적재 |
| POST | `/api/read` | `ReadRequest` | `ReadResponse` | unacked 메시지 fetch (in-flight 로 마킹) |
| POST | `/api/ack` | `AckRequest` | `AckResponse` | in-flight 메시지 영구 ack |
| POST | `/api/list_peers` | `ListPeersRequest` | `ListPeersResponse` | 등록된 topic 전체 목록 + 상태 |

### Response envelope

```ts
type ApiResponse<T> =
  | ({ ok: true } & T)
  | { ok: false; error: { code: ErrorCode; message: string; details?: unknown } };
```

HTTP status 는 §5 의 `ErrorCode → status` 매핑을 따른다. 성공은 항상 `200`.

### 2.1 `/api/register`

Request:
```json
{ "topicId": "saturn" }
```

Response (성공):
```json
{
  "ok": true,
  "topicId": "saturn",
  "monitorCommand": "cc-messagebus tail saturn",
  "dashboardUrl": "http://localhost:5959"
}
```

### 2.2 `/api/unregister`

Request:
```json
{ "topicId": "saturn", "purgeQueue": false }
```

Response:
```json
{ "ok": true, "purged": false }
```

### 2.3 `/api/send`

Request:
```json
{
  "from": "saturn",
  "to": "carme",
  "subject": "ping",
  "body": "hello",
  "threadId": "t-001"
}
```

Response:
```json
{ "ok": true, "messageId": "550e8400-e29b-41d4-a716-446655440000", "sentAt": "2026-06-17T05:23:01.123Z" }
```

### 2.4 `/api/read`

Request:
```json
{ "topicId": "carme", "max": 50 }
```

Response:
```json
{
  "ok": true,
  "messages": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "from": "saturn",
      "to": "carme",
      "subject": "ping",
      "body": "hello",
      "threadId": "t-001",
      "sentAt": "2026-06-17T05:23:01.123Z",
      "inFlightUntil": "2026-06-17T05:23:31.123Z",
      "ackedAt": null,
      "expiresAt": "2026-07-17T05:23:01.123Z"
    }
  ]
}
```

### 2.5 `/api/ack`

Request:
```json
{ "topicId": "carme", "messageId": "550e8400-e29b-41d4-a716-446655440000" }
```

Response:
```json
{ "ok": true, "ackedAt": "2026-06-17T05:23:10.456Z" }
```

### 2.6 `/api/list_peers`

Request: `{}`

Response:
```json
{
  "ok": true,
  "peers": [
    {
      "topicId": "saturn",
      "status": "connected",
      "connectedAt": "2026-06-17T05:20:00.000Z",
      "lastSeenAt": "2026-06-17T05:23:01.123Z",
      "queueLength": 0
    }
  ]
}
```

---

## 3. SSE Events

SSE wire-format (양 스트림 공통):

```
event: <type>
data: <json-one-line>
id: <optional-event-id>

```

- 라인 종결자: `\n`
- 이벤트 종결자: 빈 줄 (`\n\n`)
- `data:` 는 한 줄 JSON. 멀티라인 분할 금지
- 직렬화 헬퍼: `serializeSseEvent(event, id?)` (`src/protocol/sse.ts`)
- heartbeat 주기: 15 초 (`SSE_HEARTBEAT_INTERVAL_SEC`)
- 연결 종료 감지: client side close → broker 가 cleanup 트리거

### 3.1 `/tail/:topicId` (수신자용, Monitor 가 구독)

| `type` | 발생 시점 | Payload | TypeScript |
| --- | --- | --- | --- |
| `message_delivered` | 해당 topic 으로 메시지 enqueue 직후 push | `{ message: MessageDto }` | `MessageDeliveredEvent` |
| `heartbeat` | 15초 주기 | `{ at: IsoTimestamp }` | `TailHeartbeatEvent` |

Union: `TailEvent`.

### 3.2 `/events` (dashboard 용)

| `type` | 발생 시점 | Payload | TypeScript |
| --- | --- | --- | --- |
| `session_registered` | `POST /api/register` 성공 | `{ peer: PeerDto }` | `SessionRegisteredEvent` |
| `session_disconnected` | `/tail/:topicId` SSE close 감지 | `{ topicId, at }` | `SessionDisconnectedEvent` |
| `message_sent` | `POST /api/send` 성공 | `{ message: MessageDto }` | `MessageSentEvent` |
| `message_read` | `POST /api/read` 응답에 포함된 각 메시지마다 | `{ messageId, topicId, at }` | `MessageReadEvent` |
| `message_acked` | `POST /api/ack` 성공 | `{ messageId, topicId, at }` | `MessageAckedEvent` |
| `message_redelivered` | in-flight 타임아웃으로 deliverable 복귀 | `{ messageId, at }` | `MessageRedeliveredEvent` |
| `message_expired` | TTL cleanup 으로 삭제 | `{ messageId, at }` | `MessageExpiredEvent` |
| `heartbeat` | 15초 주기 | `{ at: IsoTimestamp }` | `DashboardHeartbeatEvent` |

Union: `DashboardEvent`.

---

## 4. MCP Tools

MCP adapter 는 stateful — `register` 시 topicId 를 메모리에 보관하고, 다른 tool 호출 시 HTTP RPC 에 자동 주입한다. 따라서 **MCP tool input 은 `topicId` 와 `from` 을 생략**한다.

| MCP name | Input type | Output type |
| --- | --- | --- |
| `register` | `RegisterToolInput` `{ topicId }` | `RegisterToolOutput` (= `RegisterResponse`) |
| `unregister` | `UnregisterToolInput` `{ purgeQueue? }` | `UnregisterToolOutput` |
| `send` | `SendToolInput` `{ to, subject, body, threadId? }` | `SendToolOutput` |
| `read` | `ReadToolInput` `{ max? }` | `ReadToolOutput` |
| `ack` | `AckToolInput` `{ messageId }` | `AckToolOutput` |
| `list_peers` | `ListPeersToolInput` `{}` | `ListPeersToolOutput` |

Descriptions (코드의 `MCP_TOOL_DESCRIPTIONS` 와 byte 단위 동일):

- `register`: Register this Claude session under a topicId on the cc-messagebus broker. After register succeeds, you MUST invoke the Monitor tool with the returned `monitorCommand` so that incoming messages are delivered to this session.
- `unregister`: Unregister the current session from the broker. By default the message queue is preserved; pass purgeQueue=true to delete it.
- `send`: Send a message to another registered topic. Target may be offline — the broker queues until delivery.
- `read`: Fetch unacked messages for this session. Each returned message enters in-flight state and must be ack-ed within the visibility timeout (default 30s) or it will be redelivered.
- `ack`: Acknowledge a previously read message by id. Until ack, the message stays in-flight and may be redelivered.
- `list_peers`: List all registered topics and their connection status.

> WARNING — `register` description 은 wire-critical. Phase 6 동작이 이 문구를 Claude 가 읽고 Monitor tool 을 자동 호출하는 데 의존한다. 의미 변경/번역 시 모델 동작 재검증 필수.

Input JSON Schema (Fastify route schema 와 공유) 는 `MCP_INPUT_SCHEMAS` (`src/protocol/mcp.ts`) 에 정의. 주요 제한:

| 필드 | 제약 |
| --- | --- |
| `topicId` | string, 1–64 |
| `subject` | string, 1–256 |
| `body` | string, ≤ 65536 |
| `threadId` | string, ≤ 64 (optional) |
| `messageId` | string, 1–64 |
| `max` | integer, 1–200 (optional) |

---

## 5. Errors

`ErrorCode` (`src/protocol/errors.ts`) ↔ HTTP status 매핑:

| ErrorCode | HTTP | 발생 시점 | 권장 대응 |
| --- | --- | --- | --- |
| `TOPIC_ALREADY_REGISTERED` | 409 | `register` 시 동일 topicId 가 이미 등록됨 | 다른 topicId 사용 또는 기존 세션 unregister 후 재시도 |
| `TOPIC_NOT_FOUND` | 404 | `unregister/read/ack` 시 caller topicId 가 미등록 | `register` 먼저 호출 |
| `PEER_NOT_FOUND` | 404 | `send` 시 `to` topic 이 한 번도 등록된 적 없음 | 수신자에게 register 요청 후 재시도. 또는 큐만 적재할지 정책 결정 필요 |
| `MESSAGE_NOT_FOUND` | 404 | `ack` 시 messageId 가 DB 에 없음 (이미 TTL 만료) | 재시도 무의미. 무시 |
| `MESSAGE_NOT_IN_FLIGHT` | 409 | `ack` 시 메시지가 in-flight 상태가 아님 (이미 ack 됨 또는 visibility 만료로 deliverable 복귀) | 재시도 무의미. 다음 `read` 에서 재수신 시 다시 ack |
| `VALIDATION_FAILED` | 400 | JSON Schema 검증 실패 | request payload 수정 |
| `INTERNAL_ERROR` | 500 | 처리되지 않은 예외 | 로그 확인. 재시도는 idempotent 한 호출 (ack 등) 에서만 안전 |

응답 본문:

```json
{ "ok": false, "error": { "code": "TOPIC_ALREADY_REGISTERED", "message": "topic 'saturn' is already registered", "details": null } }
```

---

## 6. Data Lifecycle (요약)

```
send → [enqueue] → deliverable
                       │
                  read │ (POST /api/read)
                       ▼
                  in_flight  ─── ack ──▶ acked  ─── TTL ──▶ expired (cleanup)
                       │
            visibility │ timeout (default 30s)
                       ▼
                  deliverable (재시도)
```

- **at-least-once delivery** — visibility timeout 동안 ack 없으면 deliverable 로 복귀, 재배달. 중복 수신은 가능, 누락은 없음.
- **visibility timeout**: 기본 30초 (broker config)
- **TTL**: 기본 30일 (broker config). ack 여부 무관 보존 → 만료 후 cleanup 이 삭제
- **`unregister({ purgeQueue: true })`**: 본인 큐만 즉시 삭제 (TTL 무시)
- **disconnect**: `/tail/:topicId` SSE close → session `disconnected` 마킹, 큐 보존, 재등록 시 backlog 자동 전달

대응 dashboard 이벤트: 위 라이프사이클의 각 전이가 §3.2 의 `message_*` 이벤트 1개로 1:1 발화.

---

## 7. Examples

A 세션이 B 에게 메시지를 보내고 B 가 받아서 ack 하는 시나리오:

```bash
# 1) A 등록
curl -sX POST http://127.0.0.1:5959/api/register \
  -H 'Content-Type: application/json' \
  -d '{"topicId":"saturn"}'
# => { "ok": true, "topicId": "saturn", "monitorCommand": "cc-messagebus tail saturn", "dashboardUrl": "http://localhost:5959" }

# 2) B 등록 (별도 세션)
curl -sX POST http://127.0.0.1:5959/api/register \
  -H 'Content-Type: application/json' \
  -d '{"topicId":"carme"}'

# 3) A → B 송신
curl -sX POST http://127.0.0.1:5959/api/send \
  -H 'Content-Type: application/json' \
  -d '{"from":"saturn","to":"carme","subject":"ping","body":"hello"}'
# => { "ok": true, "messageId": "<uuid>", "sentAt": "2026-06-17T..." }

# 4) B 가 fetch (in-flight 진입)
curl -sX POST http://127.0.0.1:5959/api/read \
  -H 'Content-Type: application/json' \
  -d '{"topicId":"carme","max":10}'
# => { "ok": true, "messages": [ { "id": "<uuid>", ..., "inFlightUntil": "...", "ackedAt": null } ] }

# 5) B 가 명시적 ack
curl -sX POST http://127.0.0.1:5959/api/ack \
  -H 'Content-Type: application/json' \
  -d '{"topicId":"carme","messageId":"<uuid>"}'
# => { "ok": true, "ackedAt": "2026-06-17T..." }
```

B 가 SSE 로 push 받으려면 step 2 직후:

```bash
curl -N http://127.0.0.1:5959/tail/carme
# event: message_delivered
# data: { "type": "message_delivered", "message": { ... } }
#
# event: heartbeat
# data: { "type": "heartbeat", "at": "..." }
```
