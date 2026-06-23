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
| Transports | HTTP RPC (`POST /api/*`), SSE 2종 (`/tail/:peerId`, `/events`), 정적 dashboard (`/dashboard`) |
| 인증 | v1 미적용. 외부 차단은 loopback bind 로 달성 |
| 직렬화 | JSON. 모든 시각은 ISO 8601 UTC 문자열 (`new Date().toISOString()`) |
| 메시지 ID | UUID v4 (`crypto.randomUUID()`) |

타입 import 진입점:

```ts
import { /* ... */ } from "../protocol/index.js";
```

용어:
- **peer** — Claude 세션의 식별자 (`peerId`). `register` 로 등록.
- **DM** — peer 간 1:1 메시지 (`send` / `read` / `ack`).
- **topic** — pub-sub 채널 (`topic_create` / `topic_subscribe` / `topic_send`). 구독한 peer 의 inbox 로 fan-out.

---

## 2. HTTP RPC

모든 endpoint 는 `POST`. 본문은 JSON, 응답은 `ApiResponse<T>` envelope.

| Method | Path | Request type | Response type | 의미 |
| --- | --- | --- | --- | --- |
| POST | `/api/register` | `RegisterRequest` | `RegisterResponse` | peerId 신규 등록. 중복 시 `PEER_ALREADY_REGISTERED` (409) |
| POST | `/api/unregister` | `UnregisterRequest` | `UnregisterResponse` | 본인 peer 제거. `purgeQueue=true` 시 큐도 삭제 |
| POST | `/api/send` | `SendRequest` | `SendResponse` | DM enqueue. 수신자 offline 이어도 큐에 적재 |
| POST | `/api/read` | `ReadRequest` | `ReadResponse` | unacked DM fetch (in-flight 로 마킹) |
| POST | `/api/ack` | `AckRequest` | `AckResponse` | in-flight DM 영구 ack |
| POST | `/api/list_peers` | `ListPeersRequest` | `ListPeersResponse` | 등록된 peer 전체 목록 + 상태 |
| POST | `/api/list_topics` | `ListTopicsRequest` | `ListTopicsResponse` | 모든 topic 요약 (subscriberCount, lastPublishedAt) |
| POST | `/api/topic_create` | `TopicCreateRequest` | `TopicCreateResponse` | topic 신규 생성. 중복 시 `TOPIC_ALREADY_EXISTS` (409) |
| POST | `/api/topic_subscribe` | `TopicSubscribeRequest` | `TopicSubscribeResponse` | peerId 를 topic 에 구독. 중복 시 `ALREADY_SUBSCRIBED` |
| POST | `/api/topic_send` | `TopicSendRequest` | `TopicSendResponse` | topic 에 publish — 구독자 inbox 로 fan-out (publisher 제외) |
| POST | `/api/topic_unsubscribe` | `TopicUnsubscribeRequest` | `TopicUnsubscribeResponse` | 구독 해지. 미구독 시 `NOT_SUBSCRIBED` |
| POST | `/api/topic_history` | `TopicHistoryRequest` | `TopicHistoryResponse` | topic 의 canonical 메시지 이력 (sentAt desc + 페이지네이션) |
| POST | `/api/topic_detail` | `TopicDetailRequest` | `TopicDetailResponse` | topic 의 구독자 + per-subscriber 큐 통계 |

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
{ "peerId": "saturn" }
```

Response (성공):
```json
{
  "ok": true,
  "peerId": "saturn",
  "monitorCommand": "cc-messagebus tail saturn",
  "dashboardUrl": "http://localhost:5959"
}
```

### 2.2 `/api/unregister`

Request:
```json
{ "peerId": "saturn", "purgeQueue": false }
```

Response:
```json
{ "ok": true, "purged": false }
```

### 2.3 `/api/send` (DM)

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
{ "peerId": "carme", "max": 50 }
```

Response: `{ ok: true, messages: MessageDto[] }`. DM 과 topic fan-out 메시지가 동일 inbox 에 섞여 도착한다.

### 2.5 `/api/ack`

Request:
```json
{ "peerId": "carme", "messageId": "550e8400-e29b-41d4-a716-446655440000" }
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
      "peerId": "saturn",
      "status": "connected",
      "connectedAt": "2026-06-17T05:20:00.000Z",
      "lastSeenAt": "2026-06-17T05:23:01.123Z",
      "lastActivityAt": "2026-06-17T05:23:01.123Z",
      "queueLength": 0
    }
  ]
}
```

### 2.7 `/api/list_topics`

Request: `{}`

Response: `{ ok: true, topics: TopicSummaryDto[] }`. 정렬: 최근 publish 순 (publish 없으면 createdAt 으로 tie-break, 끝).

### 2.8 `/api/topic_create`

Request:
```json
{ "topicId": "release-coord", "createdBy": "saturn" }
```

Response: `{ ok: true, topic: { topicId, createdBy, createdAt } }`.

### 2.9 `/api/topic_subscribe`

Request:
```json
{ "topicId": "release-coord", "peerId": "carme" }
```

Response: `{ ok: true, subscribedAt: "..." }`. 과거 메시지는 replay 되지 않는다 — `topic_history` 별도 호출.

### 2.10 `/api/topic_send`

Request:
```json
{ "topicId": "release-coord", "from": "saturn", "subject": "v0.2", "body": "rc1 ready" }
```

Response:
```json
{
  "ok": true,
  "topicMessageId": "tm-uuid",
  "deliveredTo": ["carme", "europa"],
  "sentAt": "2026-06-17T..."
}
```

Fan-out 의미론: 1 canonical row + N-1 inbox copies (publisher 자기 자신은 delivery 에서 제외). atomic transaction.

### 2.11 `/api/topic_unsubscribe`

Request: `{ "topicId": "release-coord", "peerId": "carme" }`. 이미 inbox 에 도착한 메시지는 그대로 유지 (ack 가능).

### 2.12 `/api/topic_history`

Request:
```json
{ "topicId": "release-coord", "limit": 50, "beforeSentAt": "2026-06-17T..." }
```

Response: `{ ok: true, messages: TopicMessageDto[], hasMore: boolean }`. canonical 메시지를 sentAt desc 정렬, cursor 페이지네이션.

### 2.13 `/api/topic_detail`

Request: `{ "topicId": "release-coord" }`

Response: `{ ok: true, topic: { topicId, createdBy, createdAt, subscribers: [{ peerId, subscribedAt, queueDepth, lastReadAt }] } }`.

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

### 3.1 `/tail/:peerId` (수신자용, Monitor 가 구독)

| `type` | 발생 시점 | Payload | TypeScript |
| --- | --- | --- | --- |
| `message_delivered` | 해당 peer 의 inbox 로 메시지 enqueue 직후 push (DM + topic fan-out 모두) | `{ message: MessageDto }` | `MessageDeliveredEvent` |
| `heartbeat` | 15초 주기 | `{ at: IsoTimestamp }` | `TailHeartbeatEvent` |

Union: `TailEvent`.

### 3.2 `/events` (dashboard 용)

| `type` | 발생 시점 | Payload | TypeScript |
| --- | --- | --- | --- |
| `session_snapshot` | 신규 dashboard 연결 직후 1회 | `{ peers: PeerDto[], at }` | `SessionSnapshotEvent` |
| `session_registered` | `POST /api/register` 성공 | `{ peer: PeerDto }` | `SessionRegisteredEvent` |
| `session_disconnected` | `/tail/:peerId` SSE close 감지 | `{ peerId, at }` | `SessionDisconnectedEvent` |
| `message_sent` | `POST /api/send` 성공 | `{ message: MessageDto }` | `MessageSentEvent` |
| `message_read` | `POST /api/read` 응답에 포함된 각 메시지마다 | `{ messageId, peerId, at }` | `MessageReadEvent` |
| `message_acked` | `POST /api/ack` 성공 | `{ messageId, peerId, at }` | `MessageAckedEvent` |
| `message_redelivered` | in-flight 타임아웃으로 deliverable 복귀 | `{ messageId, at }` | `MessageRedeliveredEvent` |
| `message_expired` | TTL cleanup 으로 삭제 | `{ messageId, at }` | `MessageExpiredEvent` |
| `topic_created` | `POST /api/topic_create` 성공 | `{ topic: TopicDto }` | `TopicCreatedEvent` |
| `topic_subscribed` | `POST /api/topic_subscribe` 성공 | `{ topicId, peerId, at }` | `TopicSubscribedEvent` |
| `topic_unsubscribed` | `POST /api/topic_unsubscribe` 성공 | `{ topicId, peerId, at }` | `TopicUnsubscribedEvent` |
| `topic_message_published` | `POST /api/topic_send` 성공 | `{ topicId, topicMessageId, from, deliveredTo, sentAt }` | `TopicMessagePublishedEvent` |
| `heartbeat` | 15초 주기 | `{ at: IsoTimestamp }` | `DashboardHeartbeatEvent` |

Union: `DashboardEvent`.

---

## 4. MCP Tools

MCP adapter 는 stateful — `register` 시 peerId 를 메모리에 보관하고, 다른 tool 호출 시 HTTP RPC 에 자동 주입한다. 따라서 **MCP tool input 은 caller `peerId` 와 `from` 을 생략**한다.

| MCP name | Input type | Output type |
| --- | --- | --- |
| `register` | `RegisterToolInput` `{ peerId }` | `RegisterToolOutput` (= `RegisterResponse`) |
| `unregister` | `UnregisterToolInput` `{ purgeQueue? }` | `UnregisterToolOutput` |
| `send` | `SendToolInput` `{ to, subject, body, threadId? }` | `SendToolOutput` |
| `read` | `ReadToolInput` `{ max? }` | `ReadToolOutput` |
| `ack` | `AckToolInput` `{ messageId }` | `AckToolOutput` |
| `list_peers` | `ListPeersToolInput` `{}` | `ListPeersToolOutput` |
| `list_topics` | `ListTopicsToolInput` `{}` | `ListTopicsToolOutput` |
| `topic_create` | `TopicCreateToolInput` `{ topicId }` | `TopicCreateToolOutput` |
| `topic_subscribe` | `TopicSubscribeToolInput` `{ topicId }` | `TopicSubscribeToolOutput` |
| `topic_send` | `TopicSendToolInput` `{ topicId, subject, body }` | `TopicSendToolOutput` |
| `topic_unsubscribe` | `TopicUnsubscribeToolInput` `{ topicId }` | `TopicUnsubscribeToolOutput` |
| `topic_history` | `TopicHistoryToolInput` `{ topicId, limit?, beforeSentAt? }` | `TopicHistoryToolOutput` |
| `topic_detail` | `TopicDetailToolInput` `{ topicId }` | `TopicDetailToolOutput` |

Descriptions (코드의 `MCP_TOOL_DESCRIPTIONS` 와 byte 단위 동일):

- `register`: Register this Claude session under a peerId on the cc-messagebus broker. After register succeeds, you MUST invoke the Monitor tool with the returned `monitorCommand` so that incoming messages are delivered to this session.
- `unregister`: Unregister the current session from the broker. By default the message queue is preserved; pass purgeQueue=true to delete it.
- `send`: Send a 1:1 DM to another registered peer. Target may be offline — the broker queues until delivery.
- `read`: Fetch unacked DMs for this session. Each returned message enters in-flight state and must be ack-ed within the visibility timeout (default 30s) or it will be redelivered.
- `ack`: Acknowledge a previously read DM by id. Until ack, the message stays in-flight and may be redelivered.
- `list_peers`: List all registered peers and their connection status.
- `list_topics`: List all pub-sub topics with subscriber count and last published timestamp. Sorted by most-recently-active first; topics with no messages appear last (creation time as tie-breaker).
- `topic_create`: Create a new pub-sub topic. The current session's peerId is recorded as createdBy. Returns TOPIC_ALREADY_EXISTS if the topicId is taken.
- `topic_subscribe`: Subscribe the current session's peerId to a topic. After subscribe succeeds, future topic_send messages will arrive in this session's inbox via the same read/ack flow as 1:1 DMs — the existing Monitor process keeps delivering them. Does NOT replay past messages; use topic_history for that. Returns ALREADY_SUBSCRIBED on duplicate.
- `topic_send`: Publish a message to all current subscribers of the topic. The broker performs an atomic fan-out: 1 canonical row plus N-1 inbox copies (the publisher itself is excluded from delivery). Returns the canonical topicMessageId and the list of recipient peerIds.
- `topic_unsubscribe`: Unsubscribe the current session's peerId from a topic. Already-delivered messages in the inbox are preserved (still ackable). Returns NOT_SUBSCRIBED if no active subscription.
- `topic_history`: Pull past canonical messages of a topic for late-joining context. Returns up to `limit` messages (default broker-decided) ordered by sentAt desc. Use `beforeSentAt` as a cursor for pagination.
- `topic_detail`: Inspect a topic's subscribers with per-subscriber queue stats (queueDepth, lastReadAt). No ACL — any session can read. Returns TOPIC_NOT_FOUND if the topicId does not exist.

> WARNING — `register` 와 `topic_subscribe` description 은 wire-critical. register flow 는 Claude 가 자동으로 Monitor tool 을 호출하는 데 의존하고, topic_subscribe flow 는 replay 없음 + `topic_history` 별도 호출 이해에 의존한다. 의미 변경/번역 시 모델 동작 재검증 필수.

Input JSON Schema (Fastify route schema 와 공유) 는 `MCP_INPUT_SCHEMAS` (`src/protocol/mcp.ts`) 에 정의. 주요 제한:

| 필드 | 제약 |
| --- | --- |
| `peerId` | string, 1–64 |
| `topicId` | string, 1–64 |
| `subject` | string, 1–256 |
| `body` | string, ≤ 65536 |
| `threadId` | string, ≤ 64 (optional) |
| `messageId` | string, 1–64 |
| `max` | integer, 1–200 (optional) |
| `limit` | integer, 1–200 (optional, topic_history) |

---

## 5. Errors

`ErrorCode` (`src/protocol/errors.ts`) ↔ HTTP status 매핑:

| ErrorCode | HTTP | 발생 시점 | 권장 대응 |
| --- | --- | --- | --- |
| `PEER_ALREADY_REGISTERED` | 409 | `register` 시 동일 peerId 가 이미 등록됨 | 다른 peerId 사용 또는 기존 세션 unregister 후 재시도 |
| `PEER_NOT_FOUND` | 404 | `unregister/read/ack/send` 시 peerId (또는 `to`) 가 미등록 | `register` 먼저 호출 |
| `MESSAGE_NOT_FOUND` | 404 | `ack` 시 messageId 가 DB 에 없음 (이미 TTL 만료) | 재시도 무의미. 무시 |
| `MESSAGE_NOT_IN_FLIGHT` | 409 | `ack` 시 메시지가 in-flight 가 아님 (이미 ack 됨 또는 visibility 만료로 deliverable 복귀) | 재시도 무의미. 다음 `read` 에서 재수신 시 다시 ack |
| `TOPIC_NOT_FOUND` | 404 | `topic_subscribe/send/unsubscribe/history/detail` 시 topicId 가 없음 | `topic_create` 먼저 호출 |
| `TOPIC_ALREADY_EXISTS` | 409 | `topic_create` 시 동일 topicId 가 이미 존재 | 다른 topicId 사용 |
| `ALREADY_SUBSCRIBED` | 409 | `topic_subscribe` 시 이미 구독 상태 | 무시 (멱등으로 취급) |
| `NOT_SUBSCRIBED` | 404 | `topic_unsubscribe` 시 구독 이력 없음 | 무시 (멱등으로 취급) |
| `VALIDATION_FAILED` | 400 | JSON Schema 검증 실패 | request payload 수정 |
| `INTERNAL_ERROR` | 500 | 처리되지 않은 예외 | 로그 확인. 재시도는 idempotent 한 호출 (ack 등) 에서만 안전 |

응답 본문:

```json
{ "ok": false, "error": { "code": "PEER_ALREADY_REGISTERED", "message": "peer 'saturn' is already registered", "details": null } }
```

---

## 6. Data Lifecycle (요약)

```
send (DM) ─┐
            ├─▶ inbox(peer) ─ deliverable
topic_send ─┘       │
              read  │ (POST /api/read)
                    ▼
              in_flight  ─── ack ──▶ acked  ─── TTL ──▶ expired (cleanup)
                    │
       visibility   │ timeout (default 30s)
                    ▼
              deliverable (재시도)
```

- **at-least-once delivery** — visibility timeout 동안 ack 없으면 deliverable 로 복귀, 재배달. 중복 수신은 가능, 누락은 없음.
- **visibility timeout**: 기본 30초 (broker config)
- **TTL**: 기본 30일 (broker config). ack 여부 무관 보존 → 만료 후 cleanup 이 삭제
- **`unregister({ purgeQueue: true })`**: 본인 inbox 만 즉시 삭제 (TTL 무시)
- **disconnect**: `/tail/:peerId` SSE close → peer `disconnected` 마킹, 큐 보존, 재등록 시 backlog 자동 전달
- **topic fan-out**: `topic_send` 는 canonical 1행 + 구독자 N-1 inbox 복사본을 atomic 트랜잭션으로 INSERT. publisher 자신은 자기 inbox 에 복사본을 받지 않는다.
- **topic history vs replay**: `topic_subscribe` 는 과거 메시지를 replay 하지 않음. 신규 구독자는 `topic_history` 로 명시적으로 pull 해야 함.

대응 dashboard 이벤트: 위 라이프사이클의 각 전이가 §3.2 의 `message_*` / `topic_*` 이벤트 1개로 1:1 발화.

---

## 7. Examples

A 세션이 B 에게 DM 을 보내고 B 가 받아서 ack 하는 시나리오:

```bash
# 1) A 등록
curl -sX POST http://127.0.0.1:5959/api/register \
  -H 'Content-Type: application/json' \
  -d '{"peerId":"saturn"}'
# => { "ok": true, "peerId": "saturn", "monitorCommand": "cc-messagebus tail saturn", "dashboardUrl": "http://localhost:5959" }

# 2) B 등록 (별도 세션)
curl -sX POST http://127.0.0.1:5959/api/register \
  -H 'Content-Type: application/json' \
  -d '{"peerId":"carme"}'

# 3) A → B 송신
curl -sX POST http://127.0.0.1:5959/api/send \
  -H 'Content-Type: application/json' \
  -d '{"from":"saturn","to":"carme","subject":"ping","body":"hello"}'
# => { "ok": true, "messageId": "<uuid>", "sentAt": "2026-06-17T..." }

# 4) B 가 fetch (in-flight 진입)
curl -sX POST http://127.0.0.1:5959/api/read \
  -H 'Content-Type: application/json' \
  -d '{"peerId":"carme","max":10}'

# 5) B 가 명시적 ack
curl -sX POST http://127.0.0.1:5959/api/ack \
  -H 'Content-Type: application/json' \
  -d '{"peerId":"carme","messageId":"<uuid>"}'
```

B 가 SSE 로 push 받으려면 step 2 직후:

```bash
curl -N http://127.0.0.1:5959/tail/carme
# event: message_delivered
# data: { "type": "message_delivered", "message": { ... } }
```

Topic pub-sub 시나리오:

```bash
# 1) A 가 topic 생성
curl -sX POST http://127.0.0.1:5959/api/topic_create \
  -H 'Content-Type: application/json' \
  -d '{"topicId":"release-coord","createdBy":"saturn"}'

# 2) B 가 구독
curl -sX POST http://127.0.0.1:5959/api/topic_subscribe \
  -H 'Content-Type: application/json' \
  -d '{"topicId":"release-coord","peerId":"carme"}'

# 3) A 가 publish (B inbox 로 fan-out)
curl -sX POST http://127.0.0.1:5959/api/topic_send \
  -H 'Content-Type: application/json' \
  -d '{"topicId":"release-coord","from":"saturn","subject":"v0.2","body":"rc1 ready"}'
# => { "ok": true, "topicMessageId": "<uuid>", "deliveredTo": ["carme"], "sentAt": "..." }

# 4) B 는 기존 /api/read 로 fan-out 메시지를 받음 (DM 과 동일 inbox)
```
