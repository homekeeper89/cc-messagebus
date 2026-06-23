# cc-messagebus PRD

> Cross-session message bus for Claude Code with central observability.

---

## 0. Naming

- 패키지/CLI 이름: **`cc-messagebus`**
- 작업 디렉토리: `~/.cc-messagebus/`
- 기본 포트: `5959`

---

## 1. 목표

같은 머신(기본) 또는 네트워크상의 여러 Claude Code 세션이 메시지를 주고받고, 중앙에서 모든 세션과 메시지 흐름을 실시간 관찰할 수 있게 한다.

---

## 2. 사용자 시나리오

### S1. 설치 (일회성)

`.claude/settings.json`에 한 블록 추가:

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

별도 `npm install` 없이 `npx -y`가 자동 fetch.

### S2. 세션 시작

```
User: "saturn으로 등록"
Claude: register({ peerId: "saturn" })
        ↓ (MCP 내부) bus daemon 자동 spawn → register → 응답
Claude: Monitor({ command: "cc-messagebus tail saturn" })  ← 자동 호출
Claude: "saturn으로 등록 완료. 대시보드: http://localhost:5959"
```

### S3. 메시지 송수신

- A 세션: `send(to, subject, body)` → bus 큐 적재 + 수신측 push
- B 세션: `Monitor`가 즉시 알림 → `read()`로 메시지 가져옴 → 처리 후 `ack(messageId)`

### S4. 세션 제거

- 명시적: `unregister({ purgeQueue?: boolean })`
- 암묵적: `tail` SSE 연결 close 감지 → 상태 `disconnected` 마킹 (큐 보존)

### S5. 중앙 관찰

브라우저로 `http://localhost:5959` → 모든 등록 세션 + 메시지 흐름 실시간 표시.

---

## 3. 기능 요구사항

### 3.1. MCP 도구 (세션 측)

| 도구 | 시그니처 | 동작 |
|------|---------|------|
| `register` | `{ peerId: string }` | 신규 등록. **중복 시 무조건 409 거부** |
| `unregister` | `{ purgeQueue?: boolean }` | 본인 peer 제거. `purgeQueue` default `false` (큐 보존, 재등록 시 backlog 받음) |
| `send` | `{ to, subject, body, threadId? }` | 메시지 전송. target offline이어도 큐 적재 |
| `read` | `{ max?: number }` | 미수신 메시지 가져옴. **자동 ack 없음** (in-flight 상태로 마킹) |
| `ack` | `{ messageId: string }` | 명시적 ack. 호출 전까지 in-flight 유지 |
| `list_peers` | `{}` | 등록된 peer 목록 + 상태 |

#### register 응답 형식
```json
{
  "ok": true,
  "peerId": "saturn",
  "monitorCommand": "cc-messagebus tail saturn",
  "dashboardUrl": "http://localhost:5959"
}
```

MCP register 도구의 description에 *"register 성공 후 반드시 `monitorCommand`를 Monitor 도구로 실행하세요"*를 명시 → Claude가 자동으로 Monitor 호출.

### 3.2. CLI 서브커맨드 (bus 측)

| 커맨드 | 동작 |
|--------|------|
| `cc-messagebus serve` | broker daemon. 명시적 실행 가능, MCP가 자동 spawn하기도 함 |
| `cc-messagebus mcp` | MCP stdio adapter. Claude Code가 spawn |
| `cc-messagebus tail <peerId>` | 구독 → stdout에 line-buffered 출력 (Monitor용) |
| `cc-messagebus dashboard` | 브라우저 자동 오픈 |
| `cc-messagebus status` | daemon 살아있는지, 등록 peer 목록, 큐 길이 표시 |
| `cc-messagebus stop` | daemon 종료 |

### 3.3. Dashboard

단일 페이지 웹 UI. 다음을 실시간 표시:

- **Registered Sessions**: peerId, 접속 시각, 마지막 활동, 큐 길이, 상태(connected/disconnected)
- **Message Flow (live)**: 시각 / from → to / subject / threadId
  - SSE로 실시간 stream
  - 필터: from, to, 검색어
- **Message Detail**: 클릭 시 body, ack 상태, in-flight timeout 등 상세

---

## 4. 비기능 요구사항

| 항목 | 요구 |
|------|------|
| 영속성 | bus daemon 재시작 + 머신 재부팅 후에도 미수신 메시지 보존 |
| 실시간성 | send → 수신측 Monitor 알림까지 100ms 이내 (로컬) |
| Monitor 호환 | `cc-messagebus tail`은 line-buffered stdout (개행 = 알림 1건) |
| 설치 마찰 | `.claude/settings.json`에 한 블록 추가 외 작업 없음 |
| 자동 라이프사이클 | 첫 register 시 daemon 자동 spawn, 마지막 client disconnect 후 idle timeout 후 자동 종료 (옵션) |
| 확장성 | TCP 기반. host/port 지정으로 remote 머신 접속 가능 |
| 관찰성 | broker가 모든 이벤트를 dashboard로 stream + 구조화된 파일 로그 별도 보관 |

---

## 5. 정책 결정

### 5.1. 중복 등록
**무조건 거부 (HTTP 409 의미론).** takeover 패턴 미지원.  
이미 같은 peerId가 등록되어 있으면 새 register는 명확한 에러로 실패.

### 5.2. ack / Redelivery
- `read()`로 가져온 메시지 → `in_flight` 상태로 마킹, `in_flight_until = now + visibility_timeout`
- `ack(messageId)` 호출 → 영구 ack 처리 (TTL 만료까지 보관, 재배달 안 함)
- `in_flight_until` 경과 시까지 ack 없으면 → 자동으로 deliverable 상태로 복귀 (다시 `read()` 대상)
- 의미론: **at-least-once delivery** (중복 가능, 누락 없음)
- 기본 visibility_timeout: 30초 (config)

### 5.3. 메시지 보존 TTL
- **모든 메시지를 TTL 기간 동안 보존** (ack 여부 무관)
- 기본 TTL: **30일** (config로 override 가능)
- TTL 경과 시 백그라운드 cleanup이 삭제
- `unregister({ purgeQueue: true })`로 본인 큐만 즉시 삭제 가능

### 5.4. 세션 disconnect 시 큐 처리
- 해당 peer의 `tail` SSE 연결이 close되면 상태를 `disconnected`로 마킹
- 큐는 보존 (default `purgeQueue: false` 정책과 일치)
- 동일 peerId로 재등록 시 backlog 자동 전달

### 5.5. 인증
- v1: 인증 없음
- **안전장치: bus daemon은 기본 `127.0.0.1`만 bind** (외부 접근 차단)
- remote 모드 활성화는 명시적으로 `--host 0.0.0.0` 필요 (사용자가 위험을 인지하고 켜야 함)
- 추후 v2에서 token 기반 인증 추가 예정

---

## 6. 비범위 (v1 제외)

- 인증/인가, TLS
- 메시지 암호화
- broadcast / fan-out (1:1만 지원) — topics (multi-subscriber pub-sub) 로 해제됨. 자세한 사항은 [.claude/PRPs/prds/channels.prd.md](.claude/PRPs/prds/channels.prd.md) 참조
- 메시지 priority, scheduling
- 파일 첨부
- 메트릭 외부 통합 (Prometheus 등)
- 클러스터링 / HA

---

## 7. 데이터 모델

SQLite 기반. 단일 파일 (`~/.cc-messagebus/data.db`).

### sessions
| 컬럼 | 타입 | 설명 |
|------|------|------|
| peer_id | TEXT PK | 유니크 peer ID |
| connected_at | TEXT | 최초 등록 시각 |
| last_seen_at | TEXT | 마지막 활동 시각 |
| status | TEXT | `connected` / `disconnected` |

### messages
| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | TEXT PK | UUID |
| from_peer_id | TEXT | 발신 |
| to_peer_id | TEXT | 수신 |
| subject | TEXT | 제목 |
| body | TEXT | 본문 |
| thread_id | TEXT NULL | 스레드 식별자 |
| sent_at | TEXT | 발신 시각 |
| in_flight_until | TEXT NULL | in-flight 만료 시각 (null = deliverable) |
| acked_at | TEXT NULL | ack 시각 (null = 미확인) |
| expires_at | TEXT | TTL 만료 시각 (cleanup 대상 판정용) |

인덱스:
- `(to_peer_id, acked_at, in_flight_until)` — deliverable 메시지 빠른 조회
- `(expires_at)` — TTL cleanup용

---

## 8. 아키텍처 개요

```
┌─────────────────────────────────────────────────────────┐
│  cc-messagebus daemon (single process, localhost:5959)         │
│                                                           │
│  - Fastify HTTP server                                  │
│  - HTTP RPC /api/* (MCP 클라이언트 register/send/...)   │
│  - SSE /tail/:peerId (수신 알림 push, tail이 구독)      │
│  - SSE /events (dashboard 라이브 업데이트)              │
│  - HTTP /dashboard (단일 페이지 UI)                     │
│  - SQLite (better-sqlite3)                              │
│  - 백그라운드 cleanup (TTL, in-flight timeout)          │
└────────────────────────┬────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┬────────────────┐
        │                │                │                │
   ┌────┴─────┐    ┌─────┴────┐    ┌──────┴─────┐   ┌──────┴────┐
   │ cc-messagebus    │   │ cc-messagebus    │   │ cc-messagebus      │   │ Browser   │
   │ mcp       │   │ mcp       │   │ tail saturn │   │ (대시보드)│
   │ (세션 A)  │   │ (세션 B)  │   │ (Monitor 용)│   │           │
   └──────────┘    └──────────┘    └────────────┘   └───────────┘
```

---

## 9. 패키지 구조 (제안)

```
cc-messagebus/
├── package.json
├── tsconfig.json
├── src/
│   ├── cli.ts              # 서브커맨드 디스패치
│   ├── server/
│   │   ├── index.ts        # Fastify HTTP + SSE 부트스트랩
│   │   ├── broker.ts       # 라우팅, in-flight 관리
│   │   ├── db.ts           # SQLite schema + queries
│   │   └── cleanup.ts      # TTL + in-flight 만료 worker
│   ├── mcp/
│   │   └── server.ts       # MCP stdio adapter
│   ├── client/
│   │   ├── tail.ts         # cc-messagebus tail
│   │   ├── status.ts       # cc-messagebus status
│   │   └── dashboard.ts    # cc-messagebus dashboard (browser open)
│   └── dashboard/
│       └── index.html      # 단일 페이지 UI
└── bin/
    └── cc-messagebus
```

---

## 10. 다음 단계

1. 본 PRD 확정
2. 아키텍처/프로토콜 상세 설계 (별도 문서)
3. 디렉토리 / 패키지 부트스트랩
4. broker 코어 + SQLite 영속화
5. MCP adapter
6. tail CLI (Monitor 호환)
7. dashboard
8. e2e 시나리오 테스트
