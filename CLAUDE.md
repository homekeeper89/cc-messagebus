# cc-messagebus

Cross-session message bus for Claude Code with central observability. See `prd.md` for full spec.

## Stack

- TypeScript (ESM, NodeNext), Node.js >= 20
- Runtime deps: `fastify`, `better-sqlite3`, `@modelcontextprotocol/sdk`
- Dev: `tsx` (test runner loader), `typescript`, `@biomejs/biome` (lint + format)

## Commands

| Purpose | Command |
| --- | --- |
| Build (emit `dist/`) | `npm run build` |
| Typecheck only | `npm run typecheck` |
| Watch build | `npm run dev` |
| Lint | `npm run lint` |
| Format | `npm run format` |
| Lint + format + organize imports (one-shot fix) | `npm run check` |
| Test | `npm test` (`node --test --import tsx 'test/**/*.test.ts'`) |

## Layout

- `src/cli.ts` — subcommand dispatcher
- `src/server/` — Fastify HTTP + SSE broker (`index`, `broker`, `db`, `cleanup`)
- `src/mcp/server.ts` — MCP stdio adapter
- `src/client/` — `tail`, `status`, `dashboard` CLI commands
- `src/dashboard/index.html` — single-page UI
- `bin/cc-messagebus` — npm bin entry, loads `dist/cli.js`
- `test/` — test files mirror `src/`

## Architecture notes

- Single Node daemon, default bind `127.0.0.1:5959`, data at `~/.cc-messagebus/data.db` (SQLite).
- Transport: HTTP RPC for MCP client calls, SSE for `tail` push and dashboard live updates. **No WebSocket.**
- Delivery semantics: at-least-once with visibility timeout (default 30s) and TTL (default 30d). See `prd.md` §5.2-§5.3.
- Disconnect detection: `tail` SSE close → mark topic `disconnected` (queue preserved).
