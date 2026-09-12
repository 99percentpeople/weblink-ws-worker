# Weblink WebSocket Worker

Cloudflare Workers + Durable Objects implementation of the Weblink signaling
protocol. Each room is routed to one `SignalingRoom` Durable Object and uses
the WebSocket Hibernation API.

- WebSocket: `wss://ws.webl.ink`
- Health check: `https://ws.webl.ink/healthcheck`
- Frontend: [`99percentpeople/weblink`](https://github.com/99percentpeople/weblink)
- Bun alternative: [`99percentpeople/weblink-ws-server`](https://github.com/99percentpeople/weblink-ws-server)

## Responsibilities

The Worker handles only room membership and the signaling needed to establish
WebRTC connections:

- room password metadata
- `connected`, `join`, versioned `joined`, and `leave` events
- SDP offer/answer and ICE candidate forwarding
- same-client reconnects with a 90-second grace period
- queued signaling messages during that reconnect period
- hibernatable WebSockets and alarm-based cleanup

The Worker emits `joined` with signaling protocol version 2 and a `resumed`
flag after membership is stored and before presence or cached signaling is
replayed. This gives clients an explicit point at which the replacement socket
is safe to use.

Each room ID maps to one Durable Object, which keeps room state isolated and
provides a single serialization point for membership changes.

## Privacy boundary

Presence records contain only `clientId`, `createdAt`, the RTC profile protocol
version, and the optional reconnect flag. Incoming `name` and `avatar` fields
are discarded before storage or forwarding. Display names, avatars,
application messages, files, and media are exchanged peer-to-peer over WebRTC.

The service can still observe room membership, client IDs, and connection
timing. SDP and ICE signaling payloads pass through the service while peers
establish WebRTC, but the Weblink frontend encrypts them with the room password.

## Development

Requirements:

- Bun
- A Cloudflare account for deployment only

```bash
bun install
bun run typegen
bun run format:check
bun run typecheck
bun run test
bunx wrangler deploy --dry-run
```

Start a local Worker runtime with:

```bash
bun run dev
```

## Deployment

The `ROOMS` Durable Object binding, SQLite-backed class export, Workers.dev
endpoint, and `ws.webl.ink` custom domain are declared in `wrangler.jsonc`.
Cloudflare Workers Traces are persisted with a 1% head sampling rate to keep
production observability lightweight.

For a local desktop session:

```bash
bunx wrangler login
```

For a remote SSH or container environment, use the device flow instead of a
localhost OAuth callback:

```bash
bunx wrangler login --device
```

Deploy only after all checks pass:

```bash
bun run deploy
curl https://ws.webl.ink/healthcheck
```

A frontend signaling switch is separate from Worker deployment. Update
`VITE_WEBSOCKET_URL` in the frontend deployment and rebuild only after
connectivity and protocol checks pass. Keep the Bun service available as a
rollback target during migration.
