# Weblink WebSocket Worker

Cloudflare Workers + Durable Objects implementation of the Weblink signaling
protocol. Each room is routed to one `SignalingRoom` Durable Object and uses
the WebSocket Hibernation API.

- WebSocket: `wss://ws.webl.ink`
- Health check: `https://ws.webl.ink/healthcheck`
- Frontend: [`99percentpeople/weblink`](https://github.com/99percentpeople/weblink)
- Bun alternative: [`99percentpeople/weblink-ws-server`](https://github.com/99percentpeople/weblink-ws-server)

## Responsibilities

The Worker handles room membership and the signaling needed to establish
WebRTC connections, plus an optional stateless TURN credential endpoint:

- room password metadata
- `connected`, `join`, versioned `joined`, and `leave` events
- SDP offer/answer and ICE candidate forwarding
- same-client reconnects with a 90-second grace period
- queued signaling messages during that reconnect period
- hibernatable WebSockets and alarm-based cleanup
- temporary Cloudflare TURN credential exchange outside Durable Objects

The Worker emits `joined` with signaling protocol version 2 and a `resumed`
flag after membership is stored and before presence or cached signaling is
replayed. This gives clients an explicit point at which the replacement socket
is safe to use.

After a retained client's socket resumes, the Worker sends `peer-online` to the
other currently online members. Its payload is `{ clientId, connectionId }`,
with a server-assigned socket ID. It uses the shared signaling version reported
by `joined`; this compatible addition does not introduce or bump a version. This wakes a single client-side
P2P recovery attempt instead of requiring repeated connection retries. It is not
a membership leave/join or a guarantee of ICE connectivity. Notifications are
never cached for offline members, and duplicate joins on the same socket do not
rebroadcast them. Deploy this server support before the event-driven frontend;
older frontends safely ignore the new event.

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

## Cloudflare TURN credentials

`POST /turn-credentials` exchanges backend-owned Cloudflare TURN configuration
for short-lived browser credentials. This is a public endpoint: it needs no
WebSocket upgrade, room, authentication, or rate limiter. The service does not
relay files or media; Cloudflare TURN carries relayed traffic.

Set `TURN_KEY_ID` and `TURN_KEY_API_TOKEN` using the existing key values.
The exchange uses Cloudflare's `credentials/generate-ice-servers` API and a fixed
24-hour TTL. The JSON response is `{ iceServers, expiresAt }`, with `expiresAt`
in Unix milliseconds. Credentials are not cached or stored by this backend.
All responses have `Cache-Control: no-store` and `Access-Control-Allow-Origin: *`.
`OPTIONS` returns 204, unsupported methods 405, unconfigured service 503,
provider failure 502, and a 10-second upstream timeout 504. Only normalized
WebRTC fields are returned; provider error details and long-term keys are not.

The frontend discovers this endpoint from the root of its WebSocket origin,
caches temporary credentials in memory, and refreshes on demand before
connection/SDP negotiation when expiry is near. A reverse proxy must forward
`/turn-credentials` as well as the WebSocket route. An unconfigured backend
continues serving signaling normally; clients can still use custom STUN/TURN.

Remove old `|cloudflare` values from frontend `VITE_TURN_SERVERS`/`PAGES_BUILD_ENV`
after configuring this endpoint. No automatic key rotation is performed.

Reference: [Cloudflare credential generation](https://developers.cloudflare.com/realtime/turn/generate-credentials/).

For local development, copy the TURN values from `.env.example` into ignored
`.dev.vars`. For production, manage both values in Cloudflare Dashboard under the Worker's
**Settings → Variables and Secrets**, using the **Secret** type for each value:

```text
TURN_KEY_ID
TURN_KEY_API_TOKEN
```

`wrangler.jsonc` declares these as required secrets so deployments fail clearly
when either is missing. It also enables `keep_vars` so plaintext variables added
through the Dashboard remain managed there instead of being removed by a later
Wrangler deployment.

The keys belong to this Worker, not to the frontend Pages build or the room
Durable Object. Deploy the endpoint before publishing the updated frontend.

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
