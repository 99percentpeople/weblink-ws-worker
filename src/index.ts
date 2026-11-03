import { SignalingRoom } from "./signaling-room";
import {
  handleTurnCredentials,
  type TurnCredentialsEnv,
} from "./turn-credentials";
import { MAX_PASSWORD_HASH_LENGTH, MAX_ROOM_ID_LENGTH } from "./protocol";

export { SignalingRoom };

export default {
  async fetch(
    request: Request,
    env: Env & TurnCredentialsEnv,
  ): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/healthcheck") {
      return new Response("OK", {
        status: 200,
        headers: {
          "content-type": "text/plain; charset=utf-8",
          "cache-control": "no-store",
        },
      });
    }

    if (url.pathname === "/turn-credentials") {
      return handleTurnCredentials(request, env);
    }

    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("Expected a WebSocket upgrade", { status: 426 });
    }

    const roomId = url.searchParams.get("room")?.trim() ?? "";
    const passwordHash = url.searchParams.get("pwd") ?? "";
    if (!roomId || roomId.length > MAX_ROOM_ID_LENGTH) {
      return new Response("Invalid room", { status: 400 });
    }
    if (passwordHash.length > MAX_PASSWORD_HASH_LENGTH) {
      return new Response("Invalid password hash", { status: 400 });
    }

    // Enforce admission before instantiating a room Durable Object.
    // Cloudflare injects CF-Connecting-IP; do not trust user X-Forwarded-For.
    const clientIp = request.headers.get("CF-Connecting-IP") ?? "unknown";
    const { success } = await env.WS_CONNECT_LIMITER.limit({
      key: `websocket:${clientIp}`,
    });
    if (!success) {
      return new Response("Too many WebSocket connections", {
        status: 429,
        headers: { "retry-after": "60", "cache-control": "no-store" },
      });
    }

    const id = env.ROOMS.idFromName(roomId);
    return env.ROOMS.get(id).fetch(request);
  },
} satisfies ExportedHandler<Env & TurnCredentialsEnv>;
