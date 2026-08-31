import { SignalingRoom } from "./signaling-room";

export { SignalingRoom };

const MAX_ROOM_ID_LENGTH = 256;
const MAX_PASSWORD_HASH_LENGTH = 1024;

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
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

    const id = env.ROOMS.idFromName(roomId);
    return env.ROOMS.get(id).fetch(request);
  },
} satisfies ExportedHandler<Env>;
