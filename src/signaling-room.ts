import { DurableObject } from "cloudflare:workers";
import {
  encodedMessageSize,
  MAX_SIGNAL_MESSAGE_BYTES,
  parseClientSignal,
  parseRawSignal,
  parseTransferClient,
  type ClientID,
  type RawSignal,
  type TransferClient,
} from "./protocol";

const ROOM_META_KEY = "room:meta";
const CLIENT_KEY_PREFIX = "client:";
const DISCONNECT_TIMEOUT_MS = 90_000;
const MAX_CACHED_SIGNALS = 256;

interface RoomMeta {
  passwordHash: string | null;
}

interface StoredClientState {
  client: TransferClient;
  status: "active" | "disconnected";
  expiresAt: number | null;
  messageCache: RawSignal[];
}

interface ClientState extends StoredClientState {
  socket: WebSocket | null;
}

interface SocketAttachment {
  clientId?: ClientID;
}

export class SignalingRoom extends DurableObject<Env> {
  private readonly clients = new Map<ClientID, ClientState>();
  private roomMeta: RoomMeta | null = null;
  private operationTail: Promise<void> = Promise.resolve();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    ctx.setWebSocketAutoResponse(
      new WebSocketRequestResponsePair(
        JSON.stringify({ type: "ping" }),
        JSON.stringify({ type: "pong" }),
      ),
    );
    ctx.blockConcurrencyWhile(() => this.restoreState());
  }

  async fetch(request: Request): Promise<Response> {
    return this.serialized(() => this.acceptConnection(request));
  }

  async webSocketMessage(
    socket: WebSocket,
    message: string | ArrayBuffer,
  ): Promise<void> {
    await this.serialized(() => this.handleWebSocketMessage(socket, message));
  }

  async webSocketClose(
    socket: WebSocket,
    _code: number,
    _reason: string,
    _wasClean: boolean,
  ): Promise<void> {
    await this.serialized(() => this.handleWebSocketClose(socket));
  }

  async webSocketError(socket: WebSocket): Promise<void> {
    await this.serialized(() => this.handleWebSocketClose(socket));
  }

  async alarm(): Promise<void> {
    await this.serialized(() => this.expireDisconnectedClients());
  }

  private async restoreState(): Promise<void> {
    this.roomMeta =
      (await this.ctx.storage.get<RoomMeta>(ROOM_META_KEY)) ?? null;

    const storedClients = await this.ctx.storage.list<StoredClientState>({
      prefix: CLIENT_KEY_PREFIX,
    });
    const invalidClientKeys: string[] = [];
    for (const [key, stored] of storedClients) {
      const clientId = key.slice(CLIENT_KEY_PREFIX.length);
      const client = parseTransferClient(stored.client);
      if (!client || client.clientId !== clientId) {
        invalidClientKeys.push(key);
        continue;
      }

      const messageCache = stored.messageCache.flatMap((signal) => {
        if (signal.type !== "join" && signal.type !== "leave") {
          return [signal];
        }
        const presence = parseTransferClient(signal.data);
        return presence ? [{ ...signal, data: presence }] : [];
      });
      this.clients.set(clientId, {
        ...stored,
        client,
        messageCache,
        socket: null,
      });
    }
    if (invalidClientKeys.length > 0) {
      await this.ctx.storage.delete(invalidClientKeys);
    }

    for (const socket of this.ctx.getWebSockets()) {
      const attachment =
        socket.deserializeAttachment() as SocketAttachment | null;
      const clientId = attachment?.clientId;
      if (!clientId) continue;

      const state = this.clients.get(clientId);
      if (!state) {
        socket.close(1011, "Client state unavailable");
        continue;
      }

      state.socket = socket;
      state.status = "active";
      state.expiresAt = null;
    }

    const now = Date.now();
    const writes: Promise<void>[] = [];
    for (const state of this.clients.values()) {
      if (state.status === "active" && !state.socket) {
        state.status = "disconnected";
        state.expiresAt = now + DISCONNECT_TIMEOUT_MS;
        writes.push(this.persistClient(state));
      }
    }
    await Promise.all(writes);
    await this.scheduleNextAlarm();
  }

  private async acceptConnection(request: Request): Promise<Response> {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("Expected a WebSocket upgrade", { status: 426 });
    }

    const url = new URL(request.url);
    if (!this.roomMeta) {
      this.roomMeta = {
        passwordHash: url.searchParams.get("pwd") || null,
      };
      await this.ctx.storage.put(ROOM_META_KEY, this.roomMeta);
    }

    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({} satisfies SocketAttachment);
    this.send(server, {
      type: "connected",
      data: this.roomMeta.passwordHash,
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  private async handleWebSocketMessage(
    socket: WebSocket,
    message: string | ArrayBuffer,
  ): Promise<void> {
    if (typeof message !== "string") {
      socket.close(1003, "Text messages only");
      return;
    }
    if (encodedMessageSize(message) > MAX_SIGNAL_MESSAGE_BYTES) {
      socket.close(1009, "Message too large");
      return;
    }

    let signal: RawSignal;
    try {
      signal = parseRawSignal(message);
    } catch {
      this.sendError(socket, "Invalid signal");
      return;
    }

    switch (signal.type) {
      case "pong":
        return;
      case "join":
        await this.handleJoin(socket, signal.data);
        return;
      case "message":
        await this.handleClientMessage(socket, signal.data);
        return;
      case "leave":
        await this.handleExplicitLeave(socket);
        return;
      default:
        this.sendError(socket, "Unknown signal type");
    }
  }

  private async handleJoin(socket: WebSocket, value: unknown): Promise<void> {
    const client = parseTransferClient(value);
    if (!client) {
      this.sendError(socket, "Invalid client");
      return;
    }

    const attachment = this.getAttachment(socket);
    if (attachment.clientId && attachment.clientId !== client.clientId) {
      socket.close(1008, "Client ID cannot change");
      return;
    }

    const existing = this.clients.get(client.clientId);
    if (existing?.socket === socket) {
      existing.client = client;
      await this.persistClient(existing);
      return;
    }

    if (existing && client.resume) {
      if (existing.socket && existing.socket !== socket) {
        existing.socket.close(1000, "Session resumed elsewhere");
      }

      const cachedSignals = existing.messageCache;
      const resumed: ClientState = {
        client,
        socket,
        status: "active",
        expiresAt: null,
        messageCache: [],
      };
      this.clients.set(client.clientId, resumed);
      socket.serializeAttachment({ clientId: client.clientId });
      await this.persistClient(resumed);
      await this.scheduleNextAlarm();

      for (const cachedSignal of cachedSignals) {
        this.send(socket, cachedSignal);
      }
      return;
    }

    if (existing) {
      if (existing.socket && existing.socket !== socket) {
        existing.socket.close(1000, "Session replaced");
      }
      this.clients.delete(client.clientId);
      await this.ctx.storage.delete(this.clientKey(client.clientId));
      await this.broadcast(
        {
          type: "leave",
          data: existing.client,
        },
        socket,
      );
    }

    for (const state of this.clients.values()) {
      this.send(socket, {
        type: "join",
        data: state.client,
      });
    }

    const joined: ClientState = {
      client,
      socket,
      status: "active",
      expiresAt: null,
      messageCache: [],
    };
    this.clients.set(client.clientId, joined);
    socket.serializeAttachment({ clientId: client.clientId });
    await this.persistClient(joined);
    await this.broadcast(
      {
        type: "join",
        data: client,
      },
      socket,
    );
    await this.scheduleNextAlarm();
  }

  private async handleClientMessage(
    socket: WebSocket,
    value: unknown,
  ): Promise<void> {
    const attachment = this.getAttachment(socket);
    const message = parseClientSignal(value);
    if (
      !attachment.clientId ||
      !message ||
      message.clientId !== attachment.clientId
    ) {
      this.sendError(socket, "Invalid client signal");
      return;
    }

    const target = this.clients.get(message.targetClientId);
    if (!target) return;

    await this.deliverOrCache(target, {
      type: "message",
      data: message,
    });
  }

  private async handleExplicitLeave(socket: WebSocket): Promise<void> {
    const clientId = this.getAttachment(socket).clientId;
    if (!clientId) {
      socket.close(1000, "Left");
      return;
    }

    const state = this.clients.get(clientId);
    if (!state || state.socket !== socket) {
      socket.close(1000, "Left");
      return;
    }

    this.clients.delete(clientId);
    await this.ctx.storage.delete(this.clientKey(clientId));
    await this.broadcast({ type: "leave", data: state.client }, socket);
    socket.close(1000, "Left");
    await this.scheduleNextAlarm();
    await this.cleanupEmptyRoom();
  }

  private async handleWebSocketClose(socket: WebSocket): Promise<void> {
    const clientId = this.getAttachment(socket).clientId;
    if (!clientId) {
      await this.cleanupEmptyRoom();
      return;
    }

    const state = this.clients.get(clientId);
    if (!state || state.socket !== socket) {
      await this.cleanupEmptyRoom();
      return;
    }

    state.socket = null;
    state.status = "disconnected";
    state.expiresAt = Date.now() + DISCONNECT_TIMEOUT_MS;
    await this.persistClient(state);
    await this.scheduleNextAlarm();
  }

  private async expireDisconnectedClients(): Promise<void> {
    const now = Date.now();
    const expired: ClientState[] = [];

    for (const [clientId, state] of this.clients) {
      if (
        state.status === "disconnected" &&
        state.expiresAt !== null &&
        state.expiresAt <= now
      ) {
        expired.push(state);
        this.clients.delete(clientId);
        await this.ctx.storage.delete(this.clientKey(clientId));
      }
    }

    for (const state of expired) {
      await this.broadcast({ type: "leave", data: state.client });
    }

    await this.scheduleNextAlarm();
    await this.cleanupEmptyRoom();
  }

  private async broadcast(
    signal: RawSignal,
    except?: WebSocket,
  ): Promise<void> {
    for (const state of this.clients.values()) {
      if (state.socket === except) continue;
      await this.deliverOrCache(state, signal);
    }
  }

  private async deliverOrCache(
    state: ClientState,
    signal: RawSignal,
  ): Promise<void> {
    if (state.socket?.readyState === WebSocket.OPEN) {
      this.send(state.socket, signal);
      return;
    }

    state.messageCache.push(signal);
    if (state.messageCache.length > MAX_CACHED_SIGNALS) {
      state.messageCache.splice(
        0,
        state.messageCache.length - MAX_CACHED_SIGNALS,
      );
    }
    await this.persistClient(state);
  }

  private send(socket: WebSocket, signal: RawSignal): void {
    if (socket.readyState !== WebSocket.OPEN) return;
    try {
      socket.send(JSON.stringify(signal));
    } catch (error) {
      console.warn("Failed to send signal", error);
    }
  }

  private sendError(socket: WebSocket, message: string): void {
    this.send(socket, { type: "error", data: message });
  }

  private getAttachment(socket: WebSocket): SocketAttachment {
    return (socket.deserializeAttachment() as SocketAttachment | null) ?? {};
  }

  private clientKey(clientId: ClientID): string {
    return `${CLIENT_KEY_PREFIX}${clientId}`;
  }

  private async persistClient(state: ClientState): Promise<void> {
    const stored: StoredClientState = {
      client: state.client,
      status: state.status,
      expiresAt: state.expiresAt,
      messageCache: state.messageCache,
    };
    await this.ctx.storage.put(this.clientKey(state.client.clientId), stored);
  }

  private async scheduleNextAlarm(): Promise<void> {
    let nextAlarm: number | null = null;
    for (const state of this.clients.values()) {
      if (state.status !== "disconnected" || state.expiresAt === null) continue;
      nextAlarm =
        nextAlarm === null
          ? state.expiresAt
          : Math.min(nextAlarm, state.expiresAt);
    }

    if (nextAlarm === null) {
      await this.ctx.storage.deleteAlarm();
      return;
    }
    await this.ctx.storage.setAlarm(nextAlarm);
  }

  private async cleanupEmptyRoom(): Promise<void> {
    if (this.clients.size > 0) return;
    const hasOpenSocket = this.ctx
      .getWebSockets()
      .some((socket) => socket.readyState === WebSocket.OPEN);
    if (hasOpenSocket) return;

    this.roomMeta = null;
    await this.ctx.storage.deleteAll();
  }

  private async serialized<T>(operation: () => Promise<T>): Promise<T> {
    const previous = this.operationTail;
    let release!: () => void;
    this.operationTail = new Promise<void>((resolve) => {
      release = resolve;
    });

    await previous;
    try {
      return await operation();
    } finally {
      release();
    }
  }
}
