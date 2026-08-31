import { env, exports as workerExports } from "cloudflare:workers";
import { evictDurableObject, runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { RawSignal, TransferClient } from "../src/protocol";

class SocketInbox {
  private readonly queued: RawSignal[] = [];
  private readonly waiting: Array<(signal: RawSignal) => void> = [];

  constructor(readonly socket: WebSocket) {
    socket.addEventListener("message", (event) => {
      const signal = JSON.parse(String(event.data)) as RawSignal;
      const resolve = this.waiting.shift();
      if (resolve) {
        resolve(signal);
      } else {
        this.queued.push(signal);
      }
    });
    socket.accept();
  }

  send(signal: RawSignal): void {
    this.socket.send(JSON.stringify(signal));
  }

  async next(): Promise<RawSignal> {
    const queued = this.queued.shift();
    if (queued) return queued;

    return new Promise<RawSignal>((resolve, reject) => {
      const timeout = setTimeout(() => {
        const index = this.waiting.indexOf(onSignal);
        if (index !== -1) this.waiting.splice(index, 1);
        reject(new Error("Timed out waiting for WebSocket message"));
      }, 2_000);
      const onSignal = (signal: RawSignal) => {
        clearTimeout(timeout);
        resolve(signal);
      };
      this.waiting.push(onSignal);
    });
  }

  close(): void {
    if (
      this.socket.readyState === WebSocket.OPEN ||
      this.socket.readyState === WebSocket.CONNECTING
    ) {
      this.socket.close(1000, "test complete");
    }
  }
}

const sockets: SocketInbox[] = [];

function client(
  clientId: string,
  options: Partial<TransferClient> = {},
): TransferClient {
  return {
    clientId,
    createdAt: options.createdAt ?? Date.now(),
    rtcProfileVersion: options.rtcProfileVersion,
    resume: options.resume,
  };
}

async function connect(
  roomId: string,
  passwordHash = "",
): Promise<{ inbox: SocketInbox; stub: DurableObjectStub }> {
  const id = env.ROOMS.idFromName(roomId);
  const stub = env.ROOMS.get(id);
  const url = new URL("https://worker.test/");
  url.searchParams.set("room", roomId);
  if (passwordHash) url.searchParams.set("pwd", passwordHash);

  const response = await stub.fetch(url, {
    headers: { Upgrade: "websocket" },
  });
  expect(response.status).toBe(101);
  expect(response.webSocket).toBeDefined();

  const inbox = new SocketInbox(response.webSocket!);
  sockets.push(inbox);
  return { inbox, stub };
}

async function join(inbox: SocketInbox, value: unknown): Promise<void> {
  inbox.send({ type: "join", data: value });
  await Promise.resolve();
}

async function waitForStoredStatus(
  stub: DurableObjectStub,
  clientId: string,
  status: string,
): Promise<void> {
  await vi.waitFor(
    async () => {
      const stored = await runInDurableObject(stub, async (_instance, state) =>
        state.storage.get<{ status: string }>(`client:${clientId}`),
      );
      expect(stored?.status).toBe(status);
    },
    { timeout: 2_000, interval: 20 },
  );
}

afterEach(async () => {
  for (const inbox of sockets.splice(0)) inbox.close();
  await new Promise((resolve) => setTimeout(resolve, 0));
});

describe("Worker entrypoint", () => {
  it("serves health checks and rejects non-WebSocket requests", async () => {
    const health = await workerExports.default.fetch(
      new Request("https://ws.webl.ink/healthcheck"),
    );
    expect(health.status).toBe(200);
    expect(await health.text()).toBe("OK");

    const regular = await workerExports.default.fetch(
      new Request("https://ws.webl.ink/?room=test"),
    );
    expect(regular.status).toBe(426);
  });
});

describe("SignalingRoom", () => {
  it("keeps the first room password and strips profile fields", async () => {
    const roomId = crypto.randomUUID();
    const alice = await connect(roomId, "password-hash");
    expect(await alice.inbox.next()).toEqual({
      type: "connected",
      data: "password-hash",
    });
    await join(alice.inbox, {
      ...client("12345678-alice", { rtcProfileVersion: 1 }),
      name: "Private Alice",
      avatar: "data:image/png;base64,private",
    });

    const bob = await connect(roomId, "different-hash");
    expect(await bob.inbox.next()).toEqual({
      type: "connected",
      data: "password-hash",
    });
    await join(bob.inbox, {
      ...client("bob"),
      name: "Legacy Bob",
      avatar: "legacy-avatar",
    });

    const aliceJoin = await bob.inbox.next();
    expect(aliceJoin).toMatchObject({
      type: "join",
      data: {
        clientId: "12345678-alice",
        rtcProfileVersion: 1,
      },
    });
    expect(aliceJoin.data).not.toHaveProperty("name");
    expect(aliceJoin.data).not.toHaveProperty("avatar");

    const bobJoin = await alice.inbox.next();
    expect(bobJoin).toMatchObject({
      type: "join",
      data: { clientId: "bob" },
    });
    expect(bobJoin.data).not.toHaveProperty("name");
    expect(bobJoin.data).not.toHaveProperty("avatar");
  });

  it("routes signaling messages only from the joined client", async () => {
    const roomId = crypto.randomUUID();
    const alice = await connect(roomId);
    const bob = await connect(roomId);
    await alice.inbox.next();
    await bob.inbox.next();
    await join(alice.inbox, client("alice"));
    await join(bob.inbox, client("bob"));
    await bob.inbox.next();
    await alice.inbox.next();

    alice.inbox.send({
      type: "message",
      data: {
        type: "offer",
        clientId: "alice",
        targetClientId: "bob",
        data: "encrypted-offer",
      },
    });
    expect(await bob.inbox.next()).toEqual({
      type: "message",
      data: {
        type: "offer",
        clientId: "alice",
        targetClientId: "bob",
        data: "encrypted-offer",
      },
    });

    alice.inbox.send({
      type: "message",
      data: {
        type: "offer",
        clientId: "mallory",
        targetClientId: "bob",
        data: "spoofed",
      },
    });
    expect(await alice.inbox.next()).toEqual({
      type: "error",
      data: "Invalid client signal",
    });
  });

  it("restores live sockets after hibernation", async () => {
    const roomId = crypto.randomUUID();
    const alice = await connect(roomId);
    const bob = await connect(roomId);
    await alice.inbox.next();
    await bob.inbox.next();
    await join(alice.inbox, client("alice"));
    await join(bob.inbox, client("bob"));
    await bob.inbox.next();
    await alice.inbox.next();

    await evictDurableObject(alice.stub);

    alice.inbox.send({
      type: "message",
      data: {
        type: "candidate",
        clientId: "alice",
        targetClientId: "bob",
        data: "candidate-after-hibernation",
      },
    });
    expect(await bob.inbox.next()).toMatchObject({
      type: "message",
      data: {
        type: "candidate",
        data: "candidate-after-hibernation",
      },
    });
  });

  it("caches signals during the reconnect grace period", async () => {
    const roomId = crypto.randomUUID();
    const alice = await connect(roomId);
    const bob = await connect(roomId);
    await alice.inbox.next();
    await bob.inbox.next();
    await join(alice.inbox, client("alice"));
    await join(bob.inbox, client("bob"));
    await bob.inbox.next();
    await alice.inbox.next();

    bob.inbox.close();
    await waitForStoredStatus(bob.stub, "bob", "disconnected");

    alice.inbox.send({
      type: "message",
      data: {
        type: "answer",
        clientId: "alice",
        targetClientId: "bob",
        data: "cached-answer",
      },
    });

    const resumedBob = await connect(roomId);
    await resumedBob.inbox.next();
    await join(
      resumedBob.inbox,
      client("bob", {
        resume: true,
        createdAt: Date.now() + 1,
      }),
    );
    expect(await resumedBob.inbox.next()).toEqual({
      type: "message",
      data: {
        type: "answer",
        clientId: "alice",
        targetClientId: "bob",
        data: "cached-answer",
      },
    });
  });
});
