import { env, exports as workerExports } from "cloudflare:workers";
import { evictDurableObject, runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { RawSignal, TransferClient } from "../src/protocol";
import worker from "../src/index";

class SocketInbox {
  challenge = "";
  private readonly queued: RawSignal[] = [];
  private readonly waiting: Array<(signal: RawSignal) => void> = [];

  constructor(readonly socket: WebSocket) {
    socket.addEventListener("message", (event) => {
      const signal = JSON.parse(String(event.data)) as RawSignal;
      if (signal.type === "connected")
        this.challenge = (
          signal as RawSignal & { identityChallenge: string }
        ).identityChallenge;
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

const keys = new Map<string, CryptoKeyPair>();
async function signed(
  inbox: SocketInbox,
  value: TransferClient,
  keyId = value.clientId,
) {
  let key = keys.get(keyId);
  if (!key) {
    key = (await crypto.subtle.generateKey(
      { name: "ECDSA", namedCurve: "P-256" },
      false,
      ["sign", "verify"],
    )) as CryptoKeyPair;
    keys.set(keyId, key);
  }
  const b64 = (b: ArrayBuffer) =>
    btoa(String.fromCharCode(...new Uint8Array(b)));
  const publicKey = b64(
    (await crypto.subtle.exportKey("raw", key.publicKey)) as ArrayBuffer,
  );
  const signature = b64(
    await crypto.subtle.sign(
      { name: "ECDSA", hash: "SHA-256" },
      key.privateKey,
      new TextEncoder().encode(
        JSON.stringify([
          "weblink-signaling-identity/1",
          inbox.challenge,
          value.clientId,
          value.createdAt,
        ]),
      ),
    ),
  );
  return { ...value, identityProof: { publicKey, signature } };
}
async function join(
  inbox: SocketInbox,
  value: unknown,
  withIdentity = true,
): Promise<RawSignal> {
  if (withIdentity) value = await signed(inbox, value as TransferClient);
  inbox.send({ type: "join", data: value });
  const acknowledgement = await inbox.next();
  expect(acknowledgement).toMatchObject({
    type: "joined",
    data: {
      protocolVersion: 2,
      resumed: expect.any(Boolean),
    },
  });
  return acknowledgement;
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
  it("rejects excess WebSocket connections before touching the room store", async () => {
    const limit = vi.fn(async () => ({ success: false }));
    const response = await worker.fetch(
      new Request("https://ws.webl.ink/?room=hidden-room", {
        headers: {
          Upgrade: "websocket",
          "CF-Connecting-IP": "192.0.2.15",
        },
      }),
      { WS_CONNECT_LIMITER: { limit } } as unknown as Env,
    );
    expect(response.status).toBe(429);
    expect(limit).toHaveBeenCalledWith({ key: "websocket:192.0.2.15" });
    expect(response.headers.get("retry-after")).toBe("60");
  });

  it("does not subject the intentionally public TURN route to WS limits", async () => {
    const response = await worker.fetch(
      new Request("https://ws.webl.ink/turn-credentials", {
        method: "OPTIONS",
      }),
      {} as Env,
    );
    expect(response.status).toBe(204);
  });
  it("routes TURN preflight and method checks without room membership", async () => {
    const preflight = await workerExports.default.fetch(
      new Request("https://ws.webl.ink/turn-credentials", {
        method: "OPTIONS",
      }),
    );
    expect(preflight.status).toBe(204);
    expect(preflight.headers.get("access-control-allow-origin")).toBe("*");
    const get = await workerExports.default.fetch(
      new Request("https://ws.webl.ink/turn-credentials"),
    );
    expect(get.status).toBe(405);
    expect(get.headers.get("cache-control")).toBe("no-store");
  });

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
  it.each(["leave", "grace expiry"])(
    "clears the last member's room despite a pending handshake (%s)",
    async (exit) => {
      const roomId = crypto.randomUUID();
      const { inbox: owner, stub } = await connect(roomId, "old-hash");
      await owner.next();
      await join(owner, client("owner"));
      if (exit === "grace expiry") {
        owner.close();
        await waitForStoredStatus(stub, "owner", "disconnected");
      }

      const { inbox: pending } = await connect(roomId, "next-hash");
      expect((await pending.next()).data).toBe("old-hash");
      if (exit === "leave") {
        owner.send({ type: "leave", data: undefined });
      } else {
        // Wake the object with an expired member and a still-fresh handshake.
        await runInDurableObject(stub, async (_instance, state) => {
          const member =
            await state.storage.get<Record<string, unknown>>("client:owner");
          await state.storage.put("client:owner", {
            ...member,
            expiresAt: Date.now() - 1,
          });
        });
        await evictDurableObject(stub);
        await runInDurableObject(stub, async (instance) => {
          if (!instance.alarm) throw new Error("Missing room alarm");
          await instance.alarm();
        });
      }

      await vi.waitFor(async () => {
        const stored = await runInDurableObject(
          stub,
          async (_instance, state) => ({
            keys: [...(await state.storage.list()).keys()],
            alarm: await state.storage.getAlarm(),
          }),
        );
        expect(stored).toEqual({ keys: [], alarm: null });
        expect(pending.socket.readyState).toBe(WebSocket.CLOSED);
      });
      const { inbox: fresh } = await connect(roomId, "next-hash");
      expect((await fresh.next()).data).toBe("next-hash");
      await join(fresh, client("next"));
    },
  );

  it("discards orphaned room metadata on wake", async () => {
    const roomId = crypto.randomUUID();
    const stub = env.ROOMS.get(env.ROOMS.idFromName(roomId));
    await runInDurableObject(stub, async (_instance, state) => {
      await state.storage.put("room:meta", { passwordHash: "old-hash" });
    });
    await evictDurableObject(stub);
    const { inbox } = await connect(roomId, "new-hash");
    expect((await inbox.next()).data).toBe("new-hash");
    await join(inbox, client("first"));
  });

  it("lets another pending socket create a room if nobody has joined yet", async () => {
    const roomId = crypto.randomUUID();
    const { inbox: abandoned } = await connect(roomId);
    const { inbox: creator, stub } = await connect(roomId, "new-hash");
    await abandoned.next();
    await creator.next();
    abandoned.close();
    await vi.waitFor(async () => {
      const open = await runInDurableObject(
        stub,
        async (_instance, state) =>
          state.getWebSockets().filter((s) => s.readyState === WebSocket.OPEN)
            .length,
      );
      expect(open).toBe(1);
    });
    await join(creator, client("creator"));
  });

  it.each(["", "owner-hash"])(
    "requires a fresh handshake when another verifier wins (owner=%s)",
    async (ownerHash) => {
      const room = crypto.randomUUID();
      const pending = (await connect(room, ownerHash ? "" : "pending-hash"))
        .inbox;
      expect((await pending.next()).data).toBe(
        ownerHash ? null : "pending-hash",
      );
      const owner = (await connect(room, ownerHash)).inbox;
      expect((await owner.next()).data).toBe(ownerHash || null);
      await join(owner, client("owner"));
      const closed = new Promise<CloseEvent>((resolve) =>
        pending.socket.addEventListener("close", resolve, { once: true }),
      );
      pending.send({ type: "join", data: client("late") });
      expect((await closed).code).toBe(1013);

      const retry = (await connect(room, "new-salted-hash")).inbox;
      expect((await retry.next()).data).toBe(ownerHash || null);
      await join(retry, client("late"));
    },
  );

  it("does not store a room until a valid join, including across multiple handshakes", async () => {
    const roomId = crypto.randomUUID();
    const first = await connect(roomId, "unjoined-hash");
    expect(await first.inbox.next()).toMatchObject({
      type: "connected",
      data: "unjoined-hash",
    });
    const before = await runInDurableObject(
      first.stub,
      async (_instance, state) => state.storage.get("room:meta"),
    );
    expect(before).toBeUndefined();

    const owner = await connect(roomId, "joined-hash");
    expect(await owner.inbox.next()).toMatchObject({
      type: "connected",
      data: "joined-hash",
    });
    await join(owner.inbox, client("joined-owner"));
    const after = await runInDurableObject(
      first.stub,
      async (_instance, state) =>
        state.storage.get<{ passwordHash: string | null }>("room:meta"),
    );
    expect(after?.passwordHash).toBe("joined-hash");

    const later = await connect(roomId, "third-hash");
    expect(await later.inbox.next()).toMatchObject({
      type: "connected",
      data: "joined-hash",
    });
  });

  it("expires idle unjoined WebSockets", async () => {
    const { inbox, stub } = await connect(crypto.randomUUID());
    await inbox.next();
    await runInDurableObject(stub, async (instance, state) => {
      const socket = state.getWebSockets()[0]!;
      const attachment = socket.deserializeAttachment() as Record<
        string,
        unknown
      >;
      socket.serializeAttachment({
        ...attachment,
        joinDeadline: Date.now() - 1,
      });
      if (!instance.alarm) throw new Error("Durable Object alarm missing");
      await instance.alarm();
    });
    await vi.waitFor(() => {
      expect(inbox.socket.readyState).toBe(WebSocket.CLOSED);
    });
  });
  it("keeps the first room password and strips profile fields", async () => {
    const roomId = crypto.randomUUID();
    const alice = await connect(roomId, "password-hash");
    expect(await alice.inbox.next()).toEqual({
      type: "connected",
      identityChallenge: expect.any(String),
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
      identityChallenge: expect.any(String),
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

  it.each([true, false])(
    "keeps the replacement socket authoritative over stale traffic (signed=%s)",
    async (withIdentity) => {
      const joinPeer = (inbox: SocketInbox, value: unknown) =>
        join(inbox, value, withIdentity);
      const roomId = crypto.randomUUID();
      const oldAlice = await connect(roomId);
      const bob = await connect(roomId);
      await oldAlice.inbox.next();
      await bob.inbox.next();

      await joinPeer(oldAlice.inbox, client("alice-owner"));
      await joinPeer(bob.inbox, client("bob-owner"));
      await bob.inbox.next();
      await oldAlice.inbox.next();

      const replacement = await connect(roomId);
      await replacement.inbox.next();
      const acknowledgement = await joinPeer(
        replacement.inbox,
        client("alice-owner", {
          createdAt: Date.now() + 1,
        }),
      );
      expect(acknowledgement.data).toMatchObject({
        resumed: false,
      });

      expect(await bob.inbox.next()).toMatchObject({
        type: "leave",
        data: { clientId: "alice-owner" },
      });
      expect(await replacement.inbox.next()).toMatchObject({
        type: "join",
        data: { clientId: "bob-owner" },
      });
      expect(await bob.inbox.next()).toMatchObject({
        type: "join",
        data: { clientId: "alice-owner" },
      });

      try {
        oldAlice.inbox.send({
          type: "message",
          data: {
            type: "candidate",
            clientId: "alice-owner",
            targetClientId: "bob-owner",
            data: "stale-candidate",
          },
        });
        oldAlice.inbox.send({ type: "leave", data: null });
      } catch {
        // The replacement may already have completed the old socket close.
      }

      replacement.inbox.send({
        type: "message",
        data: {
          type: "candidate",
          clientId: "alice-owner",
          targetClientId: "bob-owner",
          data: "current-candidate",
        },
      });

      expect(await bob.inbox.next()).toEqual({
        type: "message",
        data: {
          type: "candidate",
          clientId: "alice-owner",
          targetClientId: "bob-owner",
          data: "current-candidate",
        },
      });
    },
  );

  it.each([true, false])(
    "restores live sockets after hibernation (signed=%s)",
    async (withIdentity) => {
      const joinPeer = (inbox: SocketInbox, value: unknown) =>
        join(inbox, value, withIdentity);
      const roomId = crypto.randomUUID();
      const alice = await connect(roomId);
      const bob = await connect(roomId);
      await alice.inbox.next();
      await bob.inbox.next();
      await joinPeer(alice.inbox, client("alice"));
      await joinPeer(bob.inbox, client("bob"));
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
    },
  );

  it("does not cache transient peer-online notifications for offline members", async () => {
    const roomId = crypto.randomUUID();
    const alice = await connect(roomId);
    const bob = await connect(roomId);
    await alice.inbox.next();
    await bob.inbox.next();
    await join(alice.inbox, client("alice"));
    await join(bob.inbox, client("bob"));
    await alice.inbox.next();
    await bob.inbox.next();
    alice.inbox.close();
    bob.inbox.close();
    await waitForStoredStatus(alice.stub, "alice", "disconnected");
    await waitForStoredStatus(bob.stub, "bob", "disconnected");
    const resumed = await connect(roomId);
    await resumed.inbox.next();
    await join(resumed.inbox, client("bob", { resume: true }));
    const stored = await runInDurableObject(
      alice.stub,
      async (_instance, state) =>
        state.storage.get<{ messageCache: RawSignal[] }>("client:alice"),
    );
    expect(stored?.messageCache).toEqual([]);
  });

  it.each([true, false])(
    "caches signals during the reconnect grace period (signed=%s)",
    async (withIdentity) => {
      const joinPeer = (inbox: SocketInbox, value: unknown) =>
        join(inbox, value, withIdentity);
      const roomId = crypto.randomUUID();
      const alice = await connect(roomId);
      const bob = await connect(roomId);
      await alice.inbox.next();
      await bob.inbox.next();
      await joinPeer(alice.inbox, client("alice"));
      await joinPeer(bob.inbox, client("bob"));
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
      const acknowledgement = await joinPeer(
        resumedBob.inbox,
        client("bob", {
          resume: true,
          createdAt: Date.now() + 1,
        }),
      );
      expect(acknowledgement.data).toMatchObject({
        resumed: true,
      });
      expect(await resumedBob.inbox.next()).toEqual({
        type: "message",
        data: {
          type: "answer",
          clientId: "alice",
          targetClientId: "bob",
          data: "cached-answer",
        },
      });
      expect(await alice.inbox.next()).toEqual({
        type: "peer-online",
        data: { clientId: "bob", connectionId: expect.any(String) },
      });
      // A duplicate join on the same socket only acknowledges; no second wakeup.
      await joinPeer(resumedBob.inbox, client("bob", { resume: true }));
      resumedBob.inbox.send({
        type: "message",
        data: {
          type: "offer",
          clientId: "bob",
          targetClientId: "alice",
          data: "after-resume",
        },
      });
      expect(await alice.inbox.next()).toMatchObject({
        type: "message",
        data: { data: "after-resume" },
      });
    },
  );
});

it("ignores unsigned, wrong-key and replayed duplicate IDs without evicting the authenticated device", async () => {
  const room = crypto.randomUUID();
  const { inbox: owner } = await connect(room),
    { inbox: peer } = await connect(room);
  await owner.next();
  await peer.next();
  const presence = client("real-device");
  const original = await signed(owner, presence);
  await join(owner, presence);
  await join(peer, client("observer"));
  await owner.next();
  await peer.next();
  for (const attack of [
    "unsigned",
    "wrong-key",
    "replay",
    "wrong-key",
    "replay",
    "wrong-key",
  ]) {
    const { inbox: candidate } = await connect(room);
    await candidate.next();
    candidate.send({
      type: "join",
      data:
        attack === "unsigned"
          ? presence
          : attack === "replay"
            ? original
            : await signed(candidate, presence, "attacker"),
    });
    expect(await candidate.next()).toMatchObject({
      type: "error",
      data:
        attack === "replay" ? "INVALID_DEVICE_PROOF" : "DUPLICATE_DEVICE_ID",
    });
    owner.send({
      type: "message",
      data: {
        type: "candidate",
        clientId: "real-device",
        targetClientId: "observer",
        data: attack,
      },
    });
    expect(await peer.next()).toMatchObject({
      type: "message",
      data: { data: attack },
    });
    expect(owner.socket.readyState).toBe(WebSocket.OPEN);
  }
  owner.send({ type: "leave", data: null });
  expect(await peer.next()).toMatchObject({
    type: "leave",
    data: { clientId: "real-device" },
  });
  const { inbox: recovered } = await connect(room);
  await recovered.next();
  recovered.send({
    type: "join",
    data: await signed(recovered, presence, "regenerated-device"),
  });
  expect(await recovered.next()).toMatchObject({ type: "joined" });
  expect(await recovered.next()).toMatchObject({
    type: "join",
    data: { clientId: "observer" },
  });
  expect(await peer.next()).toMatchObject({
    type: "join",
    data: { clientId: "real-device" },
  });
});

describe("legacy signaling compatibility", () => {
  async function open(room: string) {
    const peer = (await connect(room)).inbox;
    await peer.next();
    return peer;
  }

  it.each([false, true])(
    "rejects supplied invalid proofs without treating them as legacy (occupied=%s)",
    async (occupied) => {
      const room = crypto.randomUUID();
      const presence = client("legacy-device");
      let owner: SocketInbox | undefined;
      if (occupied) {
        owner = await open(room);
        await join(owner, presence, false);
      }
      for (const mode of ["null", "malformed", "tampered"] as const) {
        const candidate = await open(room);
        const data =
          mode === "tampered"
            ? {
                ...(await signed(candidate, presence)),
                createdAt: presence.createdAt + 1,
              }
            : { ...presence, identityProof: mode === "null" ? null : {} };
        candidate.send({ type: "join", data });
        expect(await candidate.next()).toMatchObject({
          type: "error",
          data: "INVALID_DEVICE_PROOF",
        });
      }
      if (!owner) {
        owner = await open(room);
        await join(owner, presence, false);
      }
      owner.socket.send(JSON.stringify({ type: "ping" }));
      expect(await owner.next()).toMatchObject({ type: "pong" });
      expect(owner.socket.readyState).toBe(WebSocket.OPEN);
    },
  );

  it("allows a legacy session to upgrade but never downgrade a verified owner", async () => {
    const room = crypto.randomUUID();
    const presence = client("upgrading-device");
    const legacy = await open(room);
    await join(legacy, presence, false);
    const upgraded = await open(room);
    await join(upgraded, { ...presence, resume: true });
    const unsigned = await open(room);
    unsigned.send({ type: "join", data: { ...presence, resume: true } });
    expect(await unsigned.next()).toMatchObject({
      type: "error",
      data: "DUPLICATE_DEVICE_ID",
    });
    upgraded.socket.send(JSON.stringify({ type: "ping" }));
    expect(await upgraded.next()).toMatchObject({ type: "pong" });
    expect(upgraded.socket.readyState).toBe(WebSocket.OPEN);
  });
});
