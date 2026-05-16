interface Env {
  VELOCITY_RELAY: DurableObjectNamespace;
  MESSAGE_TTL_SECONDS?: string;
}

type Target = "desktop" | "phone";
type RelayKind = "clipboard" | "command" | "response";

interface RelayPayload {
  token: string;
  kind?: RelayKind;
  type?: string;
  content?: string;
  image?: string;
  filename?: string;
  command?: string;
  request_id?: string;
  correlation_id?: string;
  _relay_image_shards?: number;
}

interface StoredMessage {
  id: number;
  target: Target;
  kind: RelayKind;
  correlation_id?: string;
  payload: Omit<RelayPayload, "token">;
  created_at: number;
}

interface SseSubscriber {
  target: Target;
  writer: WritableStreamDefaultWriter<Uint8Array>;
  lastEmitted: number;
  correlationId: string | null;
  maxEvents: number;
  eventCount: number;
  closed: boolean;
  heartbeatId?: ReturnType<typeof setInterval>;
  timeoutId?: ReturnType<typeof setTimeout>;
}

const MAX_LIMIT = 100;
const INBOX_INDEX_LIMIT = 500;
const SSE_HEARTBEAT_MS = 15_000;
const IMAGE_CHUNK_CHARS = 90_000;
const MAX_IMAGE_SHARDS = 40;
const encoder = new TextEncoder();

export { VelocityRelayPair };

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (request.method === "OPTIONS") {
      return withCors(new Response(null, { status: 204 }));
    }

    const url = new URL(request.url);
    const parts = url.pathname.split("/").filter(Boolean);

    if (url.pathname === "/" && request.method === "GET") {
      return json({
        status: "ok",
        service: "Velocity Bridge Relay (Cloudflare Durable Objects)",
      });
    }

    if (parts.length >= 3 && parts[0] === "v1" && parts[1] === "pairs") {
      const pairId = parts[2];
      if (!pairId || pairId.length > 80) {
        return json({ detail: "Invalid pair id" }, 400);
      }
      const id = env.VELOCITY_RELAY.idFromName(pairId);
      return env.VELOCITY_RELAY.get(id).fetch(request);
    }

    return json({ detail: "Not found" }, 404);
  },
};

class VelocityRelayPair {
  private subscribers: Map<Target, Set<SseSubscriber>> = new Map([
    ["desktop", new Set()],
    ["phone", new Set()],
  ]);

  constructor(
    private readonly state: DurableObjectState,
    private readonly env: Env,
  ) {}

  async fetch(request: Request): Promise<Response> {
    try {
      if (request.method === "OPTIONS") {
        return withCors(new Response(null, { status: 204 }));
      }

      const url = new URL(request.url);
      const parts = url.pathname.split("/").filter(Boolean);

      if (parts.length === 5 && parts[0] === "v1" && parts[1] === "pairs") {
        const pairId = parts[2];
        const collection = parts[3];
        const action = parts[4];

        if (collection === "messages") {
          const target = requireTarget(action);
          if (request.method === "POST") {
            const payload = await readPayload(request);
            return json(await this.postMessage(pairId, target, payload));
          }
          if (request.method === "GET") {
            return json(await this.getMessages(pairId, target, url.searchParams));
          }
        }

        if (collection === "subscribe") {
          const target = requireTarget(action);
          if (request.method === "GET") {
            return await this.subscribeSSE(pairId, target, url.searchParams);
          }
        }

        if (collection === "phone") {
          if (action === "latest_clipboard" && request.method === "GET") {
            return json(await this.getLatestPhoneClipboard(pairId, url.searchParams));
          }

          if (request.method !== "POST") {
            throw new HttpError(405, "Method not allowed");
          }

          const payload = await readPayload(request);
          if (action === "send") {
            payload.kind = "clipboard";
            return json(await this.postMessage(pairId, "desktop", payload));
          }

          if (action === "request_clipboard") {
            const requestId = payload.request_id || crypto.randomUUID();
            const queued = await this.postMessage(pairId, "desktop", {
              token: payload.token,
              kind: "command",
              command: "get_clipboard",
              request_id: requestId,
              correlation_id: requestId,
            });
            return json({
              status: "queued",
              request_id: requestId,
              message_id: queued.id,
            });
          }

          throw new HttpError(404, "Unknown phone action");
        }
      }

      return json({ detail: "Not found" }, 404);
    } catch (error) {
      if (error instanceof HttpError) {
        return json({ detail: error.message }, error.status);
      }
      console.error("Relay error:", error);
      return json({ detail: "Internal server error" }, 500);
    }
  }

  private async postMessage(
    pairId: string,
    target: Target,
    payload: RelayPayload,
  ): Promise<Record<string, unknown>> {
    await this.requirePair(pairId, payload.token);
    await this.cleanupExpired(target);

    const now = Math.floor(Date.now() / 1000);
    const id = await this.nextMessageId();
    const correlationId = payload.correlation_id || payload.request_id;
    const payloadWithoutToken = withoutToken(payload);
    const kind = payload.kind || "clipboard";

    const message: StoredMessage = {
      id,
      target,
      kind,
      correlation_id: correlationId,
      payload: payloadWithoutToken,
      created_at: now,
    };

    if (target === "phone" && kind === "clipboard") {
      await this.clearPhoneClipboardQueue();
    }

    const stored = await this.storeMessage(target, message);
    await this.appendInboxIndex(target, id);
    await this.state.storage.put(latestMessageKey(target), stored);
    this.notifySubscribers(target, stored);

    return { status: "queued", id, expires_in: this.messageTtlSeconds() };
  }

  private async getMessages(
    pairId: string,
    target: Target,
    params: URLSearchParams,
  ): Promise<Record<string, unknown>> {
    const token = params.get("token") || "";
    await this.requirePair(pairId, token);
    await this.cleanupExpired(target);

    const after = safeNumber(params.get("after"), 0);
    const limit = Math.min(Math.max(safeNumber(params.get("limit"), 25), 1), MAX_LIMIT);
    const correlationId = params.get("correlation_id");
    const page = await this.collectMessages(target, after, limit, correlationId);

    return {
      status: "success",
      messages: page,
      cursor: page.length ? page[page.length - 1].id : after,
    };
  }

  private async getLatestPhoneClipboard(
    pairId: string,
    params: URLSearchParams,
  ): Promise<Record<string, unknown>> {
    const token = params.get("token") || "";
    await this.requirePair(pairId, token);
    await this.cleanupExpired("phone");

    const correlationId = params.get("correlation_id");
    const messages = await this.collectMessages("phone", 0, INBOX_INDEX_LIMIT, correlationId);
    let latestResponse: StoredMessage | null = null;
    let latestClipboard: StoredMessage | null = null;

    for (const message of messages) {
      if (message.kind === "response") {
        if (!latestResponse || message.id > latestResponse.id) {
          latestResponse = message;
        }
      } else if (message.kind === "clipboard") {
        if (!latestClipboard || message.id > latestClipboard.id) {
          latestClipboard = message;
        }
      }
    }

    const latest = latestResponse || latestClipboard;
    if (!latest) {
      throw new HttpError(404, "No clipboard queued");
    }

    return {
      status: "success",
      message: latest,
    };
  }

  private async subscribeSSE(
    pairId: string,
    target: Target,
    params: URLSearchParams,
  ): Promise<Response> {
    const token = params.get("token") || "";
    await this.requirePair(pairId, token);
    await this.cleanupExpired(target);

    const after = safeNumber(params.get("after"), 0);
    const timeoutSeconds = Math.max(1, safeNumber(params.get("timeout"), 300));
    const maxEvents = Math.max(1, safeNumber(params.get("max_events"), 500));
    const correlationId = params.get("correlation_id");

    const stream = new TransformStream<Uint8Array, Uint8Array>();
    const writer = stream.writable.getWriter();
    const subscriber: SseSubscriber = {
      target,
      writer,
      lastEmitted: after,
      correlationId,
      maxEvents,
      eventCount: 0,
      closed: false,
    };

    this.subscribers.get(target)?.add(subscriber);
    await this.writeSse(subscriber, ": ready\n\n");

    subscriber.heartbeatId = setInterval(() => {
      void this.writeSse(subscriber, ": heartbeat\n\n");
    }, SSE_HEARTBEAT_MS);

    subscriber.timeoutId = setTimeout(() => {
      this.closeSubscriber(subscriber);
    }, timeoutSeconds * 1000);

    await this.drainSubscriber(subscriber);

    return new Response(stream.readable, {
      headers: sseHeaders(),
    });
  }

  private async requirePair(pairId: string, token: string): Promise<void> {
    if (!pairId || pairId.length > 80) {
      throw new HttpError(400, "Invalid pair id");
    }
    if (!token || token.length < 8) {
      throw new HttpError(422, "Relay token must be at least 8 characters");
    }

    const tokenHash = await sha256(token);
    const existingHash = await this.state.storage.get<string>("tokenHash");
    if (!existingHash) {
      await this.state.storage.put("tokenHash", tokenHash);
      return;
    }
    if (existingHash !== tokenHash) {
      throw new HttpError(403, "Invalid relay token");
    }
  }

  private async nextMessageId(): Promise<number> {
    const previous = (await this.state.storage.get<number>("sequence")) || 0;
    const wallClockId = Date.now() * 1000 + Math.floor(Math.random() * 1000);
    const next = Math.max(previous + 1, wallClockId);
    await this.state.storage.put("sequence", next);
    return next;
  }

  private async storeMessage(target: Target, message: StoredMessage): Promise<StoredMessage> {
    const image = message.payload.image;
    if (typeof image !== "string" || image.length <= IMAGE_CHUNK_CHARS) {
      await this.state.storage.put(messageKey(target, message.id), message);
      return message;
    }

    const chunkCount = Math.ceil(image.length / IMAGE_CHUNK_CHARS);
    if (chunkCount > MAX_IMAGE_SHARDS) {
      throw new HttpError(
        413,
        `Image too large for relay (max ~${MAX_IMAGE_SHARDS * IMAGE_CHUNK_CHARS} base64 characters).`,
      );
    }

    const metaPayload: Record<string, unknown> = { ...message.payload };
    delete metaPayload.image;
    metaPayload._relay_image_shards = chunkCount;
    const metaMessage: StoredMessage = {
      ...message,
      payload: metaPayload as Omit<RelayPayload, "token">,
    };

    const entries: Record<string, StoredMessage | string> = {
      [messageKey(target, message.id)]: metaMessage,
    };
    for (let i = 0; i < chunkCount; i++) {
      const start = i * IMAGE_CHUNK_CHARS;
      entries[imageChunkKey(target, message.id, i)] = image.slice(
        start,
        start + IMAGE_CHUNK_CHARS,
      );
    }
    await this.state.storage.put(entries);
    return metaMessage;
  }

  private async hydrateMessage(message: StoredMessage): Promise<StoredMessage> {
    const shardCount = shardCountFromPayload(message.payload);
    if (shardCount <= 0) {
      return message;
    }

    const parts: string[] = [];
    for (let i = 0; i < shardCount; i++) {
      const chunk = await this.state.storage.get<string>(
        imageChunkKey(message.target, message.id, i),
      );
      if (typeof chunk !== "string") {
        throw new HttpError(500, "Relay image shard missing");
      }
      parts.push(chunk);
    }

    const { _relay_image_shards: _shards, ...clean } = message.payload;
    return {
      ...message,
      payload: {
        ...clean,
        image: parts.join(""),
      },
    };
  }

  private async appendInboxIndex(target: Target, id: number): Promise<void> {
    const key = inboxIndexKey(target);
    const current = (await this.state.storage.get<number[]>(key)) || [];
    const next = [...current.filter((existing) => existing !== id), id]
      .sort((a, b) => a - b)
      .slice(-INBOX_INDEX_LIMIT);
    await this.state.storage.put(key, next);
  }

  private async collectMessages(
    target: Target,
    after: number,
    limit: number,
    correlationId: string | null,
  ): Promise<StoredMessage[]> {
    const ids = ((await this.state.storage.get<number[]>(inboxIndexKey(target))) || [])
      .filter((id) => Number.isFinite(id) && id > after)
      .sort((a, b) => a - b);

    const messages: StoredMessage[] = [];
    for (const id of ids) {
      const message = await this.state.storage.get<StoredMessage>(messageKey(target, id));
      if (!message || message.id <= after) {
        continue;
      }
      if (correlationId && message.correlation_id !== correlationId) {
        continue;
      }
      messages.push(await this.hydrateMessage(message));
      if (messages.length >= limit) {
        return messages;
      }
    }

    if (messages.length > 0) {
      return messages;
    }

    const listed = await this.state.storage.list<StoredMessage>({
      prefix: messagePrefix(target),
    });
    for (const message of listed.values()) {
      if (!message || message.id <= after) {
        continue;
      }
      if (correlationId && message.correlation_id !== correlationId) {
        continue;
      }
      messages.push(await this.hydrateMessage(message));
    }

    return messages
      .sort((a, b) => a.id - b.id)
      .slice(0, limit);
  }

  private async drainSubscriber(subscriber: SseSubscriber): Promise<void> {
    const remaining = subscriber.maxEvents - subscriber.eventCount;
    if (remaining <= 0 || subscriber.closed) {
      this.closeSubscriber(subscriber);
      return;
    }

    const messages = await this.collectMessages(
      subscriber.target,
      subscriber.lastEmitted,
      Math.min(remaining, MAX_LIMIT),
      subscriber.correlationId,
    );

    for (const message of messages) {
      const emitted = await this.emitMessage(subscriber, message);
      if (!emitted) {
        return;
      }
    }
  }

  private notifySubscribers(target: Target, message: StoredMessage): void {
    const bucket = this.subscribers.get(target);
    if (!bucket) {
      return;
    }

    for (const subscriber of [...bucket]) {
      void this.emitMessage(subscriber, message);
    }
  }

  private async emitMessage(subscriber: SseSubscriber, message: StoredMessage): Promise<boolean> {
    if (subscriber.closed || message.id <= subscriber.lastEmitted) {
      return !subscriber.closed;
    }
    if (subscriber.correlationId && message.correlation_id !== subscriber.correlationId) {
      return true;
    }

    const ok = await this.writeSse(subscriber, `data: ${JSON.stringify(message)}\n\n`);
    if (!ok) {
      return false;
    }

    subscriber.lastEmitted = message.id;
    subscriber.eventCount += 1;
    if (subscriber.eventCount >= subscriber.maxEvents) {
      this.closeSubscriber(subscriber);
      return false;
    }
    return true;
  }

  private async writeSse(subscriber: SseSubscriber, chunk: string): Promise<boolean> {
    if (subscriber.closed) {
      return false;
    }
    try {
      await subscriber.writer.write(encoder.encode(chunk));
      return true;
    } catch {
      this.closeSubscriber(subscriber);
      return false;
    }
  }

  private closeSubscriber(subscriber: SseSubscriber): void {
    if (subscriber.closed) {
      return;
    }
    subscriber.closed = true;
    if (subscriber.heartbeatId) {
      clearInterval(subscriber.heartbeatId);
    }
    if (subscriber.timeoutId) {
      clearTimeout(subscriber.timeoutId);
    }
    this.subscribers.get(subscriber.target)?.delete(subscriber);
    try {
      void subscriber.writer.close().catch(() => {
        // The client may already have closed the stream.
      });
    } catch {
      // The client may already have closed the stream.
    }
  }

  private async clearPhoneClipboardQueue(): Promise<void> {
    const messages = await this.collectMessages("phone", 0, INBOX_INDEX_LIMIT, null);
    const deleteKeys: string[] = [];
    const remainingIds: number[] = [];

    for (const message of messages) {
      if (message.kind !== "clipboard") {
        remainingIds.push(message.id);
        continue;
      }
      deleteKeys.push(messageKey("phone", message.id));
      for (let i = 0; i < shardCountFromPayload(message.payload); i++) {
        deleteKeys.push(imageChunkKey("phone", message.id, i));
      }
    }

    if (deleteKeys.length > 0) {
      await this.state.storage.delete(deleteKeys);
    }
    await this.state.storage.put(inboxIndexKey("phone"), remainingIds);
  }

  private async cleanupExpired(target: Target): Promise<void> {
    const ttl = this.messageTtlSeconds();
    const cutoff = Math.floor(Date.now() / 1000) - ttl;
    const ids = (await this.state.storage.get<number[]>(inboxIndexKey(target))) || [];
    const keepIds: number[] = [];
    const deleteKeys: string[] = [];

    for (const id of ids) {
      const message = await this.state.storage.get<StoredMessage>(messageKey(target, id));
      if (!message) {
        continue;
      }
      if (message.created_at > cutoff) {
        keepIds.push(id);
        continue;
      }

      deleteKeys.push(messageKey(target, id));
      for (let i = 0; i < shardCountFromPayload(message.payload); i++) {
        deleteKeys.push(imageChunkKey(target, id, i));
      }
    }

    if (deleteKeys.length > 0) {
      await this.state.storage.delete(deleteKeys);
      await this.state.storage.put(inboxIndexKey(target), keepIds);
    }
  }

  private messageTtlSeconds(): number {
    return Math.max(60, safeNumber(this.env.MESSAGE_TTL_SECONDS, 86400));
  }
}

function requireTarget(value: string): Target {
  if (value === "desktop" || value === "phone") {
    return value;
  }
  throw new HttpError(400, "Invalid message target");
}

async function readPayload(request: Request): Promise<RelayPayload> {
  const contentType = request.headers.get("content-type") || "";

  if (contentType.includes("application/json")) {
    const payload = (await request.json().catch(() => null)) as RelayPayload | null;
    if (!payload || typeof payload !== "object") {
      throw new HttpError(400, "Invalid JSON payload");
    }
    return payload;
  }

  if (
    contentType.includes("application/x-www-form-urlencoded") ||
    contentType.includes("multipart/form-data")
  ) {
    const formData = await request.formData().catch(() => null);
    if (!formData) {
      throw new HttpError(400, "Invalid form payload");
    }

    const payload: Record<string, string> = {};
    for (const [key, value] of formData.entries()) {
      payload[key] = typeof value === "string" ? value : String(value);
    }
    return payload as unknown as RelayPayload;
  }

  const text = await request.text();
  if (!text.trim()) {
    throw new HttpError(400, "Invalid request payload");
  }

  try {
    return JSON.parse(text) as RelayPayload;
  } catch {
    throw new HttpError(400, "Invalid request payload");
  }
}

function withoutToken(payload: RelayPayload): Omit<RelayPayload, "token"> {
  const { token: _token, ...rest } = payload;
  return rest;
}

function shardCountFromPayload(payload: Omit<RelayPayload, "token">): number {
  const raw = payload._relay_image_shards;
  if (typeof raw === "number" && Number.isFinite(raw)) {
    return Math.max(0, Math.floor(raw));
  }
  return 0;
}

function safeNumber(value: string | null | undefined, fallback: number): number {
  if (!value) {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function messagePrefix(target: Target): string {
  return `message:${target}:`;
}

function messageKey(target: Target, id: number): string {
  return `${messagePrefix(target)}${String(id).padStart(16, "0")}`;
}

function imageChunkKey(target: Target, id: number, idx: number): string {
  return `image:${target}:${String(id).padStart(16, "0")}:${String(idx).padStart(3, "0")}`;
}

function inboxIndexKey(target: Target): string {
  return `index:${target}`;
}

function latestMessageKey(target: Target): string {
  return `latest:${target}`;
}

async function sha256(value: string): Promise<string> {
  const encoded = new TextEncoder().encode(value);
  const digest = await crypto.subtle.digest("SHA-256", encoded);
  return Array.from(new Uint8Array(digest))
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

function json(body: unknown, status = 200): Response {
  return withCors(
    new Response(JSON.stringify(body), {
      status,
      headers: {
        "content-type": "application/json; charset=utf-8",
      },
    }),
  );
}

function sseHeaders(): Headers {
  return new Headers({
    "content-type": "text/event-stream; charset=utf-8",
    "cache-control": "no-cache",
    "access-control-allow-origin": "*",
    "connection": "keep-alive",
  });
}

function withCors(response: Response): Response {
  const headers = new Headers(response.headers);
  headers.set("access-control-allow-origin", "*");
  headers.set("access-control-allow-methods", "GET,POST,OPTIONS");
  headers.set("access-control-allow-headers", "content-type");
  return new Response(response.body, { status: response.status, headers });
}

class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}
