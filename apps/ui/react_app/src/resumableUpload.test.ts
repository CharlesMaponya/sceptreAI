import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as apiModule from "./api";
import {
  createResumableUpload,
  hashFileIncrementally,
  loadPersistedUpload,
  removePersistedUpload,
  savePersistedUpload,
  waitForVerifiedUpload,
  waitForUploadResult,
  type UploadProgress,
  type UploadSession,
} from "./resumableUpload";
import { IncrementalSha256, type Sha256Snapshot } from "./incrementalSha256";

const projectId = "11111111-1111-1111-1111-111111111111";
const future = "2099-01-01T00:00:00Z";
const session: UploadSession = {
  id: "upload-1",
  project_id: projectId,
  upload_kind: "dataset",
  status: "uploading",
  provider_driver: "s3_compatible",
  protocol: "multipart",
  byte_size: 6,
  part_size: 6,
  total_parts: 1,
  confirmed_bytes: 0,
  resume_key: "resume-key",
  expires_at: future,
  original_filename: "records.csv",
  expected_object_digest: "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721",
  sensitivity: "confidential",
  data_region: "eu-west-1",
  legal_hold: false,
  next_cursor: { unit_number: 1, offset: 0, length: 6 },
  capabilities: { parallel_chunks_per_object: 4 },
};

const pendingProgress = (): UploadProgress => ({
  session_id: session.id,
  status: "uploading",
  confirmed_bytes: 0,
  total_bytes: 6,
  receipts: [],
  next_cursor: { unit_number: 1, offset: 0, length: 6 },
  expires_at: future,
  complete: false,
});

const completeProgress = (): UploadProgress => ({
  ...pendingProgress(),
  status: "object_completed",
  confirmed_bytes: 6,
  receipts: [{
    unit_number: 1,
    offset: 0,
    length: 6,
    etag: "etag-1",
    provider_headers: { ETag: "etag-1" },
  }],
  next_cursor: null,
  complete: true,
});

const options = (file: File) => ({
  projectId,
  file,
  datasetName: "Records",
  sensitivity: "confidential" as const,
  dataRegion: "eu-west-1",
});

function installSuccessfulXhr(
  outcomes: Array<number | "throw" | "error"> = [200],
  retryAfter: string | null = "0",
) {
  let attempt = 0;
  const opened: string[] = [];
  vi.spyOn(XMLHttpRequest.prototype, "open").mockImplementation(
    function (_method: string, url: string | URL) { opened.push(String(url)); },
  );
  vi.spyOn(XMLHttpRequest.prototype, "setRequestHeader").mockImplementation(() => undefined);
  vi.spyOn(XMLHttpRequest.prototype, "getResponseHeader").mockImplementation(function (name) {
    if (name.toLowerCase() === "etag") return "etag-1";
    if (name.toLowerCase() === "retry-after") return retryAfter;
    return null;
  });
  vi.spyOn(XMLHttpRequest.prototype, "send").mockImplementation(function (
    this: XMLHttpRequest,
    value,
  ) {
    expect(value).toBeInstanceOf(ArrayBuffer);
    const outcome = outcomes[Math.min(attempt, outcomes.length - 1)];
    attempt += 1;
    if (outcome === "throw") throw new Error("provider SDK bridge failed");
    if (outcome === "error") {
      this.dispatchEvent(new ProgressEvent("error"));
      return;
    }
    Object.defineProperty(this, "status", { configurable: true, value: outcome });
    this.upload.dispatchEvent(new ProgressEvent("progress", { loaded: 6, total: 6 }));
    this.dispatchEvent(new ProgressEvent("load"));
  });
  return { opened, attempts: () => attempt };
}

function installApi(progressValues: UploadProgress[], beginSession = session) {
  let progressIndex = 0;
  let instructions = 0;
  const calls: string[] = [];
  const beginBodies: Array<Record<string, unknown>> = [];
  vi.spyOn(apiModule, "api").mockImplementation(async (path: string, init?: RequestInit) => {
    calls.push(path);
    if (path === "/capabilities") return { upload_data_region: "af-south-1" };
    if (path.endsWith("/datasets/uploads")) {
      beginBodies.push(JSON.parse(String(init?.body)));
      return beginSession;
    }
    if (path.endsWith("/progress")) {
      const value = progressValues[Math.min(progressIndex, progressValues.length - 1)];
      progressIndex += 1;
      return value;
    }
    if (path.endsWith("/instructions")) {
      instructions += 1;
      return {
        instruction_id: `instruction-${instructions}`,
        method: "PUT",
        url: `https://storage.example.test/object?signature=${instructions}`,
        headers: { "Content-Length": "6" },
        cursor: pendingProgress().next_cursor,
        expires_at: future,
        expose_response_headers: ["ETag", "X-Missing"],
      };
    }
    if (path.endsWith("/complete")) return { session: { ...session, status: "object_completed" },
      dataset: { id: "dataset-1" }, version: { id: "version-1" } };
    if (path.endsWith("/abort")) return { id: session.id, status: "aborted" };
    throw new Error(`Unexpected API call ${path}`);
  });
  return { calls, instructions: () => instructions, beginBodies };
}

function installIndexedDb() {
  const values = new Map<IDBValidKey, unknown>();
  const createObjectStore = vi.fn();
  const close = vi.fn();
  const requestFor = (result: unknown) => {
    const request = {} as IDBRequest;
    queueMicrotask(() => {
      Object.defineProperty(request, "result", { configurable: true, value: result });
      request.onsuccess?.(new Event("success") as IDBRequestEventMap["success"]);
    });
    return request;
  };
  const database = {
    createObjectStore,
    close,
    transaction: () => ({
      objectStore: () => ({
        put: (value: { key: IDBValidKey }) => {
          values.set(value.key, structuredClone(value));
          return requestFor(value.key);
        },
        get: (key: IDBValidKey) => requestFor(values.get(key)),
        delete: (key: IDBValidKey) => {
          values.delete(key);
          return requestFor(undefined);
        },
      }),
    }),
  };
  const factory = {
    open: () => {
      const request = {} as IDBOpenDBRequest;
      queueMicrotask(() => {
        Object.defineProperty(request, "result", { configurable: true, value: database });
        request.onupgradeneeded?.(new Event("upgradeneeded") as IDBVersionChangeEvent);
        request.onsuccess?.(new Event("success") as IDBRequestEventMap["success"]);
      });
      return request;
    },
  };
  Object.defineProperty(globalThis, "indexedDB", {
    configurable: true,
    value: factory as unknown as IDBFactory,
  });
  return { values, createObjectStore, close };
}

async function waitFor(predicate: () => boolean) {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  throw new Error("Timed out waiting for the upload state.");
}

class HashWorker {
  onerror: (() => void) | null = null;
  onmessage: ((event: MessageEvent) => void) | null = null;
  terminated = false;
  private digest = new IncrementalSha256();

  postMessage(message: { type: string; snapshot?: Sha256Snapshot; id?: number; value?: ArrayBuffer }) {
    if (message.type === "start") this.digest = new IncrementalSha256(message.snapshot);
    if (message.type === "chunk") {
      this.digest.update(new Uint8Array(message.value!));
      queueMicrotask(() => this.onmessage?.({ data: {
        type: "checkpoint", id: message.id, snapshot: this.digest.snapshot(),
      } } as MessageEvent));
    }
    if (message.type === "finish") {
      queueMicrotask(() => this.onmessage?.({
        data: { type: "complete", sha256: this.digest.digestHex() },
      } as MessageEvent));
    }
  }

  terminate() { this.terminated = true; }
}

describe("resumable browser uploads", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    Object.defineProperty(globalThis, "Worker", { configurable: true, value: undefined });
    Object.defineProperty(globalThis, "indexedDB", { configurable: true, value: undefined });
  });

  afterEach(async () => {
    await removePersistedUpload(`${projectId}:records.csv:6`);
  });

  it("hashes and transfers directly to the provider before completing", async () => {
    const api = installApi([pendingProgress(), completeProgress()]);
    const xhr = installSuccessfulXhr();
    const telemetry: string[] = [];
    const file = new File(["abcdef"], "records.csv", {
      type: "text/csv",
      lastModified: 1,
    });

    const controller = createResumableUpload({
      ...options(file),
      onTelemetry: (value) => telemetry.push(value.stage),
    });
    const result = await controller.result;
    await controller._waitUntilResumed();

    expect(result.version).toEqual({ id: "version-1" });
    expect(xhr.opened).toEqual(["https://storage.example.test/object?signature=1"]);
    expect(api.calls.some((path) => path.endsWith("/complete"))).toBe(true);
    expect(telemetry).toContain("hashing");
    expect(telemetry).toContain("uploading");
    expect(telemetry.slice(-2)).toEqual(["verifying", "complete"]);
    await expect(loadPersistedUpload(`${projectId}:records.csv:6`)).resolves.toBeUndefined();
  });

  it("resolves the deployment storage region before creating a session", async () => {
    const api = installApi([pendingProgress(), completeProgress()]);
    installSuccessfulXhr();
    const file = new File(["abcdef"], "records.csv", { lastModified: 17 });

    await createResumableUpload({ ...options(file), dataRegion: undefined }).result;

    expect(api.calls).toContain("/capabilities");
    expect(api.beginBodies[0].data_region).toBe("af-south-1");
  });

  it("refreshes an expired instruction only after reconciling provider progress", async () => {
    const api = installApi([pendingProgress(), pendingProgress(), completeProgress()]);
    const xhr = installSuccessfulXhr([403, 200]);
    const file = new File(["abcdef"], "records.csv", { type: "text/csv", lastModified: 2 });

    await expect(createResumableUpload(options(file)).result).resolves.toMatchObject({
      version: { id: "version-1" },
    });
    expect(api.instructions()).toBe(2);
    expect(xhr.attempts()).toBe(2);
    expect(api.calls.filter((path) => path.endsWith("/progress"))).toHaveLength(3);
  });

  it("pauses before transfer, resumes, and completes without losing the cursor", async () => {
    installApi([pendingProgress(), completeProgress()]);
    const xhr = installSuccessfulXhr();
    const telemetry: string[] = [];
    const file = new File(["abcdef"], "records.csv", { type: "text/csv", lastModified: 4 });
    const controller = createResumableUpload({
      ...options(file), onTelemetry: (value) => telemetry.push(value.stage),
    });
    controller.pause();
    await waitFor(() => telemetry.includes("paused"));
    expect(xhr.attempts()).toBe(0);
    controller.resume();
    await expect(controller.result).resolves.toMatchObject({ version: { id: "version-1" } });
    expect(xhr.attempts()).toBe(1);
  });

  it("aborts the durable provider session and makes every control idempotent", async () => {
    const api = installApi([pendingProgress()]);
    const telemetry: string[] = [];
    const file = new File(["abcdef"], "records.csv", { type: "text/csv", lastModified: 5 });
    const controller = createResumableUpload({
      ...options(file), onTelemetry: (value) => telemetry.push(value.stage),
    });
    const rejected = expect(controller.result).rejects.toThrow("cancelled");
    controller.pause();
    await waitFor(() => telemetry.includes("paused"));
    await controller.cancel();
    await controller.cancel();
    controller.pause();
    controller.resume();
    await rejected;
    expect(api.calls.filter((path) => path.endsWith("/abort"))).toHaveLength(1);
  });

  it("uses a valid saved session and discards a stale file fingerprint", async () => {
    const file = new File(["abcdef"], "records.csv", { type: "text/csv", lastModified: 6 });
    const key = `${projectId}:records.csv:6`;
    await savePersistedUpload({
      key,
      projectId,
      fingerprint: `records.csv:6:${file.lastModified}`,
      contentSha256: session.expected_object_digest,
      session,
      receipts: [],
      updatedAt: new Date().toISOString(),
    });
    const api = installApi([completeProgress()]);
    await createResumableUpload(options(file)).result;
    expect(api.calls.filter((path) => path.endsWith("/datasets/uploads"))).toHaveLength(0);

    await savePersistedUpload({
      key,
      projectId,
      fingerprint: "wrong-fingerprint",
      contentSha256: session.expected_object_digest,
      session,
      receipts: [],
      updatedAt: new Date().toISOString(),
    });
    installSuccessfulXhr();
    const freshApi = installApi([pendingProgress(), completeProgress()]);
    await createResumableUpload(options(file)).result;
    expect(freshApi.calls.filter((path) => path.endsWith("/datasets/uploads"))).toHaveLength(1);
  });

  it("fails closed for a missing cursor and a repeatedly rejected fresh instruction", async () => {
    const noCursor = { ...pendingProgress(), next_cursor: null };
    installApi([noCursor]);
    const first = new File(["abcdef"], "records.csv", { lastModified: 7 });
    await expect(createResumableUpload(options(first)).result).rejects.toThrow("next required");
    await removePersistedUpload(`${projectId}:records.csv:6`);

    const retryApi = installApi(Array.from({ length: 6 }, pendingProgress));
    const xhr = installSuccessfulXhr([403]);
    const second = new File(["abcdef"], "records.csv", { lastModified: 8 });
    await expect(createResumableUpload(options(second)).result).rejects.toThrow("rejected");
    expect(retryApi.instructions()).toBe(5);
    expect(xhr.attempts()).toBe(5);
  });

  it("accepts provider-completed progress after an ambiguous transfer failure", async () => {
    installApi([pendingProgress(), completeProgress()]);
    const xhr = installSuccessfulXhr([403]);
    const file = new File(["abcdef"], "records.csv", { lastModified: 9 });
    await expect(createResumableUpload(options(file)).result).resolves.toMatchObject({
      version: { id: "version-1" },
    });
    expect(xhr.attempts()).toBe(1);
  });

  it("retries ordinary and provider failures with bounded backoff", async () => {
    installApi([pendingProgress(), completeProgress()]);
    const random = vi.spyOn(Math, "random").mockReturnValue(0);
    const xhr = installSuccessfulXhr(["throw", 500, 200]);
    const file = new File(["abcdef"], "records.csv", { lastModified: 11 });
    await expect(createResumableUpload(options(file)).result).resolves.toMatchObject({
      version: { id: "version-1" },
    });
    expect(xhr.attempts()).toBe(3);
    expect(random).toHaveBeenCalled();
  });

  it("uses jittered backoff when the provider omits Retry-After", async () => {
    installApi([pendingProgress(), completeProgress()]);
    const random = vi.spyOn(Math, "random").mockReturnValue(0);
    const xhr = installSuccessfulXhr([500, 200], null);
    const file = new File(["abcdef"], "records.csv", { lastModified: 16 });

    await expect(createResumableUpload(options(file)).result).resolves.toMatchObject({
      version: { id: "version-1" },
    });
    expect(xhr.attempts()).toBe(2);
    expect(random).toHaveBeenCalled();
  });

  it("reconciles an ambiguous provider failure after the transfer retry ceiling", async () => {
    installApi([pendingProgress(), completeProgress()]);
    const xhr = installSuccessfulXhr([500, 500, 500, 500, 500]);
    const file = new File(["abcdef"], "records.csv", { lastModified: 12 });
    await expect(createResumableUpload(options(file)).result).resolves.toMatchObject({
      version: { id: "version-1" },
    });
    expect(xhr.attempts()).toBe(5);
  });

  it("uses Blob.arrayBuffer when the browser exposes the native implementation", async () => {
    const file = new File(["abcdef"], "records.csv", { lastModified: 13 });
    Object.defineProperty(file, "arrayBuffer", {
      configurable: true,
      value: async () => new TextEncoder().encode("abcdef").buffer,
    });
    await expect(hashFileIncrementally(file)).resolves.toBe(session.expected_object_digest);
  });

  it("runs hashing through a bounded worker and terminates it", async () => {
    const workers: HashWorker[] = [];
    Object.defineProperty(globalThis, "Worker", {
      configurable: true,
      value: class extends HashWorker {
        constructor() { super(); workers.push(this); }
      },
    });
    const checkpoints: number[] = [];
    const file = new File(["abcdef"], "records.csv", { lastModified: 10 });
    await expect(hashFileIncrementally(file, (processed) => checkpoints.push(processed)))
      .resolves.toBe(session.expected_object_digest);
    expect(checkpoints).toEqual([6]);
    expect(workers[0].terminated).toBe(true);
  });

  it("rejects invalid hash checkpoints and cancellation before session creation", async () => {
    const file = new File(["abcdef"], "records.csv", { lastModified: 3 });
    await expect(hashFileIncrementally(file, undefined, {
      state: Array(8).fill(0), buffer: [], bytesHashed: 7,
    })).rejects.toThrow("exceeds");

    const controller = createResumableUpload(options(file));
    await controller.cancel();
    await expect(controller.result).rejects.toThrow("cancelled");
  });

  it("persists and removes a resumable checkpoint in the memory fallback", async () => {
    const key = "memory-upload";
    const value = {
      key,
      projectId,
      fingerprint: "records.csv:6:1",
      receipts: [],
      updatedAt: new Date().toISOString(),
    };
    await savePersistedUpload(value);
    expect(await loadPersistedUpload(key)).toEqual(value);
    await removePersistedUpload(key);
    expect(await loadPersistedUpload(key)).toBeUndefined();
  });

  it("persists checkpoints through IndexedDB when it is available", async () => {
    const indexed = installIndexedDb();
    const key = "indexed-upload";
    const value = {
      key,
      projectId,
      fingerprint: "records.csv:6:14",
      receipts: [],
      updatedAt: new Date().toISOString(),
    };
    await savePersistedUpload(value);
    expect(await loadPersistedUpload(key)).toEqual(value);
    await removePersistedUpload(key);
    expect(await loadPersistedUpload(key)).toBeUndefined();
    expect(indexed.createObjectStore).toHaveBeenCalledWith("uploads", { keyPath: "key" });
    expect(indexed.close).toHaveBeenCalledTimes(4);
  });

  it("reports a defined percentage for an empty upload", async () => {
    const emptySession = { ...session, byte_size: 0, part_size: 1, total_parts: 0 };
    installApi([{ ...completeProgress(), confirmed_bytes: 0, total_bytes: 0 }], emptySession);
    const telemetry: number[] = [];
    const file = new File([], "records.csv", { lastModified: 15 });
    await createResumableUpload({
      ...options(file), onTelemetry: (value) => telemetry.push(value.percent),
    }).result;
    expect(telemetry.every((value) => Number.isFinite(value))).toBe(true);
    expect(telemetry[0]).toBe(0);
  });

  it("waits for verification and returns the ready upload", async () => {
    vi.spyOn(apiModule, "api")
      .mockResolvedValueOnce({ ...session, status: "verifying" })
      .mockResolvedValueOnce({ ...session, status: "ready" });
    await expect(waitForVerifiedUpload(projectId, session.id, { attempts: 2, intervalMs: 0 }))
      .resolves.toMatchObject({ status: "ready" });
  });

  it("returns fresh dataset metadata after verification", async () => {
    vi.spyOn(apiModule, "api")
      .mockResolvedValueOnce({ ...session, status: "ready" })
      .mockResolvedValueOnce({
        session: { ...session, status: "ready" },
        dataset: { id: "dataset-1" },
        version: { id: "version-1", schema_json: { columns: [{ name: "account" }] } },
      });
    await expect(waitForUploadResult(projectId, session.id)).resolves.toMatchObject({
      version: { schema_json: { columns: [{ name: "account" }] } },
    });
    expect(vi.mocked(apiModule.api).mock.calls[1][0]).toContain("/result");
  });

  it("fails closed for terminal verification and timeout", async () => {
    vi.spyOn(apiModule, "api").mockResolvedValueOnce({ ...session, status: "quarantined" });
    await expect(waitForVerifiedUpload(projectId, session.id, { attempts: 1, intervalMs: 0 }))
      .rejects.toThrow("quarantined");
    vi.mocked(apiModule.api).mockResolvedValue({ ...session, status: "verifying" });
    await expect(waitForVerifiedUpload(projectId, session.id, { attempts: 1, intervalMs: 0 }))
      .rejects.toThrow("did not finish");
  });
});
