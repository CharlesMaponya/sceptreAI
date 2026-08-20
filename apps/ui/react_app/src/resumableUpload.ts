import { api, json } from "./api";
import { IncrementalSha256, type Sha256Snapshot } from "./incrementalSha256";

export type UploadKind = "dataset" | "validation" | "drift" | "offline_scoring";
export type UploadStage = "hashing" | "uploading" | "paused" | "verifying" | "complete" | "cancelled";
export type UploadCursor = { unit_number: number; offset: number; length: number; checksum_sha256?: string | null };
export type UploadReceipt = {
  unit_number: number; offset: number; length: number; etag?: string | null;
  checksum_sha256?: string | null; provider_headers: Record<string, string>;
};
export type UploadSession = {
  id: string; project_id: string; upload_kind: UploadKind; status: string;
  provider_driver: string; protocol: string; byte_size: number; part_size: number;
  total_parts: number; confirmed_bytes: number; resume_key: string; expires_at: string;
  original_filename: string; expected_object_digest: string; sensitivity: string;
  data_region: string; legal_hold: boolean; next_cursor: UploadCursor | null;
  capabilities: { parallel_chunks_per_object: number };
};
export type UploadProgress = {
  session_id: string; status: string; confirmed_bytes: number; total_bytes: number;
  receipts: UploadReceipt[]; next_cursor: UploadCursor | null; expires_at: string; complete: boolean;
};
export type UploadCompletion<TDataset = unknown, TVersion = unknown> = {
  session: UploadSession; dataset: TDataset | null; version: TVersion | null;
};
export type UploadTelemetry = {
  stage: UploadStage; confirmedBytes: number; totalBytes: number; percent: number;
  bytesPerSecond: number; retry: number; expiresAt?: string;
};
export type ResumableUploadOptions = {
  projectId: string;
  file: File;
  datasetName: string;
  description?: string;
  uploadKind?: UploadKind;
  sensitivity: "public" | "internal" | "confidential" | "restricted";
  dataRegion?: string;
  retentionDays?: number;
  legalHold?: boolean;
  tags?: Record<string, unknown>;
  targetMetadata?: Record<string, unknown>;
  onTelemetry?: (telemetry: UploadTelemetry) => void;
  onHashCheckpoint?: (snapshot: Sha256Snapshot) => void;
};

type PersistedUpload = {
  key: string; projectId: string; fingerprint: string; contentSha256?: string;
  hashSnapshot?: Sha256Snapshot; session?: UploadSession; receipts: UploadReceipt[];
  updatedAt: string;
};

const DB_NAME = "sceptre-resumable-uploads";
const STORE_NAME = "uploads";
const HASH_CHUNK_SIZE = 8 * 1024 * 1024;
const memoryFallback = new Map<string, PersistedUpload>();

export class ResumableUploadController<TDataset = unknown, TVersion = unknown> {
  readonly result: Promise<UploadCompletion<TDataset, TVersion>>;
  private paused = false;
  private cancelled = false;
  private resumeWaiters: Array<() => void> = [];
  private session: UploadSession | null = null;
  private activeRequest: XMLHttpRequest | null = null;

  constructor(options: ResumableUploadOptions) {
    this.result = runUpload(options, this);
  }

  pause() {
    if (this.cancelled) return;
    this.paused = true;
    this.activeRequest?.abort();
  }

  resume() {
    if (this.cancelled) return;
    this.paused = false;
    this.resumeWaiters.splice(0).forEach((resolve) => resolve());
  }

  async cancel() {
    if (this.cancelled) return;
    this.cancelled = true;
    this.activeRequest?.abort();
    this.paused = false;
    this.resumeWaiters.splice(0).forEach((resolve) => resolve());
    if (this.session) {
      await api(`/projects/${this.session.project_id}/datasets/uploads/${this.session.id}/abort`,
        json("POST", {}));
      await removePersistedUpload(persistenceKey(this.session.project_id, this.session.original_filename,
        this.session.byte_size));
    }
  }

  _setSession(session: UploadSession) { this.session = session; }
  _setRequest(request: XMLHttpRequest | null) { this.activeRequest = request; }
  _isCancelled() { return this.cancelled; }
  _isPaused() { return this.paused; }
  async _waitUntilResumed() {
    if (!this.paused) return;
    await new Promise<void>((resolve) => this.resumeWaiters.push(resolve));
  }
}

export function createResumableUpload<TDataset = unknown, TVersion = unknown>(
  options: ResumableUploadOptions,
) {
  return new ResumableUploadController<TDataset, TVersion>(options);
}

export async function waitForVerifiedUpload(
  projectId: string,
  sessionId: string,
  { attempts = 120, intervalMs = 1000 }: { attempts?: number; intervalMs?: number } = {},
): Promise<UploadSession> {
  const terminalFailures = new Set(["aborted", "expired", "quarantined", "failed"]);
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    const session = await api<UploadSession>(
      `/projects/${projectId}/datasets/uploads/${sessionId}`,
    );
    if (session.status === "ready") return session;
    if (terminalFailures.has(session.status)) {
      throw new Error(`Upload verification ended with status ${session.status}.`);
    }
    if (attempt + 1 < attempts) await delay(intervalMs);
  }
  throw new Error("Upload verification did not finish before the local wait deadline.");
}

export async function waitForUploadResult<TDataset = unknown, TVersion = unknown>(
  projectId: string,
  sessionId: string,
): Promise<UploadCompletion<TDataset, TVersion>> {
  await waitForVerifiedUpload(projectId, sessionId, { attempts: 3600, intervalMs: 1000 });
  return api<UploadCompletion<TDataset, TVersion>>(
    `/projects/${projectId}/datasets/uploads/${sessionId}/result`,
  );
}

export async function hashFileIncrementally(
  file: File,
  onProgress?: (processed: number, snapshot: Sha256Snapshot) => void,
  snapshot?: Sha256Snapshot,
): Promise<string> {
  if (snapshot && snapshot.bytesHashed > file.size) {
    throw new Error("The saved integrity checkpoint exceeds the selected file size.");
  }
  if (typeof Worker === "undefined") {
    const digest = new IncrementalSha256(snapshot);
    for (let offset = snapshot?.bytesHashed || 0; offset < file.size; offset += HASH_CHUNK_SIZE) {
      digest.update(new Uint8Array(await blobArrayBuffer(file.slice(offset, offset + HASH_CHUNK_SIZE))));
      onProgress?.(Math.min(file.size, offset + HASH_CHUNK_SIZE), digest.snapshot());
    }
    return digest.digestHex();
  }
  const worker = new Worker(new URL("./uploadHash.worker.ts", import.meta.url), { type: "module" });
  let checkpoint: (() => void) | null = null;
  let checkpointFailure: ((reason: Error) => void) | null = null;
  const completion = new Promise<string>((resolve, reject) => {
    worker.onerror = () => {
      const error = new Error("The upload integrity worker failed.");
      checkpointFailure?.(error);
      reject(error);
    };
    worker.onmessage = (event: MessageEvent) => {
      if (event.data.type === "checkpoint") {
        onProgress?.(event.data.id, event.data.snapshot);
        checkpoint?.();
        checkpoint = null;
        checkpointFailure = null;
      }
      if (event.data.type === "complete") resolve(event.data.sha256);
    };
  });
  try {
    worker.postMessage({ type: "start", snapshot });
    for (let offset = snapshot?.bytesHashed || 0; offset < file.size; offset += HASH_CHUNK_SIZE) {
      const end = Math.min(file.size, offset + HASH_CHUNK_SIZE);
      const chunk = await blobArrayBuffer(file.slice(offset, end));
      const acknowledged = new Promise<void>((resolve, reject) => {
        checkpoint = resolve;
        checkpointFailure = reject;
      });
      worker.postMessage({ type: "chunk", id: end, value: chunk }, [chunk]);
      await acknowledged;
    }
    worker.postMessage({ type: "finish" });
    return await completion;
  } catch (error) {
    void completion.catch(() => undefined);
    throw error;
  } finally {
    worker.terminate();
  }
}

async function runUpload<TDataset, TVersion>(
  options: ResumableUploadOptions,
  controller: ResumableUploadController<TDataset, TVersion>,
): Promise<UploadCompletion<TDataset, TVersion>> {
  const key = persistenceKey(options.projectId, options.file.name, options.file.size);
  const fingerprint = fileFingerprint(options.file);
  let persisted = await loadPersistedUpload(key);
  if (persisted && persisted.fingerprint !== fingerprint) {
    await removePersistedUpload(key);
    persisted = undefined;
  }
  emit(options, "hashing", 0, options.file.size, 0, 0);
  let checkpointWrite = Promise.resolve();
  const sha256 = persisted?.contentSha256 || await hashFileIncrementally(
    options.file,
    (processed, snapshot) => {
      options.onHashCheckpoint?.(snapshot);
      persisted = { key, projectId: options.projectId, fingerprint, hashSnapshot: snapshot,
        receipts: persisted?.receipts || [], updatedAt: new Date().toISOString() };
      checkpointWrite = checkpointWrite.then(() => savePersistedUpload(persisted!));
      emit(options, "hashing", processed, options.file.size, 0, 0);
    },
    persisted?.hashSnapshot,
  );
  await checkpointWrite;
  persisted = { ...(persisted || { key, projectId: options.projectId, fingerprint, receipts: [] }),
    contentSha256: sha256, updatedAt: new Date().toISOString() };
  await savePersistedUpload(persisted);
  if (controller._isCancelled()) throw new Error("The upload was cancelled.");

  let session = persisted.session;
  if (!session || session.expected_object_digest !== sha256 || new Date(session.expires_at) <= new Date()) {
    const dataRegion = options.dataRegion || (await api<{ upload_data_region: string }>(
      "/capabilities",
    )).upload_data_region;
    session = await api<UploadSession>(`/projects/${options.projectId}/datasets/uploads`,
      json("POST", {
        upload_kind: options.uploadKind || "dataset", dataset_name: options.datasetName,
        description: options.description || null, filename: options.file.name,
        byte_size: options.file.size, content_type: options.file.type || "application/octet-stream",
        sha256, sensitivity: options.sensitivity, data_region: dataRegion,
        retention_days: options.retentionDays || null, legal_hold: options.legalHold || false,
        tags: options.tags || {}, target_metadata: options.targetMetadata || {},
      }));
    persisted = { ...persisted, session, receipts: [], updatedAt: new Date().toISOString() };
    await savePersistedUpload(persisted);
  }
  controller._setSession(session);
  let progress = await api<UploadProgress>(
    `/projects/${options.projectId}/datasets/uploads/${session.id}/progress`);
  const startedAt = performance.now();
  let failedInstructionRefreshes = 0;
  while (!progress.complete) {
    if (controller._isCancelled()) throw new Error("The upload was cancelled.");
    if (controller._isPaused()) {
      emit(options, "paused", progress.confirmed_bytes, progress.total_bytes, 0, 0, session.expires_at);
      await controller._waitUntilResumed();
      continue;
    }
    const cursor = progress.next_cursor;
    if (!cursor) throw new Error("The provider did not expose the next required transfer cursor.");
    const value = await blobArrayBuffer(options.file.slice(cursor.offset, cursor.offset + cursor.length));
    cursor.checksum_sha256 = await arrayBufferSha256(value);
    const instruction = await api<TransferInstruction>(
      `/projects/${options.projectId}/datasets/uploads/${session.id}/instructions`,
      json("POST", { cursor }));
    let receipt: UploadReceipt;
    try {
      receipt = await transferUnit(instruction, value, controller, (loaded) => {
        const elapsed = Math.max(0.001, (performance.now() - startedAt) / 1000);
        emit(options, "uploading", progress.confirmed_bytes + loaded, progress.total_bytes,
          (progress.confirmed_bytes + loaded) / elapsed, 0, session!.expires_at);
      }, (retry) => emit(options, "uploading", progress.confirmed_bytes,
        progress.total_bytes, 0, retry, session!.expires_at));
      failedInstructionRefreshes = 0;
    } catch (error) {
      if (controller._isCancelled()) throw error;
      progress = await api<UploadProgress>(
        `/projects/${options.projectId}/datasets/uploads/${session.id}/progress`);
      const providerAdvanced = progress.complete || (
        progress.next_cursor !== null && progress.next_cursor.offset !== cursor.offset
      );
      if (providerAdvanced) continue;
      failedInstructionRefreshes += 1;
      if (failedInstructionRefreshes >= 5) throw error;
      continue;
    }
    persisted.receipts = [...persisted.receipts.filter(
      (item) => item.unit_number !== receipt.unit_number), receipt];
    persisted.updatedAt = new Date().toISOString();
    await savePersistedUpload(persisted);
    progress = await api<UploadProgress>(
      `/projects/${options.projectId}/datasets/uploads/${session.id}/progress`);
  }
  emit(options, "verifying", progress.confirmed_bytes, progress.total_bytes, 0, 0, session.expires_at);
  const completion = await api<UploadCompletion<TDataset, TVersion>>(
    `/projects/${options.projectId}/datasets/uploads/${session.id}/complete`,
    json("POST", { sha256, receipts: persisted.receipts }));
  await removePersistedUpload(key);
  emit(options, "complete", progress.total_bytes, progress.total_bytes, 0, 0, session.expires_at);
  return completion;
}

type TransferInstruction = {
  instruction_id: string; method: "PUT" | "POST"; url: string; headers: Record<string, string>;
  cursor: UploadCursor; expires_at: string; expose_response_headers: string[];
};

async function transferUnit<TDataset, TVersion>(
  instruction: TransferInstruction,
  value: ArrayBuffer,
  controller: ResumableUploadController<TDataset, TVersion>,
  onProgress: (loaded: number) => void,
  onRetry: (retry: number) => void,
): Promise<UploadReceipt> {
  let retry = 0;
  while (true) {
    try {
      return await xhrTransfer(instruction, value, controller, onProgress);
    } catch (error) {
      if (controller._isCancelled()) throw new Error("The upload was cancelled.", { cause: error });
      if (controller._isPaused()) {
        await controller._waitUntilResumed();
        continue;
      }
      retry += 1;
      onRetry(retry);
      if (error instanceof ProviderTransferError && [401, 403].includes(error.status)) throw error;
      if (retry >= 5) throw error;
      const retryAfter = error instanceof ProviderTransferError ? error.retryAfterSeconds : null;
      const ceiling = Math.min(30_000, 500 * (2 ** retry));
      await delay(retryAfter == null ? Math.random() * ceiling : retryAfter * 1000);
    }
  }
}

class ProviderTransferError extends Error {
  constructor(message: string, readonly status: number, readonly retryAfterSeconds: number | null) {
    super(message);
  }
}

function xhrTransfer<TDataset, TVersion>(
  instruction: TransferInstruction,
  value: ArrayBuffer,
  controller: ResumableUploadController<TDataset, TVersion>,
  onProgress: (loaded: number) => void,
): Promise<UploadReceipt> {
  return new Promise((resolve, reject) => {
    const request = new XMLHttpRequest();
    controller._setRequest(request);
    request.open(instruction.method, instruction.url);
    Object.entries(instruction.headers).forEach(([key, header]) => request.setRequestHeader(key, header));
    request.upload.onprogress = (event) => onProgress(event.loaded);
    request.onerror = () => reject(new ProviderTransferError("Provider transfer failed.", 0, null));
    request.onabort = () => reject(new ProviderTransferError("Provider transfer interrupted.", 0, null));
    request.onload = () => {
      controller._setRequest(null);
      if (!((request.status >= 200 && request.status < 300) || request.status === 308)) {
        const retryAfter = Number.parseFloat(request.getResponseHeader("Retry-After") || "");
        reject(new ProviderTransferError("Provider rejected the transfer unit.", request.status,
          Number.isFinite(retryAfter) ? retryAfter : null));
        return;
      }
      const providerHeaders: Record<string, string> = {};
      instruction.expose_response_headers.forEach((name) => {
        const header = request.getResponseHeader(name);
        if (header) providerHeaders[name] = header;
      });
      resolve({ unit_number: instruction.cursor.unit_number, offset: instruction.cursor.offset,
        length: instruction.cursor.length, etag: request.getResponseHeader("ETag"),
        checksum_sha256: request.getResponseHeader("x-amz-checksum-sha256"), provider_headers: providerHeaders });
    };
    request.send(value);
  });
}

async function arrayBufferSha256(value: ArrayBuffer): Promise<string> {
  return new IncrementalSha256().update(new Uint8Array(value)).digestHex();
}

function blobArrayBuffer(blob: Blob): Promise<ArrayBuffer> {
  if (typeof blob.arrayBuffer === "function") return blob.arrayBuffer();
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(reader.error || new Error("The selected file could not be read."));
    reader.onload = () => resolve(reader.result as ArrayBuffer);
    reader.readAsArrayBuffer(blob);
  });
}

function emit(options: ResumableUploadOptions,
  stage: UploadStage, confirmedBytes: number, totalBytes: number, bytesPerSecond: number,
  retry: number, expiresAt?: string) {
  options.onTelemetry?.({ stage, confirmedBytes, totalBytes,
    percent: totalBytes ? Math.min(100, Math.round((confirmedBytes / totalBytes) * 100)) : 0,
    bytesPerSecond, retry, expiresAt });
}

const delay = (milliseconds: number) => new Promise<void>((resolve) => setTimeout(resolve, milliseconds));
const persistenceKey = (projectId: string, filename: string, size: number) => `${projectId}:${filename}:${size}`;
const fileFingerprint = (file: File) => `${file.name}:${file.size}:${file.lastModified}`;

async function database(): Promise<IDBDatabase | null> {
  if (typeof indexedDB === "undefined") return null;
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, 1);
    request.onupgradeneeded = () => request.result.createObjectStore(STORE_NAME, { keyPath: "key" });
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

export async function savePersistedUpload(value: PersistedUpload) {
  const db = await database();
  if (!db) { memoryFallback.set(value.key, structuredClone(value)); return; }
  await transactionPromise(db, "readwrite", (store) => store.put(value));
  db.close();
}

export async function loadPersistedUpload(key: string): Promise<PersistedUpload | undefined> {
  const db = await database();
  if (!db) return memoryFallback.get(key);
  const value = await transactionPromise<PersistedUpload | undefined>(
    db, "readonly", (store) => store.get(key));
  db.close();
  return value;
}

export async function removePersistedUpload(key: string) {
  const db = await database();
  if (!db) { memoryFallback.delete(key); return; }
  await transactionPromise(db, "readwrite", (store) => store.delete(key));
  db.close();
}

function transactionPromise<T = void>(db: IDBDatabase, mode: IDBTransactionMode,
  execute: (store: IDBObjectStore) => IDBRequest): Promise<T> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(STORE_NAME, mode);
    const request = execute(transaction.objectStore(STORE_NAME));
    request.onsuccess = () => resolve(request.result as T);
    request.onerror = () => reject(request.error);
    transaction.onerror = () => reject(transaction.error);
  });
}
