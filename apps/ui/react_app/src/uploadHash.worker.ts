import { IncrementalSha256, type Sha256Snapshot } from "./incrementalSha256";

type HashMessage =
  | { type: "start"; snapshot?: Sha256Snapshot }
  | { type: "chunk"; id: number; value: ArrayBuffer }
  | { type: "finish" };

let digest = new IncrementalSha256();
self.onmessage = (event: MessageEvent<HashMessage>) => {
  if (event.data.type === "start") {
    digest = new IncrementalSha256(event.data.snapshot);
    self.postMessage({ type: "started" });
  } else if (event.data.type === "chunk") {
    digest.update(new Uint8Array(event.data.value));
    self.postMessage({ type: "checkpoint", id: event.data.id, snapshot: digest.snapshot() });
  } else {
    self.postMessage({ type: "complete", sha256: digest.digestHex() });
  }
};
