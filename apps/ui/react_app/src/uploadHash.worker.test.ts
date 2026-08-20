import { describe, expect, it, vi } from "vitest";

describe("upload hashing worker protocol", () => {
  it("starts, checkpoints, and completes one incremental digest", async () => {
    const messages: Array<Record<string, unknown>> = [];
    vi.spyOn(globalThis, "postMessage").mockImplementation((value) => {
      messages.push(value as Record<string, unknown>);
    });
    await import("./uploadHash.worker");
    const handler = self.onmessage as (event: MessageEvent) => void;

    handler({ data: { type: "start" } } as MessageEvent);
    const value = new TextEncoder().encode("abcdef").buffer;
    handler({ data: { type: "chunk", id: 6, value } } as MessageEvent);
    handler({ data: { type: "finish" } } as MessageEvent);

    expect(messages.map((message) => message.type)).toEqual([
      "started", "checkpoint", "complete",
    ]);
    expect(messages[1]).toMatchObject({ type: "checkpoint", id: 6 });
    expect(messages[2]).toEqual({
      type: "complete",
      sha256: "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721",
    });
  });
});
