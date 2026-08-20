import { describe, expect, it } from "vitest";
import { IncrementalSha256 } from "./incrementalSha256";

const bytes = (value: string) => new TextEncoder().encode(value);

describe("IncrementalSha256", () => {
  it.each([
    ["", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"],
    ["abc", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"],
    ["a".repeat(1000), "41edece42d63e8d9bf515a9ba6932e1c20cbc9f5a5d134645adb5db1b9737ea3"],
  ])("matches the SHA-256 standard vector", (value, expected) => {
    expect(new IncrementalSha256().update(bytes(value)).digestHex()).toBe(expected);
  });

  it("resumes from a defensive snapshot across a compression boundary", () => {
    const first = new IncrementalSha256().update(bytes("checkpoint-"));
    const snapshot = first.snapshot();
    snapshot.state[0] = 0;
    snapshot.buffer[0] = 0;

    const pristine = new IncrementalSha256().update(bytes("checkpoint-")).snapshot();
    const resumed = new IncrementalSha256(pristine).update(bytes("x".repeat(100)));
    const direct = new IncrementalSha256().update(bytes(`checkpoint-${"x".repeat(100)}`));
    expect(resumed.digestHex()).toBe(direct.digestHex());
  });

  it("rejects corrupt checkpoints and use after finalization", () => {
    expect(() => new IncrementalSha256({ state: [], buffer: [], bytesHashed: 0 }))
      .toThrow("Invalid SHA-256 snapshot");
    expect(() => new IncrementalSha256({
      state: Array(8).fill(0), buffer: Array(64).fill(0), bytesHashed: 64,
    })).toThrow("Invalid SHA-256 snapshot");
    expect(() => new IncrementalSha256({
      state: Array(8).fill(0), buffer: [1, 2], bytesHashed: 1,
    })).toThrow("Invalid SHA-256 snapshot");

    const digest = new IncrementalSha256().update(bytes("done"));
    digest.digestHex();
    expect(() => digest.update(bytes("again"))).toThrow("already finalized");
    expect(() => digest.snapshot()).toThrow("cannot be resumed");
    expect(() => digest.digestHex()).toThrow("already finalized");
  });
});
