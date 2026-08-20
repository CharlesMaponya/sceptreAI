export type Sha256Snapshot = {
  state: number[];
  buffer: number[];
  bytesHashed: number;
};

const INITIAL = [
  0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
  0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];
const K = [
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

const rotate = (value: number, bits: number) => (value >>> bits) | (value << (32 - bits));

export class IncrementalSha256 {
  private state: number[];
  private buffer: number[];
  private bytesHashed: number;
  private finished = false;

  constructor(snapshot?: Sha256Snapshot) {
    this.state = snapshot ? [...snapshot.state] : [...INITIAL];
    this.buffer = snapshot ? [...snapshot.buffer] : [];
    this.bytesHashed = snapshot?.bytesHashed || 0;
    if (this.state.length !== 8 || this.buffer.length >= 64 || this.bytesHashed < this.buffer.length) {
      throw new Error("Invalid SHA-256 snapshot.");
    }
  }

  update(input: Uint8Array): this {
    if (this.finished) throw new Error("SHA-256 digest is already finalized.");
    this.bytesHashed += input.length;
    let offset = 0;
    if (this.buffer.length) {
      const needed = Math.min(64 - this.buffer.length, input.length);
      for (let index = 0; index < needed; index += 1) this.buffer.push(input[index]);
      offset += needed;
      if (this.buffer.length === 64) {
        this.compress(Uint8Array.from(this.buffer));
        this.buffer = [];
      }
    }
    while (offset + 64 <= input.length) {
      this.compress(input.subarray(offset, offset + 64));
      offset += 64;
    }
    while (offset < input.length) this.buffer.push(input[offset++]);
    return this;
  }

  snapshot(): Sha256Snapshot {
    if (this.finished) throw new Error("A finalized SHA-256 cannot be resumed.");
    return { state: [...this.state], buffer: [...this.buffer], bytesHashed: this.bytesHashed };
  }

  digestHex(): string {
    if (this.finished) throw new Error("SHA-256 digest is already finalized.");
    this.finished = true;
    const final = [...this.buffer, 0x80];
    while (final.length % 64 !== 56) final.push(0);
    const bitsHigh = Math.floor(this.bytesHashed / 0x20000000);
    const bitsLow = (this.bytesHashed << 3) >>> 0;
    for (let shift = 24; shift >= 0; shift -= 8) final.push((bitsHigh >>> shift) & 0xff);
    for (let shift = 24; shift >= 0; shift -= 8) final.push((bitsLow >>> shift) & 0xff);
    for (let offset = 0; offset < final.length; offset += 64) {
      this.compress(Uint8Array.from(final.slice(offset, offset + 64)));
    }
    return this.state.map((value) => (value >>> 0).toString(16).padStart(8, "0")).join("");
  }

  private compress(chunk: Uint8Array) {
    const words = new Uint32Array(64);
    for (let index = 0; index < 16; index += 1) {
      const offset = index * 4;
      words[index] = ((chunk[offset] << 24) | (chunk[offset + 1] << 16)
        | (chunk[offset + 2] << 8) | chunk[offset + 3]) >>> 0;
    }
    for (let index = 16; index < 64; index += 1) {
      const x = words[index - 15];
      const y = words[index - 2];
      const s0 = rotate(x, 7) ^ rotate(x, 18) ^ (x >>> 3);
      const s1 = rotate(y, 17) ^ rotate(y, 19) ^ (y >>> 10);
      words[index] = (words[index - 16] + s0 + words[index - 7] + s1) >>> 0;
    }
    let [a, b, c, d, e, f, g, h] = this.state;
    for (let index = 0; index < 64; index += 1) {
      const s1 = rotate(e, 6) ^ rotate(e, 11) ^ rotate(e, 25);
      const choice = (e & f) ^ (~e & g);
      const t1 = (h + s1 + choice + K[index] + words[index]) >>> 0;
      const s0 = rotate(a, 2) ^ rotate(a, 13) ^ rotate(a, 22);
      const majority = (a & b) ^ (a & c) ^ (b & c);
      const t2 = (s0 + majority) >>> 0;
      h = g; g = f; f = e; e = (d + t1) >>> 0; d = c; c = b; b = a; a = (t1 + t2) >>> 0;
    }
    const values = [a, b, c, d, e, f, g, h];
    this.state = this.state.map((value, index) => (value + values[index]) >>> 0);
  }
}
