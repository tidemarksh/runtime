export interface KernelWorkerBackingSource {
  byteLength: number;
  eagerPrewarm?: boolean;
  readRange(offset: number, length: number, options?: { readAhead?: boolean }): Uint8Array | null;
}

export class Uint8ArrayBackingSource implements KernelWorkerBackingSource {
  private readonly data: Uint8Array;

  constructor(data: Uint8Array) {
    this.data = new Uint8Array(data);
  }

  get byteLength(): number {
    return this.data.byteLength;
  }

  readRange(offset: number, length: number): Uint8Array | null {
    if (
      !Number.isInteger(offset)
      || !Number.isInteger(length)
      || offset < 0
      || length < 0
      || offset + length > this.data.byteLength
    ) {
      return null;
    }
    return this.data.slice(offset, offset + length);
  }
}

export class ChunkedBackingSource implements KernelWorkerBackingSource {
  private readonly chunks = new Map<number, Uint8Array>();

  constructor(readonly byteLength: number) {
    if (!Number.isInteger(byteLength) || byteLength < 0) {
      throw new RangeError(`invalid backing source byte length: ${byteLength}`);
    }
  }

  putRange(offset: number, data: Uint8Array): void {
    if (
      !Number.isInteger(offset)
      || offset < 0
      || offset + data.byteLength > this.byteLength
    ) {
      throw new RangeError(`invalid backing source range: offset=${offset} length=${data.byteLength}`);
    }
    this.chunks.set(offset, new Uint8Array(data));
  }

  readRange(offset: number, length: number): Uint8Array | null {
    if (
      !Number.isInteger(offset)
      || !Number.isInteger(length)
      || offset < 0
      || length < 0
      || offset + length > this.byteLength
    ) {
      return null;
    }
    if (length === 0) {
      return new Uint8Array();
    }
    const direct = this.chunks.get(offset);
    if (direct && direct.byteLength >= length) {
      return direct.slice(0, length);
    }

    const out = new Uint8Array(length);
    let copied = 0;
    const ordered = [...this.chunks.entries()].sort(([left], [right]) => left - right);
    for (const [chunkOffset, chunk] of ordered) {
      const targetOffset = offset + copied;
      if (chunkOffset > targetOffset) {
        break;
      }
      const chunkEnd = chunkOffset + chunk.byteLength;
      if (chunkEnd <= targetOffset) {
        continue;
      }
      const startInChunk = targetOffset - chunkOffset;
      const copyLength = Math.min(length - copied, chunk.byteLength - startInChunk);
      out.set(chunk.subarray(startInChunk, startInChunk + copyLength), copied);
      copied += copyLength;
      if (copied === length) {
        return out;
      }
    }
    return null;
  }
}

function normalizeSha256Digest(digest: string): string {
  const value = digest.startsWith("sha256:") ? digest.slice("sha256:".length) : digest;
  const normalized = value.toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(normalized)) {
    throw new Error(`invalid sha256 digest: ${digest}`);
  }
  return normalized;
}

async function sha256Hex(data: Uint8Array): Promise<string> {
  const digest = await globalThis.crypto.subtle.digest(
    "SHA-256",
    data.slice().buffer as ArrayBuffer,
  );
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

export async function createSha256VerifiedBackingSource(
  data: Uint8Array,
  expectedDigest: string,
): Promise<KernelWorkerBackingSource> {
  const expected = normalizeSha256Digest(expectedDigest);
  const actual = await sha256Hex(data);
  if (actual !== expected) {
    throw new Error(`backing source digest mismatch: expected=sha256:${expected} actual=sha256:${actual}`);
  }
  return new Uint8ArrayBackingSource(data);
}
