export type RemoteBackingFetch = (
  url: string,
  init: { headers: Record<string, string> },
) => Promise<Pick<Response, "arrayBuffer" | "ok" | "status">>;

export interface RemoteBackingSourceRange {
  byteLength: number;
  offset: number;
  data: Uint8Array;
}

function validateRemoteRange(byteLength: number, offset: number, length: number): void {
  if (!Number.isInteger(byteLength) || byteLength < 0) {
    throw new RangeError(`invalid remote backing source byte length: ${byteLength}`);
  }
  if (!Number.isInteger(offset) || offset < 0) {
    throw new RangeError(`invalid remote backing source offset: ${offset}`);
  }
  if (!Number.isInteger(length) || length < 0) {
    throw new RangeError(`invalid remote backing source range length: ${length}`);
  }
  if (offset + length > byteLength) {
    throw new RangeError(`remote backing source range exceeds source length: offset=${offset} length=${length}`);
  }
}

export async function fetchRemoteBackingSourceRange(
  url: string,
  byteLength: number,
  offset: number,
  length: number,
  fetchImpl: RemoteBackingFetch = globalThis.fetch,
): Promise<RemoteBackingSourceRange> {
  validateRemoteRange(byteLength, offset, length);
  if (length === 0) {
    return { byteLength, offset, data: new Uint8Array() };
  }
  if (!fetchImpl) {
    throw new Error("remote backing source fetch is unavailable");
  }

  const end = offset + length - 1;
  const response = await fetchImpl(url, {
    headers: {
      range: `bytes=${offset}-${end}`,
    },
  });
  if (!response.ok) {
    throw new Error(`remote backing source fetch failed: status=${response.status}`);
  }

  const bytes = new Uint8Array(await response.arrayBuffer());
  if (response.status === 206) {
    if (bytes.byteLength !== length) {
      throw new Error(`remote backing source range length mismatch: expected=${length} actual=${bytes.byteLength}`);
    }
    return { byteLength, offset, data: bytes };
  }
  if (response.status === 200 && bytes.byteLength === byteLength) {
    return {
      byteLength,
      offset,
      data: bytes.slice(offset, offset + length),
    };
  }
  if (response.status === 200 && bytes.byteLength === length) {
    return { byteLength, offset, data: bytes };
  }
  throw new Error(`remote backing source response length mismatch: expected=${length} actual=${bytes.byteLength}`);
}
