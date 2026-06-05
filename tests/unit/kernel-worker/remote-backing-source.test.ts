import { expect, test, vi } from "vite-plus/test";
import { fetchRemoteBackingSourceRange } from "../../../src/kernel-worker/remote-backing-source.js";

function response(status: number, data: Uint8Array): Pick<Response, "arrayBuffer" | "ok" | "status"> {
  return {
    ok: status >= 200 && status < 300,
    status,
    arrayBuffer: async () => data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength),
  };
}

test("remote backing source fetches a byte range for cached registration", async () => {
  const data = new TextEncoder().encode("range-bytes");
  const fetch = vi.fn(async () => response(206, data));

  const range = await fetchRemoteBackingSourceRange("https://example.test/artifact.bin", 1024, 128, data.length, fetch);

  expect(fetch).toHaveBeenCalledWith("https://example.test/artifact.bin", {
    headers: {
      range: `bytes=128-${128 + data.length - 1}`,
    },
  });
  expect(range).toEqual({
    byteLength: 1024,
    offset: 128,
    data,
  });
});

test("remote backing source slices full-body responses when range is ignored", async () => {
  const full = new TextEncoder().encode("prefix:artifact-bytes:suffix");
  const fetch = vi.fn(async () => response(200, full));

  const range = await fetchRemoteBackingSourceRange(
    "https://example.test/artifact.bin",
    full.length,
    "prefix:".length,
    "artifact-bytes".length,
    fetch,
  );

  expect(range.data).toEqual(new TextEncoder().encode("artifact-bytes"));
});

test("remote backing source rejects short partial responses", async () => {
  const fetch = vi.fn(async () => response(206, new TextEncoder().encode("short")));

  await expect(fetchRemoteBackingSourceRange("https://example.test/artifact.bin", 1024, 0, 16, fetch))
    .rejects.toThrow("remote backing source range length mismatch");
});
