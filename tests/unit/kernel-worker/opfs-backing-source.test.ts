import { expect, test, vi } from "vite-plus/test";
import { readOpfsBackingSourceRange } from "../../../src/kernel-worker/opfs-backing-source.js";

test("OPFS backing source reads a cached blob range for registration", async () => {
  const full = new TextEncoder().encode("prefix:artifact-bytes:suffix");
  const handle = {
    getFile: vi.fn(async () => new Blob([full])),
  };

  const range = await readOpfsBackingSourceRange(
    handle,
    full.length,
    "prefix:".length,
    "artifact-bytes".length,
  );

  expect(handle.getFile).toHaveBeenCalledOnce();
  expect(range).toEqual({
    byteLength: full.length,
    offset: "prefix:".length,
    data: new TextEncoder().encode("artifact-bytes"),
  });
});

test("OPFS backing source rejects changed file sizes", async () => {
  const handle = {
    getFile: vi.fn(async () => new Blob([new TextEncoder().encode("short")])),
  };

  await expect(readOpfsBackingSourceRange(handle, 1024, 0, 5))
    .rejects.toThrow("OPFS backing source size changed");
});

test("OPFS backing source validates requested ranges before opening the file", async () => {
  const handle = {
    getFile: vi.fn(async () => new Blob([new Uint8Array()])),
  };

  await expect(readOpfsBackingSourceRange(handle, 8, 4, 8))
    .rejects.toThrow("OPFS backing source range exceeds source length");
  expect(handle.getFile).not.toHaveBeenCalled();
});
