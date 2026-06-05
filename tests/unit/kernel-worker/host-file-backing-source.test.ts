import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vite-plus/test";
import { createNodeHostFileBackingSource } from "../../../src/kernel-worker/host-file-backing-source.js";

test("node host-file backing source reads requested ranges on demand", async () => {
  const root = mkdtempSync(join(tmpdir(), "tidemark-host-source-"));
  try {
    const hostFile = join(root, "artifact.bin");
    const data = new TextEncoder().encode("0123456789abcdef");
    writeFileSync(hostFile, data);

    const source = await createNodeHostFileBackingSource(hostFile, data.byteLength);

    expect(source.byteLength).toBe(data.byteLength);
    expect(new TextDecoder().decode(source.readRange(4, 6) ?? new Uint8Array())).toBe("456789");
    expect(new TextDecoder().decode(source.readRange(10, 6) ?? new Uint8Array())).toBe("abcdef");
    expect(source.readRange(15, 2)).toBeNull();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("node host-file backing source rejects changed sizes at registration", async () => {
  const root = mkdtempSync(join(tmpdir(), "tidemark-host-source-size-"));
  try {
    const hostFile = join(root, "artifact.bin");
    writeFileSync(hostFile, new TextEncoder().encode("abc"));

    await expect(createNodeHostFileBackingSource(hostFile, 4))
      .rejects.toThrow("host-file backing source size changed");
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
