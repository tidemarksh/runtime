import { mkdtempSync, rmSync, unlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vite-plus/test";
import {
  closeNodeHostFileBackingSourceOpenFilesForTest,
  createNodeHostFileBackingSource,
} from "../../../src/kernel-worker/host-file-backing-source.js";

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
    await closeNodeHostFileBackingSourceOpenFilesForTest();
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
    await closeNodeHostFileBackingSourceOpenFilesForTest();
    rmSync(root, { recursive: true, force: true });
  }
});

test("node host-file backing source reuses a validated open file for repeated reads", async () => {
  const root = mkdtempSync(join(tmpdir(), "tidemark-host-source-cache-"));
  try {
    const hostFile = join(root, "artifact.bin");
    const data = new TextEncoder().encode("0123456789abcdef");
    writeFileSync(hostFile, data);

    const source = await createNodeHostFileBackingSource(hostFile, data.byteLength);
    expect(new TextDecoder().decode(source.readRange(0, 4) ?? new Uint8Array())).toBe("0123");

    unlinkSync(hostFile);

    expect(new TextDecoder().decode(source.readRange(12, 4) ?? new Uint8Array())).toBe("cdef");
  } finally {
    await closeNodeHostFileBackingSourceOpenFilesForTest();
    rmSync(root, { recursive: true, force: true });
  }
});

test("node host-file backing source serves nearby ranges from a read window", async () => {
  const root = mkdtempSync(join(tmpdir(), "tidemark-host-source-window-"));
  try {
    const hostFile = join(root, "artifact.bin");
    const data = new Uint8Array(8192);
    for (let index = 0; index < data.length; index += 1) {
      data[index] = index % 251;
    }
    writeFileSync(hostFile, data);

    const source = await createNodeHostFileBackingSource(hostFile, data.byteLength);
    expect(source.readRange(0, 4)).toEqual(data.subarray(0, 4));

    await closeNodeHostFileBackingSourceOpenFilesForTest();
    unlinkSync(hostFile);

    expect(source.readRange(4096, 4)).toEqual(data.subarray(4096, 4100));
  } finally {
    await closeNodeHostFileBackingSourceOpenFilesForTest();
    rmSync(root, { recursive: true, force: true });
  }
});

test("node host-file backing source does not expose mutable read-window storage", async () => {
  const root = mkdtempSync(join(tmpdir(), "tidemark-host-source-window-copy-"));
  try {
    const hostFile = join(root, "artifact.bin");
    const data = new Uint8Array(8192);
    for (let index = 0; index < data.length; index += 1) {
      data[index] = index % 251;
    }
    writeFileSync(hostFile, data);

    const source = await createNodeHostFileBackingSource(hostFile, data.byteLength);
    const first = source.readRange(0, 4);
    expect(first).toEqual(data.subarray(0, 4));

    first![0] = 0xff;
    await closeNodeHostFileBackingSourceOpenFilesForTest();
    unlinkSync(hostFile);

    expect(source.readRange(0, 4)).toEqual(data.subarray(0, 4));
    expect(source.readRange(4096, 4)).toEqual(data.subarray(4096, 4100));
  } finally {
    await closeNodeHostFileBackingSourceOpenFilesForTest();
    rmSync(root, { recursive: true, force: true });
  }
});

test("node host-file backing source skips the read window when read-ahead is disabled", async () => {
  const root = mkdtempSync(join(tmpdir(), "tidemark-host-source-no-readahead-"));
  try {
    const hostFile = join(root, "artifact.bin");
    const data = new Uint8Array(8192);
    for (let index = 0; index < data.length; index += 1) {
      data[index] = index % 251;
    }
    writeFileSync(hostFile, data);

    const source = await createNodeHostFileBackingSource(hostFile, data.byteLength);
    expect(source.readRange(0, 4, { readAhead: false })).toEqual(data.subarray(0, 4));

    await closeNodeHostFileBackingSourceOpenFilesForTest();
    unlinkSync(hostFile);

    expect(() => source.readRange(4096, 4)).toThrow();
  } finally {
    await closeNodeHostFileBackingSourceOpenFilesForTest();
    rmSync(root, { recursive: true, force: true });
  }
});

test("node host-file backing source replaces stale cached fds when a path is recreated", async () => {
  const root = mkdtempSync(join(tmpdir(), "tidemark-host-source-recreate-"));
  try {
    const hostFile = join(root, "artifact.bin");
    const initial = new TextEncoder().encode("old-bytes");
    const replacement = new TextEncoder().encode("new-bytes");
    writeFileSync(hostFile, initial);

    const oldSource = await createNodeHostFileBackingSource(hostFile, initial.byteLength);
    expect(new TextDecoder().decode(oldSource.readRange(0, initial.byteLength) ?? new Uint8Array())).toBe("old-bytes");

    unlinkSync(hostFile);
    writeFileSync(hostFile, replacement);

    const newSource = await createNodeHostFileBackingSource(hostFile, replacement.byteLength);
    expect(new TextDecoder().decode(newSource.readRange(0, replacement.byteLength) ?? new Uint8Array()))
      .toBe("new-bytes");
  } finally {
    await closeNodeHostFileBackingSourceOpenFilesForTest();
    rmSync(root, { recursive: true, force: true });
  }
});
