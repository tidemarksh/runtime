import { chmodSync, mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { expect, test } from "vite-plus/test";
import { applySnapshotSourcesToRuntime, buildSnapshotFromSources } from "../../support/snapshot-sources.js";
import type { FsSnapshotEntry } from "../../../src/fs/types.js";

test("buildSnapshotFromSources skips unreadable tree entries", () => {
  const root = mkdtempSync(join(tmpdir(), "tidemark-snapshot-"));
  try {
    const readablePath = join(root, "ok.txt");
    writeFileSync(readablePath, "ok");

    const unreadableFile = join(root, "secret.txt");
    writeFileSync(unreadableFile, "secret");
    chmodSync(unreadableFile, 0o000);

    const unreadableDir = join(root, "private");
    mkdirSync(unreadableDir, { mode: 0o700 });
    writeFileSync(join(unreadableDir, "nested.txt"), "nested");
    chmodSync(unreadableDir, 0o000);

    const result = buildSnapshotFromSources([
      { kind: "tree", hostPath: root, guestPath: "/guest" },
    ]);

    expect(result.skipReason).toBeUndefined();
    const entries = result.entries ?? [];
    expect(entries.some((entry) => entry.path === "/guest/ok.txt")).toBe(true);
    expect(entries.some((entry) => entry.path === "/guest/secret.txt")).toBe(false);
    expect(entries.some((entry) => entry.path === "/guest/private")).toBe(true);
    expect(entries.some((entry) => entry.path === "/guest/private/nested.txt")).toBe(false);
  } finally {
    try {
      chmodSync(join(root, "secret.txt"), 0o644);
    } catch {}
    try {
      chmodSync(join(root, "private"), 0o755);
    } catch {}
    rmSync(root, { recursive: true, force: true });
  }
});

test("applySnapshotSourcesToRuntime streams host files outside the metadata snapshot", async () => {
  const root = mkdtempSync(join(tmpdir(), "tidemark-snapshot-stream-"));
  const appliedSnapshots: FsSnapshotEntry[][] = [];
  const writtenFiles: Array<{ path: string; data: Uint8Array; mode?: number }> = [];
  try {
    const hostFile = join(root, "large.bin");
    const fileData = new Uint8Array((1024 * 1024) + 3).map((_, index) => (index * 5) & 0xff);
    writeFileSync(hostFile, fileData);

    const runtime = {
      applyFsSnapshot: async (entries: FsSnapshotEntry[]) => {
        appliedSnapshots.push(entries);
      },
      fs: {
        writeFile: async (path: string, data: Uint8Array, mode?: number) => {
          writtenFiles.push({ path, data: new Uint8Array(data), mode });
        },
      },
    };

    const result = await applySnapshotSourcesToRuntime(runtime as never, [
      { kind: "dir", guestPath: "/guest", mode: 0o755 },
      { kind: "inlineFile", guestPath: "/guest/config.txt", data: "ok", mode: 0o644 },
      { kind: "file", hostPath: hostFile, guestPath: "/guest/large.bin", mode: 0o600 },
    ]);

    expect(result.skipReason).toBeUndefined();
    expect(appliedSnapshots).toHaveLength(1);
    expect(appliedSnapshots[0].some((entry) => entry.kind === "file" && entry.path === "/guest/large.bin")).toBe(false);
    expect(appliedSnapshots[0].some((entry) => entry.kind === "file" && entry.path === "/guest/config.txt")).toBe(true);
    expect(writtenFiles).toHaveLength(1);
    expect(writtenFiles[0].path).toBe("/guest/large.bin");
    expect(writtenFiles[0].mode).toBe(0o600);
    expect(writtenFiles[0].data).toEqual(fileData);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("applySnapshotSourcesToRuntime uses owned host-file writes when available", async () => {
  const root = mkdtempSync(join(tmpdir(), "tidemark-snapshot-owned-"));
  const writtenFiles: Array<{ path: string; data: Uint8Array; mode?: number; owned: boolean }> = [];
  try {
    const hostFile = join(root, "owned.bin");
    const fileData = new Uint8Array((1024 * 1024) + 7).map((_, index) => (index * 11) & 0xff);
    writeFileSync(hostFile, fileData);

    const runtime = {
      applyFsSnapshot: async (_entries: FsSnapshotEntry[]) => {},
      writeFileOwned: async (path: string, data: Uint8Array, mode?: number) => {
        writtenFiles.push({ path, data: new Uint8Array(data), mode, owned: true });
      },
      fs: {
        writeFile: async (path: string, data: Uint8Array, mode?: number) => {
          writtenFiles.push({ path, data: new Uint8Array(data), mode, owned: false });
        },
      },
    };

    const result = await applySnapshotSourcesToRuntime(runtime as never, [
      { kind: "file", hostPath: hostFile, guestPath: "/guest/owned.bin", mode: 0o644 },
    ]);

    expect(result.skipReason).toBeUndefined();
    expect(writtenFiles).toHaveLength(1);
    expect(writtenFiles[0].path).toBe("/guest/owned.bin");
    expect(writtenFiles[0].owned).toBe(true);
    expect(writtenFiles[0].data).toEqual(fileData);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("applySnapshotSourcesToRuntime can register host files as backing sources", async () => {
  const root = mkdtempSync(join(tmpdir(), "tidemark-snapshot-source-"));
  const appliedSnapshots: FsSnapshotEntry[][] = [];
  const registeredSources: Array<{ sourceIdentity: string; data: Uint8Array }> = [];
  const writtenFiles: Array<{ path: string; data: Uint8Array; mode?: number; owned: boolean }> = [];
  try {
    const hostFile = join(root, "source.bin");
    const fileData = new Uint8Array((1024 * 1024) + 11).map((_, index) => (index * 13) & 0xff);
    writeFileSync(hostFile, fileData);

    const runtime = {
      applyFsSnapshot: async (entries: FsSnapshotEntry[]) => {
        appliedSnapshots.push(entries);
      },
      registerFsBackingSource: async (sourceIdentity: string, data: Uint8Array) => {
        registeredSources.push({ sourceIdentity, data: new Uint8Array(data) });
      },
      writeFileOwned: async (path: string, data: Uint8Array, mode?: number) => {
        writtenFiles.push({ path, data: new Uint8Array(data), mode, owned: true });
      },
      fs: {
        writeFile: async (path: string, data: Uint8Array, mode?: number) => {
          writtenFiles.push({ path, data: new Uint8Array(data), mode, owned: false });
        },
      },
    };

    const result = await applySnapshotSourcesToRuntime(
      runtime as never,
      [{ kind: "file", hostPath: hostFile, guestPath: "/guest/source.bin", mode: 0o600 }],
      { hostFileBackingMode: "source" },
    );

    expect(result.skipReason).toBeUndefined();
    expect(appliedSnapshots).toHaveLength(1);
    const fileEntry = appliedSnapshots[0].find((entry) => entry.kind === "file" && entry.path === "/guest/source.bin");
    expect(fileEntry).toMatchObject({
      kind: "file",
      path: "/guest/source.bin",
      size: fileData.length,
      mode: 0o600,
    });
    expect(fileEntry?.kind === "file" ? fileEntry.data : undefined).toBeUndefined();
    expect(fileEntry?.backingExtent?.length).toBe(fileData.length);
    expect(registeredSources).toHaveLength(1);
    expect(registeredSources[0].sourceIdentity).toBe(fileEntry?.backingExtent?.sourceIdentity);
    expect(registeredSources[0].data).toEqual(fileData);
    expect(writtenFiles).toHaveLength(0);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("applySnapshotSourcesToRuntime rejects over-budget host artifacts before mutating runtime", async () => {
  const root = mkdtempSync(join(tmpdir(), "tidemark-snapshot-budget-"));
  const appliedSnapshots: FsSnapshotEntry[][] = [];
  const writtenFiles: Array<{ path: string; data: Uint8Array; mode?: number; owned: boolean }> = [];
  try {
    const hostFile = join(root, "large.bin");
    writeFileSync(hostFile, new Uint8Array([1, 2, 3, 4]));

    const runtime = {
      applyFsSnapshot: async (entries: FsSnapshotEntry[]) => {
        appliedSnapshots.push(entries);
      },
      writeFileOwned: async (path: string, data: Uint8Array, mode?: number) => {
        writtenFiles.push({ path, data: new Uint8Array(data), mode, owned: true });
      },
      fs: {
        writeFile: async (path: string, data: Uint8Array, mode?: number) => {
          writtenFiles.push({ path, data: new Uint8Array(data), mode, owned: false });
        },
      },
    };

    await expect(
      applySnapshotSourcesToRuntime(
        runtime as never,
        [
          { kind: "inlineFile", guestPath: "/guest/config.txt", data: "ok", mode: 0o644 },
          { kind: "file", hostPath: hostFile, guestPath: "/guest/large.bin", mode: 0o600 },
        ],
        { artifactMaterializationBudgetBytes: 5 },
      ),
    ).rejects.toThrow("snapshot artifact materialization budget exceeded: materializedBytes=6 budgetBytes=5");
    expect(appliedSnapshots).toHaveLength(0);
    expect(writtenFiles).toHaveLength(0);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
