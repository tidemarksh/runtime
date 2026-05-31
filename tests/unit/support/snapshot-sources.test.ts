import { chmodSync, mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { expect, test } from "vite-plus/test";
import { buildSnapshotFromSources } from "../../support/snapshot-sources.js";

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
