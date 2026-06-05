import { readFileSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "vite-plus/test";

const supportRoot = join(import.meta.dirname, "../../support");

const snapshotHarnesses = [
  "archive-orchestrator-harness.ts",
  "dynamic-startup-harness.ts",
  "package-module-driver-harness.ts",
  "script-runtime-harness.ts",
  "toolchain-exec-harness.ts",
] as const;

test("snapshot-backed harnesses expose host-file backing mode explicitly", () => {
  for (const fileName of snapshotHarnesses) {
    const source = readFileSync(join(supportRoot, fileName), "utf8");

    expect(source).toContain("hostFileBackingMode?: SnapshotBuildOptions[\"hostFileBackingMode\"]");
    expect(source).toContain("hostFileBackingMode: entry.hostFileBackingMode");
    expect(source).not.toContain("pageCacheDataCapacityBytes: 640");
  }
});
