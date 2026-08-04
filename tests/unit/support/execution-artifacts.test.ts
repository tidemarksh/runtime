import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vite-plus/test";
import { runtimeExecutionArtifactDigest } from "../../support/execution-artifacts.js";

test("binds the complete Runtime bundle, Kernel Wasm, and probe into one stable digest", () => {
  const root = mkdtempSync(join(tmpdir(), "tidemark-execution-artifacts-"));
  const dist = join(root, "dist");
  mkdirSync(join(dist, "chunks"), { recursive: true });
  const files = new Map([
    ["index.mjs", "index-v1"],
    ["kernel-worker.mjs", "kernel-worker-v1"],
    ["thread-worker.mjs", "thread-worker-v1"],
    ["chunks/shared.mjs", "shared-v1"],
  ]);
  try {
    for (const [path, contents] of files) writeFileSync(join(dist, path), contents);
    const args = {
      distDirectory: dist,
      kernelWasm: new TextEncoder().encode("kernel-v1"),
      probe: new TextEncoder().encode("probe-v1"),
    };
    const baseline = runtimeExecutionArtifactDigest(args);
    expect(runtimeExecutionArtifactDigest(args)).toBe(baseline);

    for (const [path, contents] of files) {
      writeFileSync(join(dist, path), `${contents}-changed`);
      expect(runtimeExecutionArtifactDigest(args)).not.toBe(baseline);
      writeFileSync(join(dist, path), contents);
    }
    expect(runtimeExecutionArtifactDigest({
      ...args,
      kernelWasm: new TextEncoder().encode("kernel-v2"),
    })).not.toBe(baseline);
    expect(runtimeExecutionArtifactDigest({
      ...args,
      probe: new TextEncoder().encode("probe-v2"),
    })).not.toBe(baseline);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("rejects an incomplete Runtime execution bundle", () => {
  const root = mkdtempSync(join(tmpdir(), "tidemark-incomplete-execution-artifacts-"));
  try {
    writeFileSync(join(root, "index.mjs"), "index-v1");
    expect(() => runtimeExecutionArtifactDigest({
      distDirectory: root,
      kernelWasm: new Uint8Array([1]),
      probe: new Uint8Array([2]),
    })).toThrow("missing Runtime execution artifact");
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
