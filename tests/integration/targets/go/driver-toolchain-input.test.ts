import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { expect, test } from "vite-plus/test";
import { prepareToolchainExecRuntime } from "../../../support/toolchain-exec-harness.js";
import { goToolCompileVersionToolchainEntry } from "./toolchain-exec.js";

function sha256(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

test("go driver toolchain input preserves guest compile bytes", async () => {
  const prepared = await prepareToolchainExecRuntime(goToolCompileVersionToolchainEntry);
  if (!prepared) {
    return;
  }

  const { runtime } = prepared;
  try {
    const guestCompileSource = goToolCompileVersionToolchainEntry.snapshotSources.find((source) => (
      source.kind === "tree" && source.guestPath === "/usr/local/go/pkg"
    ));
    if (!guestCompileSource || guestCompileSource.kind !== "tree") {
      throw new Error("missing go pkg snapshot source");
    }

    const guestCompileBytes = await runtime.fs.readFile("/usr/local/go/pkg/tool/linux_riscv64/compile");
    const hostCompileBytes = new Uint8Array(
      readFileSync(resolve(guestCompileSource.hostPath, "tool/linux_riscv64/compile")),
    );

    expect(guestCompileBytes.byteLength).toBe(hostCompileBytes.byteLength);
    expect(sha256(guestCompileBytes)).toBe(sha256(hostCompileBytes));
  } finally {
    runtime.destroy();
  }
}, 20_000);
