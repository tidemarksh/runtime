import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { expect, test } from "vite-plus/test";
import {
  Runtime,
  logSkip,
  readKernelWasm,
  runtimeDebugSummary,
  spawnWithOutput,
} from "../../../support/runtime-workload.js";
import { buildSnapshotFromSources, type SnapshotSource } from "../../../support/snapshot-sources.js";

const hostRiscvGcc = process.env.TIDEMARK_TEST_HOST_RISCV_GCC ?? "riscv64-linux-gnu-gcc";
const busyboxBinary = process.env.TIDEMARK_TEST_BUSYBOX_BINARY ?? "";
const glibcSysrootDir = process.env.TIDEMARK_TEST_GLIBC_SYSROOT_DIR ?? "";
const loaderGuestPath = "/lib/ld-linux-riscv64-lp64d.so.1";
const libcGuestPath = "/lib/libc.so.6";
const dynamicTargetGuestPath = "/work/dynamic-backing-probe";

function buildStaticExecveLauncher(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-dynamic-backing-launcher-"));
  const sourcePath = join(tmp, "launcher.c");
  const outputPath = join(tmp, "launcher");
  try {
    writeFileSync(sourcePath, [
      "#include <unistd.h>",
      "#include <stdio.h>",
      "extern char **environ;",
      "int main(int argc, char **argv) {",
      "  if (argc < 2) {",
      "    fputs(\"usage: launcher <path>\\n\", stderr);",
      "    return 64;",
      "  }",
      "  execve(argv[1], argv + 1, environ);",
      "  perror(\"execve\");",
      "  return 127;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", outputPath, sourcePath], {
      stdio: "pipe",
    });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build static execve launcher with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildDynamicSystemProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-dynamic-backing-probe-"));
  const sourcePath = join(tmp, "dynamic_backing_probe.c");
  const outputPath = join(tmp, "dynamic-backing-probe");
  try {
    writeFileSync(sourcePath, [
      "#include <sys/wait.h>",
      "#include <stdlib.h>",
      "#include <stdio.h>",
      "",
      "int main(void) {",
      "  int rc = system(\"echo dynamic-backing-ok > out.txt; cat out.txt\");",
      "  if (rc == -1) { perror(\"system\"); return 111; }",
      "  if (WIFSIGNALED(rc)) {",
      "    printf(\"system-signal:%d\\n\", WTERMSIG(rc));",
      "    return 120 + WTERMSIG(rc);",
      "  }",
      "  if (!WIFEXITED(rc)) { printf(\"system-status:%d\\n\", rc); return 112; }",
      "  return WEXITSTATUS(rc);",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, [
      "-O2",
      "-Wl,--dynamic-linker=/lib/ld-linux-riscv64-lp64d.so.1",
      "-o",
      outputPath,
      sourcePath,
    ], { stdio: "pipe" });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build dynamic backing probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

test("dynamic execve target reads executable loader and libc from snapshot backing store", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  if (!busyboxBinary || !existsSync(busyboxBinary)) {
    return logSkip(`missing BusyBox binary: ${busyboxBinary || "set TIDEMARK_TEST_BUSYBOX_BINARY"}`);
  }
  if (!glibcSysrootDir || !existsSync(glibcSysrootDir)) {
    return logSkip(`missing glibc sysroot: ${glibcSysrootDir || "set TIDEMARK_TEST_GLIBC_SYSROOT_DIR"}`);
  }

  const launcherBytes = buildStaticExecveLauncher();
  const dynamicTargetBytes = buildDynamicSystemProbe();
  if (!launcherBytes || !dynamicTargetBytes) {
    return;
  }

  const loaderHostPath = resolve(glibcSysrootDir, "ld-linux-riscv64-lp64d.so.1");
  const libcHostPath = resolve(glibcSysrootDir, "libc.so.6");
  const snapshotSources: readonly SnapshotSource[] = [
    {
      kind: "fileCopies",
      hostPath: busyboxBinary,
      guestPaths: ["/bin/busybox", "/bin/sh", "/bin/cat", "/bin/echo"],
      mode: 0o755,
    },
    { kind: "file", hostPath: loaderHostPath, guestPath: loaderGuestPath, mode: 0o755 },
    { kind: "file", hostPath: libcHostPath, guestPath: libcGuestPath, mode: 0o755 },
    { kind: "inlineFile", guestPath: dynamicTargetGuestPath, data: dynamicTargetBytes, mode: 0o755 },
  ];
  const snapshot = buildSnapshotFromSources(snapshotSources, {
    missingLabel: "dynamic loader backing artifact",
  });
  if (snapshot.skipReason) {
    return logSkip(snapshot.skipReason);
  }

  const runtime = await Runtime.create({ wasmBytes, guestMemoryPolicyLimitBytes: 512 * 1024 * 1024 });
  try {
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    for (const path of [dynamicTargetGuestPath, loaderGuestPath, libcGuestPath, "/bin/sh"]) {
      const record = await runtime.debugPageCachePath(path);
      expect(record, `missing page-cache record for ${path}`).not.toBeNull();
      expect(record?.dataOff, `expected ${path} to be backing-store backed, not resident SAB backed`).toBe(0);
      expect(record?.size, `expected ${path} to publish a non-empty size`).toBeGreaterThan(0);
      expect(record?.residentPages ?? 0, `expected ${path} to start without resident pages`).toBe(0);
    }

    const result = await spawnWithOutput(
      runtime,
      launcherBytes,
      ["/bin/dynamic-backing-launcher", dynamicTargetGuestPath],
      ["PATH=/bin", "LD_LIBRARY_PATH=/lib"],
      20_000,
      { cwd: "/work" },
    ).catch(async (error) => {
      const summary = await runtimeDebugSummary(runtime);
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([detail, summary].join("\n"));
    });

    expect(
      result.exitCode,
      `${result.output}\n${result.debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(
      result.output,
      `${result.output}\n${result.debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe("dynamic-backing-ok\n");
    for (const path of [dynamicTargetGuestPath, loaderGuestPath, libcGuestPath, "/bin/sh"]) {
      const record = await runtime.debugPageCachePath(path);
      expect(record, `missing page-cache record for ${path}`).not.toBeNull();
      expect(record?.dataOff, `expected ${path} to remain backing-store backed`).toBe(0);
      expect(record?.size, `expected ${path} to keep a non-empty backing size`).toBeGreaterThan(0);
    }
  } finally {
    runtime.destroy();
  }
}, 40_000);
