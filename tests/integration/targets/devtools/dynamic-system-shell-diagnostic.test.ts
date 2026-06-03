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

const hostRiscvGcc = process.env.TIDEMARK_TEST_HOST_RISCV_GCC ?? "riscv64-linux-gnu-gcc";
const busyboxBinary = process.env.TIDEMARK_TEST_BUSYBOX_BINARY ?? "";
const glibcSysrootDir = process.env.TIDEMARK_TEST_GLIBC_SYSROOT_DIR ?? "";

function buildDynamicSystemProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-dynamic-system-shell-"));
  const sourcePath = join(tmp, "dynamic_system_shell.c");
  const outputPath = join(tmp, "dynamic-system-shell");
  try {
    writeFileSync(sourcePath, [
      "#include <sys/wait.h>",
      "#include <stdlib.h>",
      "#include <stdio.h>",
      "",
      "int main(void) {",
      "  int rc = system(\"echo make-evidence > out.txt; cat out.txt\");",
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
    return logSkip(`failed to build dynamic system shell probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildDynamicTwoSystemProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-dynamic-two-system-shell-"));
  const sourcePath = join(tmp, "dynamic_two_system_shell.c");
  const outputPath = join(tmp, "dynamic-two-system-shell");
  try {
    writeFileSync(sourcePath, [
      "#include <sys/wait.h>",
      "#include <stdlib.h>",
      "#include <stdio.h>",
      "",
      "static int check_status(int rc) {",
      "  if (rc == -1) { perror(\"system\"); return 111; }",
      "  if (WIFSIGNALED(rc)) {",
      "    printf(\"system-signal:%d\\n\", WTERMSIG(rc));",
      "    return 120 + WTERMSIG(rc);",
      "  }",
      "  if (!WIFEXITED(rc)) { printf(\"system-status:%d\\n\", rc); return 112; }",
      "  return WEXITSTATUS(rc);",
      "}",
      "",
      "int main(void) {",
      "  int first = check_status(system(\"echo make-evidence > out.txt\"));",
      "  if (first != 0) return first;",
      "  return check_status(system(\"cat out.txt\"));",
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
    return logSkip(`failed to build dynamic two-system shell probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

async function runDynamicSystemProbe(probeBytes: Uint8Array, guestPath: string) {
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
  const runtime = await Runtime.create({ wasmBytes, guestMemoryPolicyLimitBytes: 512 * 1024 * 1024 });
  try {
    await runtime.fs.mkdir("/bin", 0o755).catch(() => {});
    await runtime.fs.mkdir("/lib", 0o755).catch(() => {});
    await runtime.fs.mkdir("/work", 0o755).catch(() => {});
    const busybox = new Uint8Array(readFileSync(busyboxBinary));
    await runtime.fs.writeFile("/bin/busybox", busybox, 0o755);
    await runtime.fs.writeFile("/bin/sh", busybox, 0o755);
    await runtime.fs.writeFile("/bin/cat", busybox, 0o755);
    await runtime.fs.writeFile("/bin/echo", busybox, 0o755);
    for (const name of ["ld-linux-riscv64-lp64d.so.1", "libc.so.6"]) {
      await runtime.fs.writeFile(
        `/lib/${name}`,
        new Uint8Array(readFileSync(resolve(glibcSysrootDir, name))),
        0o755,
      );
    }
    await runtime.fs.writeFile(guestPath, probeBytes, 0o755);

    const result = await spawnWithOutput(
      runtime,
      probeBytes,
      [guestPath],
      ["PATH=/bin", "LD_LIBRARY_PATH=/lib"],
      15_000,
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
      `${result.debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe("make-evidence\n");
  } finally {
    runtime.destroy();
  }
}

test("dynamic glibc parent system() runs BusyBox shell command", async () => {
  const probeBytes = buildDynamicSystemProbe();
  if (!probeBytes) {
    return;
  }
  await runDynamicSystemProbe(probeBytes, "/work/dynamic-system-shell");
}, 30_000);

test("dynamic glibc parent runs two BusyBox system() shell commands", async () => {
  const probeBytes = buildDynamicTwoSystemProbe();
  if (!probeBytes) {
    return;
  }
  await runDynamicSystemProbe(probeBytes, "/work/dynamic-two-system-shell");
}, 30_000);
