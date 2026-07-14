import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vite-plus/test";
import {
  Runtime,
  logSkip,
  readKernelWasm,
  runtimeDebugSummary,
  spawnWithOutput,
} from "../../../support/runtime-workload.js";

const hostRiscvGcc = process.env.TIDEMARK_TEST_HOST_RISCV_GCC ?? "riscv64-linux-gnu-gcc";

function buildBlockedChildSignalProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-blocked-child-signal-"));
  const sourcePath = join(tmp, "blocked_child_signal.c");
  const outputPath = join(tmp, "blocked-child-signal");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <signal.h>",
      "#include <stdio.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "int main(void) {",
      "  int ready[2];",
      "  int blocked[2];",
      "  if (pipe(ready) != 0 || pipe(blocked) != 0) { perror(\"pipe\"); return 10; }",
      "  pid_t child = fork();",
      "  if (child < 0) { perror(\"fork\"); return 11; }",
      "  if (child == 0) {",
      "    close(ready[0]);",
      "    close(blocked[1]);",
      "    char byte = 'R';",
      "    if (write(ready[1], &byte, 1) != 1) _exit(20);",
      "    if (read(blocked[0], &byte, 1) < 0 && errno != EINTR) _exit(21);",
      "    _exit(22);",
      "  }",
      "  close(ready[1]);",
      "  close(blocked[0]);",
      "  char byte = 0;",
      "  if (read(ready[0], &byte, 1) != 1 || byte != 'R') return 12;",
      "  usleep(100000);",
      "  if (kill(child, SIGTERM) != 0) { perror(\"kill\"); return 13; }",
      "  int status = 0;",
      "  if (waitpid(child, &status, 0) != child) { perror(\"waitpid\"); return 14; }",
      "  if (!WIFSIGNALED(status) || WTERMSIG(status) != SIGTERM) {",
      "    fprintf(stderr, \"unexpected-status=%d\\n\", status);",
      "    return 15;",
      "  }",
      "  puts(\"blocked-child-signal-ok\");",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", outputPath, sourcePath], { stdio: "pipe" });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build blocked child signal probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

test("process signal interrupts a child blocked on a pipe read", async () => {
  const wasmBytes = readKernelWasm();
  const probeBytes = buildBlockedChildSignalProbe();
  if (!wasmBytes || !probeBytes) {
    return;
  }

  const runtime = await Runtime.create({ wasmBytes });
  try {
    const result = await spawnWithOutput(
      runtime,
      probeBytes,
      ["/blocked-child-signal"],
      ["LC_ALL=C", "LANG=C"],
      15_000,
      { cwd: "/", stdin: "closed" },
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([detail, await runtimeDebugSummary(runtime)].join("\n"));
    });
    expect(result.exitCode, `${result.output}\n${result.debugInfo ?? ""}`).toBe(0);
    expect(result.output).toBe("blocked-child-signal-ok\n");
  } finally {
    runtime.destroy();
  }
}, 30_000);
