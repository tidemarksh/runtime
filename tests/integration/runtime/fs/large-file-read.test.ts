import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vite-plus/test";
import {
  Runtime,
  logSkip,
  readKernelWasm,
} from "../../../support/runtime-workload.js";
import { buildSnapshotFromSources } from "../../../support/snapshot-sources.js";

type RuntimeInstance = Awaited<ReturnType<typeof Runtime.create>>;

const hostRiscvGcc = process.env.TIDEMARK_TEST_HOST_RISCV_GCC ?? "riscv64-linux-gnu-gcc";
const largeFileReadBytes = Number(process.env.TIDEMARK_TEST_LARGE_FILE_READ_BYTES ?? 8 * 1024 * 1024);
const largeFileReadInput = process.env.TIDEMARK_TEST_LARGE_FILE_READ_INPUT ?? "";
const largeFilePath = "/work/large-file-read.bin";

function buildLargeFileReadProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-large-file-read-"));
  const sourcePath = join(tmp, "large_file_read.c");
  const outputPath = join(tmp, "large-file-read");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <stdio.h>",
      "#include <unistd.h>",
      "",
      "#define CHUNK_SIZE 65536",
      "",
      "int main(int argc, char **argv) {",
      "  const char *path = argc > 1 ? argv[1] : \"/work/large-file-read.bin\";",
      "  int fd = open(path, O_RDONLY);",
      "  if (fd < 0) { perror(\"open-input\"); return 20; }",
      "  static unsigned char buf[CHUNK_SIZE];",
      "  unsigned long long total = 0;",
      "  unsigned long long checksum = 0;",
      "  for (;;) {",
      "    ssize_t n = read(fd, buf, sizeof(buf));",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      perror(\"read-input\");",
      "      close(fd);",
      "      return 21;",
      "    }",
      "    if (n == 0) break;",
      "    for (ssize_t i = 0; i < n; i++) checksum += (unsigned long long)buf[i];",
      "    total += (unsigned long long)n;",
      "  }",
      "  if (close(fd) != 0) { perror(\"close-input\"); return 22; }",
      "  printf(\"large-file-read bytes=%llu checksum=%llu\\n\", total, checksum);",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", outputPath, sourcePath], {
      stdio: "pipe",
    });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build large file read probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildInput(): Uint8Array {
  if (largeFileReadInput) {
    return new Uint8Array(readFileSync(largeFileReadInput));
  }
  const data = new Uint8Array(Math.max(1, Math.trunc(largeFileReadBytes)));
  for (let offset = 0; offset < data.length; offset += 1) {
    data[offset] = (offset * 17 + 23) & 0xff;
  }
  return data;
}

function checksum(bytes: Uint8Array): bigint {
  let sum = 0n;
  for (const byte of bytes) {
    sum += BigInt(byte);
  }
  return sum;
}

function compactExecutionStats(
  stats: ReturnType<Awaited<ReturnType<typeof Runtime.create>>["debugExecutionStats"]>,
): string {
  return JSON.stringify(stats.map((proc) => ({
    pid: proc.pid,
    cmd: proc.cmd.split(/\s+/).slice(0, 3).join(" ").slice(0, 96),
    running: proc.running,
    memoryBytes: proc.memoryBytes,
    readyWorkers: proc.readyWorkers,
    blockedWorkers: proc.blockedWorkers,
    transferDebugStats: proc.transferDebugStats,
    cpuDebugStats: proc.cpuDebugStats,
    workers: proc.workers.map((worker) => ({
      ownerState: worker.ownerState,
      stepCount: worker.stepCount,
      totalStepBudget: worker.totalStepBudget,
      lastStatusName: worker.lastStatusName,
      lastStepPc: worker.lastStepPc,
      hotStepPcs: worker.hotStepPcs.slice(0, 3),
      statusCountsByName: Object.fromEntries(
        Object.entries(worker.statusCountsByName)
          .sort((left, right) => right[1] - left[1])
          .slice(0, 5),
      ),
    })),
  })));
}

async function spawnWithOutputAndStats(
  runtime: RuntimeInstance,
  elfBytes: Uint8Array,
  argv: string[],
  env: string[],
  timeoutMs: number,
  options: Parameters<RuntimeInstance["spawn"]>[3],
): Promise<{ exitCode: number; output: string; debugInfo?: string; execution: string }> {
  const decoder = new TextDecoder();
  const proc = await runtime.spawn(new Uint8Array(elfBytes), argv, env, options);
  let output = "";
  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  let sampleId: ReturnType<typeof setInterval> | null = null;
  let execution = compactExecutionStats(runtime.debugExecutionStats());

  proc.onStdout((data) => {
    output += decoder.decode(data);
  });
  sampleId = setInterval(() => {
    const stats = runtime.debugExecutionStats();
    if (stats.length > 0) {
      execution = compactExecutionStats(stats);
    }
  }, 1000);
  if (typeof sampleId === "object" && sampleId !== null && "unref" in sampleId) {
    sampleId.unref();
  }

  try {
    const exitCode = await Promise.race([
      proc.waitForExit(),
      new Promise<never>((_resolve, reject) => {
        timeoutId = setTimeout(() => reject(new Error(`timeout after ${timeoutMs}ms`)), timeoutMs);
        if (typeof timeoutId === "object" && timeoutId !== null && "unref" in timeoutId) {
          timeoutId.unref();
        }
      }),
    ]);
    return { exitCode, output, debugInfo: proc.debugInfo?.(), execution };
  } finally {
    if (timeoutId !== null) {
      clearTimeout(timeoutId);
    }
    if (sampleId !== null) {
      clearInterval(sampleId);
    }
  }
}

test("large regular file read local gate: guest sequentially reads package-index sized input", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildLargeFileReadProbe();
  if (!probeBytes) {
    return;
  }
  const input = buildInput();
  const expected = checksum(input);

  const runtime = await Runtime.create({
    wasmBytes,
    ramSize: 512 * 1024 * 1024,
    maxProcessMemoryPages: 32768,
  });
  const startedMs = Date.now();
  try {
    const snapshot = buildSnapshotFromSources([
      { kind: "inlineFile", guestPath: largeFilePath, data: input, mode: 0o644 },
    ]);
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    const spawnStartedMs = Date.now();
    const result = await spawnWithOutputAndStats(
      runtime,
      probeBytes,
      ["/work/large-file-read", largeFilePath],
      ["LC_ALL=C", "LANG=C"],
      180_000,
      { cwd: "/", stdin: "closed" },
    );
    const spawnDurationMs = Date.now() - spawnStartedMs;
    expect(result.exitCode, result.output + (result.debugInfo ? `\n${result.debugInfo}` : "")).toBe(0);
    expect(result.output).toContain(`large-file-read bytes=${input.length}`);
    expect(result.output).toContain(`checksum=${expected.toString()}`);
    console.log([
      `large regular file read gate durationMs=${Date.now() - startedMs}`,
      `spawnDurationMs=${spawnDurationMs}`,
      `bytes=${input.length}`,
      `input=${largeFileReadInput || "synthetic"}`,
      `execution=${result.execution}`,
    ].join(" "));
  } finally {
    runtime.destroy();
  }
}, 210_000);
