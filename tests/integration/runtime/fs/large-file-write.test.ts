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

type RuntimeInstance = Awaited<ReturnType<typeof Runtime.create>>;

const hostRiscvGcc = process.env.TIDEMARK_TEST_HOST_RISCV_GCC ?? "riscv64-linux-gnu-gcc";
const largeFileBytes = Number(process.env.TIDEMARK_TEST_LARGE_FILE_WRITE_BYTES ?? 8 * 1024 * 1024);
const largeFileWriteGuestMemoryPolicyLimitBytes = Number(process.env.TIDEMARK_TEST_LARGE_FILE_WRITE_GUEST_MEMORY_POLICY_BYTES ?? 512 * 1024 * 1024);
const largeFileWriteMaxProcessMemoryPages = Number(
  process.env.TIDEMARK_TEST_LARGE_FILE_WRITE_MAX_PROCESS_MEMORY_PAGES ?? 32768,
);
const largeFilePath = "/work/large-file-write.bin";

function buildLargeFileWriteProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-large-file-write-"));
  const sourcePath = join(tmp, "large_file_write.c");
  const outputPath = join(tmp, "large-file-write");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <stdio.h>",
      "#include <stdlib.h>",
      "#include <string.h>",
      "#include <unistd.h>",
      "",
      "#define CHUNK_SIZE 65536",
      "",
      "static int write_all(int fd, const unsigned char *data, size_t len) {",
      "  size_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, len - off);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    if (n == 0) return -1;",
      "    off += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static unsigned char pattern_byte(unsigned long long offset) {",
      "  return (unsigned char)((offset * 17ull + 23ull) & 0xffu);",
      "}",
      "",
      "int main(int argc, char **argv) {",
      "  const char *path = argc > 1 ? argv[1] : \"/work/large-file-write.bin\";",
      "  unsigned long long total = argc > 2 ? strtoull(argv[2], NULL, 10) : 2097152ull;",
      "  int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0644);",
      "  if (fd < 0) { perror(\"open-output\"); return 20; }",
      "  static unsigned char buf[CHUNK_SIZE];",
      "  unsigned long long written = 0;",
      "  unsigned long long checksum = 0;",
      "  while (written < total) {",
      "    size_t chunk = (size_t)((total - written) < CHUNK_SIZE ? (total - written) : CHUNK_SIZE);",
      "    for (size_t i = 0; i < chunk; i++) {",
      "      unsigned char value = pattern_byte(written + (unsigned long long)i);",
      "      buf[i] = value;",
      "      checksum += (unsigned long long)value;",
      "    }",
      "    if (write_all(fd, buf, chunk) != 0) { perror(\"write-output\"); close(fd); return 21; }",
      "    written += (unsigned long long)chunk;",
      "  }",
      "  if (close(fd) != 0) { perror(\"close-output\"); return 22; }",
      "  printf(\"large-file-write bytes=%llu checksum=%llu\\n\", written, checksum);",
      "  return written == total ? 0 : 23;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", outputPath, sourcePath], {
      stdio: "pipe",
    });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build large file write probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function expectedByte(offset: number): number {
  return (offset * 17 + 23) & 0xff;
}

function checksum(bytes: Uint8Array): bigint {
  let sum = 0n;
  for (const byte of bytes) {
    sum += BigInt(byte);
  }
  return sum;
}

function expectedChecksum(length: number): bigint {
  let sum = 0n;
  for (let offset = 0; offset < length; offset += 1) {
    sum += BigInt(expectedByte(offset));
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

test("large regular file write local gate: guest sequential writes publish byte-for-byte", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildLargeFileWriteProbe();
  if (!probeBytes) {
    return;
  }

  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: largeFileWriteGuestMemoryPolicyLimitBytes,
    maxProcessMemoryPages: largeFileWriteMaxProcessMemoryPages,
  });
  const startedMs = Date.now();
  try {
    await runtime.applyFsSnapshot([
      { kind: "dir", path: "/work", mode: 0o755 },
    ]);
    const spawnStartedMs = Date.now();
    const result = await spawnWithOutputAndStats(
      runtime,
      probeBytes,
      ["/work/large-file-write", largeFilePath, String(largeFileBytes)],
      ["LC_ALL=C", "LANG=C"],
      120_000,
      { cwd: "/", stdin: "closed" },
    );
    const spawnDurationMs = Date.now() - spawnStartedMs;
    const expected = expectedChecksum(largeFileBytes);
    expect(result.exitCode, result.output + (result.debugInfo ? `\n${result.debugInfo}` : "")).toBe(0);
    expect(result.output).toContain(`large-file-write bytes=${largeFileBytes}`);
    expect(result.output).toContain(`checksum=${expected.toString()}`);

    const readStartedMs = Date.now();
    const saved = await runtime.readFile(largeFilePath);
    const readDurationMs = Date.now() - readStartedMs;
    expect(saved.length).toBe(largeFileBytes);
    expect(checksum(saved)).toBe(expected);
    expect(saved[0]).toBe(expectedByte(0));
    expect(saved[saved.length - 1]).toBe(expectedByte(saved.length - 1));
    console.log([
      `large regular file write gate durationMs=${Date.now() - startedMs}`,
      `spawnDurationMs=${spawnDurationMs}`,
      `readbackDurationMs=${readDurationMs}`,
      `bytes=${largeFileBytes}`,
      `guestPolicyBytes=${largeFileWriteGuestMemoryPolicyLimitBytes}`,
      `maxProcessMemoryPages=${largeFileWriteMaxProcessMemoryPages}`,
      `execution=${result.execution}`,
    ].join(" "));
  } finally {
    runtime.destroy();
  }
}, 150_000);
