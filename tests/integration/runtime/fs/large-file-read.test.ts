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
const largeFileReadBytes = Number(process.env.TIDEMARK_TEST_LARGE_FILE_READ_BYTES ?? 16 * 1024 * 1024 + 17);
const largeFileReadInput = process.env.TIDEMARK_TEST_LARGE_FILE_READ_INPUT ?? "";
const largeFilePath = "/work/large-file-read.bin";
const defaultLargeFileReadTimeoutMs = 180_000;

function resolveLargeFileReadTimeoutMs(): number {
  const parsed = Number(process.env.TIDEMARK_TEST_LARGE_FILE_READ_TIMEOUT_MS ?? defaultLargeFileReadTimeoutMs);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return defaultLargeFileReadTimeoutMs;
  }
  return Math.trunc(parsed);
}

const largeFileReadTimeoutMs = resolveLargeFileReadTimeoutMs();
const largeFileReadTestTimeoutMs = Math.max(210_000, largeFileReadTimeoutMs + 30_000);

function buildLargeFileReadProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-large-file-read-"));
  const sourcePath = join(tmp, "large_file_read.c");
  const outputPath = join(tmp, "large-file-read");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <stdio.h>",
      "#include <stdlib.h>",
      "#include <string.h>",
      "#include <sys/mman.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "#define CHUNK_SIZE 65536",
      "",
      "static int checksum_by_read(const char *path, unsigned long long *total, unsigned long long *checksum) {",
      "  int fd = open(path, O_RDONLY);",
      "  if (fd < 0) { perror(\"open-input\"); return 20; }",
      "  static unsigned char buf[CHUNK_SIZE];",
      "  *total = 0;",
      "  *checksum = 0;",
      "  for (;;) {",
      "    ssize_t n = read(fd, buf, sizeof(buf));",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      perror(\"read-input\");",
      "      close(fd);",
      "      return 21;",
      "    }",
      "    if (n == 0) break;",
      "    for (ssize_t i = 0; i < n; i++) *checksum += (unsigned long long)buf[i];",
      "    *total += (unsigned long long)n;",
      "  }",
      "  if (close(fd) != 0) { perror(\"close-input\"); return 22; }",
      "  return 0;",
      "}",
      "",
      "static int checksum_by_mmap(const char *path, unsigned long long *total, unsigned long long *checksum) {",
      "  int fd = open(path, O_RDONLY);",
      "  if (fd < 0) { perror(\"open-mmap-input\"); return 30; }",
      "  off_t size = lseek(fd, 0, SEEK_END);",
      "  if (size < 0) { perror(\"lseek-mmap-input\"); close(fd); return 31; }",
      "  if (size == 0) { close(fd); *total = 0; *checksum = 0; return 0; }",
      "  void *mapped = mmap(NULL, (size_t)size, PROT_READ, MAP_PRIVATE, fd, 0);",
      "  if (mapped == MAP_FAILED) { perror(\"mmap-input\"); close(fd); return 32; }",
      "  const unsigned char *bytes = (const unsigned char *)mapped;",
      "  *total = (unsigned long long)size;",
      "  *checksum = 0;",
      "  for (off_t i = 0; i < size; i++) *checksum += (unsigned long long)bytes[i];",
      "  if (munmap(mapped, (size_t)size) != 0) { perror(\"munmap-input\"); close(fd); return 33; }",
      "  if (close(fd) != 0) { perror(\"close-mmap-input\"); return 34; }",
      "  return 0;",
      "}",
      "",
      "static int checksum_sample_by_mmap(const char *path, unsigned long long *total, unsigned long long *samples, unsigned long long *checksum) {",
      "  int fd = open(path, O_RDONLY);",
      "  if (fd < 0) { perror(\"open-mmap-sample-input\"); return 50; }",
      "  off_t size = lseek(fd, 0, SEEK_END);",
      "  if (size < 0) { perror(\"lseek-mmap-sample-input\"); close(fd); return 51; }",
      "  if (size == 0) { close(fd); *total = 0; *samples = 0; *checksum = 0; return 0; }",
      "  void *mapped = mmap(NULL, (size_t)size, PROT_READ, MAP_PRIVATE, fd, 0);",
      "  if (mapped == MAP_FAILED) { perror(\"mmap-sample-input\"); close(fd); return 52; }",
      "  const unsigned char *bytes = (const unsigned char *)mapped;",
      "  off_t offsets[] = {",
      "    0,",
      "    1,",
      "    4095,",
      "    4096,",
      "    size / 2,",
      "    size > 4097 ? size - 4097 : 0,",
      "    size > 4096 ? size - 4096 : 0,",
      "    size > 2 ? size - 2 : 0,",
      "    size - 1,",
      "  };",
      "  *total = (unsigned long long)size;",
      "  *samples = 0;",
      "  *checksum = 0;",
      "  for (size_t i = 0; i < sizeof(offsets) / sizeof(offsets[0]); i++) {",
      "    if (offsets[i] >= 0 && offsets[i] < size) {",
      "      *checksum += (unsigned long long)bytes[offsets[i]];",
      "      *samples += 1;",
      "    }",
      "  }",
      "  if (munmap(mapped, (size_t)size) != 0) { perror(\"munmap-sample-input\"); close(fd); return 53; }",
      "  if (close(fd) != 0) { perror(\"close-mmap-sample-input\"); return 54; }",
      "  return 0;",
      "}",
      "",
      "int main(int argc, char **argv) {",
      "  const char *path = argc > 1 ? argv[1] : \"/work/large-file-read.bin\";",
      "  const char *mode = argc > 2 ? argv[2] : \"read\";",
      "  unsigned long long total = 0;",
      "  unsigned long long checksum = 0;",
      "  unsigned long long samples = 0;",
      "  if (strcmp(mode, \"fork\") == 0) {",
      "    pid_t child = fork();",
      "    if (child < 0) { perror(\"fork\"); return 40; }",
      "    if (child == 0) {",
      "      int rc = checksum_by_read(path, &total, &checksum);",
      "      if (rc == 0) printf(\"large-file-fork-read bytes=%llu checksum=%llu\\n\", total, checksum);",
      "      fflush(stdout);",
      "      _exit(rc);",
      "    }",
      "    int status = 0;",
      "    if (waitpid(child, &status, 0) < 0) { perror(\"waitpid\"); return 41; }",
      "    if (!WIFEXITED(status)) return 42;",
      "    return WEXITSTATUS(status);",
      "  }",
      "  if (strcmp(mode, \"mmap\") == 0) {",
      "    int rc = checksum_by_mmap(path, &total, &checksum);",
      "    if (rc == 0) printf(\"large-file-mmap-read bytes=%llu checksum=%llu\\n\", total, checksum);",
      "    return rc;",
      "  }",
      "  if (strcmp(mode, \"mmap-sample\") == 0) {",
      "    int rc = checksum_sample_by_mmap(path, &total, &samples, &checksum);",
      "    if (rc == 0) printf(\"large-file-mmap-sample-read bytes=%llu samples=%llu checksum=%llu\\n\", total, samples, checksum);",
      "    return rc;",
      "  }",
      "  int rc = checksum_by_read(path, &total, &checksum);",
      "  if (rc == 0) printf(\"large-file-read bytes=%llu checksum=%llu\\n\", total, checksum);",
      "  return rc;",
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

function mmapSampleOffsets(length: number): number[] {
  const size = Math.max(0, Math.trunc(length));
  if (size === 0) {
    return [];
  }
  return [
    0,
    1,
    4095,
    4096,
    Math.trunc(size / 2),
    size > 4097 ? size - 4097 : 0,
    size > 4096 ? size - 4096 : 0,
    size > 2 ? size - 2 : 0,
    size - 1,
  ].filter((offset) => offset >= 0 && offset < size);
}

function mmapSampleChecksum(bytes: Uint8Array): { samples: number; checksum: bigint } {
  let sum = 0n;
  const offsets = mmapSampleOffsets(bytes.length);
  for (const offset of offsets) {
    sum += BigInt(bytes[offset]);
  }
  return { samples: offsets.length, checksum: sum };
}

function buildResidentWitnessInput(length = 4096): Uint8Array {
  const data = new Uint8Array(length);
  for (let offset = 0; offset < data.length; offset += 1) {
    data[offset] = (offset * 29 + 7) & 0xff;
  }
  return data;
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

test("resident page fill witness: repeated guest reads keep snapshot input resident", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildLargeFileReadProbe();
  if (!probeBytes) {
    return;
  }
  const input = buildResidentWitnessInput();
  const expected = checksum(input);

  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: 256 * 1024 * 1024,
    maxProcessMemoryPages: 32768,
  });
  try {
    const snapshot = buildSnapshotFromSources([
      { kind: "inlineFile", guestPath: largeFilePath, data: input, mode: 0o644 },
    ]);
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    const before = await runtime.debugPageCachePath(largeFilePath);
    expect(before, "snapshot input should publish metadata").not.toBeNull();
    expect(before?.dataOff, "snapshot input should be backing-store backed").toBe(0);
    expect(before?.residentPages ?? 0).toBe(0);

    for (const runLabel of ["first", "second"]) {
      const result = await spawnWithOutputAndStats(
        runtime,
        probeBytes,
        ["/work/large-file-read", largeFilePath],
        ["LC_ALL=C", "LANG=C"],
        60_000,
        { cwd: "/", stdin: "closed" },
      );
      expect(result.exitCode, `${runLabel} run\n${result.output}${result.debugInfo ? `\n${result.debugInfo}` : ""}`).toBe(0);
      expect(result.output).toContain(`large-file-read bytes=${input.length}`);
      expect(result.output).toContain(`checksum=${expected.toString()}`);
    }

    const after = await runtime.debugPageCachePath(largeFilePath);
    expect(after, "snapshot input should remain published").not.toBeNull();
    expect(after?.dataOff, "resident fill must not materialize whole-file inode data").toBe(0);
    expect(after?.residentPages ?? 0).toBeGreaterThan(0);
  } finally {
    runtime.destroy();
  }
}, 90_000);

test("resident eviction reload witness: kernel-worker evicts then reloads snapshot input", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildLargeFileReadProbe();
  if (!probeBytes) {
    return;
  }
  const input = buildResidentWitnessInput();
  const expected = checksum(input);

  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: 256 * 1024 * 1024,
    maxProcessMemoryPages: 32768,
  });
  try {
    const snapshot = buildSnapshotFromSources([
      { kind: "inlineFile", guestPath: largeFilePath, data: input, mode: 0o644 },
    ]);
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    const before = await runtime.debugPageCachePath(largeFilePath);
    expect(before, "snapshot input should publish metadata").not.toBeNull();
    expect(before?.dataOff, "snapshot input should be backing-store backed").toBe(0);
    expect(before?.residentPages ?? 0).toBe(0);

    const first = await spawnWithOutputAndStats(
      runtime,
      probeBytes,
      ["/work/large-file-read", largeFilePath],
      ["LC_ALL=C", "LANG=C"],
      60_000,
      { cwd: "/", stdin: "closed" },
    );
    expect(first.exitCode, first.output + (first.debugInfo ? `\n${first.debugInfo}` : "")).toBe(0);
    expect(first.output).toContain(`large-file-read bytes=${input.length}`);
    expect(first.output).toContain(`checksum=${expected.toString()}`);

    const filled = await runtime.debugPageCachePath(largeFilePath);
    expect(filled?.dataOff, "resident fill must not materialize whole-file inode data").toBe(0);
    expect(filled?.residentPages ?? 0).toBeGreaterThan(0);

    const evicted = await runtime.debugEvictPageCachePath(largeFilePath, 0);
    expect(evicted?.status).toBe("evicted");
    expect(evicted?.residentPages ?? 0).toBe(0);

    const second = await spawnWithOutputAndStats(
      runtime,
      probeBytes,
      ["/work/large-file-read", largeFilePath],
      ["LC_ALL=C", "LANG=C"],
      60_000,
      { cwd: "/", stdin: "closed" },
    );
    expect(second.exitCode, second.output + (second.debugInfo ? `\n${second.debugInfo}` : "")).toBe(0);
    expect(second.output).toContain(`large-file-read bytes=${input.length}`);
    expect(second.output).toContain(`checksum=${expected.toString()}`);

    const reloaded = await runtime.debugPageCachePath(largeFilePath);
    expect(reloaded?.dataOff, "reload must keep file data in backing store").toBe(0);
    expect(reloaded?.residentPages ?? 0).toBeGreaterThan(0);
  } finally {
    runtime.destroy();
  }
}, 90_000);

test("resident multi-page fill witness: guest read fills all pages in requested range", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildLargeFileReadProbe();
  if (!probeBytes) {
    return;
  }
  const input = buildResidentWitnessInput(8192);
  const expected = checksum(input);

  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: 256 * 1024 * 1024,
    maxProcessMemoryPages: 32768,
  });
  try {
    const snapshot = buildSnapshotFromSources([
      { kind: "inlineFile", guestPath: largeFilePath, data: input, mode: 0o644 },
    ]);
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    const before = await runtime.debugPageCachePath(largeFilePath);
    expect(before, "snapshot input should publish metadata").not.toBeNull();
    expect(before?.dataOff, "snapshot input should be backing-store backed").toBe(0);
    expect(before?.residentPages ?? 0).toBe(0);

    const result = await spawnWithOutputAndStats(
      runtime,
      probeBytes,
      ["/work/large-file-read", largeFilePath],
      ["LC_ALL=C", "LANG=C"],
      60_000,
      { cwd: "/", stdin: "closed" },
    );
    expect(result.exitCode, result.output + (result.debugInfo ? `\n${result.debugInfo}` : "")).toBe(0);
    expect(result.output).toContain(`large-file-read bytes=${input.length}`);
    expect(result.output).toContain(`checksum=${expected.toString()}`);

    const after = await runtime.debugPageCachePath(largeFilePath);
    expect(after?.dataOff, "multi-page fill must not materialize whole-file inode data").toBe(0);
    expect(after?.residentPages ?? 0).toBeGreaterThanOrEqual(2);
  } finally {
    runtime.destroy();
  }
}, 90_000);

test("remote source demand fill witness: guest read fills metadata-only backing extent", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildLargeFileReadProbe();
  if (!probeBytes) {
    return;
  }
  const input = buildResidentWitnessInput(8192);
  const expected = checksum(input);
  const prefix = new TextEncoder().encode("remote-prefix:");
  const suffix = new TextEncoder().encode(":remote-suffix");
  const source = new Uint8Array(prefix.length + input.length + suffix.length);
  source.set(prefix, 0);
  source.set(input, prefix.length);
  source.set(suffix, prefix.length + input.length);
  const sourceIdentity = "remote:runtime-large-file-read.bin";
  const sourceUrl = `data:application/octet-stream;base64,${Buffer.from(source).toString("base64")}`;

  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: 256 * 1024 * 1024,
    maxProcessMemoryPages: 32768,
  });
  try {
    await runtime.applyFsSnapshot([
      { kind: "dir", path: "/", mode: 0o755 },
      { kind: "dir", path: "/work", mode: 0o755 },
      {
        kind: "file",
        path: largeFilePath,
        size: input.length,
        mode: 0o644,
        backingExtent: {
          sourceIdentity,
          offset: prefix.length,
          length: input.length,
        },
      },
    ]);
    await runtime.registerFsRemoteBackingSource(sourceIdentity, source.length, sourceUrl);
    const before = await runtime.debugPageCachePath(largeFilePath);
    expect(before, "metadata-only source-backed input should publish inode metadata").not.toBeNull();
    expect(before?.dataOff, "source-backed input must not be inline-published").toBe(0);
    expect(before?.backingBytes, "async source should not be materialized before first read").toBeUndefined();
    expect(before?.residentPages ?? 0).toBe(0);

    const result = await spawnWithOutputAndStats(
      runtime,
      probeBytes,
      ["/work/large-file-read", largeFilePath],
      ["LC_ALL=C", "LANG=C"],
      60_000,
      { cwd: "/", stdin: "closed" },
    );
    expect(result.exitCode, result.output + (result.debugInfo ? `\n${result.debugInfo}` : "")).toBe(0);
    expect(result.output).toContain(`large-file-read bytes=${input.length}`);
    expect(result.output).toContain(`checksum=${expected.toString()}`);

    const after = await runtime.debugPageCachePath(largeFilePath);
    expect(after?.dataOff, "remote demand fill must keep file data out of inline SAB inode data").toBe(0);
    expect(after?.backingBytes, "range demand fill must not materialize the whole backing file").toBeUndefined();
    expect(after?.residentPages ?? 0).toBeGreaterThanOrEqual(2);
  } finally {
    runtime.destroy();
  }
}, 90_000);

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
    guestMemoryPolicyLimitBytes: 512 * 1024 * 1024,
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
      largeFileReadTimeoutMs,
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
      `timeoutMs=${largeFileReadTimeoutMs}`,
      `execution=${result.execution}`,
    ].join(" "));
  } finally {
    runtime.destroy();
  }
}, largeFileReadTestTimeoutMs);

test("large regular file read local gate: fork child reads backing-store backed input", async () => {
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
    guestMemoryPolicyLimitBytes: 512 * 1024 * 1024,
    maxProcessMemoryPages: 32768,
  });
  const startedMs = Date.now();
  try {
    const snapshot = buildSnapshotFromSources([
      { kind: "inlineFile", guestPath: largeFilePath, data: input, mode: 0o644 },
    ]);
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    const result = await spawnWithOutputAndStats(
      runtime,
      probeBytes,
      ["/work/large-file-read", largeFilePath, "fork"],
      ["LC_ALL=C", "LANG=C"],
      largeFileReadTimeoutMs,
      { cwd: "/", stdin: "closed" },
    );
    expect(result.exitCode, result.output + (result.debugInfo ? `\n${result.debugInfo}` : "")).toBe(0);
    expect(result.output).toContain(`large-file-fork-read bytes=${input.length}`);
    expect(result.output).toContain(`checksum=${expected.toString()}`);
    console.log([
      `large regular file fork read gate durationMs=${Date.now() - startedMs}`,
      `bytes=${input.length}`,
      `input=${largeFileReadInput || "synthetic"}`,
      `timeoutMs=${largeFileReadTimeoutMs}`,
      `execution=${result.execution}`,
    ].join(" "));
  } finally {
    runtime.destroy();
  }
}, largeFileReadTestTimeoutMs);

test("large regular file read local gate: mmap reads backing-store backed input", async () => {
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
    guestMemoryPolicyLimitBytes: 512 * 1024 * 1024,
    maxProcessMemoryPages: 32768,
  });
  const startedMs = Date.now();
  try {
    const snapshot = buildSnapshotFromSources([
      { kind: "inlineFile", guestPath: largeFilePath, data: input, mode: 0o644 },
    ]);
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    const result = await spawnWithOutputAndStats(
      runtime,
      probeBytes,
      ["/work/large-file-read", largeFilePath, "mmap"],
      ["LC_ALL=C", "LANG=C"],
      largeFileReadTimeoutMs,
      { cwd: "/", stdin: "closed" },
    );
    expect(result.exitCode, result.output + (result.debugInfo ? `\n${result.debugInfo}` : "")).toBe(0);
    expect(result.output).toContain(`large-file-mmap-read bytes=${input.length}`);
    expect(result.output).toContain(`checksum=${expected.toString()}`);
    console.log([
      `large regular file mmap read gate durationMs=${Date.now() - startedMs}`,
      `bytes=${input.length}`,
      `input=${largeFileReadInput || "synthetic"}`,
      `timeoutMs=${largeFileReadTimeoutMs}`,
      `execution=${result.execution}`,
    ].join(" "));
  } finally {
    runtime.destroy();
  }
}, largeFileReadTestTimeoutMs);

test("large regular file read local gate: mmap samples backing-store backed input", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildLargeFileReadProbe();
  if (!probeBytes) {
    return;
  }
  const input = buildInput();
  const expected = mmapSampleChecksum(input);

  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: 512 * 1024 * 1024,
    maxProcessMemoryPages: 32768,
  });
  const startedMs = Date.now();
  try {
    const snapshot = buildSnapshotFromSources([
      { kind: "inlineFile", guestPath: largeFilePath, data: input, mode: 0o644 },
    ]);
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    const result = await spawnWithOutputAndStats(
      runtime,
      probeBytes,
      ["/work/large-file-read", largeFilePath, "mmap-sample"],
      ["LC_ALL=C", "LANG=C"],
      largeFileReadTimeoutMs,
      { cwd: "/", stdin: "closed" },
    );
    expect(result.exitCode, result.output + (result.debugInfo ? `\n${result.debugInfo}` : "")).toBe(0);
    expect(result.output).toContain(`large-file-mmap-sample-read bytes=${input.length}`);
    expect(result.output).toContain(`samples=${expected.samples}`);
    expect(result.output).toContain(`checksum=${expected.checksum.toString()}`);
    console.log([
      `large regular file mmap sample gate durationMs=${Date.now() - startedMs}`,
      `bytes=${input.length}`,
      `input=${largeFileReadInput || "synthetic"}`,
      `timeoutMs=${largeFileReadTimeoutMs}`,
      `execution=${result.execution}`,
    ].join(" "));
  } finally {
    runtime.destroy();
  }
}, largeFileReadTestTimeoutMs);
