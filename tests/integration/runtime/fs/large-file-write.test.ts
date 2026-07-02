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
import type { FsSnapshotEntry } from "../../../../src/fs/types.js";

type RuntimeInstance = Awaited<ReturnType<typeof Runtime.create>>;
type SnapshotFsRuntime = RuntimeInstance & {
  sendKernelRequest<T>(message: { type: "snapshot-fs"; includeData?: boolean }): Promise<T>;
};

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

function buildResidentRewriteProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-resident-rewrite-"));
  const sourcePath = join(tmp, "resident_rewrite.c");
  const outputPath = join(tmp, "resident-rewrite");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <stdio.h>",
      "#include <string.h>",
      "#include <unistd.h>",
      "",
      "static int write_exact(int fd, const unsigned char *data, size_t len) {",
      "  size_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, len - off);",
      "    if (n < 0) { if (errno == EINTR) continue; perror(\"write\"); return 1; }",
      "    if (n == 0) { fputs(\"write-zero\\n\", stderr); return 2; }",
      "    off += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "int main(int argc, char **argv) {",
      "  const char *path = argc > 1 ? argv[1] : \"/work/large-file-write.bin\";",
      "  static const unsigned char replacement[] = \"updated-resident-write\";",
      "  int fd = open(path, O_RDWR);",
      "  if (fd < 0) { perror(\"open\"); return 20; }",
      "  unsigned char head[4];",
      "  ssize_t n = read(fd, head, sizeof(head));",
      "  if (n != 4 || memcmp(head, \"seed\", 4) != 0) {",
      "    fprintf(stderr, \"bad-head n=%ld data=%.*s\\n\", (long)n, (int)(n > 0 ? n : 0), head);",
      "    close(fd);",
      "    return 21;",
      "  }",
      "  if (lseek(fd, 0, SEEK_SET) < 0) { perror(\"lseek\"); close(fd); return 22; }",
      "  if (write_exact(fd, replacement, sizeof(replacement) - 1) != 0) { close(fd); return 23; }",
      "  if (ftruncate(fd, (off_t)(sizeof(replacement) - 1)) != 0) { perror(\"ftruncate\"); close(fd); return 24; }",
      "  if (close(fd) != 0) { perror(\"close\"); return 25; }",
      "  printf(\"resident-rewrite bytes=%zu\\n\", sizeof(replacement) - 1);",
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
    return logSkip(`failed to build resident rewrite probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildResidentFsyncProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-resident-fsync-"));
  const sourcePath = join(tmp, "resident_fsync.c");
  const outputPath = join(tmp, "resident-fsync");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <stdio.h>",
      "#include <string.h>",
      "#include <unistd.h>",
      "",
      "static int write_exact(int fd, const unsigned char *data, size_t len) {",
      "  size_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, len - off);",
      "    if (n < 0) { if (errno == EINTR) continue; perror(\"write\"); return 1; }",
      "    if (n == 0) { fputs(\"write-zero\\n\", stderr); return 2; }",
      "    off += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "int main(int argc, char **argv) {",
      "  const char *path = argc > 1 ? argv[1] : \"/work/large-file-write.bin\";",
      "  static const unsigned char first[] = \"fdatasync-resident-stage\";",
      "  static const unsigned char second[] = \"fsync-resident-final\";",
      "  int fd = open(path, O_RDWR);",
      "  if (fd < 0) { perror(\"open\"); return 20; }",
      "  unsigned char head[4];",
      "  ssize_t n = read(fd, head, sizeof(head));",
      "  if (n != 4 || memcmp(head, \"seed\", 4) != 0) {",
      "    fprintf(stderr, \"bad-head n=%ld data=%.*s\\n\", (long)n, (int)(n > 0 ? n : 0), head);",
      "    close(fd);",
      "    return 21;",
      "  }",
      "  if (lseek(fd, 0, SEEK_SET) < 0) { perror(\"lseek-first\"); close(fd); return 22; }",
      "  if (write_exact(fd, first, sizeof(first) - 1) != 0) { close(fd); return 23; }",
      "  if (ftruncate(fd, (off_t)(sizeof(first) - 1)) != 0) { perror(\"ftruncate-first\"); close(fd); return 24; }",
      "  if (fdatasync(fd) != 0) { perror(\"fdatasync\"); close(fd); return 25; }",
      "  if (lseek(fd, 0, SEEK_SET) < 0) { perror(\"lseek-second\"); close(fd); return 26; }",
      "  if (write_exact(fd, second, sizeof(second) - 1) != 0) { close(fd); return 27; }",
      "  if (ftruncate(fd, (off_t)(sizeof(second) - 1)) != 0) { perror(\"ftruncate-second\"); close(fd); return 28; }",
      "  if (fsync(fd) != 0) { perror(\"fsync\"); close(fd); return 29; }",
      "  if (close(fd) != 0) { perror(\"close\"); return 30; }",
      "  printf(\"resident-fsync bytes=%zu\\n\", sizeof(second) - 1);",
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
    return logSkip(`failed to build resident fsync probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildDirtyResidentMmapForkProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-dirty-resident-mmap-fork-"));
  const sourcePath = join(tmp, "dirty_resident_mmap_fork.c");
  const outputPath = join(tmp, "dirty-resident-mmap-fork");
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
      "#define PAGE_SIZE 4096",
      "",
      "static unsigned char source_byte(unsigned long long offset) {",
      "  return (unsigned char)((offset * 13ull + 7ull) & 0xffu);",
      "}",
      "",
      "static unsigned char dirty_byte(unsigned long long offset) {",
      "  return (unsigned char)((offset * 17ull + 23ull) & 0xffu);",
      "}",
      "",
      "static void fill_dirty(unsigned char *buf, unsigned long long base) {",
      "  for (unsigned long long i = 0; i < PAGE_SIZE; i++) buf[i] = dirty_byte(base + i);",
      "}",
      "",
      "static int write_page(int fd, unsigned long long offset) {",
      "  static unsigned char buf[PAGE_SIZE];",
      "  fill_dirty(buf, offset);",
      "  if (lseek(fd, (off_t)offset, SEEK_SET) < 0) { perror(\"lseek-write\"); return 10; }",
      "  size_t written = 0;",
      "  while (written < PAGE_SIZE) {",
      "    ssize_t n = write(fd, buf + written, PAGE_SIZE - written);",
      "    if (n < 0) { if (errno == EINTR) continue; perror(\"write-page\"); return 11; }",
      "    if (n == 0) return 12;",
      "    written += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static int check_page_bytes(const unsigned char *bytes, unsigned long long base) {",
      "  for (unsigned long long i = 0; i < PAGE_SIZE; i++) {",
      "    if (bytes[i] != dirty_byte(base + i)) return 20;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static int check_file_samples(const char *path, unsigned long long total) {",
      "  int fd = open(path, O_RDONLY);",
      "  if (fd < 0) { perror(\"open-check\"); return 30; }",
      "  unsigned char buf[PAGE_SIZE];",
      "  unsigned long long last = total - PAGE_SIZE;",
      "  if (pread(fd, buf, PAGE_SIZE, 0) != PAGE_SIZE) { perror(\"pread-head\"); close(fd); return 31; }",
      "  if (check_page_bytes(buf, 0) != 0) { close(fd); return 32; }",
      "  if (pread(fd, buf, PAGE_SIZE, (off_t)last) != PAGE_SIZE) { perror(\"pread-tail\"); close(fd); return 33; }",
      "  if (check_page_bytes(buf, last) != 0) { close(fd); return 34; }",
      "  unsigned long long middle = (total / 2ull) & ~4095ull;",
      "  if (pread(fd, buf, 16, (off_t)middle) != 16) { perror(\"pread-middle\"); close(fd); return 35; }",
      "  for (unsigned long long i = 0; i < 16; i++) {",
      "    unsigned char expected = source_byte(middle + i);",
      "    if (buf[i] != expected) {",
      "      fprintf(stderr, \"file-middle mismatch off=%llu index=%llu got=%u expected=%u\\n\", middle, i, buf[i], expected);",
      "      close(fd);",
      "      return 36;",
      "    }",
      "  }",
      "  if (close(fd) != 0) { perror(\"close-check\"); return 37; }",
      "  return 0;",
      "}",
      "",
      "static int check_mmap(const char *path, unsigned long long total) {",
      "  int fd = open(path, O_RDONLY);",
      "  if (fd < 0) { perror(\"open-mmap\"); return 40; }",
      "  void *head = mmap(NULL, PAGE_SIZE, PROT_READ, MAP_PRIVATE, fd, 0);",
      "  if (head == MAP_FAILED) { perror(\"mmap-head\"); close(fd); return 41; }",
      "  int rc = check_page_bytes((const unsigned char *)head, 0);",
      "  if (munmap(head, PAGE_SIZE) != 0) { perror(\"munmap-head\"); rc = 43; }",
      "  unsigned long long last = total - PAGE_SIZE;",
      "  void *tail = mmap(NULL, PAGE_SIZE, PROT_READ, MAP_PRIVATE, fd, (off_t)last);",
      "  if (tail == MAP_FAILED) { perror(\"mmap-tail\"); close(fd); return 44; }",
      "  if (rc == 0) rc = check_page_bytes((const unsigned char *)tail, last);",
      "  if (munmap(tail, PAGE_SIZE) != 0) { perror(\"munmap-tail\"); rc = 45; }",
      "  unsigned long long middle = (total / 2ull) & ~4095ull;",
      "  unsigned char middle_buf[16];",
      "  if (pread(fd, middle_buf, sizeof(middle_buf), (off_t)middle) != (ssize_t)sizeof(middle_buf)) {",
      "    perror(\"pread-mmap-middle\");",
      "    rc = 46;",
      "  }",
      "  for (unsigned long long i = 0; rc == 0 && i < sizeof(middle_buf); i++) {",
      "    unsigned char expected = source_byte(middle + i);",
      "    if (middle_buf[i] != expected) {",
      "      fprintf(stderr, \"mmap-middle mismatch off=%llu index=%llu got=%u expected=%u\\n\", middle, i, middle_buf[i], expected);",
      "      rc = 42;",
      "    }",
      "  }",
      "  if (close(fd) != 0) { perror(\"close-mmap\"); return 47; }",
      "  return rc;",
      "}",
      "",
      "int main(int argc, char **argv) {",
      "  const char *path = argc > 1 ? argv[1] : \"/work/large-file-write.bin\";",
      "  unsigned long long total = argc > 2 ? strtoull(argv[2], NULL, 10) : 16785408ull;",
      "  if (total < PAGE_SIZE * 3ull) return 50;",
      "  int fd = open(path, O_RDWR);",
      "  if (fd < 0) { perror(\"open-write\"); return 51; }",
      "  int rc = write_page(fd, 0);",
      "  if (rc == 0) rc = write_page(fd, total - PAGE_SIZE);",
      "  if (close(fd) != 0 && rc == 0) { perror(\"close-write\"); rc = 52; }",
      "  if (rc != 0) return rc;",
      "  rc = check_mmap(path, total);",
      "  if (rc != 0) return rc;",
      "  pid_t child = fork();",
      "  if (child < 0) { perror(\"fork\"); return 60; }",
      "  if (child == 0) _exit(check_file_samples(path, total));",
      "  int status = 0;",
      "  if (waitpid(child, &status, 0) < 0) { perror(\"waitpid\"); return 61; }",
      "  if (!WIFEXITED(status)) return 62;",
      "  rc = WEXITSTATUS(status);",
      "  if (rc != 0) return rc;",
      "  printf(\"dirty-resident-mmap-fork bytes=%llu\\n\", total);",
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
    return logSkip(`failed to build dirty resident mmap/fork probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function expectedByte(offset: number): number {
  return (offset * 17 + 23) & 0xff;
}

function sourceByte(offset: number): number {
  return (offset * 13 + 7) & 0xff;
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
    vforkReadyWorkers: proc.vforkReadyWorkers,
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

async function snapshotRuntimeFs(runtime: RuntimeInstance, includeData: boolean): Promise<FsSnapshotEntry[]> {
  const response = await (runtime as SnapshotFsRuntime).sendKernelRequest<{
    type: "snapshot-fs-result";
    entries: FsSnapshotEntry[];
  }>({ type: "snapshot-fs", includeData });
  return response.entries;
}

test("resident write witness: guest rewrite keeps dirty resident pages authoritative", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildResidentRewriteProbe();
  if (!probeBytes) {
    return;
  }
  const initial = new TextEncoder().encode("seed-original-resident-bytes");
  const expected = new TextEncoder().encode("updated-resident-write");

  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: 256 * 1024 * 1024,
    maxProcessMemoryPages: 32768,
  });
  try {
    const snapshot = buildSnapshotFromSources([
      { kind: "inlineFile", guestPath: largeFilePath, data: initial, mode: 0o644 },
    ]);
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    const before = await runtime.debugPageCachePath(largeFilePath);
    expect(before?.dataOff).toBe(0);
    expect(before?.residentPages ?? 0).toBe(0);

    const result = await spawnWithOutputAndStats(
      runtime,
      probeBytes,
      ["/work/resident-rewrite", largeFilePath],
      ["LC_ALL=C", "LANG=C"],
      60_000,
      { cwd: "/", stdin: "closed" },
    );
    expect(result.exitCode, result.output + (result.debugInfo ? `\n${result.debugInfo}` : "")).toBe(0);
    expect(result.output).toContain(`resident-rewrite bytes=${expected.length}`);

    const after = await runtime.debugPageCachePath(largeFilePath);
    expect(after?.dataOff).toBe(0);
    expect(after?.residentPages ?? 0).toBeGreaterThan(0);
    expect(after?.backingBytes).toBe(expected.length);
    const evictedAfterWrite = await runtime.debugEvictPageCachePath(largeFilePath, 0);
    expect(evictedAfterWrite?.status).toBe("dirty-pinned");
    expect(evictedAfterWrite?.residentPages ?? 0).toBeGreaterThan(0);
    expect(await runtime.readFile(largeFilePath)).toEqual(expected);
  } finally {
    runtime.destroy();
  }
}, 90_000);

test("resident fsync witness: guest fsync flushes dirty resident pages clean", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildResidentFsyncProbe();
  if (!probeBytes) {
    return;
  }
  const initial = new TextEncoder().encode("seed-original-resident-bytes");
  const expected = new TextEncoder().encode("fsync-resident-final");

  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: 256 * 1024 * 1024,
    maxProcessMemoryPages: 32768,
  });
  try {
    const snapshot = buildSnapshotFromSources([
      { kind: "inlineFile", guestPath: largeFilePath, data: initial, mode: 0o644 },
    ]);
    await runtime.applyFsSnapshot(snapshot.entries ?? []);

    const result = await spawnWithOutputAndStats(
      runtime,
      probeBytes,
      ["/work/resident-fsync", largeFilePath],
      ["LC_ALL=C", "LANG=C"],
      60_000,
      { cwd: "/", stdin: "closed" },
    );
    expect(result.exitCode, result.output + (result.debugInfo ? `\n${result.debugInfo}` : "")).toBe(0);
    expect(result.output).toContain(`resident-fsync bytes=${expected.length}`);

    const after = await runtime.debugPageCachePath(largeFilePath);
    expect(after?.dataOff).toBe(0);
    expect(after?.backingBytes).toBe(expected.length);
    expect(after?.residentPages ?? 0).toBeGreaterThan(0);
    const evictedAfterFsync = await runtime.debugEvictPageCachePath(largeFilePath, 0);
    expect(evictedAfterFsync?.status).toBe("evicted");
    expect(await runtime.readFile(largeFilePath)).toEqual(expected);
  } finally {
    runtime.destroy();
  }
}, 90_000);

test("source-backed guest rewrite export witness: snapshot clears stale backing extent", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildResidentRewriteProbe();
  if (!probeBytes) {
    return;
  }
  const initial = new TextEncoder().encode("seed-original-resident-bytes");
  const expected = new TextEncoder().encode("updated-resident-write");
  const prefix = new TextEncoder().encode("source-prefix:");
  const suffix = new TextEncoder().encode(":source-suffix");
  const source = new Uint8Array(prefix.length + initial.length + suffix.length);
  source.set(prefix, 0);
  source.set(initial, prefix.length);
  source.set(suffix, prefix.length + initial.length);
  const sourceIdentity = "remote:runtime-large-file-write.bin";
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
        size: initial.length,
        mode: 0o644,
        backingExtent: {
          sourceIdentity,
          offset: prefix.length,
          length: initial.length,
        },
      },
    ]);
    await runtime.registerFsRemoteBackingSource(sourceIdentity, source.length, sourceUrl);
    const before = (await snapshotRuntimeFs(runtime, false)).find((entry) => (
      entry.kind === "file" && entry.path === largeFilePath
    ));
    expect(before?.kind === "file" ? before.backingExtent : undefined)
      .toEqual({ sourceIdentity, offset: prefix.length, length: initial.length });

    const result = await spawnWithOutputAndStats(
      runtime,
      probeBytes,
      ["/work/resident-rewrite", largeFilePath],
      ["LC_ALL=C", "LANG=C"],
      60_000,
      { cwd: "/", stdin: "closed" },
    );
    expect(result.exitCode, result.output + (result.debugInfo ? `\n${result.debugInfo}` : "")).toBe(0);
    expect(result.output).toContain(`resident-rewrite bytes=${expected.length}`);
    expect(await runtime.readFile(largeFilePath)).toEqual(expected);

    const metadataSnapshot = (await snapshotRuntimeFs(runtime, false)).find((entry) => (
      entry.kind === "file" && entry.path === largeFilePath
    ));
    expect(metadataSnapshot?.kind === "file" ? metadataSnapshot.backingExtent : undefined).toBeUndefined();
    const dataSnapshot = (await snapshotRuntimeFs(runtime, true)).find((entry) => (
      entry.kind === "file" && entry.path === largeFilePath
    ));
    expect(dataSnapshot?.kind === "file" ? dataSnapshot.data : undefined).toEqual(expected);
    expect(dataSnapshot?.kind === "file" ? dataSnapshot.backingExtent : undefined).toBeUndefined();
  } finally {
    runtime.destroy();
  }
}, 90_000);

test("source-backed dirty resident mmap/fork witness survives capacity pressure", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildDirtyResidentMmapForkProbe();
  if (!probeBytes) {
    return;
  }
  const bytes = Number(process.env.TIDEMARK_TEST_DIRTY_RESIDENT_MMAP_FORK_BYTES ?? 16 * 1024 * 1024 + 8192);
  const source = new Uint8Array(bytes);
  for (let offset = 0; offset < source.length; offset += 1) {
    source[offset] = sourceByte(offset);
  }
  const sourceIdentity = "remote:runtime-dirty-resident-mmap-fork.bin";
  const sourceUrl = `data:application/octet-stream;base64,${Buffer.from(source).toString("base64")}`;

  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: 512 * 1024 * 1024,
    maxProcessMemoryPages: 32768,
    pageCacheDataCapacityBytes: 16 * 1024 * 1024,
  });
  try {
    await runtime.applyFsSnapshot([
      { kind: "dir", path: "/", mode: 0o755 },
      { kind: "dir", path: "/work", mode: 0o755 },
      {
        kind: "file",
        path: largeFilePath,
        size: source.length,
        mode: 0o644,
        backingExtent: {
          sourceIdentity,
          offset: 0,
          length: source.length,
        },
      },
    ]);
    await runtime.registerFsRemoteBackingSource(sourceIdentity, source.length, sourceUrl);
    const before = await runtime.debugPageCachePath(largeFilePath);
    expect(before?.dataOff).toBe(0);
    expect(before?.residentPages ?? 0).toBe(0);

    const result = await spawnWithOutputAndStats(
      runtime,
      probeBytes,
      ["/work/dirty-resident-mmap-fork", largeFilePath, String(source.length)],
      ["LC_ALL=C", "LANG=C"],
      180_000,
      { cwd: "/", stdin: "closed" },
    );
    expect(result.exitCode, result.output + (result.debugInfo ? `\n${result.debugInfo}` : "")).toBe(0);
    expect(result.output).toContain(`dirty-resident-mmap-fork bytes=${source.length}`);

    const after = await runtime.debugPageCachePath(largeFilePath);
    expect(after?.dataOff).toBe(0);
    expect(after?.residentPages ?? 0).toBeGreaterThan(0);
    expect(after?.backingBytes).toBeUndefined();
    const headEvict = await runtime.debugEvictPageCachePath(largeFilePath, 0);
    expect(headEvict?.status).toBe("dirty-pinned");
    const tailEvict = await runtime.debugEvictPageCachePath(
      largeFilePath,
      Math.floor((source.length - 1) / 4096),
    );
    expect(tailEvict?.status).toBe("dirty-pinned");
  } finally {
    runtime.destroy();
  }
}, 210_000);

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
