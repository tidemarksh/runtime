import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
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
const largeSha256Bytes = Number(process.env.TIDEMARK_TEST_LARGE_SHA256_BYTES ?? 8 * 1024 * 1024);
const largeSha256Input = process.env.TIDEMARK_TEST_LARGE_SHA256_INPUT ?? "";
const largeSha256Path = "/work/large-sha256.bin";

function buildPipeReadBlockerProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-pipe-read-blocker-"));
  const sourcePath = join(tmp, "pipe_read_blocker.c");
  const outputPath = join(tmp, "pipe-read-blocker");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <stdio.h>",
      "#include <unistd.h>",
      "",
      "int main(void) {",
      "  unsigned char byte = 0;",
      "  for (;;) {",
      "    ssize_t n = read(0, &byte, 1);",
      "    if (n < 0 && errno == EINTR) continue;",
      "    if (n < 0) { perror(\"read-stdin\"); return 20; }",
      "    if (n == 0) return 0;",
      "  }",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", outputPath, sourcePath], {
      stdio: "pipe",
    });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build pipe read blocker probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildLargeSha256Probe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-large-sha256-"));
  const sourcePath = join(tmp, "large_sha256.c");
  const outputPath = join(tmp, "large-sha256");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <stdint.h>",
      "#include <stdio.h>",
      "#include <string.h>",
      "#include <unistd.h>",
      "",
      "#define CHUNK_SIZE 65536",
      "",
      "typedef struct {",
      "  uint8_t data[64];",
      "  uint32_t datalen;",
      "  uint64_t bitlen;",
      "  uint32_t state[8];",
      "} SHA256_CTX_LOCAL;",
      "",
      "static const uint32_t k[64] = {",
      "  0x428a2f98u,0x71374491u,0xb5c0fbcfu,0xe9b5dba5u,0x3956c25bu,0x59f111f1u,0x923f82a4u,0xab1c5ed5u,",
      "  0xd807aa98u,0x12835b01u,0x243185beu,0x550c7dc3u,0x72be5d74u,0x80deb1feu,0x9bdc06a7u,0xc19bf174u,",
      "  0xe49b69c1u,0xefbe4786u,0x0fc19dc6u,0x240ca1ccu,0x2de92c6fu,0x4a7484aau,0x5cb0a9dcu,0x76f988dau,",
      "  0x983e5152u,0xa831c66du,0xb00327c8u,0xbf597fc7u,0xc6e00bf3u,0xd5a79147u,0x06ca6351u,0x14292967u,",
      "  0x27b70a85u,0x2e1b2138u,0x4d2c6dfcu,0x53380d13u,0x650a7354u,0x766a0abbu,0x81c2c92eu,0x92722c85u,",
      "  0xa2bfe8a1u,0xa81a664bu,0xc24b8b70u,0xc76c51a3u,0xd192e819u,0xd6990624u,0xf40e3585u,0x106aa070u,",
      "  0x19a4c116u,0x1e376c08u,0x2748774cu,0x34b0bcb5u,0x391c0cb3u,0x4ed8aa4au,0x5b9cca4fu,0x682e6ff3u,",
      "  0x748f82eeu,0x78a5636fu,0x84c87814u,0x8cc70208u,0x90befffau,0xa4506cebu,0xbef9a3f7u,0xc67178f2u",
      "};",
      "",
      "static uint32_t rotr(uint32_t x, uint32_t n) { return (x >> n) | (x << (32u - n)); }",
      "static uint32_t ch(uint32_t x, uint32_t y, uint32_t z) { return (x & y) ^ (~x & z); }",
      "static uint32_t maj(uint32_t x, uint32_t y, uint32_t z) { return (x & y) ^ (x & z) ^ (y & z); }",
      "static uint32_t ep0(uint32_t x) { return rotr(x, 2) ^ rotr(x, 13) ^ rotr(x, 22); }",
      "static uint32_t ep1(uint32_t x) { return rotr(x, 6) ^ rotr(x, 11) ^ rotr(x, 25); }",
      "static uint32_t sig0(uint32_t x) { return rotr(x, 7) ^ rotr(x, 18) ^ (x >> 3); }",
      "static uint32_t sig1(uint32_t x) { return rotr(x, 17) ^ rotr(x, 19) ^ (x >> 10); }",
      "",
      "static void sha256_transform(SHA256_CTX_LOCAL *ctx, const uint8_t data[]) {",
      "  uint32_t a, b, c, d, e, f, g, h, i, j, t1, t2, m[64];",
      "  for (i = 0, j = 0; i < 16; ++i, j += 4) {",
      "    m[i] = ((uint32_t)data[j] << 24) | ((uint32_t)data[j + 1] << 16) | ((uint32_t)data[j + 2] << 8) | (uint32_t)data[j + 3];",
      "  }",
      "  for (; i < 64; ++i) m[i] = sig1(m[i - 2]) + m[i - 7] + sig0(m[i - 15]) + m[i - 16];",
      "  a = ctx->state[0]; b = ctx->state[1]; c = ctx->state[2]; d = ctx->state[3];",
      "  e = ctx->state[4]; f = ctx->state[5]; g = ctx->state[6]; h = ctx->state[7];",
      "  for (i = 0; i < 64; ++i) {",
      "    t1 = h + ep1(e) + ch(e, f, g) + k[i] + m[i];",
      "    t2 = ep0(a) + maj(a, b, c);",
      "    h = g; g = f; f = e; e = d + t1; d = c; c = b; b = a; a = t1 + t2;",
      "  }",
      "  ctx->state[0] += a; ctx->state[1] += b; ctx->state[2] += c; ctx->state[3] += d;",
      "  ctx->state[4] += e; ctx->state[5] += f; ctx->state[6] += g; ctx->state[7] += h;",
      "}",
      "",
      "static void sha256_init(SHA256_CTX_LOCAL *ctx) {",
      "  ctx->datalen = 0; ctx->bitlen = 0;",
      "  ctx->state[0] = 0x6a09e667u; ctx->state[1] = 0xbb67ae85u; ctx->state[2] = 0x3c6ef372u; ctx->state[3] = 0xa54ff53au;",
      "  ctx->state[4] = 0x510e527fu; ctx->state[5] = 0x9b05688cu; ctx->state[6] = 0x1f83d9abu; ctx->state[7] = 0x5be0cd19u;",
      "}",
      "",
      "static void sha256_update(SHA256_CTX_LOCAL *ctx, const uint8_t data[], size_t len) {",
      "  for (size_t i = 0; i < len; ++i) {",
      "    ctx->data[ctx->datalen++] = data[i];",
      "    if (ctx->datalen == 64) {",
      "      sha256_transform(ctx, ctx->data);",
      "      ctx->bitlen += 512;",
      "      ctx->datalen = 0;",
      "    }",
      "  }",
      "}",
      "",
      "static void sha256_final(SHA256_CTX_LOCAL *ctx, uint8_t hash[]) {",
      "  uint32_t i = ctx->datalen;",
      "  if (ctx->datalen < 56) {",
      "    ctx->data[i++] = 0x80;",
      "    while (i < 56) ctx->data[i++] = 0x00;",
      "  } else {",
      "    ctx->data[i++] = 0x80;",
      "    while (i < 64) ctx->data[i++] = 0x00;",
      "    sha256_transform(ctx, ctx->data);",
      "    memset(ctx->data, 0, 56);",
      "  }",
      "  ctx->bitlen += (uint64_t)ctx->datalen * 8ull;",
      "  ctx->data[63] = (uint8_t)(ctx->bitlen);",
      "  ctx->data[62] = (uint8_t)(ctx->bitlen >> 8);",
      "  ctx->data[61] = (uint8_t)(ctx->bitlen >> 16);",
      "  ctx->data[60] = (uint8_t)(ctx->bitlen >> 24);",
      "  ctx->data[59] = (uint8_t)(ctx->bitlen >> 32);",
      "  ctx->data[58] = (uint8_t)(ctx->bitlen >> 40);",
      "  ctx->data[57] = (uint8_t)(ctx->bitlen >> 48);",
      "  ctx->data[56] = (uint8_t)(ctx->bitlen >> 56);",
      "  sha256_transform(ctx, ctx->data);",
      "  for (i = 0; i < 4; ++i) {",
      "    hash[i]      = (uint8_t)((ctx->state[0] >> (24 - i * 8)) & 0xff);",
      "    hash[i + 4]  = (uint8_t)((ctx->state[1] >> (24 - i * 8)) & 0xff);",
      "    hash[i + 8]  = (uint8_t)((ctx->state[2] >> (24 - i * 8)) & 0xff);",
      "    hash[i + 12] = (uint8_t)((ctx->state[3] >> (24 - i * 8)) & 0xff);",
      "    hash[i + 16] = (uint8_t)((ctx->state[4] >> (24 - i * 8)) & 0xff);",
      "    hash[i + 20] = (uint8_t)((ctx->state[5] >> (24 - i * 8)) & 0xff);",
      "    hash[i + 24] = (uint8_t)((ctx->state[6] >> (24 - i * 8)) & 0xff);",
      "    hash[i + 28] = (uint8_t)((ctx->state[7] >> (24 - i * 8)) & 0xff);",
      "  }",
      "}",
      "",
      "int main(int argc, char **argv) {",
      "  const char *path = argc > 1 ? argv[1] : \"/work/large-sha256.bin\";",
      "  int fd = open(path, O_RDONLY);",
      "  if (fd < 0) { perror(\"open-input\"); return 20; }",
      "  static uint8_t buf[CHUNK_SIZE];",
      "  SHA256_CTX_LOCAL ctx;",
      "  uint8_t digest[32];",
      "  unsigned long long total = 0;",
      "  sha256_init(&ctx);",
      "  for (;;) {",
      "    ssize_t n = read(fd, buf, sizeof(buf));",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      perror(\"read-input\");",
      "      close(fd);",
      "      return 21;",
      "    }",
      "    if (n == 0) break;",
      "    sha256_update(&ctx, buf, (size_t)n);",
      "    total += (unsigned long long)n;",
      "  }",
      "  if (close(fd) != 0) { perror(\"close-input\"); return 22; }",
      "  sha256_final(&ctx, digest);",
      "  printf(\"large-sha256 bytes=%llu sha256=\", total);",
      "  for (int i = 0; i < 32; ++i) printf(\"%02x\", digest[i]);",
      "  printf(\"\\n\");",
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
    return logSkip(`failed to build large sha256 probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildInput(): Uint8Array {
  if (largeSha256Input) {
    return new Uint8Array(readFileSync(largeSha256Input));
  }
  const data = new Uint8Array(Math.max(1, Math.trunc(largeSha256Bytes)));
  for (let offset = 0; offset < data.length; offset += 1) {
    data[offset] = (offset * 17 + 23) & 0xff;
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
    syscallDebugStats: proc.syscallDebugStats,
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

async function waitForBlockedWorkerCount(
  runtime: RuntimeInstance,
  expected: number,
  timeoutMs: number,
): Promise<string> {
  const started = Date.now();
  let execution = compactExecutionStats(runtime.debugExecutionStats());
  while (Date.now() - started < timeoutMs) {
    const stats = runtime.debugExecutionStats();
    execution = compactExecutionStats(stats);
    const blocked = stats.reduce((sum, proc) => sum + proc.blockedWorkers, 0);
    if (blocked >= expected) {
      return execution;
    }
    await new Promise((resolveDone) => setTimeout(resolveDone, 25));
  }
  throw new Error(`timeout waiting for ${expected} blocked workers: execution=${execution}`);
}

test("large sha256 local gate: guest hashes package-index sized input", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildLargeSha256Probe();
  if (!probeBytes) {
    return;
  }
  const input = buildInput();
  const expected = createHash("sha256").update(Buffer.from(input)).digest("hex");

  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: 512 * 1024 * 1024,
    maxProcessMemoryPages: 32768,
  });
  const startedMs = Date.now();
  try {
    const snapshot = buildSnapshotFromSources([
      { kind: "inlineFile", guestPath: largeSha256Path, data: input, mode: 0o644 },
    ]);
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    const spawnStartedMs = Date.now();
    const result = await spawnWithOutputAndStats(
      runtime,
      probeBytes,
      ["/work/large-sha256", largeSha256Path],
      ["LC_ALL=C", "LANG=C"],
      180_000,
      { cwd: "/", stdin: "closed" },
    );
    const spawnDurationMs = Date.now() - spawnStartedMs;
    expect(result.exitCode, result.output + (result.debugInfo ? `\n${result.debugInfo}` : "")).toBe(0);
    expect(result.output).toContain(`large-sha256 bytes=${input.length}`);
    expect(result.output).toContain(`sha256=${expected}`);
    console.log([
      `large sha256 gate durationMs=${Date.now() - startedMs}`,
      `spawnDurationMs=${spawnDurationMs}`,
      `bytes=${input.length}`,
      `input=${largeSha256Input || "synthetic"}`,
      `sha256=${expected}`,
      `execution=${result.execution}`,
    ].join(" "));
  } finally {
    runtime.destroy();
  }
}, 210_000);

test("large sha256 local gate: blocked pipe siblings do not starve hashing worker", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildLargeSha256Probe();
  const blockerBytes = buildPipeReadBlockerProbe();
  if (!probeBytes || !blockerBytes) {
    return;
  }
  const input = buildInput();
  const expected = createHash("sha256").update(Buffer.from(input)).digest("hex");
  const blockedSiblingCount = Number(process.env.TIDEMARK_TEST_LARGE_SHA256_BLOCKED_SIBLINGS ?? "2");

  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: 512 * 1024 * 1024,
    maxProcessMemoryPages: 32768,
  });
  const startedMs = Date.now();
  try {
    const snapshot = buildSnapshotFromSources([
      { kind: "inlineFile", guestPath: largeSha256Path, data: input, mode: 0o644 },
    ]);
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    for (let index = 0; index < blockedSiblingCount; index += 1) {
      await runtime.spawn(
        blockerBytes,
        ["/work/pipe-read-blocker"],
        ["LC_ALL=C", "LANG=C"],
        { cwd: "/", stdin: "pipe" },
      );
    }
    const preExecution = await waitForBlockedWorkerCount(runtime, blockedSiblingCount, 10_000);
    const spawnStartedMs = Date.now();
    const result = await spawnWithOutputAndStats(
      runtime,
      probeBytes,
      ["/work/large-sha256", largeSha256Path],
      ["LC_ALL=C", "LANG=C"],
      180_000,
      { cwd: "/", stdin: "closed" },
    );
    const spawnDurationMs = Date.now() - spawnStartedMs;
    expect(result.exitCode, result.output + (result.debugInfo ? `\n${result.debugInfo}` : "")).toBe(0);
    expect(result.output).toContain(`large-sha256 bytes=${input.length}`);
    expect(result.output).toContain(`sha256=${expected}`);
    console.log([
      `large sha256 blocked siblings gate durationMs=${Date.now() - startedMs}`,
      `spawnDurationMs=${spawnDurationMs}`,
      `blockedSiblingCount=${blockedSiblingCount}`,
      `bytes=${input.length}`,
      `input=${largeSha256Input || "synthetic"}`,
      `sha256=${expected}`,
      `preExecution=${preExecution}`,
      `execution=${result.execution}`,
    ].join(" "));
  } finally {
    runtime.destroy();
  }
}, 230_000);
