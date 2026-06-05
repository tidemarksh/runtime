import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vite-plus/test";
import { runBrowserRuntimeScenario } from "../../../support/browser-runtime-harness.js";
import { logSkip } from "../../../support/runtime-workload.js";

const largeFilePath = "/work/browser-opfs-demand-fill.bin";
const hostRiscvGcc = process.env.TIDEMARK_TEST_HOST_RISCV_GCC ?? "riscv64-linux-gnu-gcc";

interface BrowserPageCacheDebugRecord {
  path: string;
  ino: number;
  dataOff: number;
  size: number;
  mode: number;
  backingBytes?: number;
  residentPages?: number;
}

interface BrowserOpfsDemandFillResult {
  exitCode: number;
  error: string | null;
  guestOutput: string;
  guestDebugInfo: string | null;
  executionStats: string | null;
  before: BrowserPageCacheDebugRecord | null;
  afterGuest: BrowserPageCacheDebugRecord | null;
  expectedBytes: number;
  expectedChecksum: string;
  opfsAvailable: boolean;
}

function buildGuestReadProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-browser-opfs-read-"));
  const sourcePath = join(tmp, "browser_opfs_read.c");
  const outputPath = join(tmp, "browser-opfs-read");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <stdio.h>",
      "#include <unistd.h>",
      "",
      "#define CHUNK_SIZE 4096",
      "",
      "int main(int argc, char **argv) {",
      `  const char *path = argc > 1 ? argv[1] : "${largeFilePath}";`,
      "  int fd = open(path, O_RDONLY);",
      "  if (fd < 0) { perror(\"open\"); return 20; }",
      "  unsigned char buf[CHUNK_SIZE];",
      "  unsigned long long total = 0;",
      "  unsigned long long checksum = 0;",
      "  for (;;) {",
      "    ssize_t n = read(fd, buf, sizeof(buf));",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      perror(\"read\");",
      "      close(fd);",
      "      return 21;",
      "    }",
      "    if (n == 0) break;",
      "    for (ssize_t i = 0; i < n; i++) checksum += (unsigned long long)buf[i];",
      "    total += (unsigned long long)n;",
      "  }",
      "  if (close(fd) != 0) { perror(\"close\"); return 22; }",
      "  printf(\"browser-opfs-guest-read bytes=%llu checksum=%llu\\n\", total, checksum);",
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
    return logSkip(`failed to build browser OPFS read probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildInput(length = 8192): Uint8Array {
  const data = new Uint8Array(length);
  for (let offset = 0; offset < data.length; offset += 1) {
    data[offset] = (offset * 31 + 11) & 0xff;
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

function browserOpfsDemandFillModuleSource(): string {
  return [
    "import { Runtime } from '/runtime/index.mjs';",
    "const decoder = new TextDecoder();",
    "function decodeBase64(value) {",
    "  const raw = atob(value);",
    "  const bytes = new Uint8Array(raw.length);",
    "  for (let index = 0; index < raw.length; index += 1) bytes[index] = raw.charCodeAt(index);",
    "  return bytes;",
    "}",
    "function stringifyDebug(value) {",
    "  return JSON.stringify(value, (_key, item) => typeof item === 'bigint' ? item.toString() : item);",
    "}",
    "async function spawnGuestRead(runtime, elfBytes, path) {",
    "  const proc = await runtime.spawn(elfBytes, ['/work/browser-opfs-read', path], ['LC_ALL=C', 'LANG=C'], { cwd: '/', stdin: 'closed' });",
    "  let output = '';",
    "  proc.onStdout((data) => { output += decoder.decode(data); });",
    "  proc.onStderr?.((data) => { output += decoder.decode(data); });",
    "  let timeoutId = null;",
    "  try {",
    "    const exitCode = await Promise.race([",
    "      proc.waitForExit(),",
    "      new Promise((resolve) => { timeoutId = setTimeout(() => resolve(-1000), 90000); }),",
    "    ]);",
    "    return {",
    "      exitCode,",
    "      output,",
    "      debugInfo: typeof proc.debugInfo === 'function' ? proc.debugInfo() : null,",
    "      executionStats: typeof runtime.debugExecutionStats === 'function' ? stringifyDebug(runtime.debugExecutionStats()) : null,",
    "    };",
    "  } finally {",
    "    if (timeoutId !== null) clearTimeout(timeoutId);",
    "  }",
    "}",
    "async function boot() {",
    "  const [configResponse, wasmResponse] = await Promise.all([fetch('/config.json'), fetch('/kernel.wasm')]);",
    "  if (!configResponse.ok) throw new Error(`config fetch failed: ${configResponse.status}`);",
    "  if (!wasmResponse.ok) throw new Error(`kernel fetch failed: ${wasmResponse.status}`);",
    "  const config = await configResponse.json();",
    "  const wasmBytes = await wasmResponse.arrayBuffer();",
    "  window.__tidemarkBrowserRuntime = {",
    "    async run() {",
    "      const opfsAvailable = typeof navigator?.storage?.getDirectory === 'function';",
    "      if (!opfsAvailable) {",
    "        return {",
    "          exitCode: -1,",
    "          error: 'OPFS unavailable',",
    "          guestOutput: '',",
    "          guestDebugInfo: null,",
    "          executionStats: null,",
    "          before: null,",
    "          afterGuest: null,",
    "          expectedBytes: config.inputLength,",
    "          expectedChecksum: config.expectedChecksum,",
    "          opfsAvailable,",
    "        };",
    "      }",
    "      const input = decodeBase64(config.inputBase64);",
    "      const elfBytes = decodeBase64(config.elfBase64);",
    "      const prefix = decodeBase64(config.prefixBase64);",
    "      const suffix = decodeBase64(config.suffixBase64);",
    "      const source = new Uint8Array(prefix.length + input.length + suffix.length);",
    "      source.set(prefix, 0);",
    "      source.set(input, prefix.length);",
    "      source.set(suffix, prefix.length + input.length);",
    "      const root = await navigator.storage.getDirectory();",
    "      const fileName = `tidemark-opfs-demand-fill-${crypto.randomUUID()}.bin`;",
    "      const handle = await root.getFileHandle(fileName, { create: true });",
    "      const writable = await handle.createWritable();",
    "      await writable.write(source);",
    "      await writable.close();",
    "      const runtime = await Runtime.create({",
    "        wasmBytes,",
    "        guestMemoryPolicyLimitBytes: 256 * 1024 * 1024,",
    "        kernelWorkerMaxMemoryPages: 7168,",
    "        maxProcessMemoryPages: 16384,",
    "        maxForkMemoryPages: 16384,",
    "      });",
    "      try {",
    "        await runtime.applyFsSnapshot([",
    "          { kind: 'dir', path: '/', mode: 0o755 },",
    "          { kind: 'dir', path: '/work', mode: 0o755 },",
    "          { kind: 'file', path: '/work/browser-opfs-read', data: elfBytes, mode: 0o755 },",
    "          {",
    "            kind: 'file',",
    `            path: '${largeFilePath}',`,
    "            size: input.length,",
    "            mode: 0o644,",
    "            backingExtent: {",
    "              sourceIdentity: config.sourceIdentity,",
    "              offset: prefix.length,",
    "              length: input.length,",
    "            },",
    "          },",
    "        ]);",
    "        await runtime.registerFsOpfsBackingSource(config.sourceIdentity, source.length, handle);",
    `        const before = await runtime.debugPageCachePath('${largeFilePath}');`,
    "        const guestElf = await runtime.readFile('/work/browser-opfs-read');",
    `        const guest = await spawnGuestRead(runtime, guestElf, '${largeFilePath}');`,
    `        const afterGuest = await runtime.debugPageCachePath('${largeFilePath}');`,
    "        return {",
    "          exitCode: guest.exitCode,",
    "          error: null,",
    "          guestOutput: guest.output,",
    "          guestDebugInfo: guest.debugInfo,",
    "          executionStats: guest.executionStats,",
    "          before,",
    "          afterGuest,",
    "          expectedBytes: config.inputLength,",
    "          expectedChecksum: config.expectedChecksum,",
    "          opfsAvailable,",
    "        };",
    "      } finally {",
    "        runtime.destroy();",
    "        await root.removeEntry(fileName).catch(() => {});",
    "      }",
    "    },",
    "  };",
    "  window.__tidemarkBrowserRuntimeReady = true;",
    "  document.getElementById('app').textContent = 'ready';",
    "}",
    "boot().catch((error) => {",
    "  window.__tidemarkBrowserRuntimeBootError = error instanceof Error ? (error.stack ?? error.message) : String(error);",
    "  document.getElementById('app').textContent = `boot failed: ${window.__tidemarkBrowserRuntimeBootError}`;",
    "});",
  ].join("\n");
}

test("browser OPFS demand fill witness: guest read fills metadata-only backing extent", async () => {
  const probeBytes = buildGuestReadProbe();
  if (!probeBytes) {
    return;
  }
  const input = buildInput();
  const expected = checksum(input);
  const prefix = new TextEncoder().encode("browser-opfs-prefix:");
  const suffix = new TextEncoder().encode(":browser-opfs-suffix");
  const expectedText = expected.toString();
  const result = await runBrowserRuntimeScenario<BrowserOpfsDemandFillResult>({
    timeoutMs: 90_000,
    moduleSource: browserOpfsDemandFillModuleSource(),
    config: {
      sourceIdentity: "opfs:browser-demand-fill.bin",
      inputLength: input.length,
      expectedChecksum: expectedText,
      elfBase64: Buffer.from(probeBytes).toString("base64"),
      inputBase64: Buffer.from(input).toString("base64"),
      prefixBase64: Buffer.from(prefix).toString("base64"),
      suffixBase64: Buffer.from(suffix).toString("base64"),
    },
  });

  expect(result, "browser runtime scenario should run").not.toBeNull();
  const resultDetail = JSON.stringify(result);
  expect(result?.opfsAvailable, resultDetail).toBe(true);
  expect(result?.exitCode, resultDetail).toBe(0);
  expect(result?.guestOutput, result?.guestDebugInfo ?? resultDetail).toContain(`browser-opfs-guest-read bytes=${input.length}`);
  expect(result?.guestOutput, result?.guestDebugInfo ?? resultDetail).toContain(`checksum=${expectedText}`);

  expect(result?.before, "metadata-only OPFS input should publish inode metadata").not.toBeNull();
  expect(result?.before?.dataOff, "OPFS input must not be inline-published before guest read").toBe(0);
  expect(result?.before?.backingBytes, "OPFS source should not be materialized before first read").toBeUndefined();
  expect(result?.before?.residentPages ?? 0).toBe(0);

  expect(result?.afterGuest?.dataOff, "OPFS guest demand fill must keep file data out of inline SAB inode data").toBe(0);
  expect(result?.afterGuest?.backingBytes, "OPFS guest demand fill must not materialize the whole backing file").toBeUndefined();
  expect(result?.afterGuest?.residentPages ?? 0).toBeGreaterThanOrEqual(2);
}, 120_000);
