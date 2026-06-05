import { expect, test } from "vite-plus/test";
import { runBrowserRuntimeScenario } from "../../../support/browser-runtime-harness.js";

const largeFilePath = "/work/browser-opfs-demand-fill.bin";

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
  readBytes: number;
  readChecksum: string;
  before: BrowserPageCacheDebugRecord | null;
  after: BrowserPageCacheDebugRecord | null;
  expectedBytes: number;
  expectedChecksum: string;
  opfsAvailable: boolean;
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
    "function checksum(bytes) {",
    "  let sum = 0n;",
    "  for (const byte of bytes) sum += BigInt(byte);",
    "  return sum.toString();",
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
    "          readBytes: 0,",
    "          readChecksum: '0',",
    "          before: null,",
    "          after: null,",
    "          expectedBytes: config.inputLength,",
    "          expectedChecksum: config.expectedChecksum,",
    "          opfsAvailable,",
    "        };",
    "      }",
    "      const input = decodeBase64(config.inputBase64);",
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
    `        const readData = await runtime.fs.readFile('${largeFilePath}');`,
    `        const after = await runtime.debugPageCachePath('${largeFilePath}');`,
    "        return {",
    "          exitCode: 0,",
    "          error: null,",
    "          readBytes: readData.length,",
    "          readChecksum: checksum(readData),",
    "          before,",
    "          after,",
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

test("browser OPFS demand fill witness: Runtime.fs.readFile fills metadata-only backing extent", async () => {
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
      inputBase64: Buffer.from(input).toString("base64"),
      prefixBase64: Buffer.from(prefix).toString("base64"),
      suffixBase64: Buffer.from(suffix).toString("base64"),
    },
  });

  expect(result, "browser runtime scenario should run").not.toBeNull();
  const resultDetail = JSON.stringify(result);
  expect(result?.opfsAvailable, resultDetail).toBe(true);
  expect(result?.exitCode, resultDetail).toBe(0);
  expect(result?.readBytes).toBe(input.length);
  expect(result?.readChecksum).toBe(expectedText);

  expect(result?.before, "metadata-only OPFS input should publish inode metadata").not.toBeNull();
  expect(result?.before?.dataOff, "OPFS input must not be inline-published before guest read").toBe(0);
  expect(result?.before?.backingBytes, "OPFS source should not be materialized before first read").toBeUndefined();
  expect(result?.before?.residentPages ?? 0).toBe(0);

  expect(result?.after?.dataOff, "OPFS demand fill must keep file data out of inline SAB inode data").toBe(0);
  expect(result?.after?.backingBytes, "OPFS readFile should materialize only the file extent").toBe(input.length);
  expect(result?.after?.residentPages ?? 0).toBe(0);
}, 120_000);
