import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { expect, test } from "vite-plus/test";
import {
  Runtime,
  logSkip,
  readKernelWasm,
} from "../../../support/runtime-workload.js";
import { buildSnapshotFromSources } from "../../../support/snapshot-sources.js";

type RuntimeInstance = Awaited<ReturnType<typeof Runtime.create>>;

const devtoolsToolsetDir = process.env.TIDEMARK_TEST_DEVTOOLS_TOOLSET_DIR ?? "";
const packageCount = Number(process.env.TIDEMARK_TEST_LARGE_XZ_PACKAGE_COUNT ?? 20_000);
const packageXzInput = process.env.TIDEMARK_TEST_LARGE_XZ_INPUT ?? "";
const outputMode = process.env.TIDEMARK_TEST_LARGE_XZ_OUTPUT_MODE === "file" ? "file" : "stdout";
const packagesXzPath = "/work/Packages.xz";
const packagesOutputPath = "/work/Packages";

function buildPackagesCorpus(): { packages: Uint8Array; packagesXz: Uint8Array } | null {
  if (packageXzInput) {
    try {
      const packagesXz = new Uint8Array(readFileSync(packageXzInput));
      const packages = new Uint8Array(execFileSync("xz", ["-dc", packageXzInput], {
        maxBuffer: 256 * 1024 * 1024,
        stdio: "pipe",
      }));
      return { packages, packagesXz };
    } catch (error) {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      return logSkip(`failed to read large xz input ${packageXzInput}: ${detail}`);
    }
  }

  const tmp = mkdtempSync(join(tmpdir(), "tidemark-large-xz-"));
  try {
    const packagesHostPath = join(tmp, "Packages");
    const packageStanzas: string[] = [];
    for (let index = 0; index < Math.max(1, Math.trunc(packageCount)); index += 1) {
      packageStanzas.push([
        `Package: tidemark-local-${index}`,
        "Version: 1",
        "Architecture: riscv64",
        `Description: local xz gate ${index}`,
        "",
      ].join("\n"));
    }
    const packagesText = packageStanzas.join("");
    writeFileSync(packagesHostPath, packagesText);
    const packages = new TextEncoder().encode(packagesText);
    const packagesXz = new Uint8Array(execFileSync("xz", ["-zc", packagesHostPath], { stdio: "pipe" }));
    return { packages, packagesXz };
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build large xz corpus with host xz: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function checksum(bytes: Uint8Array): bigint {
  let sum = 0n;
  for (const byte of bytes) {
    sum += BigInt(byte);
  }
  return sum;
}

function appendTail(tail: Uint8Array, chunk: Uint8Array, limit: number): Uint8Array {
  if (chunk.byteLength >= limit) {
    return chunk.slice(chunk.byteLength - limit);
  }
  const combinedLength = tail.byteLength + chunk.byteLength;
  const combined = new Uint8Array(combinedLength);
  combined.set(tail);
  combined.set(chunk, tail.byteLength);
  return combinedLength <= limit ? combined : combined.slice(combinedLength - limit);
}

function concatChunks(chunks: Uint8Array[], byteLength: number): Uint8Array {
  const result = new Uint8Array(byteLength);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return result;
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
): Promise<{
  exitCode: number;
  output: string;
  outputByteLength: number;
  outputChecksum: bigint;
  outputPrefix: Uint8Array;
  outputTail: Uint8Array;
  debugInfo?: string;
  execution: string;
}> {
  const decoder = new TextDecoder();
  const proc = await runtime.spawn(new Uint8Array(elfBytes), argv, env, options);
  const outputPreviewLimit = 4096;
  const outputPrefixChunks: Uint8Array[] = [];
  let outputPrefixByteLength = 0;
  let outputTail = new Uint8Array();
  let outputByteLength = 0;
  let outputChecksum = 0n;
  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  let sampleId: ReturnType<typeof setInterval> | null = null;
  let execution = compactExecutionStats(runtime.debugExecutionStats());

  proc.onStdout((data) => {
    const chunk = new Uint8Array(data);
    outputByteLength += chunk.byteLength;
    outputTail = appendTail(outputTail, chunk, outputPreviewLimit);
    for (const byte of chunk) {
      outputChecksum += BigInt(byte);
    }
    if (outputPrefixByteLength < outputPreviewLimit) {
      const prefixBytes = Math.min(outputPreviewLimit - outputPrefixByteLength, chunk.byteLength);
      outputPrefixChunks.push(chunk.slice(0, prefixBytes));
      outputPrefixByteLength += prefixBytes;
    }
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
    const outputPrefix = concatChunks(outputPrefixChunks, outputPrefixByteLength);
    const output = [
      decoder.decode(outputPrefix),
      outputByteLength > outputPrefixByteLength ? `\n[omitted ${outputByteLength - outputPrefixByteLength} stdout bytes]\n` : "",
      outputByteLength > outputPrefixByteLength ? decoder.decode(outputTail) : "",
    ].join("");
    return {
      exitCode,
      output,
      outputByteLength,
      outputChecksum,
      outputPrefix,
      outputTail,
      debugInfo: proc.debugInfo?.(),
      execution,
    };
  } finally {
    if (timeoutId !== null) {
      clearTimeout(timeoutId);
    }
    if (sampleId !== null) {
      clearInterval(sampleId);
    }
  }
}

test("large xz local gate: guest xz decompresses package-index sized input", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const xzHostPath = resolve(devtoolsToolsetDir, "usr/bin/xz");
  const loaderHostPath = resolve(devtoolsToolsetDir, "usr/lib/ld-linux-riscv64-lp64d.so.1");
  const libRootHostPath = resolve(devtoolsToolsetDir, "usr/lib/riscv64-linux-gnu");
  if (!existsSync(xzHostPath) || !existsSync(loaderHostPath) || !existsSync(libRootHostPath)) {
    return logSkip(`missing devtools xz toolset: ${devtoolsToolsetDir}`);
  }
  const corpus = buildPackagesCorpus();
  if (!corpus) {
    return;
  }

  const snapshot = buildSnapshotFromSources([
    { kind: "file", hostPath: xzHostPath, guestPath: "/usr/bin/xz", mode: 0o755 },
    { kind: "file", hostPath: loaderHostPath, guestPath: "/lib/ld-linux-riscv64-lp64d.so.1", mode: 0o755 },
    { kind: "tree", hostPath: libRootHostPath, guestPath: "/usr/lib/riscv64-linux-gnu" },
    { kind: "inlineFile", guestPath: packagesXzPath, data: corpus.packagesXz, mode: 0o644 },
  ], { missingLabel: "large xz toolset" });
  if (snapshot.skipReason) {
    return logSkip(snapshot.skipReason);
  }

  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: 512 * 1024 * 1024,
    maxProcessMemoryPages: 32768,
  });
  const startedMs = Date.now();
  try {
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    const spawnStartedMs = Date.now();
    const argv = outputMode === "file"
      ? ["/usr/bin/xz", "-dk", packagesXzPath]
      : ["/usr/bin/xz", "-dc", packagesXzPath];
    const result = await spawnWithOutputAndStats(
      runtime,
      new Uint8Array(readFileSync(xzHostPath)),
      argv,
      ["PATH=/usr/bin:/bin", "HOME=/tmp", "LD_LIBRARY_PATH=/usr/lib/riscv64-linux-gnu:/lib"],
      180_000,
      { cwd: "/work", stdin: "closed" },
    );
    const spawnDurationMs = Date.now() - spawnStartedMs;
    expect(result.exitCode, result.output + (result.debugInfo ? `\n${result.debugInfo}` : "")).toBe(0);
    const fileOutputBytes = outputMode === "file"
      ? await runtime.readFile(packagesOutputPath)
      : null;
    const outputByteLength = fileOutputBytes?.length ?? result.outputByteLength;
    if (packageXzInput) {
      const outputPrefix = fileOutputBytes
        ? new TextDecoder().decode(fileOutputBytes.subarray(0, 128))
        : new TextDecoder().decode(result.outputPrefix);
      expect(outputPrefix).toContain("Package:");
    } else {
      expect(outputByteLength).toBe(corpus.packages.length);
      expect(fileOutputBytes ? checksum(fileOutputBytes) : result.outputChecksum).toBe(checksum(corpus.packages));
      const outputPrefix = fileOutputBytes
        ? new TextDecoder().decode(fileOutputBytes.subarray(0, 4096))
        : new TextDecoder().decode(result.outputPrefix);
      const outputTail = fileOutputBytes
        ? new TextDecoder().decode(fileOutputBytes.subarray(Math.max(0, fileOutputBytes.length - 4096)))
        : new TextDecoder().decode(result.outputTail);
      expect(outputPrefix).toContain("Package: tidemark-local-0\n");
      expect(outputTail).toContain(`Package: tidemark-local-${Math.max(1, Math.trunc(packageCount)) - 1}\n`);
    }
    console.log([
      `large xz gate durationMs=${Date.now() - startedMs}`,
      `spawnDurationMs=${spawnDurationMs}`,
      `outputMode=${outputMode}`,
      `packageCount=${Math.max(1, Math.trunc(packageCount))}`,
      `packagesBytes=${corpus.packages.length}`,
      `outputBytes=${outputByteLength}`,
      `packagesXzBytes=${corpus.packagesXz.length}`,
      `input=${packageXzInput || "synthetic"}`,
      `execution=${result.execution}`,
    ].join(" "));
  } finally {
    runtime.destroy();
  }
}, 210_000);
