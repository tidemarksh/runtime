import { afterAll, beforeAll, expect, test } from "vite-plus/test";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { resolve } from "node:path";
import { Worker } from "node:worker_threads";
import type { KernelExports } from "../src/abi.js";
import type { FsSnapshotEntry } from "../src/fs-types.js";
import type { Runtime as RuntimeClass } from "../src/index.js";

type RuntimeInstance = RuntimeClass;
type RuntimeProcess = Awaited<ReturnType<RuntimeInstance["spawn"]>>;
type SpawnOptions = Parameters<RuntimeInstance["spawn"]>[3];

const KERNEL_WASM_PATH = resolve(
  __dirname,
  "../../kernel/target/wasm32-unknown-unknown/release/tidemark_kernel.wasm",
);
const RUNTIME_DIST_PATH = resolve(__dirname, "../dist/index.mjs");
const GO_PATH = resolve(__dirname, "../../core/images/go");
const BUSYBOX_PATH = resolve(__dirname, "../../core/images/busybox");
const GO_TOOLCHAIN_DIR = resolve(__dirname, "../../core/images/go-toolchain");
const SAMPLE_MODULE_DIR = "/work/hello";
const encoder = new TextEncoder();
const decoder = new TextDecoder();

const originalWorker = globalThis.Worker;
let Runtime: typeof import("../dist/index.mjs").Runtime;

beforeAll(async () => {
  globalThis.Worker = Worker as unknown as typeof globalThis.Worker;
  ({ Runtime } = await import(RUNTIME_DIST_PATH));
});

afterAll(() => {
  globalThis.Worker = originalWorker;
});

function logSkip(reason: string): null {
  console.log(`skip runtime integration: ${reason}`);
  return null;
}

function readKernelWasm(): Uint8Array | null {
  if (!existsSync(KERNEL_WASM_PATH)) {
    return logSkip("kernel release wasm not found");
  }
  return new Uint8Array(readFileSync(KERNEL_WASM_PATH));
}

function findRiscvElf(name: string): Uint8Array | null {
  const buildDir = resolve(
    __dirname,
    "../../kernel/target/wasm32-unknown-unknown/release/build",
  );
  if (!existsSync(buildDir)) {
    return logSkip("kernel release build outputs not found");
  }

  for (const entry of readdirSync(buildDir)) {
    const elfPath = resolve(buildDir, entry, "out/rv64ui", `${name}.elf`);
    if (existsSync(elfPath)) {
      return new Uint8Array(readFileSync(elfPath));
    }
  }
  return logSkip(`riscv test ELF not found: ${name}`);
}

function normalizeGuestPath(path: string): string {
  const normalized = `/${path.split("/").filter(Boolean).join("/")}`;
  return normalized || "/";
}

function ensureSnapshotDir(
  entries: Map<string, FsSnapshotEntry>,
  path: string,
  mode = 0o755,
): void {
  const normalized = normalizeGuestPath(path);
  if (normalized !== "/") {
    const parent = normalized.split("/").slice(0, -1).join("/") || "/";
    ensureSnapshotDir(entries, parent, 0o755);
  }
  if (!entries.has(normalized)) {
    entries.set(normalized, { kind: "dir", path: normalized, mode });
  }
}

function setSnapshotFile(
  entries: Map<string, FsSnapshotEntry>,
  path: string,
  data: Uint8Array,
  mode = 0o644,
): void {
  const normalized = normalizeGuestPath(path);
  ensureSnapshotDir(entries, normalized.split("/").slice(0, -1).join("/") || "/", 0o755);
  entries.set(normalized, {
    kind: "file",
    path: normalized,
    data,
    mode,
  });
}

function appendHostDirSnapshot(
  entries: Map<string, FsSnapshotEntry>,
  hostDir: string,
  guestDir: string,
): void {
  ensureSnapshotDir(entries, guestDir, 0o755);

  for (const entry of readdirSync(hostDir, { withFileTypes: true })) {
    const hostPath = resolve(hostDir, entry.name);
    const guestPath = `${guestDir}/${entry.name}`;
    if (entry.isSymbolicLink()) {
      continue;
    }
    if (entry.isDirectory()) {
      appendHostDirSnapshot(entries, hostPath, guestPath);
      continue;
    }
    const st = statSync(hostPath);
    const mode = (st.mode & 0o111) !== 0 ? 0o755 : 0o644;
    setSnapshotFile(entries, guestPath, new Uint8Array(readFileSync(hostPath)), mode);
  }
}

function buildGoRuntimeSnapshot(options: {
  includeToolchain?: boolean;
  includeSampleModule?: boolean;
} = {}): FsSnapshotEntry[] {
  const {
    includeToolchain = true,
    includeSampleModule = true,
  } = options;

  const entries = new Map<string, FsSnapshotEntry>();
  ensureSnapshotDir(entries, "/");
  ensureSnapshotDir(entries, "/usr");
  ensureSnapshotDir(entries, "/usr/local");
  ensureSnapshotDir(entries, "/usr/local/go");
  ensureSnapshotDir(entries, "/usr/local/go/bin");
  ensureSnapshotDir(entries, "/usr/local/go/pkg");
  ensureSnapshotDir(entries, "/usr/local/go/pkg/tool");
  ensureSnapshotDir(entries, "/usr/local/go/pkg/tool/linux_riscv64");
  setSnapshotFile(entries, "/usr/local/go/bin/go", new Uint8Array(readFileSync(GO_PATH)), 0o755);
  setSnapshotFile(entries, "/usr/local/go/go.env", encoder.encode("GOTOOLCHAIN=local\n"), 0o644);

  if (includeToolchain) {
    appendHostDirSnapshot(entries, GO_TOOLCHAIN_DIR, "/usr/local/go");
    setSnapshotFile(entries, "/usr/local/go/bin/go", new Uint8Array(readFileSync(GO_PATH)), 0o755);
    setSnapshotFile(entries, "/usr/local/go/go.env", encoder.encode("GOTOOLCHAIN=local\n"), 0o644);
  }

  if (existsSync(BUSYBOX_PATH)) {
    const busybox = new Uint8Array(readFileSync(BUSYBOX_PATH));
    ensureSnapshotDir(entries, "/bin");
    setSnapshotFile(entries, "/bin/busybox", busybox, 0o755);
    setSnapshotFile(entries, "/bin/sh", busybox, 0o755);
  }

  ensureSnapshotDir(entries, "/tmp", 0o777);
  ensureSnapshotDir(entries, "/work");
  if (includeSampleModule) {
    ensureSnapshotDir(entries, SAMPLE_MODULE_DIR);
    setSnapshotFile(
      entries,
      `${SAMPLE_MODULE_DIR}/main.go`,
      encoder.encode('package main\nfunc main() { println("Hello from Go!") }\n'),
      0o644,
    );
    setSnapshotFile(
      entries,
      `${SAMPLE_MODULE_DIR}/go.mod`,
      encoder.encode("module hello\ngo 1.21\n"),
      0o644,
    );
  }

  return [...entries.values()];
}

async function spawnWithOutput(
  runtime: RuntimeInstance,
  elfBytes: Uint8Array,
  argv: string[],
  env: string[],
  timeoutMs: number,
  options?: SpawnOptions,
): Promise<{ exitCode: number; output: string; debugInfo?: string }> {
  const proc = await runtime.spawn(new Uint8Array(elfBytes), argv, env, options);
  let output = "";
  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  proc.onStdout((data) => {
    output += decoder.decode(data);
  });

  try {
    const exitCode = await Promise.race([
      proc.waitForExit(),
      new Promise<never>((_, reject) => {
        timeoutId = setTimeout(() => reject(new Error(`timeout after ${timeoutMs}ms`)), timeoutMs);
        if (typeof timeoutId === "object" && timeoutId !== null && "unref" in timeoutId) {
          timeoutId.unref();
        }
      }),
    ]);
    return { exitCode, output, debugInfo: proc.debugInfo?.() };
  } finally {
    if (timeoutId !== null) {
      clearTimeout(timeoutId);
    }
  }
}

type RuntimeDebugHandle = {
  blockedWorkers?: Map<unknown, { vmPtr: number; status: number }>;
  kernel: KernelExports;
  stdinPipe?: number;
  stdoutPipe?: number;
  stderrPipe?: number;
  vmPtr?: number;
  lastDebugMessage?: string;
};

type RuntimeDebugView = RuntimeInstance & {
  processes?: Map<number, RuntimeDebugHandle>;
  sendKernelRequest?: (message: { type: "debug-processes" }) => Promise<{
    pids: number[];
    canonicalPids: number[];
  }>;
  debugSnapshot?: () => string;
};

async function runtimeDebugSummary(runtime: RuntimeInstance): Promise<string> {
  const runtimeView = runtime as RuntimeDebugView;
  const processes = runtimeView.processes;
  const localSummary = !(processes instanceof Map) || processes.size === 0
    ? ""
    : [...processes.entries()]
      .map(([pid, handle]) => {
        const blocked = handle?.blockedWorkers instanceof Map
          ? [...handle.blockedWorkers.values()].map((state) => {
              const vmPtr = state.vmPtr;
              const fd = handle.kernel.thread_blocked_fd?.(vmPtr) ?? -1;
              const ofdId = handle.kernel.thread_blocked_ofd_id?.(vmPtr) ?? 0;
              const ofdType = handle.kernel.kernel_ofd_fd_type?.(ofdId) ?? 0;
              const ofdIno = handle.kernel.kernel_ofd_ino?.(ofdId) ?? 0;
              return `${state.status}:vm=0x${vmPtr.toString(16)}:fd=${fd}:ofd=${ofdId}:type=${ofdType}:ino=${ofdIno}`;
            }).join(",")
          : "";
        const debug = handle?.lastDebugMessage ?? "n/a";
        const stdinPipe = handle?.stdinPipe ?? 0;
        const stdoutPipe = handle?.stdoutPipe ?? 0;
        const stderrPipe = handle?.stderrPipe ?? 0;
        const vmPtr = handle?.vmPtr ?? 0;
        const fdSummary = vmPtr !== 0
          ? Array.from({ length: 16 }, (_, fd) => {
              const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
              if (ofdId === 0) {
                return null;
              }
              const type = handle.kernel.kernel_ofd_fd_type(ofdId);
              const ino = handle.kernel.kernel_ofd_ino(ofdId);
              const hostFd = handle.kernel.kernel_ofd_host_fd(ofdId);
              return `${fd}->${ofdId}/${type}/${ino}/${hostFd}`;
            }).filter(Boolean).join(" ")
          : "";
        const ofdSummary = Array.from({ length: 16 }, (_, ofdId) => {
          if (ofdId === 0) {
            return null;
          }
          const type = handle.kernel.kernel_ofd_fd_type(ofdId);
          if (type === 0) {
            return null;
          }
          const ino = handle.kernel.kernel_ofd_ino(ofdId);
          const hostFd = handle.kernel.kernel_ofd_host_fd(ofdId);
          const refcount = handle.kernel.kernel_ofd_refcount(ofdId);
          return `${ofdId}:${type}/${ino}/${hostFd}/rc=${refcount}`;
        }).filter(Boolean).join(" ");
        return `pid=${pid} stdin=${stdinPipe} stdout=${stdoutPipe} stderr=${stderrPipe} blocked=[${blocked}] fds=[${fdSummary}] ofds=[${ofdSummary}] debug=${debug}`;
      })
      .join("\n");

  const kernelSummary = await runtimeView.sendKernelRequest?.({
    type: "debug-processes",
  }).then((response) => (
    `kernel_pids=[${response.pids.join(",")}] canonical_pids=[${response.canonicalPids.join(",")}]`
  )).catch(() => "");

  const internalSummary = runtimeView.debugSnapshot?.() ?? "";

  return [
    localSummary ? `local:\n${localSummary}` : "",
    internalSummary ? `internal:\n${internalSummary}` : "",
    kernelSummary,
  ].filter(Boolean).join("\n");
}

async function prepareGoRuntime(options: {
  includeToolchain?: boolean;
  includeSampleModule?: boolean;
} = {}): Promise<{ runtime: RuntimeInstance; goBytes: Uint8Array } | null> {
  const {
    includeToolchain = true,
    includeSampleModule = true,
  } = options;
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return null;
  }
  if (!existsSync(GO_PATH)) {
    return logSkip(`missing Go binary: ${GO_PATH}`);
  }
  if (includeToolchain && !existsSync(GO_TOOLCHAIN_DIR)) {
    return logSkip(`missing Go toolchain dir: ${GO_TOOLCHAIN_DIR}`);
  }

  const runtime = await Runtime.create({
    wasmBytes,
    ramSize: 512 * 1024 * 1024,
  });
  await runtime.applyFsSnapshot(buildGoRuntimeSnapshot({
    includeToolchain,
    includeSampleModule,
  }));

  return {
    runtime,
    goBytes: new Uint8Array(readFileSync(GO_PATH)),
  };
}

function goEnv(gomaxprocs: number): string[] {
  return [
    "GOROOT=/usr/local/go",
    "GOPATH=/tmp/gopath",
    "GOCACHE=/tmp/gocache",
    "PATH=/usr/local/go/bin:/bin",
    `GOMAXPROCS=${gomaxprocs}`,
    "GOTELEMETRY=off",
    "GOTELEMETRYDIR=/dev/null",
    "GONOSUMDB=*",
    "GOFLAGS=-mod=mod",
    "HOME=/root",
    "GOGC=off",
    "GOEXPERIMENT=nodwarf5",
  ];
}

function goVersionEnv(gomaxprocs: number): string[] {
  return [
    "GOROOT=/usr/local/go",
    `GOMAXPROCS=${gomaxprocs}`,
  ];
}

test("Runtime.spawn preserves seeded files in process MemFS", async () => {
  const wasmBytes = readKernelWasm();
  const elfData = findRiscvElf("add");
  if (!wasmBytes || !elfData) {
    return;
  }

  let runtime: RuntimeInstance | null = null;

  try {
    runtime = await Runtime.create({
      wasmBytes,
      ramSize: 64 * 1024 * 1024,
    });

    await runtime.fs.mkdir("/work", 0o755);
    await runtime.fs.mkdir("/work/hello", 0o755);
    await runtime.fs.writeFile(
      "/work/hello/module.txt",
      new TextEncoder().encode("hello from tidemark\n"),
      0o644,
    );

    const proc = await runtime.spawn(elfData, ["add"], [], {
      cwd: "/work/hello",
    });
    await expect(proc.waitForExit()).resolves.toBe(0);

    const handle = (proc as unknown as { handle: {
      kernel: KernelExports;
      memory: WebAssembly.Memory;
      guestBase: number;
      vmPtr: number;
    } }).handle;
    const encoder = new TextEncoder();
    const scratch = handle.kernel.kernel_heap_ptr() + 0x30000;
    const pathBytes = encoder.encode("/work/hello/module.txt");
    new Uint8Array(handle.memory.buffer).set(pathBytes, scratch);

    const ino = handle.kernel.memfs_resolve_path(scratch, pathBytes.length);
    const cwdBytes = encoder.encode("/work/hello");
    new Uint8Array(handle.memory.buffer).set(cwdBytes, scratch);
    const cwdIno = handle.kernel.memfs_resolve_path(scratch, cwdBytes.length);
    expect(ino).toBeGreaterThan(0);
    expect(cwdIno).toBeGreaterThan(0);
    expect(handle.kernel.process_cwd_ino(handle.vmPtr)).toBe(cwdIno);
    expect(handle.guestBase).toBeGreaterThan(handle.kernel.kernel_heap_ptr());
  } finally {
    runtime?.destroy();
  }
});

for (const name of ["add", "addi", "beq", "jal", "ld_st", "simple"]) {
  test(`Runtime executes bundled riscv test ELF: ${name}`, async () => {
    const wasmBytes = readKernelWasm();
    const elfBytes = findRiscvElf(name);
    if (!wasmBytes || !elfBytes) {
      return;
    }

    const runtime = await Runtime.create({ wasmBytes });
    try {
      const { exitCode } = await spawnWithOutput(
        runtime,
        elfBytes,
        [name],
        [],
        5_000,
      );
      expect(exitCode).toBe(0);
    } finally {
      runtime.destroy();
    }
  }, 30_000);
}

for (const gomaxprocs of [1, 2]) {
  test(`Runtime executes go version with GOMAXPROCS=${gomaxprocs}`, async () => {
    const includeToolchain = process.env.TIDEMARK_ACCEPTANCE_FULL_GOROOT === "1";
    const prepared = await prepareGoRuntime({
      includeToolchain,
      includeSampleModule: false,
    });
    if (!prepared) {
      return;
    }

    const { runtime, goBytes } = prepared;
    try {
      const { exitCode, output, debugInfo } = await spawnWithOutput(
        runtime,
        goBytes,
        ["go", "version"],
        goVersionEnv(gomaxprocs),
        120_000,
      );
      expect(exitCode, `${output}\n${debugInfo ?? ""}`).toBe(0);
      expect(output).toContain("go version");
    } finally {
      runtime.destroy();
    }
  }, 150_000);
}

test("Runtime go build makes bounded compile progress", async () => {
  const gomaxprocs = Number(process.env.TIDEMARK_ACCEPTANCE_GO_BUILD_GOMAXPROCS ?? "1");
  const timeoutMs = Number(process.env.TIDEMARK_ACCEPTANCE_GO_BUILD_TIMEOUT_MS ?? "120000");
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: true,
  });
  if (!prepared) {
    return;
  }

  const { runtime, goBytes } = prepared;
  try {
    const proc: RuntimeProcess = await runtime.spawn(
      goBytes,
      ["go", "build", "-x", "-v", "-o", `${SAMPLE_MODULE_DIR}/hello`, "."],
      goEnv(gomaxprocs),
      { cwd: SAMPLE_MODULE_DIR },
    );
    let output = "";
    let partialLine = "";
    let timeoutId: ReturnType<typeof setTimeout> | null = null;
    let timedOut = false;
    let compiledPackages = 0;
    let buildCommands = 0;

    proc.onStdout((data) => {
      const text = decoder.decode(data);
      output += text;
      partialLine += text;

      const lines = partialLine.split("\n");
      partialLine = lines.pop() ?? "";
      for (const line of lines) {
        if (line.startsWith("internal/") || line.startsWith("runtime")) {
          compiledPackages += 1;
        }
        if (
          line.startsWith("WORK=")
          || line.includes("/pkg/tool/linux_riscv64/compile ")
          || line.includes("/pkg/tool/linux_riscv64/link ")
        ) {
          buildCommands += 1;
        }
      }
    });

    const exitCode = await Promise.race([
      proc.waitForExit().catch(async (error) => {
        const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
        throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
      }),
      new Promise<null>((resolve) => {
        timeoutId = setTimeout(() => {
          timedOut = true;
          resolve(null);
        }, timeoutMs);
        if (typeof timeoutId === "object" && timeoutId !== null && "unref" in timeoutId) {
          timeoutId.unref();
        }
      }),
    ]);

    if (timeoutId !== null) {
      clearTimeout(timeoutId);
    }

    if (timedOut) {
      expect(
        compiledPackages > 0 || buildCommands > 0,
        `go build timed out without compile progress\n${output}\n${proc.debugInfo?.() ?? ""}\n${await runtimeDebugSummary(runtime)}`,
      ).toBe(true);
      return;
    }

    expect(
      exitCode,
      `go build exited with ${exitCode}\n${output}\n${proc.debugInfo?.() ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(compiledPackages > 0 || buildCommands > 0 || output.includes("runtime")).toBe(true);
  } finally {
    runtime.destroy();
  }
}, Number(process.env.TIDEMARK_ACCEPTANCE_GO_BUILD_TIMEOUT_MS ?? "120000") + 60_000);
