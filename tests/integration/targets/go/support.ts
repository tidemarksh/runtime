import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { resolve } from "node:path";
import type { FsSnapshotEntry } from "../../../../src/fs/types.js";
import {
  Runtime,
  type RuntimeInstance,
  encoder,
  logSkip,
  readKernelWasm,
} from "../../../support/runtime-workload.js";

export const SAMPLE_MODULE_DIR = "/work/hello";
export const BUSYBOX_BINARY_PATH = process.env.TIDEMARK_TEST_BUSYBOX_BINARY ?? "";
export const GO_EXEC_CAPTURE_HELPER_PATH = process.env.TIDEMARK_TEST_GO_EXEC_CAPTURE_HELPER
  ?? resolve(__dirname, "./fixtures/exec-capture-helper.riscv64");
export const GO_TOOL_VERSION = process.env.TIDEMARK_TEST_GO_TOOL_VERSION ?? "go1.26.1";
export const GO_BUILD_EXPERIMENT = process.env.TIDEMARK_ACCEPTANCE_GO_EXPERIMENT ?? "";
export const GO_RUNTIME_RAM_SIZE = 1024 * 1024 * 1024;

const REQUIRED_GO_TOOL_RELATIVE_PATH = "pkg/tool/linux_riscv64/compile";

function hasRequiredGoToolchainBinary(candidate: string): boolean {
  return candidate !== "" && existsSync(resolve(candidate, REQUIRED_GO_TOOL_RELATIVE_PATH));
}

function resolveGoToolchainDir(): { dir: string; error: string | null } {
  const envDir = process.env.TIDEMARK_TEST_GO_TOOLCHAIN_DIR ?? "";
  if (envDir !== "") {
    return hasRequiredGoToolchainBinary(envDir)
      ? { dir: envDir, error: null }
      : {
        dir: "",
        error: `missing riscv64 Go tool binary: ${resolve(envDir, REQUIRED_GO_TOOL_RELATIVE_PATH)}`,
      };
  }

  return {
    dir: "",
    error: `missing Go toolchain dir: set TIDEMARK_TEST_GO_TOOLCHAIN_DIR with ${REQUIRED_GO_TOOL_RELATIVE_PATH}`,
  };
}

const GO_TOOLCHAIN_RESOLUTION = resolveGoToolchainDir();
export const GO_TOOLCHAIN_DIR = GO_TOOLCHAIN_RESOLUTION.dir;
export const GO_TOOLCHAIN_DIR_ERROR = GO_TOOLCHAIN_RESOLUTION.error;
export const GO_BINARY_PATH = process.env.TIDEMARK_TEST_GO_BINARY
  ?? (GO_TOOLCHAIN_DIR ? resolve(GO_TOOLCHAIN_DIR, "bin/go") : "");

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

export function buildGoRuntimeSnapshot(options: {
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
  setSnapshotFile(
    entries,
    "/usr/local/go/bin/go",
    new Uint8Array(readFileSync(GO_BINARY_PATH)),
    0o755,
  );
  setSnapshotFile(entries, "/usr/local/go/go.env", encoder.encode("GOTOOLCHAIN=local\n"), 0o644);

  if (includeToolchain) {
    if (!GO_TOOLCHAIN_DIR) {
      throw new Error(GO_TOOLCHAIN_DIR_ERROR ?? "missing Go toolchain dir");
    }
    appendHostDirSnapshot(entries, GO_TOOLCHAIN_DIR, "/usr/local/go");
    setSnapshotFile(
      entries,
      "/usr/local/go/bin/go",
      new Uint8Array(readFileSync(GO_BINARY_PATH)),
      0o755,
    );
    setSnapshotFile(entries, "/usr/local/go/go.env", encoder.encode("GOTOOLCHAIN=local\n"), 0o644);
  }

  if (BUSYBOX_BINARY_PATH && existsSync(BUSYBOX_BINARY_PATH)) {
    const busybox = new Uint8Array(readFileSync(BUSYBOX_BINARY_PATH));
    ensureSnapshotDir(entries, "/bin");
    setSnapshotFile(entries, "/bin/busybox", busybox, 0o755);
    setSnapshotFile(entries, "/bin/sh", busybox, 0o755);
  }

  ensureSnapshotDir(entries, "/root", 0o755);
  ensureSnapshotDir(entries, "/root/.config", 0o755);
  ensureSnapshotDir(entries, "/root/.config/go", 0o755);
  ensureSnapshotDir(entries, "/root/.config/go/telemetry", 0o755);
  setSnapshotFile(
    entries,
    "/root/.config/go/telemetry/mode",
    encoder.encode("off 2000-01-01\n"),
    0o644,
  );
  ensureSnapshotDir(entries, "/tmp", 0o777);
  ensureSnapshotDir(entries, "/tmp/gopath", 0o755);
  ensureSnapshotDir(entries, "/tmp/gocache", 0o755);
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

export async function prepareGoRuntime(options: {
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
  if (!GO_BINARY_PATH || !existsSync(GO_BINARY_PATH)) {
    return logSkip(`missing Go binary: ${GO_BINARY_PATH || "TIDEMARK_TEST_GO_BINARY"}`);
  }
  if (includeToolchain && !GO_TOOLCHAIN_DIR) {
    return logSkip(GO_TOOLCHAIN_DIR_ERROR ?? "missing Go toolchain dir");
  }

  const runtime = await Runtime.create({
    wasmBytes,
    ramSize: GO_RUNTIME_RAM_SIZE,
  });
  await runtime.applyFsSnapshot(buildGoRuntimeSnapshot({
    includeToolchain,
    includeSampleModule,
  }));

  return {
    runtime,
    goBytes: new Uint8Array(readFileSync(GO_BINARY_PATH)),
  };
}

export function goEnv(gomaxprocs: number): string[] {
  const env = [
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
  ];
  if (GO_BUILD_EXPERIMENT) {
    env.push(`GOEXPERIMENT=${GO_BUILD_EXPERIMENT}`);
  }
  if (process.env.TIDEMARK_ACCEPTANCE_GO_BUILD_GODEBUG) {
    env.push(`GODEBUG=${process.env.TIDEMARK_ACCEPTANCE_GO_BUILD_GODEBUG}`);
  }
  return env;
}

export function goVersionEnv(gomaxprocs: number): string[] {
  return [
    "GOROOT=/usr/local/go",
    `GOMAXPROCS=${gomaxprocs}`,
  ];
}

export function readGoExecCaptureHelperBinary(): Uint8Array | null {
  if (!existsSync(GO_EXEC_CAPTURE_HELPER_PATH)) {
    return logSkip(`missing Go exec capture helper: ${GO_EXEC_CAPTURE_HELPER_PATH}`);
  }
  return new Uint8Array(readFileSync(GO_EXEC_CAPTURE_HELPER_PATH));
}

export function readGoToolBinary(relativePath: string): Uint8Array | null {
  if (!GO_TOOLCHAIN_DIR) {
    return logSkip(GO_TOOLCHAIN_DIR_ERROR ?? "missing Go toolchain dir");
  }
  const absolutePath = resolve(GO_TOOLCHAIN_DIR, relativePath);
  if (!existsSync(absolutePath)) {
    return logSkip(`missing Go tool binary: ${absolutePath}`);
  }
  return new Uint8Array(readFileSync(absolutePath));
}
