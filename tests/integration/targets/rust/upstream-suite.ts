import { resolve } from "node:path";
import type { SnapshotSource } from "../../../support/snapshot-sources.js";
import type { ToolchainExecEntry } from "../../../support/toolchain-exec-harness.js";
import { rustCLinkerSnapshotSources, rustToolchainSnapshotSources } from "./toolchain-layout.js";

const rustToolchainDir = process.env.TIDEMARK_TEST_RUST_TOOLCHAIN_DIR ?? "";
const rustUpstreamSuiteDir = process.env.TIDEMARK_TEST_RUST_UPSTREAM_SUITE_DIR ?? "";
const cToolchainDir = process.env.TIDEMARK_TEST_C_TOOLCHAIN_DIR ?? "";
const glibcSysrootDir = process.env.TIDEMARK_TEST_GLIBC_SYSROOT_DIR ?? "";
const rustUpstreamGuestMemoryPolicyBytes = Number(
  process.env.TIDEMARK_TEST_RUST_UPSTREAM_GUEST_MEMORY_POLICY_BYTES ?? 1024 * 1024 * 1024,
);
const rustUpstreamTimeoutMs = Number(process.env.TIDEMARK_TEST_RUST_UPSTREAM_TIMEOUT_MS ?? 120_000);

const GLIBC_RUNTIME_LIBS = [
  "ld-linux-riscv64-lp64d.so.1",
  "libc.so.6",
  "libdl.so.2",
  "libpthread.so.0",
  "libm.so.6",
  "librt.so.1",
  "libutil.so.1",
  "libgcc_s.so.1",
] as const;

function rustUpstreamSnapshotSources(extra: readonly SnapshotSource[] = []): SnapshotSource[] {
  return [
    ...rustToolchainSnapshotSources(rustToolchainDir),
    ...rustCLinkerSnapshotSources(cToolchainDir),
    { kind: "tree", hostPath: resolve(rustUpstreamSuiteDir, "rust-src"), guestPath: "/rust-src" },
    ...GLIBC_RUNTIME_LIBS.map((name) => ({
      kind: "file" as const,
      hostPath: resolve(glibcSysrootDir, name),
      guestPath: `/lib/${name}`,
      mode: 0o755,
    })),
    ...extra,
  ];
}

function rustStdSuiteEntry(
  id: string,
  testName: string,
  timeoutMs: number,
): ToolchainExecEntry {
  return {
    id,
    targetClass: "Rust toolchain",
    workflowKind: "upstream suite invocation",
    hostExecutablePath: resolve(rustToolchainDir, "bin/cargo"),
    guestExecutablePath: "/toolchain/bin/cargo",
    argv: ["/toolchain/bin/cargo", "test", "-p", "std", "--test", testName],
    env: [
      "PATH=/toolchain/bin:/usr/bin:/bin",
      "HOME=/tmp",
      "CARGO_HOME=/tmp/cargo-home",
      "CARGO_TARGET_DIR=/tmp/cargo-target",
      "CARGO_NET_OFFLINE=true",
      "RUSTUP_HOME=/tmp/rustup",
      "RUSTC_BOOTSTRAP=1",
      "RUST_TEST_THREADS=1",
      "RUSTFLAGS=-C linker=/usr/bin/riscv64-linux-gnu-gcc-16",
      "LD_LIBRARY_PATH=/toolchain/lib:/usr/lib/riscv64-linux-gnu:/usr/lib:/lib",
    ],
    cwd: "/rust-src/library",
    timeoutMs,
    guestMemoryPolicyLimitBytes: rustUpstreamGuestMemoryPolicyBytes,
    snapshotSources: rustUpstreamSnapshotSources([
      { kind: "dir", guestPath: "/tmp/cargo-home" },
      { kind: "dir", guestPath: "/tmp/cargo-target" },
      { kind: "dir", guestPath: "/tmp/rustup" },
    ]),
    expectedDriverSubstrings: ["test result: ok"],
    artifactChecks: [],
  };
}

export const rustStdEnvSuiteEntry = rustStdSuiteEntry("rust-upstream-std-env", "env", rustUpstreamTimeoutMs);
export const rustStdPathSuiteEntry = rustStdSuiteEntry("rust-upstream-std-path", "path", rustUpstreamTimeoutMs);
export const rustStdThreadSuiteEntry = rustStdSuiteEntry("rust-upstream-std-thread", "thread", rustUpstreamTimeoutMs);
export const rustStdProcessSpawningSuiteEntry = rustStdSuiteEntry(
  "rust-upstream-std-process-spawning",
  "process_spawning",
  rustUpstreamTimeoutMs,
);
export const rustStdPipeSubprocessSuiteEntry = rustStdSuiteEntry(
  "rust-upstream-std-pipe-subprocess",
  "pipe-subprocess",
  rustUpstreamTimeoutMs,
);
