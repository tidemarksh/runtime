import { resolve } from "node:path";
import type { SnapshotSource } from "../../../support/snapshot-sources.js";
import type { ToolchainExecEntry } from "../../../support/toolchain-exec-harness.js";
import { rustCLinkerSnapshotSources, rustToolchainSnapshotSources } from "./toolchain-layout.js";

const rustToolchainDir = process.env.TIDEMARK_TEST_RUST_TOOLCHAIN_DIR ?? "";
const cToolchainDir = process.env.TIDEMARK_TEST_C_TOOLCHAIN_DIR ?? "";
const glibcSysrootDir = process.env.TIDEMARK_TEST_GLIBC_SYSROOT_DIR ?? "";
const RUST_TOOLCHAIN_GUEST_MEMORY_POLICY_LIMIT_BYTES = 896 * 1024 * 1024;
const RUST_TOOLCHAIN_PAGE_CACHE_DATA_CAPACITY_BYTES = 640 * 1024 * 1024;

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

function rustSnapshotSources(extra: readonly SnapshotSource[]): SnapshotSource[] {
  return [
    ...rustToolchainSnapshotSources(rustToolchainDir),
    ...rustCLinkerSnapshotSources(cToolchainDir),
    ...GLIBC_RUNTIME_LIBS.map((name) => ({
      kind: "file" as const,
      hostPath: resolve(glibcSysrootDir, name),
      guestPath: `/lib/${name}`,
      mode: 0o755,
    })),
    ...extra,
  ];
}

export const rustcCompileToolchainEntry: ToolchainExecEntry = {
  id: "rustc-compile",
  targetClass: "Rust toolchain",
  workflowKind: "single compiler exec",
  hostExecutablePath: resolve(rustToolchainDir, "bin/rustc"),
  guestExecutablePath: "/toolchain/bin/rustc",
  argv: [
    "/toolchain/bin/rustc",
    "/work/hello.rs",
    "-C",
    "linker=/usr/bin/riscv64-linux-gnu-gcc-16",
    "-o",
    "/work/hello",
  ],
  env: [
    "PATH=/toolchain/bin:/usr/bin:/bin",
    "HOME=/tmp",
    "LD_LIBRARY_PATH=/toolchain/lib:/usr/lib/riscv64-linux-gnu:/usr/lib:/lib",
  ],
  timeoutMs: 20_000,
  ramSize: 768 * 1024 * 1024,
  guestMemoryPolicyLimitBytes: RUST_TOOLCHAIN_GUEST_MEMORY_POLICY_LIMIT_BYTES,
  pageCacheDataCapacityBytes: RUST_TOOLCHAIN_PAGE_CACHE_DATA_CAPACITY_BYTES,
  snapshotSources: rustSnapshotSources([
    {
      kind: "inlineFile",
      guestPath: "/work/hello.rs",
      data: 'fn main() { println!("rustc-evidence"); }\n',
    },
  ]),
  artifactChecks: [{ path: "/work/hello" }],
  runChecks: [{
    path: "/work/hello",
    timeoutMs: 10_000,
    expectedSubstrings: ["rustc-evidence"],
  }],
};

export const cargoBuildToolchainEntry: ToolchainExecEntry = {
  id: "cargo-build",
  targetClass: "Rust toolchain",
  workflowKind: "build-driver + child tool exec",
  hostExecutablePath: resolve(rustToolchainDir, "bin/cargo"),
  guestExecutablePath: "/toolchain/bin/cargo",
  argv: [
    "/toolchain/bin/cargo",
    "build",
    "--manifest-path",
    "/work/evidence/Cargo.toml",
  ],
  env: [
    "PATH=/toolchain/bin:/usr/bin:/bin",
    "HOME=/tmp",
    "CARGO_HOME=/tmp/cargo-home",
    "CARGO_TARGET_DIR=/tmp/cargo-target",
    "RUSTUP_HOME=/tmp/rustup",
    "RUSTFLAGS=-C linker=/usr/bin/riscv64-linux-gnu-gcc-16",
    "LD_LIBRARY_PATH=/toolchain/lib:/usr/lib/riscv64-linux-gnu:/usr/lib:/lib",
  ],
  timeoutMs: 30_000,
  ramSize: 1024 * 1024 * 1024,
  guestMemoryPolicyLimitBytes: RUST_TOOLCHAIN_GUEST_MEMORY_POLICY_LIMIT_BYTES,
  pageCacheDataCapacityBytes: RUST_TOOLCHAIN_PAGE_CACHE_DATA_CAPACITY_BYTES,
  snapshotSources: rustSnapshotSources([
    { kind: "dir", guestPath: "/tmp/cargo-home" },
    { kind: "dir", guestPath: "/tmp/cargo-target" },
    { kind: "dir", guestPath: "/tmp/rustup" },
    { kind: "dir", guestPath: "/work/evidence/src" },
    {
      kind: "inlineFile",
      guestPath: "/work/evidence/Cargo.toml",
      data: [
        "[package]",
        'name = "evidence"',
        'version = "0.1.0"',
        'edition = "2024"',
        "",
      ].join("\n"),
    },
    {
      kind: "inlineFile",
      guestPath: "/work/evidence/src/main.rs",
      data: 'fn main() { println!("cargo-evidence"); }\n',
    },
  ]),
  artifactChecks: [{ path: "/tmp/cargo-target/debug/evidence" }],
  runChecks: [{
    path: "/tmp/cargo-target/debug/evidence",
    timeoutMs: 10_000,
    expectedSubstrings: ["cargo-evidence"],
  }],
};
