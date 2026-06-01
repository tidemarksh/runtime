import { resolve } from "node:path";
import type { SnapshotSource } from "../../../support/snapshot-sources.js";
import type { ToolchainExecEntry } from "../../../support/toolchain-exec-harness.js";

const haskellToolchainDir = process.env.TIDEMARK_TEST_HASKELL_TOOLCHAIN_DIR ?? "";
const glibcSysrootDir = process.env.TIDEMARK_TEST_GLIBC_SYSROOT_DIR ?? "";

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

function haskellSnapshotSources(extra: readonly SnapshotSource[]): SnapshotSource[] {
  return [
    { kind: "tree", hostPath: resolve(haskellToolchainDir, "usr"), guestPath: "/usr" },
    { kind: "tree", hostPath: resolve(haskellToolchainDir, "etc"), guestPath: "/etc" },
    { kind: "tree", hostPath: resolve(haskellToolchainDir, "var"), guestPath: "/var" },
    ...GLIBC_RUNTIME_LIBS.map((name) => ({
      kind: "file" as const,
      hostPath: resolve(glibcSysrootDir, name),
      guestPath: `/lib/${name}`,
      mode: 0o755,
    })),
    ...extra,
  ];
}

export const haskellCompileToolchainEntry: ToolchainExecEntry = {
  id: "haskell-compile",
  targetClass: "Haskell runtime / toolchain",
  workflowKind: "compiler + linker",
  hostExecutablePath: resolve(haskellToolchainDir, "usr/bin/ghc"),
  guestExecutablePath: "/usr/bin/ghc",
  argv: ["/usr/bin/ghc", "-o", "/work/hello", "/work/Main.hs"],
  env: [
    "PATH=/usr/bin:/bin",
    "HOME=/tmp",
    "LD_LIBRARY_PATH=/usr/lib/riscv64-linux-gnu:/usr/lib:/lib",
  ],
  timeoutMs: 30_000,
  ramSize: 1024 * 1024 * 1024,
  snapshotSources: haskellSnapshotSources([
    {
      kind: "inlineFile",
      guestPath: "/work/Main.hs",
      data: 'main = putStrLn "haskell-evidence"\n',
    },
  ]),
  artifactChecks: [{ path: "/work/hello" }],
  runChecks: [{
    path: "/work/hello",
    timeoutMs: 10_000,
    expectedSubstrings: ["haskell-evidence"],
  }],
};
