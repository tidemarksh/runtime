import { resolve } from "node:path";
import type { SnapshotSource } from "../../../support/snapshot-sources.js";
import type { ToolchainExecEntry } from "../../../support/toolchain-exec-harness.js";

const cppToolchainDir = process.env.TIDEMARK_TEST_CPP_TOOLCHAIN_DIR ?? "";
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

function cppToolchainSnapshotSources(extra: readonly SnapshotSource[]): SnapshotSource[] {
  return [
    { kind: "tree", hostPath: resolve(cppToolchainDir, "usr/bin"), guestPath: "/usr/bin" },
    { kind: "tree", hostPath: resolve(cppToolchainDir, "usr/lib"), guestPath: "/usr/lib" },
    { kind: "tree", hostPath: resolve(cppToolchainDir, "etc"), guestPath: "/etc" },
    ...GLIBC_RUNTIME_LIBS.map((name) => ({
      kind: "file" as const,
      hostPath: resolve(glibcSysrootDir, name),
      guestPath: `/lib/${name}`,
      mode: 0o755,
    })),
    ...extra,
  ];
}

export const cppCompileLinkToolchainEntry: ToolchainExecEntry = {
  id: "cpp-compile-link",
  targetClass: "C / C++ toolchain",
  workflowKind: "compiler + linker",
  hostExecutablePath: resolve(cppToolchainDir, "usr/bin/riscv64-linux-gnu-g++-16"),
  guestExecutablePath: "/usr/bin/riscv64-linux-gnu-g++-16",
  argv: ["/usr/bin/riscv64-linux-gnu-g++-16", "/work/hello.cpp", "-o", "/work/hello"],
  env: [
    "PATH=/usr/bin:/bin",
    "HOME=/tmp",
    "LD_LIBRARY_PATH=/usr/lib/riscv64-linux-gnu:/usr/lib:/lib",
  ],
  timeoutMs: 20_000,
  ramSize: 1024 * 1024 * 1024,
  snapshotSources: cppToolchainSnapshotSources([
    {
      kind: "inlineFile",
      guestPath: "/work/hello.cpp",
      data: '#include <iostream>\nint main() { std::cout << "cpp-evidence" << std::endl; }\n',
    },
  ]),
  artifactChecks: [{ path: "/work/hello" }],
  runChecks: [{
    path: "/work/hello",
    timeoutMs: 10_000,
    expectedSubstrings: ["cpp-evidence"],
  }],
};
