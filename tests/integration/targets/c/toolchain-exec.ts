import { resolve } from "node:path";
import type { SnapshotSource } from "../../../support/snapshot-sources.js";
import type { ToolchainExecEntry } from "../../../support/toolchain-exec-harness.js";

const cToolchainDir = process.env.TIDEMARK_TEST_C_TOOLCHAIN_DIR ?? "";
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

function cToolchainSnapshotSources(extra: readonly SnapshotSource[]): SnapshotSource[] {
  return [
    { kind: "dir", guestPath: "/tmp", mode: 0o777 },
    { kind: "tree", hostPath: resolve(cToolchainDir, "usr/bin"), guestPath: "/usr/bin" },
    { kind: "tree", hostPath: resolve(cToolchainDir, "usr/include"), guestPath: "/usr/include" },
    { kind: "tree", hostPath: resolve(cToolchainDir, "usr/lib"), guestPath: "/usr/lib" },
    { kind: "tree", hostPath: resolve(cToolchainDir, "usr/libexec"), guestPath: "/usr/libexec" },
    { kind: "tree", hostPath: resolve(cToolchainDir, "etc"), guestPath: "/etc" },
    ...GLIBC_RUNTIME_LIBS.map((name) => ({
      kind: "file" as const,
      hostPath: resolve(glibcSysrootDir, name),
      guestPath: `/lib/${name}`,
      mode: 0o755,
    })),
    ...extra,
  ];
}

export const cCompileLinkToolchainEntry: ToolchainExecEntry = {
  id: "c-compile-link",
  targetClass: "C toolchain",
  workflowKind: "compiler + linker",
  hostExecutablePath: resolve(cToolchainDir, "usr/bin/riscv64-linux-gnu-gcc-16"),
  guestExecutablePath: "/usr/bin/riscv64-linux-gnu-gcc-16",
  argv: ["/usr/bin/riscv64-linux-gnu-gcc-16", "/work/hello.c", "-o", "/work/hello"],
  env: [
    "PATH=/usr/bin:/bin",
    "HOME=/tmp",
    "LD_LIBRARY_PATH=/usr/lib/riscv64-linux-gnu:/usr/lib:/lib",
  ],
  timeoutMs: 20_000,
  guestMemoryPolicyLimitBytes: 768 * 1024 * 1024,
  snapshotSources: cToolchainSnapshotSources([
    {
      kind: "inlineFile",
      guestPath: "/work/hello.c",
      data: '#include <stdio.h>\nint main(void) { puts("c-evidence"); return 0; }\n',
    },
  ]),
  artifactChecks: [{ path: "/work/hello" }],
  runChecks: [{
    path: "/work/hello",
    timeoutMs: 10_000,
    expectedSubstrings: ["c-evidence"],
  }],
};
