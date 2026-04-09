import { resolve } from "node:path";
import type { ToolchainExecEntry } from "../../support/toolchain-exec-harness.js";

const toolsetDir = process.env.TIDEMARK_TEST_DEVTOOLS_TOOLSET_DIR ?? "";
const busyboxBinary = process.env.TIDEMARK_TEST_BUSYBOX_BINARY ?? "";
const glibcSysrootDir = process.env.TIDEMARK_TEST_GLIBC_SYSROOT_DIR ?? "";

const DEVTOOLS_RUNTIME_LIBS = [
  "ld-linux-riscv64-lp64d.so.1",
  "libc.so.6",
  "libdl.so.2",
  "libpthread.so.0",
  "libm.so.6",
  "librt.so.1",
  "libutil.so.1",
  "libgcc_s.so.1",
  "libstdc++.so.6",
] as const;

function devtoolsSnapshotSources(extra: readonly import("../../support/snapshot-sources.js").SnapshotSource[]) {
  return [
    {
      kind: "fileCopies" as const,
      hostPath: busyboxBinary,
      guestPaths: ["/bin/busybox", "/bin/sh", "/bin/echo", "/bin/cat", "/bin/mkdir"],
      mode: 0o755,
    },
    { kind: "tree" as const, hostPath: resolve(toolsetDir, "usr/lib/riscv64-linux-gnu"), guestPath: "/usr/lib/riscv64-linux-gnu" },
    ...DEVTOOLS_RUNTIME_LIBS.map((name) => ({
      kind: "file" as const,
      hostPath: resolve(glibcSysrootDir, name),
      guestPath: `/lib/${name}`,
      mode: 0o755,
    })),
    ...extra,
  ];
}

export const makeBuildToolchainEntry: ToolchainExecEntry = {
  id: "make-build",
  targetClass: "major developer tools",
  workflowKind: "build-driver",
  hostExecutablePath: resolve(toolsetDir, "usr/bin/make"),
  guestExecutablePath: "/usr/bin/make",
  argv: ["/usr/bin/make", "-C", "/work"],
  env: ["PATH=/usr/bin:/bin", "HOME=/tmp", "LD_LIBRARY_PATH=/usr/lib/riscv64-linux-gnu:/lib"],
  timeoutMs: 15_000,
  ramSize: 512 * 1024 * 1024,
  snapshotSources: devtoolsSnapshotSources([
    { kind: "file", hostPath: resolve(toolsetDir, "usr/bin/make"), guestPath: "/usr/bin/make", mode: 0o755 },
    {
      kind: "inlineFile",
      guestPath: "/work/Makefile",
      data: [
        "all:",
        "\t@echo make-evidence > out.txt",
        "\t@cat out.txt",
        "",
      ].join("\n"),
    },
  ]),
  expectedDriverSubstrings: ["make-evidence"],
  artifactChecks: [{ path: "/work/out.txt" }],
};

export const cmakeScriptToolchainEntry: ToolchainExecEntry = {
  id: "cmake-script",
  targetClass: "major developer tools",
  workflowKind: "build-driver / generator",
  hostExecutablePath: resolve(toolsetDir, "usr/bin/cmake"),
  guestExecutablePath: "/usr/bin/cmake",
  argv: ["/usr/bin/cmake", "-P", "/work/script.cmake"],
  env: ["PATH=/usr/bin:/bin", "HOME=/tmp", "LD_LIBRARY_PATH=/usr/lib/riscv64-linux-gnu:/lib"],
  timeoutMs: 15_000,
  ramSize: 512 * 1024 * 1024,
  snapshotSources: devtoolsSnapshotSources([
    { kind: "file", hostPath: resolve(toolsetDir, "usr/bin/cmake"), guestPath: "/usr/bin/cmake", mode: 0o755 },
    { kind: "tree", hostPath: resolve(toolsetDir, "usr/share/cmake"), guestPath: "/usr/share/cmake" },
    { kind: "tree", hostPath: resolve(toolsetDir, "usr/share/cmake-4.3"), guestPath: "/usr/share/cmake-4.3" },
    {
      kind: "inlineFile",
      guestPath: "/work/script.cmake",
      data: [
        'file(WRITE "/work/out.txt" "cmake-evidence\\n")',
        'message(STATUS "cmake-evidence")',
        "",
      ].join("\n"),
    },
  ]),
  expectedDriverSubstrings: ["cmake-evidence"],
  artifactChecks: [{ path: "/work/out.txt" }],
};

export const ninjaBuildToolchainEntry: ToolchainExecEntry = {
  id: "ninja-build",
  targetClass: "major developer tools",
  workflowKind: "build-driver",
  hostExecutablePath: resolve(toolsetDir, "usr/bin/ninja"),
  guestExecutablePath: "/usr/bin/ninja",
  argv: ["/usr/bin/ninja", "-C", "/work"],
  env: ["PATH=/usr/bin:/bin", "HOME=/tmp", "LD_LIBRARY_PATH=/usr/lib/riscv64-linux-gnu:/lib"],
  timeoutMs: 15_000,
  ramSize: 512 * 1024 * 1024,
  snapshotSources: devtoolsSnapshotSources([
    { kind: "file", hostPath: resolve(toolsetDir, "usr/bin/ninja"), guestPath: "/usr/bin/ninja", mode: 0o755 },
    {
      kind: "inlineFile",
      guestPath: "/work/guest-emit.sh",
      data: "#!/bin/sh\necho ninja-evidence > \"$1\"\ncat \"$1\"\n",
      mode: 0o755,
    },
    {
      kind: "inlineFile",
      guestPath: "/work/build.ninja",
      data: [
        "rule emit",
        "  command = /work/guest-emit.sh out.txt",
        "",
        "build out.txt: emit",
        "",
      ].join("\n"),
    },
  ]),
  expectedDriverSubstrings: ["guest-emit.sh out.txt"],
  artifactChecks: [{ path: "/work/out.txt" }],
};
