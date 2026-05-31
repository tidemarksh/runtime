import { resolve } from "node:path";
import type { SnapshotSource } from "../../../support/snapshot-sources.js";
import type { ToolchainExecEntry } from "../../../support/toolchain-exec-harness.js";

const libuvUpstreamSuiteDir = process.env.TIDEMARK_TEST_LIBUV_UPSTREAM_SUITE_DIR ?? "";
const cToolchainDir = process.env.TIDEMARK_TEST_C_TOOLCHAIN_DIR ?? "";
const devtoolsToolsetDir = process.env.TIDEMARK_TEST_DEVTOOLS_TOOLSET_DIR ?? "";
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

function libuvSnapshotSources(extra: readonly SnapshotSource[] = []): SnapshotSource[] {
  return [
    {
      kind: "fileCopies",
      hostPath: busyboxBinary,
      guestPaths: ["/bin/busybox", "/bin/sh", "/bin/echo", "/bin/cat", "/bin/mkdir"],
      mode: 0o755,
    },
    { kind: "tree", hostPath: resolve(devtoolsToolsetDir, "usr/lib/riscv64-linux-gnu"), guestPath: "/usr/lib/riscv64-linux-gnu" },
    { kind: "tree", hostPath: resolve(devtoolsToolsetDir, "usr/share/cmake"), guestPath: "/usr/share/cmake" },
    { kind: "tree", hostPath: resolve(devtoolsToolsetDir, "usr/share/cmake-4.3"), guestPath: "/usr/share/cmake-4.3" },
    { kind: "tree", hostPath: resolve(cToolchainDir, "usr/bin"), guestPath: "/usr/bin" },
    { kind: "tree", hostPath: resolve(cToolchainDir, "usr/lib"), guestPath: "/usr/lib" },
    { kind: "tree", hostPath: resolve(cToolchainDir, "etc"), guestPath: "/etc" },
    { kind: "file", hostPath: resolve(devtoolsToolsetDir, "usr/bin/cmake"), guestPath: "/usr/bin/cmake", mode: 0o755 },
    { kind: "file", hostPath: resolve(devtoolsToolsetDir, "usr/bin/ninja"), guestPath: "/usr/bin/ninja", mode: 0o755 },
    { kind: "tree", hostPath: resolve(libuvUpstreamSuiteDir, "libuv-1.51.0"), guestPath: "/work/libuv" },
    ...DEVTOOLS_RUNTIME_LIBS.map((name) => ({
      kind: "file" as const,
      hostPath: resolve(glibcSysrootDir, name),
      guestPath: `/lib/${name}`,
      mode: 0o755,
    })),
    {
      kind: "inlineFile",
      guestPath: "/work/run-libuv-suite.sh",
      mode: 0o755,
      data: [
        "#!/bin/sh",
        "set -eu",
        "test_name=\"$1\"",
        "cmake -S /work/libuv -B /work/build -G Ninja -DBUILD_TESTING=ON",
        "cmake --build /work/build --target uv_run_tests_a",
        "/work/build/uv_run_tests_a \"$test_name\" \"$test_name\"",
        "echo \"libuv-upstream-ok:$test_name\"",
        "",
      ].join("\n"),
    },
    ...extra,
  ];
}

function libuvUpstreamSuiteEntry(testName: string, timeoutMs = 180_000): ToolchainExecEntry {
  return {
    id: `libuv-upstream-${testName}`,
    targetClass: "JavaScript runtime",
    workflowKind: "upstream suite invocation",
    hostExecutablePath: busyboxBinary,
    guestExecutablePath: "/bin/sh",
    argv: ["/bin/sh", "/work/run-libuv-suite.sh", testName],
    env: [
      "PATH=/usr/bin:/bin",
      "HOME=/tmp",
      "TMPDIR=/tmp",
      "CC=/usr/bin/riscv64-linux-gnu-gcc-16",
      "CXX=/usr/bin/riscv64-linux-gnu-g++-16",
      "LD_LIBRARY_PATH=/usr/lib/riscv64-linux-gnu:/usr/lib:/lib",
    ],
    timeoutMs,
    ramSize: 1024 * 1024 * 1024,
    snapshotSources: libuvSnapshotSources(),
    expectedDriverSubstrings: [`libuv-upstream-ok:${testName}`],
    artifactChecks: [{ path: "/work/build/uv_run_tests_a", minSize: 1 }],
  };
}

export const libuvRunOnceSuiteEntry = libuvUpstreamSuiteEntry("run_once");
export const libuvTimerSuiteEntry = libuvUpstreamSuiteEntry("timer");
export const libuvTimerZeroTimeoutSuiteEntry = libuvUpstreamSuiteEntry("timer_zero_timeout");
export const libuvAsyncSuiteEntry = libuvUpstreamSuiteEntry("async");
export const libuvFsFileAsyncSuiteEntry = libuvUpstreamSuiteEntry("fs_file_async");
export const libuvFsReaddirNonEmptyDirSuiteEntry = libuvUpstreamSuiteEntry("fs_readdir_non_empty_dir");
export const libuvThreadpoolQueueWorkSimpleSuiteEntry = libuvUpstreamSuiteEntry(
  "threadpool_queue_work_simple",
);
