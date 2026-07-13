import { resolve } from "node:path";
import type { SnapshotSource } from "../../../support/snapshot-sources.js";
import type { ToolchainExecEntry } from "../../../support/toolchain-exec-harness.js";

const libuvUpstreamSuiteDir = process.env.TIDEMARK_TEST_LIBUV_UPSTREAM_SUITE_DIR ?? "";
const glibcSysrootDir = process.env.TIDEMARK_TEST_GLIBC_SYSROOT_DIR ?? "";
const libuvSuiteBinary = resolve(
  libuvUpstreamSuiteDir,
  "libuv-1.51.0/bin/uv_run_tests_a",
);

const DEVTOOLS_RUNTIME_LIBS = [
  "ld-linux-riscv64-lp64d.so.1",
  "libc.so.6",
  "libdl.so.2",
  "libpthread.so.0",
  "libm.so.6",
  "librt.so.1",
  "libutil.so.1",
  "libgcc_s.so.1",
] as const;

function libuvSnapshotSources(extra: readonly SnapshotSource[] = []): SnapshotSource[] {
  return [
    {
      kind: "file",
      hostPath: libuvSuiteBinary,
      guestPath: "/opt/tidemark/libuv/uv_run_tests_a",
      mode: 0o755,
    },
    ...DEVTOOLS_RUNTIME_LIBS.map((name) => ({
      kind: "file" as const,
      hostPath: resolve(glibcSysrootDir, name),
      guestPath: `/lib/${name}`,
      mode: 0o755,
    })),
    ...extra,
  ];
}

function libuvUpstreamSuiteEntry(testName: string, timeoutMs = 60_000): ToolchainExecEntry {
  return {
    id: `libuv-upstream-${testName}`,
    targetClass: "JavaScript runtime",
    workflowKind: "upstream suite invocation",
    hostExecutablePath: libuvSuiteBinary,
    guestExecutablePath: "/opt/tidemark/libuv/uv_run_tests_a",
    argv: ["/opt/tidemark/libuv/uv_run_tests_a", testName, testName],
    env: [
      "HOME=/tmp",
      "TMPDIR=/tmp",
      "LD_LIBRARY_PATH=/lib",
      "UV_RUN_AS_ROOT=1",
    ],
    timeoutMs,
    guestMemoryPolicyLimitBytes: 1024 * 1024 * 1024,
    hostFileBackingMode: "host-file",
    snapshotSources: libuvSnapshotSources(),
    artifactChecks: [],
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
