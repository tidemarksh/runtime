import { expect, test } from "vite-plus/test";
import {
  bucketDynamicStartupFailure,
  classifyDynamicStartupFailureStatus,
  resolveDynamicStartupTestTimeoutMs,
  resolveDynamicStartupTimeoutMs,
  type DynamicStartupEntry,
} from "../../../support/dynamic-startup-harness.js";

const minimalEntry: DynamicStartupEntry = {
  id: "timeout-test",
  targetClass: "diagnostic",
  runnerKind: "direct-exec",
  hostExecutablePath: "/host/bin/app",
  guestExecutablePath: "/bin/app",
  argv: ["/bin/app"],
  env: [],
  timeoutMs: 5_000,
  guestMemoryPolicyLimitBytes: 1024,
  snapshotSources: [],
  expectedSubstrings: [],
};

test("dynamic startup bucket: loader faults stay in foundation", () => {
  expect(
    bucketDynamicStartupFailure({
      error: new Error("ELF inspection failed: PT_INTERP missing interpreter"),
    }),
  ).toBe("foundation");
  expect(
    classifyDynamicStartupFailureStatus({
      error: new Error("ELF inspection failed: PT_INTERP missing interpreter"),
    }),
  ).toBe("foundation-suspected");
});

test("dynamic startup bucket: process_create failures stay in runtime-ownership", () => {
  expect(
    bucketDynamicStartupFailure({
      error: new Error("plain-fork-child-ready: process_create failed during attach publish"),
    }),
  ).toBe("runtime-ownership");
  expect(
    classifyDynamicStartupFailureStatus({
      error: new Error("plain-fork-child-ready: process_create failed during attach publish"),
    }),
  ).toBe("runtime-ownership-suspected");
});

test("dynamic startup bucket: missing shared library stays in consumer-runtime", () => {
  expect(
    bucketDynamicStartupFailure({
      error: new Error("error while loading shared libraries: libexample.so: cannot open shared object file"),
    }),
  ).toBe("consumer-runtime");
  expect(
    classifyDynamicStartupFailureStatus({
      error: new Error("error while loading shared libraries: libexample.so: cannot open shared object file"),
    }),
  ).toBe("consumer-runtime-suspected");
});

test("dynamic startup bucket: startup timeout stays conservative", () => {
  expect(
    bucketDynamicStartupFailure({
      error: new Error('timeout after 5000ms\nproc_self_exe="/usr/bin/guest-runtime"'),
    }),
  ).toBe("unknown");
  expect(
    classifyDynamicStartupFailureStatus({
      error: new Error('timeout after 5000ms\nproc_self_exe="/usr/bin/guest-runtime"'),
    }),
  ).toBe("unclassified");
});

test("dynamic startup bucket: FS progress timeout stays in runtime ownership", () => {
  const error = new Error(
    [
      "timeout after 5000ms",
      "threads=[0:tid=0 pc=0x4fe06 status=6]",
      "fs_lifecycle_tail=[fs-start:1:0x5821670:op=1/READ/fd=3, fs-done:1:0x5821670:rv=832, fs-start:1:0x5821670:op=12/MMAP/fd=3]",
    ].join("\n"),
  );
  expect(bucketDynamicStartupFailure({ error })).toBe("runtime-ownership");
  expect(classifyDynamicStartupFailureStatus({ error })).toBe("runtime-ownership-suspected");
});

test("dynamic startup bucket: guest signal and debug-collection traps stay conservative", () => {
  expect(
    bucketDynamicStartupFailure({
      result: {
        exitCode: 139,
        output: "",
        debugInfo:
          'proc_self_exe="/usr/bin/guest-runtime"\nsig_deliver=11\ndebug_collect_error="RuntimeError: operation does not support unaligned accesses"',
      },
    }),
  ).toBe("unknown");
  expect(
    classifyDynamicStartupFailureStatus({
      result: {
        exitCode: 139,
        output: "",
        debugInfo:
          'proc_self_exe="/usr/bin/guest-runtime"\nsig_deliver=11\ndebug_collect_error="RuntimeError: operation does not support unaligned accesses"',
      },
    }),
  ).toBe("unclassified");
});

test("dynamic startup timeout override is opt-in", () => {
  const original = process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS;
  try {
    delete process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS;
    expect(resolveDynamicStartupTimeoutMs(minimalEntry)).toBe(5_000);
    expect(resolveDynamicStartupTestTimeoutMs(minimalEntry)).toBe(20_000);
    process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS = "20000";
    expect(resolveDynamicStartupTimeoutMs(minimalEntry)).toBe(20_000);
    expect(resolveDynamicStartupTestTimeoutMs(minimalEntry)).toBe(200_000);
    process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS = "0";
    expect(resolveDynamicStartupTimeoutMs(minimalEntry)).toBe(5_000);
    expect(resolveDynamicStartupTestTimeoutMs(minimalEntry)).toBe(20_000);
  } finally {
    if (original === undefined) {
      delete process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS;
    } else {
      process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS = original;
    }
  }
});
