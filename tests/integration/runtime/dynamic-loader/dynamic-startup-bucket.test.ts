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

test("dynamic startup bucket: scheduler ownership timeout stays in runtime ownership", () => {
  const error = new Error(
    [
      "timeout after 20000ms",
      "local_safe:",
      "pid=1 vm=0x5823830 running=1 ready=1 blocked=[] threads=[0:status=20 1:status=0]",
      "worker_log_focus:",
      "[{\"workers\":[{\"ownerState\":\"wait:20\",\"schedulerState\":\"0x5823830:owner=wait:kernelStatus=20\"},{\"ownerState\":\"running\",\"schedulerState\":\"0x6849f70:owner=running:kernelStatus=0\"},{\"ownerState\":\"ready\",\"schedulerState\":\"0x6d81f48:owner=ready:kernelStatus=0\"}]}]",
      "executionStats=[{\"workers\":[{\"ownerState\":\"wait:20\",\"lastStatusName\":\"FUTEX_WAIT\"},{\"ownerState\":\"running\",\"lastStatusName\":\"CLONE_SPAWN\"}]}]",
    ].join("\n"),
  );
  expect(bucketDynamicStartupFailure({ error })).toBe("runtime-ownership");
  expect(classifyDynamicStartupFailureStatus({ error })).toBe("runtime-ownership-suspected");
});

test("dynamic startup bucket: stale futex wait owner stays in runtime ownership", () => {
  const error = new Error(
    [
      "timeout after 60000ms",
      "local_safe:",
      "pid=1 vm=0x5823830 running=1 ready=0 blocked=[] threads=[]",
      "internal:skipped-active-running-worker",
      "worker_log_focus:",
      "[{\"workers\":[{\"ownerState\":\"wait:20\",\"schedulerState\":\"0x5823830:owner=wait:kernelStatus=3:steps=57:statuses=57:last=20\"},{\"ownerState\":\"running\",\"schedulerState\":\"0x6849f70:owner=running:kernelStatus=3:last=6\"},{\"ownerState\":\"idle\",\"schedulerState\":\"0x6d81f48:owner=idle:kernelStatus=3:last=0\"}]}]",
    ].join("\n"),
  );
  expect(bucketDynamicStartupFailure({ error })).toBe("runtime-ownership");
  expect(classifyDynamicStartupFailureStatus({ error })).toBe("runtime-ownership-suspected");
});

test("dynamic startup bucket: stack frame line columns do not override pipe wait ownership", () => {
  const error = new Error(
    [
      "timeout after 30000ms",
      'proc_self_exe="/usr/lib/erlang/erts-16.2/bin/beam.smp"',
      "local_safe:",
      "pid=1 vm=0x5823830 running=1 ready=0 blocked=[21@0x70c1610] threads=[0:status=0 1:status=21 2:status=20 3:status=20]",
      "worker_log_focus:",
      "[{\"workers\":[{\"ownerState\":\"running\",\"schedulerState\":\"0x5823830:owner=running:kernelStatus=0\"},{\"ownerState\":\"blocked\",\"schedulerState\":\"0x70c1610:owner=blocked:kernelStatus=21:last=21\"},{\"ownerState\":\"wait:20\",\"schedulerState\":\"0x70c3858:owner=wait:kernelStatus=20:last=20\"}]}]",
      "    at runDynamicStartupEntry (/home/example/runtime/tests/support/dynamic-startup-harness.ts:262:11)",
    ].join("\n"),
  );

  expect(bucketDynamicStartupFailure({ error })).toBe("runtime-ownership");
  expect(classifyDynamicStartupFailureStatus({ error })).toBe("runtime-ownership-suspected");
});

test("dynamic startup bucket: timeout ownership evidence can out-rank guest fatal signal text", () => {
  const error = new Error(
    [
      "timeout after 20000ms",
      'capturedOutput="#\\n# A fatal error has been detected by the Java Runtime Environment:\\n#\\n#  SIGSEGV (0xb) at pc=0x00000000015ec3e2, pid=1, tid=2\\n#\\n# An error report file with more information is saved as:\\n# //hs_err_pid1.log\\n"',
      "worker_log_focus:",
      "[{\"workers\":[{\"ownerState\":\"wait:20\",\"schedulerState\":\"0x5823830:owner=wait:kernelStatus=20\"},{\"ownerState\":\"async:6\",\"schedulerState\":\"0x6849f70:owner=async:6:kernelStatus=6\"}]}]",
      "fs_lifecycle_tail=[fs-start:1:0x6849f70:op=2/WRITE/fd=4, fs-done:1:0x6849f70:rv=67]",
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
  const originalTestTimeout = process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TEST_TIMEOUT_MS;
  try {
    delete process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS;
    delete process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TEST_TIMEOUT_MS;
    expect(resolveDynamicStartupTimeoutMs(minimalEntry)).toBe(5_000);
    expect(resolveDynamicStartupTestTimeoutMs(minimalEntry)).toBe(20_000);
    process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS = "20000";
    expect(resolveDynamicStartupTimeoutMs(minimalEntry)).toBe(20_000);
    expect(resolveDynamicStartupTestTimeoutMs(minimalEntry)).toBe(200_000);
    process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TEST_TIMEOUT_MS = "60000";
    expect(resolveDynamicStartupTimeoutMs(minimalEntry)).toBe(20_000);
    expect(resolveDynamicStartupTestTimeoutMs(minimalEntry)).toBe(60_000);
    process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS = "5000";
    expect(resolveDynamicStartupTimeoutMs(minimalEntry)).toBe(5_000);
    expect(resolveDynamicStartupTestTimeoutMs(minimalEntry)).toBe(60_000);
    process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS = "0";
    process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TEST_TIMEOUT_MS = "0";
    expect(resolveDynamicStartupTimeoutMs(minimalEntry)).toBe(5_000);
    expect(resolveDynamicStartupTestTimeoutMs(minimalEntry)).toBe(20_000);
  } finally {
    if (original === undefined) {
      delete process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS;
    } else {
      process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS = original;
    }
    if (originalTestTimeout === undefined) {
      delete process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TEST_TIMEOUT_MS;
    } else {
      process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TEST_TIMEOUT_MS = originalTestTimeout;
    }
  }
});
