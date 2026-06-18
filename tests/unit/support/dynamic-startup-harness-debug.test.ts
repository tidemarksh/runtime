import { expect, test } from "vite-plus/test";
import {
  bucketDynamicStartupFailure,
  classifyDynamicStartupFailureStatus,
  parseDebugGuestWriteWatchRegions,
  runDynamicStartupEntry,
  type DynamicStartupEntry,
} from "../../support/dynamic-startup-harness.js";
import type { RuntimeInstance } from "../../support/runtime-workload.js";

const minimalEntry: DynamicStartupEntry = {
  id: "unit-timeout",
  targetClass: "diagnostic",
  runnerKind: "direct-exec",
  hostExecutablePath: "/host/bin/app",
  guestExecutablePath: "/bin/app",
  argv: ["/bin/app"],
  env: [],
  timeoutMs: 5,
  guestMemoryPolicyLimitBytes: 1024,
  snapshotSources: [],
  expectedSubstrings: [],
};

test("parseDebugGuestWriteWatchRegions ignores empty or invalid input", () => {
  expect(parseDebugGuestWriteWatchRegions(undefined)).toEqual([]);
  expect(parseDebugGuestWriteWatchRegions("")).toEqual([]);
  expect(parseDebugGuestWriteWatchRegions("0x20:0x10 invalid 0x10")).toEqual([]);
});

test("parseDebugGuestWriteWatchRegions parses up to two address ranges", () => {
  expect(
    parseDebugGuestWriteWatchRegions("0x1000:0x1020,0x2000:0x2010 0x3000:0x3010"),
  ).toEqual([
    { slot: 0, start: 0x1000, end: 0x1020 },
    { slot: 1, start: 0x2000, end: 0x2010 },
  ]);
});

test("classifies timeout with PIPE_WAIT as runtime ownership suspected", () => {
  const error = new Error([
    "timeout after 5000ms",
    "local_safe:",
    "pid=1 vm=0x100 running=1 ready=0 blocked=[21@0x200]",
    "schedulerState=\"0x200:owner=blocked:kernelStatus=21:last=21\"",
    "proc_self_exe=\"/usr/lib/erlang/erts-16.2/bin/beam.smp\"",
  ].join("\n"));

  expect(bucketDynamicStartupFailure({ error })).toBe("runtime-ownership");
  expect(classifyDynamicStartupFailureStatus({ error })).toBe("runtime-ownership-suspected");
});

test("runDynamicStartupEntry timeout covers a spawn that never resolves", async () => {
  const originalTimeout = process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS;
  try {
    delete process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS;
    const runtime = {
      spawn: () => new Promise<never>(() => {}),
      processes: new Map(),
    } as unknown as RuntimeInstance;

    await expect(runDynamicStartupEntry(runtime, minimalEntry, new Uint8Array()))
      .rejects.toThrow(/timeout after 5ms/);
  } finally {
    if (originalTimeout === undefined) {
      delete process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS;
    } else {
      process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS = originalTimeout;
    }
  }
});

test("runDynamicStartupEntry timeout preserves captured output preview", async () => {
  const originalTimeout = process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS;
  try {
    delete process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS;
    const runtime = {
      spawn: async () => ({
        pid: 1,
        onStdout: (callback: (data: Uint8Array) => void) => {
          callback(new TextEncoder().encode("openjdk version \"25.0.2\"\n"));
        },
        waitForExit: () => new Promise<never>(() => {}),
        debugInfo: () => "debug=proc",
      }),
      processes: new Map(),
    } as unknown as RuntimeInstance;

    await expect(runDynamicStartupEntry(runtime, minimalEntry, new Uint8Array()))
      .rejects.toThrow(/capturedOutput=.*openjdk version .*25\.0\.2/s);
  } finally {
    if (originalTimeout === undefined) {
      delete process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS;
    } else {
      process.env.TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS = originalTimeout;
    }
  }
});
