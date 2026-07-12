import { expect, test } from "vite-plus/test";
import {
  formatScriptRuntimeGateResultContext,
  scriptRuntimeGateTimeoutMs,
} from "../../support/script-runtime-test-runner.js";
import {
  runScriptRuntimeEntry,
  type ScriptRuntimeEntry,
} from "../../support/script-runtime-harness.js";
import type { RuntimeInstance } from "../../support/runtime-workload.js";

function scriptRuntimeEntry(overrides: Partial<ScriptRuntimeEntry> = {}): ScriptRuntimeEntry {
  return {
    id: "timeout-policy",
    targetClass: "test",
    runnerKind: "direct-exec",
    hostExecutablePath: "/host/runtime",
    guestExecutablePath: "/guest/runtime",
    argv: ["/guest/runtime"],
    env: [],
    timeoutMs: 10_000,
    guestMemoryPolicyLimitBytes: 1,
    snapshotSources: [],
    expectedSubstrings: [],
    ...overrides,
  };
}

test("script runtime gate timeout includes snapshot preparation headroom", () => {
  const entry = scriptRuntimeEntry();
  expect(scriptRuntimeGateTimeoutMs(entry)).toBe(70_000);
});

test("script runtime gate timeout honors explicit test timeout override", () => {
  const previous = process.env.TIDEMARK_TEST_SCRIPT_RUNTIME_TEST_TIMEOUT_MS;
  process.env.TIDEMARK_TEST_SCRIPT_RUNTIME_TEST_TIMEOUT_MS = "90000";
  try {
    expect(scriptRuntimeGateTimeoutMs(scriptRuntimeEntry())).toBe(90_000);
  } finally {
    if (previous === undefined) {
      delete process.env.TIDEMARK_TEST_SCRIPT_RUNTIME_TEST_TIMEOUT_MS;
    } else {
      process.env.TIDEMARK_TEST_SCRIPT_RUNTIME_TEST_TIMEOUT_MS = previous;
    }
  }
});

test("script runtime entries expose an optional page cache resource profile", () => {
  expect(scriptRuntimeEntry({ pageCacheDataCapacityBytes: 64 * 1024 * 1024 }).pageCacheDataCapacityBytes)
    .toBe(64 * 1024 * 1024);
});

test("script runtime gate result context preserves exit output and debug evidence", () => {
  const context = formatScriptRuntimeGateResultContext(
    scriptRuntimeEntry({ id: "exit-evidence" }),
    {
      exitCode: 1,
      output: "runtime stderr line\n",
      debugInfo: "process debug summary",
    },
  );

  expect(context).toContain("script runtime gate result: exit-evidence");
  expect(context).toContain("bucket=consumer-runtime");
  expect(context).toContain("exit=1");
  expect(context).toContain("runtime stderr line");
  expect(context).toContain("process debug summary");
});

test("script runtime entry closes stdin unless the entry requests a pipe", async () => {
  const spawnOptions: unknown[] = [];
  const runtime = {
    spawn: async (_elfBytes: Uint8Array, _argv: string[], _env: string[], options: unknown) => {
      spawnOptions.push(options);
      return {
        onStdout: () => {},
        waitForExit: async () => 0,
        debugInfo: () => "debug",
      };
    },
  } as unknown as RuntimeInstance;

  await runScriptRuntimeEntry(runtime, scriptRuntimeEntry(), new Uint8Array([1, 2, 3]));
  await runScriptRuntimeEntry(runtime, scriptRuntimeEntry({ stdin: "pipe" }), new Uint8Array([1, 2, 3]));

  expect(spawnOptions).toEqual([
    { stdin: "closed" },
    { stdin: "pipe" },
  ]);
});
