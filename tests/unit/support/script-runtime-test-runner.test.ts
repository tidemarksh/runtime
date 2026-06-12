import { expect, test } from "vite-plus/test";
import {
  formatScriptRuntimeGateResultContext,
  scriptRuntimeGateTimeoutMs,
} from "../../support/script-runtime-test-runner.js";
import type { ScriptRuntimeEntry } from "../../support/script-runtime-harness.js";

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
