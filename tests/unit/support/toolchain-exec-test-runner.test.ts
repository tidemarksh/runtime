import { expect, test } from "vite-plus/test";
import { toolchainExecGateTimeoutMs } from "../../support/toolchain-exec-test-runner.js";
import type { ToolchainExecEntry } from "../../support/toolchain-exec-harness.js";

test("toolchain exec gate timeout includes fixture preparation and artifact run checks", () => {
  const entry: ToolchainExecEntry = {
    id: "timeout-policy",
    targetClass: "test",
    workflowKind: "test",
    hostExecutablePath: "/host/tool",
    guestExecutablePath: "/guest/tool",
    argv: ["/guest/tool"],
    env: [],
    timeoutMs: 20_000,
    guestMemoryPolicyLimitBytes: 1,
    snapshotSources: [],
    artifactChecks: [],
    runChecks: [{
      path: "/work/out",
      timeoutMs: 10_000,
      expectedSubstrings: [],
    }],
  };

  expect(toolchainExecGateTimeoutMs(entry)).toBe(90_000);
});
