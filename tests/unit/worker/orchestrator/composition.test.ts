// Orchestrator tests in this file validate composition and facade shape only.
import { readFileSync } from "node:fs";
import { expect, test, vi } from "vite-plus/test";
import { createWorkerOrchestrator } from "../../../../src/worker/orchestrator/index.js";

const PUBLIC_ORCHESTRATOR_METHODS = [
  "clearRuntimeFailureSnapshot",
  "clearRuntimeFsState",
  "createProcess",
  "debugRuntimeExecutionStats",
  "debugRuntimeLastFailureSnapshot",
  "debugRuntimeProcessHandles",
  "debugRuntimeWorkerLogFocus",
  "destroyProcessTree",
  "destroyRuntimeProcesses",
  "flushRuntimeFsStateFromDirtyHandle",
  "getRuntimeFsState",
  "injectHttpRequest",
  "mirrorRuntimeFsEntry",
  "primeRuntimeFsState",
  "publishRuntimeFsState",
  "replaceRuntimeFsSnapshot",
  "writeProcessStdin",
] as const;

test("createWorkerOrchestrator returns only the public runtime worker facade", () => {
  const orchestrator = createWorkerOrchestrator({
    ensureCompiledModule: vi.fn(async () => ({} as WebAssembly.Module)),
  });

  expect(Object.keys(orchestrator).sort()).toEqual([...PUBLIC_ORCHESTRATOR_METHODS].sort());
  for (const method of PUBLIC_ORCHESTRATOR_METHODS) {
    expect(typeof orchestrator[method]).toBe("function");
  }
});

test("orchestrator composition delegates owner policy instead of embedding it", () => {
  const source = readFileSync(
    new URL("../../../../src/worker/orchestrator/composition.ts", import.meta.url),
    "utf8",
  );

  expect(source).not.toContain("blockedRpcEffectRequiresResumeContinuation");
  expect(source).not.toContain("blockedRpcOutcomeRequiresResumeContinuation");
  expect(source).not.toContain("thread_resume_blocked_syscall");
  expect(source).not.toContain("collectTrackableForkSharedOfds");
  expect(source).not.toContain("!handle.kernelWorkerRequest || handle.pid <= 0");
});
