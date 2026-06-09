import { expect, test } from "vite-plus/test";
import {
  bucketToolchainExecFailure,
  prepareToolchainExecRuntime,
  runToolchainArtifactChecks,
  runToolchainExecEntry,
  validateToolchainArtifacts,
  validateToolchainDriverOutput,
  validateToolchainRunOutput,
} from "../../../support/toolchain-exec-harness.js";
import { toolchainExecGateTimeoutMs } from "../../../support/toolchain-exec-test-runner.js";
import { cppCompileLinkToolchainEntry } from "./toolchain-exec.js";

function expectCppRuntimeOwnershipSurface(detail: string): void {
  expect(
    /vfork-execve:handoff-failed|vfork-execve:rollback|timeout after \d+ms|complete-exit-request:transitions-fault|kernel_debug=timeout/.test(detail),
    detail,
  ).toBe(true);
}

test(`toolchain exec local gate stays inside runtime-ownership failure surface: ${cppCompileLinkToolchainEntry.id}`, async () => {
  const prepared = await prepareToolchainExecRuntime(cppCompileLinkToolchainEntry);
  if (!prepared) {
    return;
  }

  const { runtime, executableBytes } = prepared;
  try {
    const outcome = await runToolchainExecEntry(
      runtime,
      cppCompileLinkToolchainEntry,
      executableBytes,
    ).then((result) => ({ ok: true as const, result }))
      .catch((error: unknown) => ({ ok: false as const, error }));

    if (outcome.ok) {
      const driverMismatches = validateToolchainDriverOutput(cppCompileLinkToolchainEntry, outcome.result.output);
      const artifactMismatches = await validateToolchainArtifacts(runtime, cppCompileLinkToolchainEntry);
      const success = outcome.result.exitCode === 0
        && driverMismatches.length === 0
        && artifactMismatches.length === 0;

      if (success) {
        const runResults = await runToolchainArtifactChecks(runtime, cppCompileLinkToolchainEntry);
        for (let index = 0; index < runResults.length; index += 1) {
          const runResult = runResults[index]!;
          const runCheck = cppCompileLinkToolchainEntry.runChecks?.[index];
          expect(runResult.exitCode).toBe(0);
          if (runCheck) {
            expect(validateToolchainRunOutput(runCheck, runResult.output)).toEqual([]);
          }
        }
        return;
      }

      const detail = [
        `exitCode=${outcome.result.exitCode}`,
        outcome.result.output,
        outcome.result.debugInfo ?? "",
        ...driverMismatches,
        ...artifactMismatches,
      ].filter(Boolean).join("\n");
      expect(bucketToolchainExecFailure({ result: outcome.result }), detail).toBe("runtime-ownership");
      expectCppRuntimeOwnershipSurface(detail);
      return;
    }

    const detail = outcome.error instanceof Error
      ? (outcome.error.stack ?? outcome.error.message)
      : String(outcome.error);
    expect(bucketToolchainExecFailure({ error: outcome.error }), detail).toBe("runtime-ownership");
    expectCppRuntimeOwnershipSurface(detail);
  } finally {
    runtime.destroy();
  }
}, toolchainExecGateTimeoutMs(cppCompileLinkToolchainEntry));
