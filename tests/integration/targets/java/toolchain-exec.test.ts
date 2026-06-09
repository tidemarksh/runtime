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
import { javaJavacToolchainEntry } from "./toolchain-exec.js";

function expectJavaRuntimeOwnershipSurface(detail: string): void {
  expect(
    /timeout after \d+ms|Native memory allocation|Not enough space|errno=12|blocked-rpc|wait4-retry-skip|pipe-sync-|vfork-execve:|complete-exit-request:transitions-fault|kernel_debug=timeout/.test(detail),
    detail,
  ).toBe(true);
}

test(`toolchain exec local gate stays inside runtime-ownership failure surface: ${javaJavacToolchainEntry.id}`, async () => {
  const prepared = await prepareToolchainExecRuntime(javaJavacToolchainEntry);
  if (!prepared) {
    return;
  }

  const { runtime, executableBytes } = prepared;
  try {
    const outcome = await runToolchainExecEntry(
      runtime,
      javaJavacToolchainEntry,
      executableBytes,
    ).then((result) => ({ ok: true as const, result }))
      .catch((error: unknown) => ({ ok: false as const, error }));

    if (outcome.ok) {
      const driverMismatches = validateToolchainDriverOutput(javaJavacToolchainEntry, outcome.result.output);
      const artifactMismatches = await validateToolchainArtifacts(runtime, javaJavacToolchainEntry);
      const success = outcome.result.exitCode === 0
        && driverMismatches.length === 0
        && artifactMismatches.length === 0;

      if (success) {
        const runResults = await runToolchainArtifactChecks(runtime, javaJavacToolchainEntry);
        for (let index = 0; index < runResults.length; index += 1) {
          const runResult = runResults[index]!;
          const runCheck = javaJavacToolchainEntry.runChecks?.[index];
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
      expectJavaRuntimeOwnershipSurface(detail);
      return;
    }

    const detail = outcome.error instanceof Error
      ? (outcome.error.stack ?? outcome.error.message)
      : String(outcome.error);
    expect(bucketToolchainExecFailure({ error: outcome.error }), detail).toBe("runtime-ownership");
    expectJavaRuntimeOwnershipSurface(detail);
  } finally {
    runtime.destroy();
  }
}, toolchainExecGateTimeoutMs(javaJavacToolchainEntry));
