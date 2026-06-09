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
import { ocamlCompileToolchainEntry } from "./toolchain-exec.js";

function expectOcamlRuntimeOwnershipSurface(detail: string): void {
  expect(
    /timeout after \d+ms|complete-exit-request:transitions-fault|pipe-sync-|blocked-rpc|wait4-retry-skip|vfork-execve:|kernel_debug=timeout/.test(detail),
    detail,
  ).toBe(true);
}

test(`toolchain exec local gate stays inside runtime-ownership failure surface: ${ocamlCompileToolchainEntry.id}`, async () => {
  const prepared = await prepareToolchainExecRuntime(ocamlCompileToolchainEntry);
  if (!prepared) {
    return;
  }

  const { runtime, executableBytes } = prepared;
  try {
    const outcome = await runToolchainExecEntry(
      runtime,
      ocamlCompileToolchainEntry,
      executableBytes,
    ).then((result) => ({ ok: true as const, result }))
      .catch((error: unknown) => ({ ok: false as const, error }));

    if (outcome.ok) {
      const driverMismatches = validateToolchainDriverOutput(ocamlCompileToolchainEntry, outcome.result.output);
      const artifactMismatches = await validateToolchainArtifacts(runtime, ocamlCompileToolchainEntry);
      const success = outcome.result.exitCode === 0
        && driverMismatches.length === 0
        && artifactMismatches.length === 0;

      if (success) {
        const runResults = await runToolchainArtifactChecks(runtime, ocamlCompileToolchainEntry);
        for (let index = 0; index < runResults.length; index += 1) {
          const runResult = runResults[index]!;
          const runCheck = ocamlCompileToolchainEntry.runChecks?.[index];
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
      expectOcamlRuntimeOwnershipSurface(detail);
      return;
    }

    const detail = outcome.error instanceof Error
      ? (outcome.error.stack ?? outcome.error.message)
      : String(outcome.error);
    expect(bucketToolchainExecFailure({ error: outcome.error }), detail).toBe("runtime-ownership");
    expectOcamlRuntimeOwnershipSurface(detail);
  } finally {
    runtime.destroy();
  }
}, toolchainExecGateTimeoutMs(ocamlCompileToolchainEntry));
