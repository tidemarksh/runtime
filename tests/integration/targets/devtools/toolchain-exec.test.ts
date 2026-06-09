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
import {
  cmakeScriptToolchainEntry,
  makeBuildToolchainEntry,
  ninjaBuildToolchainEntry,
} from "./toolchain-exec.js";

const devtoolsRuntimeOwnershipEntries = [
  makeBuildToolchainEntry,
  cmakeScriptToolchainEntry,
  ninjaBuildToolchainEntry,
] as const;

function expectDevtoolsRuntimeOwnershipSurface(detail: string): void {
  expect(
    /timeout after \d+ms|complete-exit-request:transitions-fault|dirty-memfs|publish-pending|pipe-sync-|output-branch|wait4-retry-skip|blocked-rpc|kernel_debug=timeout|vfork-execve:/.test(detail),
    detail,
  ).toBe(true);
}

for (const entry of devtoolsRuntimeOwnershipEntries) {
  test(`toolchain exec local gate stays inside runtime-ownership failure surface: ${entry.id}`, async () => {
    const prepared = await prepareToolchainExecRuntime(entry);
    if (!prepared) {
      return;
    }

    const { runtime, executableBytes } = prepared;
    try {
      const outcome = await runToolchainExecEntry(
        runtime,
        entry,
        executableBytes,
      ).then((result) => ({ ok: true as const, result }))
        .catch((error: unknown) => ({ ok: false as const, error }));

      if (outcome.ok) {
        const driverMismatches = validateToolchainDriverOutput(entry, outcome.result.output);
        const artifactMismatches = await validateToolchainArtifacts(runtime, entry);
        const success = outcome.result.exitCode === 0
          && driverMismatches.length === 0
          && artifactMismatches.length === 0;

        if (success) {
          const runResults = await runToolchainArtifactChecks(runtime, entry);
          for (let index = 0; index < runResults.length; index += 1) {
            const runResult = runResults[index]!;
            const runCheck = entry.runChecks?.[index];
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
        expectDevtoolsRuntimeOwnershipSurface(detail);
        return;
      }

      const detail = outcome.error instanceof Error
        ? (outcome.error.stack ?? outcome.error.message)
        : String(outcome.error);
      expect(bucketToolchainExecFailure({ error: outcome.error }), detail).toBe("runtime-ownership");
      expectDevtoolsRuntimeOwnershipSurface(detail);
    } finally {
      runtime.destroy();
    }
  }, Math.max(toolchainExecGateTimeoutMs(entry), 180_000));
}
