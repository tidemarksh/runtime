import { expect, test } from "vite-plus/test";
import {
  bucketToolchainExecFailure,
  prepareToolchainExecRuntime,
  runToolchainArtifactChecks,
  runToolchainExecEntry,
  validateToolchainArtifacts,
  validateToolchainDriverOutput,
} from "../../../support/toolchain-exec-harness.js";
import {
  goHelperCombinedGoarchToolchainEntry,
  goHelperTwoStepGoarchExplicitEnvToolchainEntry,
  goHelperTwoStepGoarchToolchainEntry,
} from "./toolchain-exec.js";

const goHelperEntries = [
  goHelperCombinedGoarchToolchainEntry,
  goHelperTwoStepGoarchToolchainEntry,
  goHelperTwoStepGoarchExplicitEnvToolchainEntry,
] as const;

const GO_HELPER_DIAGNOSTIC_TEST_TIMEOUT_MARGIN_MS = 70_000;

function expectGoHelperRuntimeOwnershipSurface(detail: string): void {
  expect(detail, detail).not.toContain("memory access out of bounds");
  expect(
    /Invalid atomic access index|timeout after \d+ms|assertProcessStandardFdIntegrity|vfork-execve:|runtime: epollctl failed/.test(detail),
    detail,
  ).toBe(true);
}

for (const entry of goHelperEntries) {
  test(`go helper toolchain exec stays inside runtime-ownership failure surface: ${entry.id}`, async () => {
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
          expect(await runToolchainArtifactChecks(runtime, entry)).toEqual([]);
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
        expectGoHelperRuntimeOwnershipSurface(detail);
        return;
      }

      const detail = outcome.error instanceof Error
        ? (outcome.error.stack ?? outcome.error.message)
        : String(outcome.error);
      expect(bucketToolchainExecFailure({ error: outcome.error }), detail).toBe("runtime-ownership");
      expectGoHelperRuntimeOwnershipSurface(detail);
    } finally {
      runtime.destroy();
    }
  }, entry.timeoutMs + GO_HELPER_DIAGNOSTIC_TEST_TIMEOUT_MARGIN_MS);
}
