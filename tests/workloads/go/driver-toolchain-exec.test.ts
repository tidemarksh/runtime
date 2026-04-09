import { expect, test } from "vite-plus/test";
import {
  bucketToolchainExecFailure,
  prepareToolchainExecRuntime,
  runToolchainArtifactChecks,
  runToolchainExecEntry,
  validateToolchainArtifacts,
  validateToolchainDriverOutput,
  validateToolchainRunOutput,
} from "../../support/toolchain-exec-harness.js";
import {
  goBuildToolchainEntry,
  goToolCompileVersionToolchainEntry,
  goToolCompileVersionSingleThreadToolchainEntry,
} from "./toolchain-exec.js";

const goDriverEntries = [
  goToolCompileVersionToolchainEntry,
  goToolCompileVersionSingleThreadToolchainEntry,
  goBuildToolchainEntry,
] as const;

function expectGoDriverRuntimeOwnershipSurface(detail: string): void {
  expect(
    /(vfork-execve:handoff-failed:\d+:-8|fatal: bad g in signal handler|exec format error|missing generated artifact|vfork-execve:child-state:.*memory access out of bounds)/.test(detail),
  ).toBe(true);
  if (detail.includes("vfork-execve:preflight:")) {
    expect(detail).toContain("head=7f454c46");
  }
  if (detail.includes("vfork-execve:handoff-failed:")) {
    const maybeStageTagged = /vfork-execve:handoff-failed:\d+:-8:(\d+):/.exec(detail);
    if (maybeStageTagged) {
      expect(maybeStageTagged[1]).toBe("3");
    }
  }
}

for (const entry of goDriverEntries) {
  test(`go driver toolchain exec stays inside runtime-ownership failure surface: ${entry.id}`, async () => {
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
        expectGoDriverRuntimeOwnershipSurface(detail);
        return;
      }

      const detail = outcome.error instanceof Error
        ? (outcome.error.stack ?? outcome.error.message)
        : String(outcome.error);
      expect(bucketToolchainExecFailure({ error: outcome.error })).toBe("runtime-ownership");
      expectGoDriverRuntimeOwnershipSurface(detail);
    } finally {
      runtime.destroy();
    }
  }, entry.timeoutMs + 10_000);
}
