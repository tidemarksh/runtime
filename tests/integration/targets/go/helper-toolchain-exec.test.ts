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
        expect(outcome.result.exitCode).toBe(0);
        expect(validateToolchainDriverOutput(entry, outcome.result.output)).toEqual([]);
        expect(await validateToolchainArtifacts(runtime, entry)).toEqual([]);
        expect(await runToolchainArtifactChecks(runtime, entry)).toEqual([]);
        return;
      }

      const detail = outcome.error instanceof Error
        ? (outcome.error.stack ?? outcome.error.message)
        : String(outcome.error);
      expect(bucketToolchainExecFailure({ error: outcome.error })).toBe("runtime-ownership");
      expect(detail).not.toContain("memory access out of bounds");
      expect(detail).toMatch(/Invalid atomic access index|vfork-execve:handoff-failed/);
    } finally {
      runtime.destroy();
    }
  }, entry.timeoutMs + 10_000);
}
