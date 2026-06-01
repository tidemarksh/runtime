import { expect, test } from "vite-plus/test";
import {
  bucketToolchainExecFailure,
  type ToolchainExecEntry,
  prepareToolchainExecRuntime,
  runToolchainArtifactChecks,
  runToolchainExecEntry,
  validateToolchainArtifacts,
  validateToolchainDriverOutput,
  validateToolchainRunOutput,
} from "./toolchain-exec-harness.js";
import { runtimeDebugSummary } from "./runtime-workload.js";

export function registerToolchainExecGate(entry: ToolchainExecEntry): void {
  test(`toolchain exec local gate: ${entry.id}`, async () => {
    let prepared;
    try {
      prepared = await prepareToolchainExecRuntime(entry);
    } catch (error) {
      const bucket = bucketToolchainExecFailure({ error });
      throw new Error(
        [
          `toolchain exec gate prepare failed: ${entry.id}`,
          `bucket=${bucket}`,
          error instanceof Error ? (error.stack ?? error.message) : String(error),
        ].join("\n"),
      );
    }
    if (!prepared) {
      return;
    }

    const { runtime, executableBytes } = prepared;
    try {
      let result;
      try {
        result = await runToolchainExecEntry(runtime, entry, executableBytes);
      } catch (error) {
        const bucket = bucketToolchainExecFailure({ error });
        throw new Error(
          [
            `toolchain exec gate failed: ${entry.id}`,
            `bucket=${bucket}`,
            error instanceof Error ? (error.stack ?? error.message) : String(error),
          ].join("\n"),
        );
      }

      const outputMismatches = validateToolchainDriverOutput(entry, result.output);
      const artifactMismatches = await validateToolchainArtifacts(runtime, entry);
      const runResults = await runToolchainArtifactChecks(runtime, entry);

      if (result.exitCode !== 0) {
        const summary = await runtimeDebugSummary(runtime).catch((error) => (
          `runtimeDebugSummary failed: ${error instanceof Error ? (error.stack ?? error.message) : String(error)}`
        ));
        throw new Error([
          `toolchain exec driver exited nonzero: ${entry.id}`,
          `exitCode=${result.exitCode}`,
          "output:",
          result.output,
          result.debugInfo ? `debug:\n${result.debugInfo}` : null,
          summary ? `runtime:\n${summary}` : null,
        ].filter((part): part is string => part !== null).join("\n"));
      }
      expect(result.exitCode).toBe(0);
      expect(outputMismatches).toEqual([]);
      expect(artifactMismatches).toEqual([]);

      for (let index = 0; index < runResults.length; index += 1) {
        const runResult = runResults[index]!;
        const runCheck = entry.runChecks?.[index];
        expect(runResult.exitCode).toBe(0);
        if (runCheck) {
          expect(validateToolchainRunOutput(runCheck, runResult.output)).toEqual([]);
        }
      }
    } finally {
      runtime.destroy();
    }
  }, entry.timeoutMs + 10_000);
}
