import { expect, test } from "vite-plus/test";
import {
  bucketToolchainExecFailure,
  prepareToolchainExecRuntime,
  runToolchainExecEntry,
  validateToolchainArtifacts,
  validateToolchainDriverOutput,
} from "../../support/toolchain-exec-harness.js";
import {
  rustStdEnvSuiteEntry,
  rustStdPathSuiteEntry,
  rustStdPipeSubprocessSuiteEntry,
  rustStdProcessSpawningSuiteEntry,
  rustStdThreadSuiteEntry,
} from "./upstream-suite.js";

const rustUpstreamSuiteEntries = [
  rustStdEnvSuiteEntry,
  rustStdPathSuiteEntry,
  rustStdThreadSuiteEntry,
  rustStdProcessSpawningSuiteEntry,
  rustStdPipeSubprocessSuiteEntry,
] as const;

for (const entry of rustUpstreamSuiteEntries) {
  test(`rust upstream suite candidate: ${entry.id}`, async () => {
    let prepared;
    try {
      prepared = await prepareToolchainExecRuntime(entry);
    } catch (error) {
      const bucket = bucketToolchainExecFailure({ error });
      throw new Error(
        [
          `rust upstream suite prepare failed: ${entry.id}`,
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
      const result = await runToolchainExecEntry(runtime, entry, executableBytes).catch((error) => {
        const bucket = bucketToolchainExecFailure({ error });
        throw new Error(
          [
            `rust upstream suite failed: ${entry.id}`,
            `bucket=${bucket}`,
            error instanceof Error ? (error.stack ?? error.message) : String(error),
          ].join("\n"),
        );
      });

      const outputMismatches = validateToolchainDriverOutput(entry, result.output);
      const artifactMismatches = await validateToolchainArtifacts(runtime, entry);

      expect(
        result.exitCode,
        [
          `rust upstream suite exit mismatch: ${entry.id}`,
          `bucket=${bucketToolchainExecFailure({ result })}`,
          `exit=${result.exitCode}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toBe(0);
      expect(
        outputMismatches,
        [
          `rust upstream suite output mismatch: ${entry.id}`,
          `bucket=${bucketToolchainExecFailure({ result })}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toEqual([]);
      expect(
        artifactMismatches,
        [
          `rust upstream suite artifact mismatch: ${entry.id}`,
          `bucket=${bucketToolchainExecFailure({ result })}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toEqual([]);
    } finally {
      runtime.destroy();
    }
  }, entry.timeoutMs + 10_000);
}
