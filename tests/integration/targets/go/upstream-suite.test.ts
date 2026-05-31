import { expect, test } from "vite-plus/test";
import {
  bucketToolchainExecFailure,
  prepareToolchainExecRuntime,
  runToolchainExecEntry,
  validateToolchainArtifacts,
  validateToolchainDriverOutput,
} from "../../../support/toolchain-exec-harness.js";
import {
  goCmdGoSuiteEntry,
  goOsExecSuiteEntry,
  goOsSuiteEntry,
  goRuntimeSuiteEntry,
} from "./upstream-suite.js";

const goUpstreamSuiteEntries = [
  goRuntimeSuiteEntry,
  goOsSuiteEntry,
  goOsExecSuiteEntry,
  goCmdGoSuiteEntry,
] as const;

for (const entry of goUpstreamSuiteEntries) {
  test(`go upstream suite candidate: ${entry.id}`, async () => {
    let prepared;
    try {
      prepared = await prepareToolchainExecRuntime(entry);
    } catch (error) {
      const bucket = bucketToolchainExecFailure({ error });
      throw new Error(
        [
          `go upstream suite prepare failed: ${entry.id}`,
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
            `go upstream suite failed: ${entry.id}`,
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
          `go upstream suite exit mismatch: ${entry.id}`,
          `bucket=${bucketToolchainExecFailure({ result })}`,
          `exit=${result.exitCode}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toBe(0);
      expect(
        outputMismatches,
        [
          `go upstream suite output mismatch: ${entry.id}`,
          `bucket=${bucketToolchainExecFailure({ result })}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toEqual([]);
      expect(
        artifactMismatches,
        [
          `go upstream suite artifact mismatch: ${entry.id}`,
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
