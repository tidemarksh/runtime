import { expect, test } from "vite-plus/test";
import {
  bucketArchiveOrchestratorFailure,
  prepareArchiveOrchestratorRuntime,
  runArchiveOrchestratorStep,
  validateArchiveOrchestratorArtifacts,
  validateArchiveOrchestratorStepOutput,
} from "../support/archive-orchestrator-harness.js";
import {
  cmakeScriptArchiveEntry,
  gzipRoundtripArchiveEntry,
  xzRoundtripArchiveEntry,
} from "../integration/targets/devtools/archive-orchestrator.js";

const archiveOrchestratorEntries = [
  gzipRoundtripArchiveEntry,
  xzRoundtripArchiveEntry,
  cmakeScriptArchiveEntry,
] as const;

for (const entry of archiveOrchestratorEntries) {
  const totalTimeoutMs = entry.steps.reduce((sum, step) => sum + step.timeoutMs, 0) + 10_000;
  test(`archive orchestrator local gate: ${entry.id}`, async () => {
    let runtime;
    try {
      runtime = await prepareArchiveOrchestratorRuntime(entry);
    } catch (error) {
      const bucket = bucketArchiveOrchestratorFailure({ error });
      throw new Error(
        [
          `archive orchestrator gate prepare failed: ${entry.id}`,
          `bucket=${bucket}`,
          error instanceof Error ? (error.stack ?? error.message) : String(error),
        ].join("\n"),
      );
    }
    if (!runtime) {
      return;
    }

    try {
      for (const step of entry.steps) {
        let result;
        try {
          result = await runArchiveOrchestratorStep(runtime, step);
        } catch (error) {
          const bucket = bucketArchiveOrchestratorFailure({ error });
          throw new Error(
            [
              `archive orchestrator gate failed: ${entry.id}`,
              `step=${step.id}`,
              `bucket=${bucket}`,
              error instanceof Error ? (error.stack ?? error.message) : String(error),
            ].join("\n"),
          );
        }

        expect(
          result.exitCode,
          [
            `archive orchestrator exit mismatch: ${entry.id}`,
            `step=${step.id}`,
            `bucket=${bucketArchiveOrchestratorFailure({ result })}`,
            `exit=${result.exitCode}`,
            `output=${JSON.stringify(result.output)}`,
            `debug=${result.debugInfo ?? ""}`,
          ].join("\n"),
        ).toBe(0);

        expect(
          validateArchiveOrchestratorStepOutput(step, result.output),
          [
            `archive orchestrator output mismatch: ${entry.id}`,
            `step=${step.id}`,
            `bucket=${bucketArchiveOrchestratorFailure({ result })}`,
            `output=${JSON.stringify(result.output)}`,
            `debug=${result.debugInfo ?? ""}`,
          ].join("\n"),
        ).toEqual([]);
      }

      expect(
        await validateArchiveOrchestratorArtifacts(runtime, entry),
        [`archive orchestrator artifact mismatch: ${entry.id}`].join("\n"),
      ).toEqual([]);
    } finally {
      runtime.destroy();
    }
  }, totalTimeoutMs);
}
