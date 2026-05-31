import { expect, test } from "vite-plus/test";
import {
  bucketDynamicStartupFailure,
  classifyDynamicStartupFailureStatus,
  type DynamicStartupEntry,
  prepareDynamicStartupRuntime,
  runDynamicStartupEntry,
  validateDynamicStartupOutput,
} from "./dynamic-startup-harness.js";

export function registerDynamicStartupGate(entry: DynamicStartupEntry): void {
  test(`dynamic startup local gate: ${entry.id}`, async () => {
    let prepared;
    try {
      prepared = await prepareDynamicStartupRuntime(entry);
    } catch (error) {
      throw new Error(
        [
          `dynamic startup gate prepare failed: ${entry.id}`,
          `bucket=${bucketDynamicStartupFailure({ error })}`,
          `status=${classifyDynamicStartupFailureStatus({ error })}`,
          error instanceof Error ? (error.stack ?? error.message) : String(error),
        ].filter(Boolean).join("\n"),
      );
    }
    if (!prepared) {
      return;
    }

    const { runtime, elfBytes } = prepared;
    try {
      let result;
      try {
        result = await runDynamicStartupEntry(runtime, entry, elfBytes);
      } catch (error) {
        throw new Error(
          [
            `dynamic startup gate failed: ${entry.id}`,
            `bucket=${bucketDynamicStartupFailure({ error })}`,
            `status=${classifyDynamicStartupFailureStatus({ error })}`,
            error instanceof Error ? (error.stack ?? error.message) : String(error),
          ].filter(Boolean).join("\n"),
        );
      }

      const mismatches = validateDynamicStartupOutput(entry, result.output);
      expect(
        result.exitCode,
        [
          `dynamic startup gate exit mismatch: ${entry.id}`,
          `bucket=${bucketDynamicStartupFailure({ result })}`,
          `status=${classifyDynamicStartupFailureStatus({ result })}`,
          `exit=${result.exitCode}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].filter(Boolean).join("\n"),
      ).toBe(0);
      expect(
        mismatches,
        [
          `dynamic startup gate output mismatch: ${entry.id}`,
          `bucket=${bucketDynamicStartupFailure({ result })}`,
          `status=${classifyDynamicStartupFailureStatus({ result })}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].filter(Boolean).join("\n"),
      ).toEqual([]);
    } finally {
      runtime.destroy();
    }
  }, 20_000);
}
