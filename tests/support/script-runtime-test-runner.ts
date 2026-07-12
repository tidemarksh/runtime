import { expect, test } from "vite-plus/test";
import {
  bucketScriptRuntimeFailure,
  type ScriptRuntimeEntry,
  prepareScriptRuntime,
  resolveScriptRuntimeTestTimeoutMs,
  runScriptRuntimeEntry,
  validateScriptRuntimeOutput,
} from "./script-runtime-harness.js";

const SCRIPT_RUNTIME_PREPARE_HEADROOM_MS = 60_000;

export function scriptRuntimeGateTimeoutMs(entry: ScriptRuntimeEntry): number {
  return resolveScriptRuntimeTestTimeoutMs(entry, SCRIPT_RUNTIME_PREPARE_HEADROOM_MS);
}

export function formatScriptRuntimeGateResultContext(
  entry: ScriptRuntimeEntry,
  result: { exitCode: number; output: string; debugInfo?: string },
): string {
  return [
    `script runtime gate result: ${entry.id}`,
    `bucket=${bucketScriptRuntimeFailure({ result })}`,
    `exit=${result.exitCode}`,
    `output=${JSON.stringify(result.output)}`,
    `debug=${result.debugInfo ?? ""}`,
  ].join("\n");
}

export function registerScriptRuntimeGate(entry: ScriptRuntimeEntry): void {
  test(`script runtime local gate: ${entry.id}`, async () => {
    let prepared;
    try {
      prepared = await prepareScriptRuntime(entry);
    } catch (error) {
      const bucket = bucketScriptRuntimeFailure({ error });
      throw new Error(
        [
          `script runtime gate prepare failed: ${entry.id}`,
          `bucket=${bucket}`,
          error instanceof Error ? (error.stack ?? error.message) : String(error),
        ].join("\n"),
      );
    }
    if (!prepared) {
      return;
    }

    const { runtime, elfBytes } = prepared;
    try {
      let result;
      try {
        result = await runScriptRuntimeEntry(runtime, entry, elfBytes);
      } catch (error) {
        const bucket = bucketScriptRuntimeFailure({ error });
        throw new Error(
          [
            `script runtime gate failed: ${entry.id}`,
            `bucket=${bucket}`,
            error instanceof Error ? (error.stack ?? error.message) : String(error),
          ].join("\n"),
        );
      }

      const mismatches = validateScriptRuntimeOutput(entry, result.output);
      const resultContext = formatScriptRuntimeGateResultContext(entry, result);
      expect(result.exitCode, resultContext).toBe(0);
      expect(
        mismatches,
        [
          resultContext,
          `mismatches=${JSON.stringify(mismatches)}`,
        ].join("\n"),
      ).toEqual([]);
    } finally {
      runtime.destroy();
    }
  }, scriptRuntimeGateTimeoutMs(entry));
}
