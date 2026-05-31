import { expect, test } from "vite-plus/test";
import {
  bucketScriptRuntimeFailure,
  type ScriptRuntimeEntry,
  prepareScriptRuntime,
  runScriptRuntimeEntry,
  validateScriptRuntimeOutput,
} from "./script-runtime-harness.js";

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
      expect(result.exitCode).toBe(0);
      expect(mismatches).toEqual([]);
    } finally {
      runtime.destroy();
    }
  }, 20_000);
}
