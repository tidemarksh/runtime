import { expect, test } from "vite-plus/test";
import {
  bucketScriptRuntimeFailure,
  prepareScriptRuntime,
  runScriptRuntimeEntry,
  validateScriptRuntimeOutput,
} from "../support/script-runtime-harness.js";
import { rubyScriptRuntimeEntry } from "../integration/targets/ruby/script-runtime.js";
import { phpScriptRuntimeEntry } from "../integration/targets/php/script-runtime.js";
import { perlScriptRuntimeEntry } from "../integration/targets/perl/script-runtime.js";
import { luaScriptRuntimeEntry } from "../integration/targets/lua/script-runtime.js";
import { rScriptRuntimeEntry } from "../integration/targets/r/script-runtime.js";
import { erlEvalScriptRuntimeEntry, elixirEvalScriptRuntimeEntry } from "../integration/targets/elixir/script-runtime.js";

const scriptRuntimeEntries = [
  rubyScriptRuntimeEntry,
  phpScriptRuntimeEntry,
  perlScriptRuntimeEntry,
  luaScriptRuntimeEntry,
  rScriptRuntimeEntry,
  erlEvalScriptRuntimeEntry,
  elixirEvalScriptRuntimeEntry,
] as const;

for (const entry of scriptRuntimeEntries) {
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
      expect(
        result.exitCode,
        [
          `script runtime gate exit mismatch: ${entry.id}`,
          `bucket=${bucketScriptRuntimeFailure({ result })}`,
          `exit=${result.exitCode}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toBe(0);
      expect(
        mismatches,
        [
          `script runtime gate output mismatch: ${entry.id}`,
          `bucket=${bucketScriptRuntimeFailure({ result })}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toEqual([]);
    } finally {
      runtime.destroy();
    }
  }, 20_000);
}
