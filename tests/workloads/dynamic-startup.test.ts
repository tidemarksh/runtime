import { expect, test } from "vite-plus/test";
import {
  bucketDynamicStartupFailure,
  classifyDynamicStartupFailureStatus,
  prepareDynamicStartupRuntime,
  runDynamicStartupEntry,
  validateDynamicStartupOutput,
} from "../support/dynamic-startup-harness.js";
import { nodeDynamicStartupEntry } from "./node/dynamic-startup.js";
import { pythonDynamicStartupEntry } from "./python/dynamic-startup.js";
import { javaDynamicStartupEntry } from "./java/dynamic-startup.js";
import { rubyDynamicStartupEntry } from "./ruby/dynamic-startup.js";
import { elixirDynamicStartupEntry } from "./elixir/dynamic-startup.js";
import { phpDynamicStartupEntry } from "./php/dynamic-startup.js";
import { luaDynamicStartupEntry } from "./lua/dynamic-startup.js";
import { perlDynamicStartupEntry } from "./perl/dynamic-startup.js";
import { rDynamicStartupEntry } from "./r/dynamic-startup.js";

const dynamicStartupEntries = [
  nodeDynamicStartupEntry,
  pythonDynamicStartupEntry,
  javaDynamicStartupEntry,
  rubyDynamicStartupEntry,
  elixirDynamicStartupEntry,
  phpDynamicStartupEntry,
  luaDynamicStartupEntry,
  perlDynamicStartupEntry,
  rDynamicStartupEntry,
] as const;

for (const entry of dynamicStartupEntries) {
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
