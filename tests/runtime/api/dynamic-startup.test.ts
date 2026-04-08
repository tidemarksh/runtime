import { expect, test } from "vite-plus/test";
import {
  bucketDynamicStartupFailure,
  prepareDynamicStartupRuntime,
  runDynamicStartupEntry,
  validateDynamicStartupOutput,
} from "../../support/dynamic-startup-harness.js";
import { nodeDynamicStartupEntry } from "../../workloads/node/dynamic-startup.js";
import { pythonDynamicStartupEntry } from "../../workloads/python/dynamic-startup.js";
import { javaDynamicStartupEntry } from "../../workloads/java/dynamic-startup.js";
import { rubyDynamicStartupEntry } from "../../workloads/ruby/dynamic-startup.js";
import { elixirDynamicStartupEntry } from "../../workloads/elixir/dynamic-startup.js";
import { phpDynamicStartupEntry } from "../../workloads/php/dynamic-startup.js";
import { luaDynamicStartupEntry } from "../../workloads/lua/dynamic-startup.js";
import { perlDynamicStartupEntry } from "../../workloads/perl/dynamic-startup.js";
import { rDynamicStartupEntry } from "../../workloads/r/dynamic-startup.js";

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
      const bucket = bucketDynamicStartupFailure({ error });
      throw new Error(
        [
          `dynamic startup gate prepare failed: ${entry.id}`,
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
        result = await runDynamicStartupEntry(runtime, entry, elfBytes);
      } catch (error) {
        const bucket = bucketDynamicStartupFailure({ error });
        throw new Error(
          [
            `dynamic startup gate failed: ${entry.id}`,
            `bucket=${bucket}`,
            error instanceof Error ? (error.stack ?? error.message) : String(error),
          ].join("\n"),
        );
      }

      const mismatches = validateDynamicStartupOutput(entry, result.output);
      expect(
        result.exitCode,
        [
          `dynamic startup gate exit mismatch: ${entry.id}`,
          `bucket=${bucketDynamicStartupFailure({ result })}`,
          `exit=${result.exitCode}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toBe(0);
      expect(
        mismatches,
        [
          `dynamic startup gate output mismatch: ${entry.id}`,
          `bucket=${bucketDynamicStartupFailure({ result })}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toEqual([]);
    } finally {
      runtime.destroy();
    }
  }, 20_000);
}
