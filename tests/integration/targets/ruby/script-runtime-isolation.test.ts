import { expect, test } from "vite-plus/test";
import {
  bucketScriptRuntimeFailure,
  prepareScriptRuntime,
  resolveScriptRuntimeTestTimeoutMs,
  runScriptRuntimeEntry,
  validateScriptRuntimeOutput,
} from "../../../support/script-runtime-harness.js";
import { rubyScriptRuntimeEntry } from "./script-runtime.js";

const SCRIPT_RUNTIME_ISOLATION_PREPARE_HEADROOM_MS = 60_000;

const rubyIsolationEntries = [
  {
    ...rubyScriptRuntimeEntry,
    id: "ruby-eval-minimal",
    argv: ["/usr/bin/ruby3.3", "-e", 'puts "ruby-ok"'],
    snapshotSources: rubyScriptRuntimeEntry.snapshotSources.filter((source) => source.kind !== "inlineFile"),
    expectedSubstrings: ["ruby-ok"],
  },
  {
    ...rubyScriptRuntimeEntry,
    id: "ruby-require-json",
    argv: ["/usr/bin/ruby3.3", "-e", 'require "json"; puts JSON.generate({ "lang" => "ruby", "n" => 42 })'],
    snapshotSources: rubyScriptRuntimeEntry.snapshotSources.filter((source) => source.kind !== "inlineFile"),
    expectedSubstrings: ['{"lang":"ruby","n":42}'],
  },
] as const;

for (const entry of rubyIsolationEntries) {
  test(`ruby script runtime isolation: ${entry.id}`, async () => {
    const prepared = await prepareScriptRuntime(entry);
    if (!prepared) {
      return;
    }

    const { runtime, elfBytes } = prepared;
    try {
      const result = await runScriptRuntimeEntry(runtime, entry, elfBytes).catch((error) => {
        const bucket = bucketScriptRuntimeFailure({ error });
        throw new Error(
          [
            `ruby script runtime isolation failed: ${entry.id}`,
            `bucket=${bucket}`,
            error instanceof Error ? (error.stack ?? error.message) : String(error),
          ].join("\n"),
        );
      });

      const mismatches = validateScriptRuntimeOutput(entry, result.output);
      expect(
        result.exitCode,
        [
          `ruby script runtime isolation exit mismatch: ${entry.id}`,
          `bucket=${bucketScriptRuntimeFailure({ result })}`,
          `exit=${result.exitCode}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toBe(0);
      expect(
        mismatches,
        [
          `ruby script runtime isolation output mismatch: ${entry.id}`,
          `bucket=${bucketScriptRuntimeFailure({ result })}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toEqual([]);
    } finally {
      runtime.destroy();
    }
  }, resolveScriptRuntimeTestTimeoutMs(entry, SCRIPT_RUNTIME_ISOLATION_PREPARE_HEADROOM_MS));
}
