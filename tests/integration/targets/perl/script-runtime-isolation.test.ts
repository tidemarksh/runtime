import { expect, test } from "vite-plus/test";
import {
  bucketScriptRuntimeFailure,
  prepareScriptRuntime,
  runScriptRuntimeEntry,
  validateScriptRuntimeOutput,
} from "../../../support/script-runtime-harness.js";
import { scriptRuntimeGateTimeoutMs } from "../../../support/script-runtime-test-runner.js";
import { perlScriptRuntimeEntry } from "./script-runtime.js";

const perlIsolationEntries = [
  {
    ...perlScriptRuntimeEntry,
    id: "perl-eval-minimal",
    argv: ["/usr/bin/perl", "-e", 'print "perl-ok\\n";'],
    snapshotSources: perlScriptRuntimeEntry.snapshotSources.filter((source) => source.kind !== "inlineFile"),
    expectedSubstrings: ["perl-ok"],
  },
  {
    ...perlScriptRuntimeEntry,
    id: "perl-jsonpp-eval",
    argv: [
      "/usr/bin/perl",
      "-MJSON::PP",
      "-e",
      'print JSON::PP->new->canonical->encode({ lang => "perl", n => 42 }), qq(\\n);',
    ],
    snapshotSources: perlScriptRuntimeEntry.snapshotSources.filter((source) => source.kind !== "inlineFile"),
    expectedSubstrings: ['{"lang":"perl","n":42}'],
  },
] as const;

for (const entry of perlIsolationEntries) {
  test(`perl script runtime isolation: ${entry.id}`, async () => {
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
            `perl script runtime isolation failed: ${entry.id}`,
            `bucket=${bucket}`,
            error instanceof Error ? (error.stack ?? error.message) : String(error),
          ].join("\n"),
        );
      });

      const mismatches = validateScriptRuntimeOutput(entry, result.output);
      expect(
        result.exitCode,
        [
          `perl script runtime isolation exit mismatch: ${entry.id}`,
          `bucket=${bucketScriptRuntimeFailure({ result })}`,
          `exit=${result.exitCode}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toBe(0);
      expect(
        mismatches,
        [
          `perl script runtime isolation output mismatch: ${entry.id}`,
          `bucket=${bucketScriptRuntimeFailure({ result })}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toEqual([]);
    } finally {
      runtime.destroy();
    }
  }, scriptRuntimeGateTimeoutMs(entry));
}
