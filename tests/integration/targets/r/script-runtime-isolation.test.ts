import { expect, test } from "vite-plus/test";
import { resolve } from "node:path";
import {
  bucketScriptRuntimeFailure,
  prepareScriptRuntime,
  runScriptRuntimeEntry,
  validateScriptRuntimeOutput,
} from "../../../support/script-runtime-harness.js";
import { rScriptRuntimeEntry } from "./script-runtime.js";

const runtimeDir = process.env.TIDEMARK_TEST_R_RUNTIME_DIR ?? "";

const rIsolationEntries = [
  {
    ...rScriptRuntimeEntry,
    id: "r-exec-version",
    hostExecutablePath: resolve(runtimeDir, "usr/lib/R/bin/exec/R"),
    guestExecutablePath: "/usr/lib/R/bin/exec/R",
    argv: ["/usr/lib/R/bin/exec/R", "--version"],
    snapshotSources: rScriptRuntimeEntry.snapshotSources.filter((source) => source.kind !== "inlineFile"),
    expectedSubstrings: ["R version 4.6.0 alpha"],
    forbiddenSubstrings: ["cannot find system Renviron"],
  },
  {
    ...rScriptRuntimeEntry,
    id: "rscript-eval-minimal",
    argv: ["/usr/bin/Rscript", "-e", 'cat("r-ok\\n")'],
    snapshotSources: rScriptRuntimeEntry.snapshotSources.filter((source) => source.kind !== "inlineFile"),
    expectedSubstrings: ["r-ok"],
    forbiddenSubstrings: ["cannot find system Renviron"],
  },
  {
    ...rScriptRuntimeEntry,
    id: "r-exec-minimal",
    hostExecutablePath: resolve(runtimeDir, "usr/lib/R/bin/exec/R"),
    guestExecutablePath: "/usr/lib/R/bin/exec/R",
    argv: ["/usr/lib/R/bin/exec/R", "--slave", "-e", 'cat("r-core-ok\\n")'],
    snapshotSources: rScriptRuntimeEntry.snapshotSources.filter((source) => source.kind !== "inlineFile"),
    expectedSubstrings: ["r-core-ok"],
    forbiddenSubstrings: ["cannot find system Renviron"],
  },
  {
    ...rScriptRuntimeEntry,
    id: "r-exec-file-minimal",
    hostExecutablePath: resolve(runtimeDir, "usr/lib/R/bin/exec/R"),
    guestExecutablePath: "/usr/lib/R/bin/exec/R",
    argv: ["/usr/lib/R/bin/exec/R", "--slave", "-f", "/work/minimal.R"],
    snapshotSources: [
      ...rScriptRuntimeEntry.snapshotSources.filter((source) => source.kind !== "inlineFile"),
      {
        kind: "inlineFile",
        guestPath: "/work/minimal.R",
        data: 'cat("r-file-ok\\n")\n',
      },
    ],
    expectedSubstrings: ["r-file-ok"],
    forbiddenSubstrings: ["cannot find system Renviron"],
  },
] as const;

for (const entry of rIsolationEntries) {
  test(`r script runtime isolation: ${entry.id}`, async () => {
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
            `r script runtime isolation failed: ${entry.id}`,
            `bucket=${bucket}`,
            error instanceof Error ? (error.stack ?? error.message) : String(error),
          ].join("\n"),
        );
      });

      const mismatches = validateScriptRuntimeOutput(entry, result.output);
      expect(
        result.exitCode,
        [
          `r script runtime isolation exit mismatch: ${entry.id}`,
          `bucket=${bucketScriptRuntimeFailure({ result })}`,
          `exit=${result.exitCode}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toBe(0);
      expect(
        mismatches,
        [
          `r script runtime isolation output mismatch: ${entry.id}`,
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
