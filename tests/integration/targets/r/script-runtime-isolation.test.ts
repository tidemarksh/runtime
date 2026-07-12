import { expect, test } from "vite-plus/test";
import { resolve } from "node:path";
import {
  bucketScriptRuntimeFailure,
  prepareScriptRuntime,
  runScriptRuntimeEntry,
  validateScriptRuntimeOutput,
} from "../../../support/script-runtime-harness.js";
import { scriptRuntimeGateTimeoutMs } from "../../../support/script-runtime-test-runner.js";
import { rScriptRuntimeEntry } from "./script-runtime.js";

const runtimeDir = process.env.TIDEMARK_TEST_R_RUNTIME_DIR ?? "";

const rBaseSnapshotSources = rScriptRuntimeEntry.snapshotSources.filter((source) => {
  return source.kind !== "inlineFile" || source.guestPath !== "/work/script.R";
});

const rIsolationEntries = [
  {
    ...rScriptRuntimeEntry,
    id: "r-exec-version",
    hostExecutablePath: resolve(runtimeDir, "usr/lib/R/bin/exec/R"),
    guestExecutablePath: "/usr/lib/R/bin/exec/R",
    argv: ["/usr/lib/R/bin/exec/R", "--version"],
    snapshotSources: rBaseSnapshotSources,
    expectedSubstrings: ["R version 4.6.0"],
    forbiddenSubstrings: ["cannot find system Renviron"],
  },
  {
    ...rScriptRuntimeEntry,
    id: "rscript-eval-minimal",
    hostExecutablePath: resolve(runtimeDir, "usr/bin/Rscript"),
    guestExecutablePath: "/usr/bin/Rscript",
    argv: ["/usr/bin/Rscript", "-e", 'cat("r-ok\\n")'],
    snapshotSources: rBaseSnapshotSources,
    expectedSubstrings: ["r-ok"],
    forbiddenSubstrings: ["cannot find system Renviron"],
  },
  {
    ...rScriptRuntimeEntry,
    id: "r-exec-minimal",
    hostExecutablePath: resolve(runtimeDir, "usr/lib/R/bin/exec/R"),
    guestExecutablePath: "/usr/lib/R/bin/exec/R",
    argv: [
      "/usr/lib/R/bin/exec/R",
      "--slave",
      "--no-restore",
      "--no-save",
      "--no-echo",
      "-e",
      'cat("r-core-ok\\n")',
    ],
    snapshotSources: rBaseSnapshotSources,
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
      ...rBaseSnapshotSources,
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
  }, scriptRuntimeGateTimeoutMs(entry));
}
