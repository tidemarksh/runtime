import { expect, test } from "vite-plus/test";
import {
  bucketToolchainExecFailure,
  prepareToolchainExecRuntime,
  runToolchainArtifactChecks,
  runToolchainExecEntry,
  validateToolchainArtifacts,
  validateToolchainDriverOutput,
  validateToolchainRunOutput,
} from "../support/toolchain-exec-harness.js";
import {
  goBuildToolchainEntry,
  goCompileVersionToolchainEntry,
  goHelperCombinedGoarchToolchainEntry,
  goToolCompileVersionSingleThreadToolchainEntry,
  goToolCompileVersionToolchainEntry,
} from "../integration/targets/go/toolchain-exec.js";
import { javaJavacToolchainEntry } from "../integration/targets/java/toolchain-exec.js";
import { rustcCompileToolchainEntry, cargoBuildToolchainEntry } from "../integration/targets/rust/toolchain-exec.js";
import { cCompileLinkToolchainEntry } from "../integration/targets/c/toolchain-exec.js";
import { cppCompileLinkToolchainEntry } from "../integration/targets/cpp/toolchain-exec.js";
import { haskellCompileToolchainEntry } from "../integration/targets/haskell/toolchain-exec.js";
import {
  cmakeScriptToolchainEntry,
  makeBuildToolchainEntry,
  ninjaBuildToolchainEntry,
} from "../integration/targets/devtools/toolchain-exec.js";

const toolchainExecEntries = [
  goCompileVersionToolchainEntry,
  goHelperCombinedGoarchToolchainEntry,
  goToolCompileVersionToolchainEntry,
  goToolCompileVersionSingleThreadToolchainEntry,
  goBuildToolchainEntry,
  rustcCompileToolchainEntry,
  cargoBuildToolchainEntry,
  cCompileLinkToolchainEntry,
  cppCompileLinkToolchainEntry,
  javaJavacToolchainEntry,
  haskellCompileToolchainEntry,
  makeBuildToolchainEntry,
  cmakeScriptToolchainEntry,
  ninjaBuildToolchainEntry,
] as const;

for (const entry of toolchainExecEntries) {
  test(`toolchain exec local gate: ${entry.id}`, async () => {
    let prepared;
    try {
      prepared = await prepareToolchainExecRuntime(entry);
    } catch (error) {
      const bucket = bucketToolchainExecFailure({ error });
      throw new Error(
        [
          `toolchain exec gate prepare failed: ${entry.id}`,
          `bucket=${bucket}`,
          error instanceof Error ? (error.stack ?? error.message) : String(error),
        ].join("\n"),
      );
    }
    if (!prepared) {
      return;
    }

    const { runtime, executableBytes } = prepared;
    try {
      let result;
      try {
        result = await runToolchainExecEntry(runtime, entry, executableBytes);
      } catch (error) {
        const bucket = bucketToolchainExecFailure({ error });
        throw new Error(
          [
            `toolchain exec gate failed: ${entry.id}`,
            `bucket=${bucket}`,
            error instanceof Error ? (error.stack ?? error.message) : String(error),
          ].join("\n"),
        );
      }

      const outputMismatches = validateToolchainDriverOutput(entry, result.output);
      const artifactMismatches = await validateToolchainArtifacts(runtime, entry);
      const runResults = await runToolchainArtifactChecks(runtime, entry);

      expect(
        result.exitCode,
        [
          `toolchain exec exit mismatch: ${entry.id}`,
          `bucket=${bucketToolchainExecFailure({ result })}`,
          `exit=${result.exitCode}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toBe(0);
      expect(
        outputMismatches,
        [
          `toolchain exec output mismatch: ${entry.id}`,
          `bucket=${bucketToolchainExecFailure({ result })}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toEqual([]);
      expect(
        artifactMismatches,
        [
          `toolchain exec artifact mismatch: ${entry.id}`,
          `bucket=${bucketToolchainExecFailure({ result })}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toEqual([]);

      for (let index = 0; index < runResults.length; index += 1) {
        const runResult = runResults[index]!;
        const runCheck = entry.runChecks?.[index];
        expect(
          runResult.exitCode,
          [
            `toolchain exec run mismatch: ${entry.id}`,
            `artifact=${runResult.path}`,
            `exit=${runResult.exitCode}`,
            `output=${JSON.stringify(runResult.output)}`,
            `debug=${runResult.debugInfo ?? ""}`,
          ].join("\n"),
        ).toBe(0);
        if (runCheck) {
          expect(
            validateToolchainRunOutput(runCheck, runResult.output),
            [
              `toolchain exec run output mismatch: ${entry.id}`,
              `artifact=${runResult.path}`,
              `output=${JSON.stringify(runResult.output)}`,
              `debug=${runResult.debugInfo ?? ""}`,
            ].join("\n"),
          ).toEqual([]);
        }
      }
    } finally {
      runtime.destroy();
    }
  }, entry.timeoutMs + 10_000);
}
