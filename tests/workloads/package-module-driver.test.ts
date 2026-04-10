import { expect, test } from "vite-plus/test";
import {
  bucketPackageModuleDriverFailure,
  preparePackageModuleDriverRuntime,
  runPackageModuleDriverEntry,
  validatePackageModuleArtifacts,
  validatePackageModuleDriverOutput,
} from "../support/package-module-driver-harness.js";
import { rubyGemPackageModuleEntry } from "./ruby/package-module-driver.js";
import { perlProvePackageModuleEntry } from "./perl/package-module-driver.js";
import { luaLuacPackageModuleEntry } from "./lua/package-module-driver.js";
import { javaClassLoadPackageModuleEntry } from "./java/package-module-driver.js";

const packageModuleDriverEntries = [
  rubyGemPackageModuleEntry,
  perlProvePackageModuleEntry,
  luaLuacPackageModuleEntry,
  javaClassLoadPackageModuleEntry,
] as const;

for (const entry of packageModuleDriverEntries) {
  test(`package/module driver local gate: ${entry.id}`, async () => {
    let prepared;
    try {
      prepared = await preparePackageModuleDriverRuntime(entry);
    } catch (error) {
      const bucket = bucketPackageModuleDriverFailure({ error });
      throw new Error(
        [
          `package/module driver prepare failed: ${entry.id}`,
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
        result = await runPackageModuleDriverEntry(runtime, entry, executableBytes);
      } catch (error) {
        const bucket = bucketPackageModuleDriverFailure({ error });
        throw new Error(
          [
            `package/module driver gate failed: ${entry.id}`,
            `bucket=${bucket}`,
            error instanceof Error ? (error.stack ?? error.message) : String(error),
          ].join("\n"),
        );
      }

      const outputMismatches = validatePackageModuleDriverOutput(entry, result.output);
      const artifactMismatches = await validatePackageModuleArtifacts(runtime, entry);
      expect(
        result.exitCode,
        [
          `package/module driver exit mismatch: ${entry.id}`,
          `bucket=${bucketPackageModuleDriverFailure({ result })}`,
          `exit=${result.exitCode}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toBe(0);
      expect(
        outputMismatches,
        [
          `package/module driver output mismatch: ${entry.id}`,
          `bucket=${bucketPackageModuleDriverFailure({ result })}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toEqual([]);
      expect(
        artifactMismatches,
        [
          `package/module driver artifact mismatch: ${entry.id}`,
          `bucket=${bucketPackageModuleDriverFailure({ result })}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toEqual([]);
    } finally {
      runtime.destroy();
    }
  }, 30_000);
}
