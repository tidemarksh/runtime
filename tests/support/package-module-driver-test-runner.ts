import { expect, test } from "vite-plus/test";
import {
  bucketPackageModuleDriverFailure,
  type PackageModuleDriverEntry,
  preparePackageModuleDriverRuntime,
  runPackageModuleDriverEntry,
  validatePackageModuleArtifacts,
  validatePackageModuleDriverOutput,
} from "./package-module-driver-harness.js";

const PACKAGE_MODULE_DRIVER_PREPARE_HEADROOM_MS = 60_000;

export function registerPackageModuleDriverGate(entry: PackageModuleDriverEntry): void {
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
      expect(outputMismatches).toEqual([]);
      expect(artifactMismatches).toEqual([]);
    } finally {
      runtime.destroy();
    }
  }, entry.timeoutMs + PACKAGE_MODULE_DRIVER_PREPARE_HEADROOM_MS);
}
