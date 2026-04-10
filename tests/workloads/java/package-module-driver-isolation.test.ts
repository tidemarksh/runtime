import { expect, test } from "vite-plus/test";
import {
  bucketPackageModuleDriverFailure,
  preparePackageModuleDriverRuntime,
  runPackageModuleDriverEntry,
  validatePackageModuleDriverOutput,
} from "../../support/package-module-driver-harness.js";
import { javaClassLoadPackageModuleEntry } from "./package-module-driver.js";

const javaIsolationEntry = {
  ...javaClassLoadPackageModuleEntry,
  id: "java-package-driver-lower-bound",
  argv: ["/jdk/bin/java", "-Xshare:off", "-XX:CompressedClassSpaceSize=64m", "-version"],
  expectedDriverSubstrings: ['openjdk version "25.0.2"'],
} as const;

test(`java package/module driver isolation: ${javaIsolationEntry.id}`, async () => {
  const prepared = await preparePackageModuleDriverRuntime(javaIsolationEntry);
  if (!prepared) {
    return;
  }

  const { runtime, executableBytes } = prepared;
  try {
    const result = await runPackageModuleDriverEntry(runtime, javaIsolationEntry, executableBytes).catch((error) => {
      const bucket = bucketPackageModuleDriverFailure({ error });
      throw new Error(
        [
          `java package/module driver isolation failed: ${javaIsolationEntry.id}`,
          `bucket=${bucket}`,
          error instanceof Error ? (error.stack ?? error.message) : String(error),
        ].join("\n"),
      );
    });

    const mismatches = validatePackageModuleDriverOutput(javaIsolationEntry, result.output);
    expect(
      result.exitCode,
      [
        `java package/module driver isolation exit mismatch: ${javaIsolationEntry.id}`,
        `bucket=${bucketPackageModuleDriverFailure({ result })}`,
        `exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `debug=${result.debugInfo ?? ""}`,
      ].join("\n"),
    ).toBe(0);
    expect(
      mismatches,
      [
        `java package/module driver isolation output mismatch: ${javaIsolationEntry.id}`,
        `bucket=${bucketPackageModuleDriverFailure({ result })}`,
        `output=${JSON.stringify(result.output)}`,
        `debug=${result.debugInfo ?? ""}`,
      ].join("\n"),
    ).toEqual([]);
  } finally {
    runtime.destroy();
  }
}, 20_000);
