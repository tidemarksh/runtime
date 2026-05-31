import { expect, test } from "vite-plus/test";
import {
  bucketPackageModuleDriverFailure,
  preparePackageModuleDriverRuntime,
  runPackageModuleDriverEntry,
  validatePackageModuleDriverOutput,
} from "../../../support/package-module-driver-harness.js";
import { rubyGemPackageModuleEntry } from "./package-module-driver.js";

const rubyIsolationEntries = [
  {
    ...rubyGemPackageModuleEntry,
    id: "ruby-package-driver-lower-bound",
    argv: ["/usr/bin/ruby3.3", "--version"],
    expectedDriverSubstrings: ["ruby 3.3.8"],
  },
  {
    ...rubyGemPackageModuleEntry,
    id: "ruby-rubygems-module-load",
    argv: ["/usr/bin/ruby3.3", "-e", 'require "rubygems"; puts Gem::VERSION'],
    expectedDriverSubstrings: ["3.6.7"],
  },
] as const;

for (const entry of rubyIsolationEntries) {
  test(`ruby package/module driver isolation: ${entry.id}`, async () => {
    const prepared = await preparePackageModuleDriverRuntime(entry);
    if (!prepared) {
      return;
    }

    const { runtime, executableBytes } = prepared;
    try {
      const result = await runPackageModuleDriverEntry(runtime, entry, executableBytes).catch((error) => {
        const bucket = bucketPackageModuleDriverFailure({ error });
        throw new Error(
          [
            `ruby package/module driver isolation failed: ${entry.id}`,
            `bucket=${bucket}`,
            error instanceof Error ? (error.stack ?? error.message) : String(error),
          ].join("\n"),
        );
      });

      const mismatches = validatePackageModuleDriverOutput(entry, result.output);
      expect(
        result.exitCode,
        [
          `ruby package/module driver isolation exit mismatch: ${entry.id}`,
          `bucket=${bucketPackageModuleDriverFailure({ result })}`,
          `exit=${result.exitCode}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toBe(0);
      expect(
        mismatches,
        [
          `ruby package/module driver isolation output mismatch: ${entry.id}`,
          `bucket=${bucketPackageModuleDriverFailure({ result })}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toEqual([]);
    } finally {
      runtime.destroy();
    }
  }, 20_000);
}
