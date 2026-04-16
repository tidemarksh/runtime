import { expect, test } from "vite-plus/test";
import {
  prepareDynamicStartupRuntime,
  runDynamicStartupEntry,
} from "../../support/dynamic-startup-harness.js";
import { javaDynamicStartupEntry } from "./dynamic-startup.js";

test("java dynamic startup contract: java-version-exit-0", async () => {
  const prepared = await prepareDynamicStartupRuntime(javaDynamicStartupEntry);
  if (!prepared) {
    return;
  }

  const { runtime, elfBytes } = prepared;
  try {
    const result = await runDynamicStartupEntry(runtime, javaDynamicStartupEntry, elfBytes);

    expect(
      result.exitCode,
      [
        "java dynamic startup contract should exit cleanly",
        `exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `debug=${result.debugInfo ?? ""}`,
      ].join("\n"),
    ).toBe(0);
    expect(result.output).toContain('openjdk version "25.0.2"');
  } finally {
    runtime.destroy();
  }
}, 20_000);
