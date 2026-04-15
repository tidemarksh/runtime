import { expect, test } from "vite-plus/test";
import {
  prepareDynamicStartupRuntime,
  runDynamicStartupEntry,
} from "../../support/dynamic-startup-harness.js";
import { rubyDynamicStartupEntry } from "./dynamic-startup.js";

test("ruby dynamic startup contract: ruby-version-exit-0", async () => {
  const prepared = await prepareDynamicStartupRuntime(rubyDynamicStartupEntry);
  if (!prepared) {
    return;
  }

  const { runtime, elfBytes } = prepared;
  try {
    const result = await runDynamicStartupEntry(runtime, rubyDynamicStartupEntry, elfBytes);

    expect(result.exitCode, "ruby dynamic startup contract should exit cleanly").toBe(0);
    expect(result.output, "ruby dynamic startup contract should print the version banner").toContain("ruby 3.3.8");
    expect(
      result.debugInfo ?? "",
      "ruby dynamic startup contract should preserve the guest exit witness",
    ).toContain('exit:1:"/usr/bin/ruby3.3":0:0');
    expect(
      result.debugInfo ?? "",
      "ruby dynamic startup contract should stay on the ruby startup surface",
    ).toContain('proc_self_exe="/usr/bin/ruby3.3"');
  } finally {
    runtime.destroy();
  }
}, 20_000);
