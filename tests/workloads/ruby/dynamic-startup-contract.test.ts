import { expect, test } from "vite-plus/test";
import {
  prepareDynamicStartupRuntime,
  runDynamicStartupEntry,
} from "../../support/dynamic-startup-harness.js";
import { rubyDynamicStartupEntry } from "./dynamic-startup.js";

test("ruby dynamic startup contract: ruby-version-timeout", async () => {
  const prepared = await prepareDynamicStartupRuntime(rubyDynamicStartupEntry);
  if (!prepared) {
    return;
  }

  const { runtime, elfBytes } = prepared;
  try {
    let caught: unknown;
    try {
      await runDynamicStartupEntry(runtime, rubyDynamicStartupEntry, elfBytes);
    } catch (error) {
      caught = error;
    }

    const detail = caught instanceof Error ? (caught.stack ?? caught.message) : String(caught ?? "");
    expect(detail, "ruby dynamic startup contract should reproduce lower-bound failure").toContain("timeout after 5000ms");
    expect(detail, "ruby dynamic startup contract should stay on the ruby startup surface").toContain('proc_self_exe="/usr/bin/ruby3.3"');
  } finally {
    runtime.destroy();
  }
}, 20_000);
