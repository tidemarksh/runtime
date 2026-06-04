import { expect, test } from "vite-plus/test";
import {
  prepareDynamicStartupRuntime,
  runDynamicStartupEntry,
  type DynamicStartupEntry,
} from "../../../support/dynamic-startup-harness.js";
import { javaDynamicStartupEntry } from "./dynamic-startup.js";

const fourGiB = 4 * 1024 * 1024 * 1024;
const fourGiBPages = fourGiB / 65536;

const javaFourGiBHeadroomEntry: DynamicStartupEntry = {
  ...javaDynamicStartupEntry,
  id: "java-version-memory-headroom-4g",
  guestMemoryPolicyLimitBytes: fourGiB,
  runtimeOptions: {
    ...javaDynamicStartupEntry.runtimeOptions,
    maxProcessMemoryPages: fourGiBPages,
    maxForkMemoryPages: fourGiBPages,
  },
};

test("java dynamic startup diagnostic: 4g memory headroom exposes process memory backing gap", async () => {
  const prepared = await prepareDynamicStartupRuntime(javaFourGiBHeadroomEntry);
  if (!prepared) {
    return;
  }

  const { runtime, elfBytes } = prepared;
  try {
    let detail = "";
    try {
      const result = await runDynamicStartupEntry(
        runtime,
        javaFourGiBHeadroomEntry,
        elfBytes,
      );
      detail = [
        `unexpected exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `debug=${result.debugInfo ?? ""}`,
      ].join("\n");
    } catch (error) {
      detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    }

    expect(detail).toContain("RuntimeError: memory access out of bounds");
    expect(detail).toContain("guest_memory_policy_limit=4294967296");
    expect(detail).toContain("mem_bytes=");
  } finally {
    runtime.destroy();
  }
}, 60_000);
