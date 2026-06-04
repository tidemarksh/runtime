import { expect, test } from "vite-plus/test";
import {
  prepareDynamicStartupRuntime,
  runDynamicStartupEntry,
  type DynamicStartupEntry,
} from "../../../support/dynamic-startup-harness.js";
import { javaDynamicStartupEntry } from "./dynamic-startup.js";

const fourGiB = 4 * 1024 * 1024 * 1024;
const fourGiBPages = fourGiB / 65536;
const memory64HeadroomPages = fourGiBPages + 1;
const memory64HeadroomBytes = memory64HeadroomPages * 65536;

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

const javaMemory64HeadroomEntry: DynamicStartupEntry = {
  ...javaDynamicStartupEntry,
  id: "java-version-memory64-headroom",
  guestMemoryPolicyLimitBytes: memory64HeadroomBytes,
  runtimeOptions: {
    ...javaDynamicStartupEntry.runtimeOptions,
    maxProcessMemoryPages: memory64HeadroomPages,
    maxForkMemoryPages: memory64HeadroomPages,
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

test("java dynamic startup diagnostic: memory64 headroom exposes remaining backing gap", async () => {
  const prepared = await prepareDynamicStartupRuntime(javaMemory64HeadroomEntry);
  if (!prepared) {
    return;
  }

  const { runtime, elfBytes } = prepared;
  try {
    let detail = "";
    try {
      const result = await runDynamicStartupEntry(
        runtime,
        javaMemory64HeadroomEntry,
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

    expect(detail).not.toContain("module's declared maximum 65536");
    expect(detail).toContain("RuntimeError: memory access out of bounds");
    expect(detail).toContain("guest_memory_policy_limit=4295032832");
    expect(detail).toContain("mem_bytes=");
  } finally {
    runtime.destroy();
  }
}, 60_000);
