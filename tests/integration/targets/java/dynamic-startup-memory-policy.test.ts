import { expect, test } from "vite-plus/test";
import {
  prepareDynamicStartupRuntime,
  runDynamicStartupEntry,
} from "../../../support/dynamic-startup-harness.js";
import {
  javaFourGiBHeadroomEntry,
  javaMemory64HeadroomEntry,
} from "./dynamic-startup.js";

function appendRuntimeFailureEvidence(
  detail: string,
  runtime: {
    debugExecutionStats?: () => unknown;
    debugLastFailureSnapshot?: () => unknown;
  },
): string {
  return [
    detail,
    `lastFailure=${JSON.stringify(runtime.debugLastFailureSnapshot?.() ?? null)}`,
    `executionStats=${JSON.stringify(runtime.debugExecutionStats?.() ?? [])}`,
  ].join("\n");
}

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
    detail = appendRuntimeFailureEvidence(detail, runtime);

    expect(detail).not.toContain("RuntimeError: memory access out of bounds");
    expect(detail).toContain("unexpected exit=134");
    expect(detail).toContain("A fatal error has been detected by the Java Runtime Environment");
    expect(detail).toContain("SIGSEGV");
    expect(detail).toContain("\"guestMemoryPolicyLimitBytes\":\"4294967296\"");
    expect(detail).toContain("\"processMaxMemoryBytes\":4294967296");
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
    detail = appendRuntimeFailureEvidence(detail, runtime);

    expect(detail).not.toContain("module's declared maximum 65536");
    expect(detail).not.toContain("RuntimeError: memory access out of bounds");
    expect(detail).toContain("unexpected exit=134");
    expect(detail).toContain("A fatal error has been detected by the Java Runtime Environment");
    expect(detail).toContain("SIGSEGV");
    expect(detail).toContain("\"guestMemoryPolicyLimitBytes\":\"4295032832\"");
    expect(detail).toContain("\"processMaxMemoryBytes\":4295032832");
    expect(detail).toContain("mem_bytes=");
  } finally {
    runtime.destroy();
  }
}, 60_000);
