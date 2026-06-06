import { expect, test } from "vite-plus/test";
import {
  prepareDynamicStartupRuntime,
  runDynamicStartupEntry,
  type DynamicStartupEntry,
} from "../../../support/dynamic-startup-harness.js";
import { javaMemory64HeadroomEntry } from "./dynamic-startup.js";

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

const javaInterpretedHeadroomEntry: DynamicStartupEntry = {
  ...javaMemory64HeadroomEntry,
  id: "java-version-memory64-headroom-xint",
  argv: [
    "/jdk/bin/java",
    "-Xint",
    "-Xshare:off",
    "-XX:CompressedClassSpaceSize=64m",
    "-version",
  ],
};

test("java dynamic startup diagnostic: interpreted variant preserves fatal JVM signal", async () => {
  const prepared = await prepareDynamicStartupRuntime(javaInterpretedHeadroomEntry);
  if (!prepared) {
    return;
  }

  const { runtime, elfBytes } = prepared;
  try {
    let detail = "";
    try {
      const result = await runDynamicStartupEntry(
        runtime,
        javaInterpretedHeadroomEntry,
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

    expect(detail).not.toContain("Unsupported satp mode");
    expect(detail).not.toContain("RuntimeError: memory access out of bounds");
    expect(detail).toContain("unexpected exit=134");
    expect(detail).toContain("A fatal error has been detected by the Java Runtime Environment");
    expect(detail).toContain("SIGSEGV");
  } finally {
    runtime.destroy();
  }
}, 90_000);
