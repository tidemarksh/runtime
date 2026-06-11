import { expect, test } from "vite-plus/test";
import {
  prepareDynamicStartupRuntime,
  runDynamicStartupEntry,
  type DynamicStartupEntry,
} from "../../../support/dynamic-startup-harness.js";
import { stringifyJavaDiagnosticValue } from "./diagnostic-json.js";
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
    `lastFailure=${stringifyJavaDiagnosticValue(runtime.debugLastFailureSnapshot?.() ?? null)}`,
    `executionStats=${stringifyJavaDiagnosticValue(runtime.debugExecutionStats?.() ?? [])}`,
  ].join("\n");
}

function expectJavaTimeoutDiagnostic(detail: string): void {
  expect(detail).not.toContain("Unsupported satp mode");
  expect(detail).not.toContain("RuntimeError: memory access out of bounds");
  expect(detail).toContain("timeout after 20000ms");
  expect(detail).toContain('"ownerState":"wait:20"');
  expect(detail).toContain('"lastStatusName":"FUTEX_WAIT"');
  expect(detail).toContain('"lastStatusName":"FS"');
  expect(
    detail.includes('"ownerState":"async:6"') || detail.includes('"ownerState":"running"'),
  ).toBe(true);
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

const javaInterpretedNoErrorFileEntry: DynamicStartupEntry = {
  ...javaMemory64HeadroomEntry,
  id: "java-version-memory64-headroom-xint-no-error-file",
  argv: [
    "/jdk/bin/java",
    "-Xint",
    "-Xshare:off",
    "-XX:-CreateCoredumpOnCrash",
    "-XX:ErrorFile=/dev/null",
    "-XX:CompressedClassSpaceSize=64m",
    "-version",
  ],
};

test("java dynamic startup diagnostic: interpreted variant preserves timeout surface", async () => {
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

    expectJavaTimeoutDiagnostic(detail);
  } finally {
    runtime.destroy();
  }
}, 90_000);

test("java dynamic startup diagnostic: error-file-disabled interpreted variant preserves timeout surface", async () => {
  const prepared = await prepareDynamicStartupRuntime(javaInterpretedNoErrorFileEntry);
  if (!prepared) {
    return;
  }

  const { runtime, elfBytes } = prepared;
  try {
    let detail = "";
    try {
      const result = await runDynamicStartupEntry(
        runtime,
        javaInterpretedNoErrorFileEntry,
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

    expectJavaTimeoutDiagnostic(detail);
    expect(detail).not.toContain("hs_err_pid1.log");
    expect(detail).not.toContain("rv=-95");
  } finally {
    runtime.destroy();
  }
}, 120_000);
