import { expect, test } from "vite-plus/test";
import {
  prepareDynamicStartupRuntime,
  runDynamicStartupEntry,
} from "../../../support/dynamic-startup-harness.js";
import {
  javaFourGiBHeadroomEntry,
  javaMemory64HeadroomEntry,
} from "./dynamic-startup.js";
import { stringifyJavaDiagnosticValue } from "./diagnostic-json.js";

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

function expectJavaTimeoutDiagnostic(
  detail: string,
  expectedMemoryLimitBytes: number,
): void {
  expect(detail).not.toContain("RuntimeError: memory access out of bounds");
  expect(detail).toContain("timeout after 20000ms");
  expect(detail).toContain(
    `"guestMemoryPolicyLimitBytes":"${expectedMemoryLimitBytes}"`,
  );
  expect(detail).toContain(`"processMaxMemoryBytes":${expectedMemoryLimitBytes}`);
  expect(detail).toContain('"memoryBytes":');
  expect(detail).toContain('"processCommittedBytes":');
  expect(detail).toContain('"ownerState":"wait:20"');
  expect(detail).toContain('"lastStatusName":"FUTEX_WAIT"');
  expect(detail).toContain('"lastStatusName":"FS"');
  expect(
    detail.includes('"ownerState":"async:6"') || detail.includes('"ownerState":"running"'),
  ).toBe(true);
}

test("java dynamic startup diagnostic: 4g memory headroom preserves timeout surface", async () => {
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

    expectJavaTimeoutDiagnostic(detail, 4_294_967_296);
  } finally {
    runtime.destroy();
  }
}, 60_000);

test("java dynamic startup diagnostic: memory64 headroom preserves timeout surface", async () => {
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
    expectJavaTimeoutDiagnostic(detail, 4_295_032_832);
  } finally {
    runtime.destroy();
  }
}, 60_000);
