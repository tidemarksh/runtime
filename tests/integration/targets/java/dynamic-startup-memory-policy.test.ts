import { expect, test } from "vite-plus/test";
import {
  prepareDynamicStartupRuntime,
  resolveDynamicStartupTestTimeoutMs,
  runDynamicStartupEntry,
} from "../../../support/dynamic-startup-harness.js";
import {
  javaDynamicStartupEntry,
  javaFourGiBHeadroomEntry,
  javaMemory64HeadroomEntry,
} from "./dynamic-startup.js";

const oneGiB = 1024 * 1024 * 1024;
const oneGiBPages = oneGiB / 65536;

const javaOneGiBPolicyMatchedProcessMaxEntry = {
  ...javaDynamicStartupEntry,
  id: "java-version-policy-matched-process-max-1g",
  guestMemoryPolicyLimitBytes: oneGiB,
  runtimeOptions: {
    ...javaDynamicStartupEntry.runtimeOptions,
    maxProcessMemoryPages: oneGiBPages,
    maxForkMemoryPages: oneGiBPages,
  },
};

const javaOneGiBPolicyDefaultProcessBackingEntry = {
  ...javaDynamicStartupEntry,
  id: "java-version-policy-1g-default-process-backing",
  guestMemoryPolicyLimitBytes: oneGiB,
  runtimeOptions: {},
};

function expectJavaCleanExit(result: { exitCode: number; output: string; debugInfo?: string }): void {
  const detail = `output=${JSON.stringify(result.output)}\ndebug=${result.debugInfo ?? ""}`;
  expect(result.exitCode, detail).toBe(0);
  expect(result.output).toContain('openjdk version "25.0.2"');
  expectNoJvmFatalSignal(detail);
}

function expectNoJvmFatalSignal(detail: string): void {
  expect(detail).not.toContain(
    "A fatal error has been detected by the Java Runtime Environment",
  );
  expect(detail).not.toContain("SIGSEGV");
  expect(detail).not.toContain("SIGILL");
  expect(detail).not.toContain("hs_err_pid1.log");
}

function expectJvmNativeMemoryLimitExit(result: { exitCode: number; output: string; debugInfo?: string }): void {
  const detail = `output=${JSON.stringify(result.output)}\ndebug=${result.debugInfo ?? ""}`;
  expect(result.exitCode, detail).toBe(1);
  expect(result.output).toContain("os::commit_memory");
  expect(result.output).toContain("There is insufficient memory for the Java Runtime Environment");
  expect(result.output).toContain("Native memory allocation (mmap) failed");
  expect(detail).not.toContain("SIGSEGV");
  expect(detail).not.toContain("SIGILL");
  expect(result.output).not.toContain('openjdk version "25.0.2"');
}

test("java dynamic startup diagnostic: 4g memory headroom exits cleanly", async () => {
  const prepared = await prepareDynamicStartupRuntime(javaFourGiBHeadroomEntry);
  if (!prepared) {
    return;
  }

  const { runtime, elfBytes } = prepared;
  try {
    const result = await runDynamicStartupEntry(runtime, javaFourGiBHeadroomEntry, elfBytes);
    expectJavaCleanExit(result);
  } finally {
    runtime.destroy();
  }
}, resolveDynamicStartupTestTimeoutMs(javaFourGiBHeadroomEntry));

test("java dynamic startup diagnostic: 1g policy matched process max exits with ENOMEM", async () => {
  const prepared = await prepareDynamicStartupRuntime(javaOneGiBPolicyMatchedProcessMaxEntry);
  if (!prepared) {
    return;
  }

  const { runtime, elfBytes } = prepared;
  try {
    const result = await runDynamicStartupEntry(runtime, javaOneGiBPolicyMatchedProcessMaxEntry, elfBytes);
    expectJvmNativeMemoryLimitExit(result);
  } finally {
    runtime.destroy();
  }
}, resolveDynamicStartupTestTimeoutMs(javaOneGiBPolicyMatchedProcessMaxEntry));

test("java dynamic startup diagnostic: 1g policy with default process backing exits with ENOMEM", async () => {
  const prepared = await prepareDynamicStartupRuntime(javaOneGiBPolicyDefaultProcessBackingEntry);
  if (!prepared) {
    return;
  }

  const { runtime, elfBytes } = prepared;
  try {
    const result = await runDynamicStartupEntry(
      runtime,
      javaOneGiBPolicyDefaultProcessBackingEntry,
      elfBytes,
    );
    expectJvmNativeMemoryLimitExit(result);
  } finally {
    runtime.destroy();
  }
}, resolveDynamicStartupTestTimeoutMs(javaOneGiBPolicyDefaultProcessBackingEntry));

test("java dynamic startup diagnostic: memory64 headroom exits cleanly", async () => {
  const prepared = await prepareDynamicStartupRuntime(javaMemory64HeadroomEntry);
  if (!prepared) {
    return;
  }

  const { runtime, elfBytes } = prepared;
  try {
    const result = await runDynamicStartupEntry(runtime, javaMemory64HeadroomEntry, elfBytes);
    expectJavaCleanExit(result);
  } finally {
    runtime.destroy();
  }
}, resolveDynamicStartupTestTimeoutMs(javaMemory64HeadroomEntry));
