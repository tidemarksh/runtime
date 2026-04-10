import { expect, test } from "vite-plus/test";
import {
  prepareDynamicStartupRuntime,
  runDynamicStartupEntry,
} from "../../support/dynamic-startup-harness.js";
import { javaDynamicStartupEntry } from "./dynamic-startup.js";

const JAVA_STARTUP_SIGNAL_PC = "sig_pc=0xfd7286";

function expectJavaStartupSigsegvSurface(debugInfo: string | undefined, label: string): void {
  expect(
    debugInfo ?? "",
    `${label} should stay on the java startup surface`,
  ).toContain('proc_self_exe="/jdk/bin/java"');
  expect(
    debugInfo ?? "",
    `${label} should retain the guest-side signal-11 evidence`,
  ).toContain("sig_deliver=11");
  expect(
    debugInfo ?? "",
    `${label} should keep the same startup fault site across variants`,
  ).toContain(JAVA_STARTUP_SIGNAL_PC);
}

function expectJavaBaselineThreadWorkerFaultSurface(debugInfo: string | undefined): void {
  expect(
    debugInfo ?? "",
    "java dynamic startup baseline should preserve debug collection fallback evidence",
  ).toMatch(/debug_collect_error=|path_syscalls=\[/);
  expect(
    debugInfo ?? "",
    "java dynamic startup baseline should load libjvm before the fault",
  ).toContain("/jdk/lib/server/libjvm.so");
  expect(
    debugInfo ?? "",
    "java dynamic startup baseline should reach the modules image before the fault",
  ).toContain("/jdk/lib/modules");
  expect(
    debugInfo ?? "",
    "java dynamic startup baseline should probe the archived classes image before the fault",
  ).toContain("/jdk/lib/server/classes.jsa");
}

test("java dynamic startup contract: java-version-sigsegv", async () => {
  const prepared = await prepareDynamicStartupRuntime(javaDynamicStartupEntry);
  if (!prepared) {
    return;
  }

  const { runtime, elfBytes } = prepared;
  try {
    const result = await runDynamicStartupEntry(runtime, javaDynamicStartupEntry, elfBytes);

    expect(result.exitCode, "java dynamic startup contract should reproduce lower-bound exit status").toBe(139);
    expect(
      result.output,
      "java dynamic startup baseline should reach the archived-heap warning before the fault",
    ).toContain("Archived objects are disabled");
    expectJavaStartupSigsegvSurface(result.debugInfo, "java dynamic startup contract");
    expectJavaBaselineThreadWorkerFaultSurface(result.debugInfo);
  } finally {
    runtime.destroy();
  }
}, 20_000);

test("java dynamic startup contract: java-version-xshare-off-sigsegv", async () => {
  const xshareOffEntry = {
    ...javaDynamicStartupEntry,
    argv: ["/jdk/bin/java", "-Xshare:off", "-XX:CompressedClassSpaceSize=64m", "-version"],
  } as const;

  const prepared = await prepareDynamicStartupRuntime(xshareOffEntry);
  if (!prepared) {
    return;
  }

  const { runtime, elfBytes } = prepared;
  try {
    const result = await runDynamicStartupEntry(runtime, xshareOffEntry, elfBytes);

    expect(
      result.exitCode,
      "java dynamic startup xshare-off contract should reproduce lower-bound exit status",
    ).toBe(139);
    expectJavaStartupSigsegvSurface(result.debugInfo, "java dynamic startup xshare-off contract");
  } finally {
    runtime.destroy();
  }
}, 20_000);

test("java dynamic startup contract: java-version-xint-xshare-off-sigsegv", async () => {
  const xintEntry = {
    ...javaDynamicStartupEntry,
    argv: ["/jdk/bin/java", "-Xint", "-Xshare:off", "-XX:CompressedClassSpaceSize=64m", "-version"],
  } as const;

  const prepared = await prepareDynamicStartupRuntime(xintEntry);
  if (!prepared) {
    return;
  }

  const { runtime, elfBytes } = prepared;
  try {
    const result = await runDynamicStartupEntry(runtime, xintEntry, elfBytes);

    expect(
      result.exitCode,
      "java dynamic startup xint xshare-off contract should reproduce lower-bound exit status",
    ).toBe(139);
    expectJavaStartupSigsegvSurface(result.debugInfo, "java dynamic startup xint xshare-off contract");
  } finally {
    runtime.destroy();
  }
}, 20_000);

test("java dynamic startup contract: java-version-serial-singlecpu-sigsegv", async () => {
  const serialEntry = {
    ...javaDynamicStartupEntry,
    argv: [
      "/jdk/bin/java",
      "-Xint",
      "-Xshare:off",
      "-XX:+UseSerialGC",
      "-XX:ActiveProcessorCount=1",
      "-XX:CompressedClassSpaceSize=64m",
      "-version",
    ],
  } as const;

  const prepared = await prepareDynamicStartupRuntime(serialEntry);
  if (!prepared) {
    return;
  }

  const { runtime, elfBytes } = prepared;
  try {
    const result = await runDynamicStartupEntry(runtime, serialEntry, elfBytes);

    expect(
      result.exitCode,
      "java dynamic startup serial singlecpu contract should reproduce lower-bound exit status",
    ).toBe(139);
    expectJavaStartupSigsegvSurface(result.debugInfo, "java dynamic startup serial singlecpu contract");
  } finally {
    runtime.destroy();
  }
}, 20_000);
