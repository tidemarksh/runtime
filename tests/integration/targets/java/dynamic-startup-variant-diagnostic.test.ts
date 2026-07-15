import { expect, test } from "vite-plus/test";
import {
  prepareDynamicStartupRuntime,
  runDynamicStartupEntry,
  type DynamicStartupEntry,
} from "../../../support/dynamic-startup-harness.js";
import { javaDynamicStartupEntry, javaMemory64HeadroomEntry } from "./dynamic-startup.js";

function expectJavaCleanExit(result: { exitCode: number; output: string; debugInfo?: string }): void {
  const detail = `output=${JSON.stringify(result.output)}\ndebug=${result.debugInfo ?? ""}`;
  expect(result.exitCode, detail).toBe(0);
  expect(result.output).toContain('openjdk version "25.0.2"');
  expect(detail).not.toContain("A fatal error has been detected by the Java Runtime Environment");
  expect(detail).not.toContain("SIGSEGV");
  expect(detail).not.toContain("SIGILL");
  expect(detail).not.toContain("hs_err_pid1.log");
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

const javaStrictNoErrorFileEntry: DynamicStartupEntry = {
  ...javaDynamicStartupEntry,
  id: "java-version-strict-no-error-file",
  argv: [
    "/jdk/bin/java",
    "-Xshare:off",
    "-XX:-CreateCoredumpOnCrash",
    "-XX:ErrorFile=/dev/null",
    "-XX:CompressedClassSpaceSize=64m",
    "-version",
  ],
};

test("java dynamic startup diagnostic: interpreted variant exits cleanly", async () => {
  const prepared = await prepareDynamicStartupRuntime(javaInterpretedHeadroomEntry);
  if (!prepared) {
    return;
  }

  const { runtime, elfBytes } = prepared;
  try {
    const result = await runDynamicStartupEntry(runtime, javaInterpretedHeadroomEntry, elfBytes);
    expectJavaCleanExit(result);
  } finally {
    runtime.destroy();
  }
}, 90_000);

test("java dynamic startup diagnostic: error-file-disabled interpreted variant exits cleanly", async () => {
  const prepared = await prepareDynamicStartupRuntime(javaInterpretedNoErrorFileEntry);
  if (!prepared) {
    return;
  }

  const { runtime, elfBytes } = prepared;
  try {
    const result = await runDynamicStartupEntry(runtime, javaInterpretedNoErrorFileEntry, elfBytes);
    expectJavaCleanExit(result);
  } finally {
    runtime.destroy();
  }
}, 120_000);

test("java dynamic startup diagnostic: strict error-file-disabled variant exits cleanly", async () => {
  const prepared = await prepareDynamicStartupRuntime(javaStrictNoErrorFileEntry);
  if (!prepared) {
    return;
  }

  const { runtime, elfBytes } = prepared;
  try {
    const result = await runDynamicStartupEntry(runtime, javaStrictNoErrorFileEntry, elfBytes);
    expectJavaCleanExit(result);
  } finally {
    runtime.destroy();
  }
}, 60_000);
