import { expect, test } from "vite-plus/test";
import { bucketDynamicStartupFailure } from "../support/dynamic-startup-harness.js";

test("dynamic startup bucket: loader faults stay in foundation", () => {
  expect(
    bucketDynamicStartupFailure({
      error: new Error("ELF inspection failed: PT_INTERP missing interpreter"),
    }),
  ).toBe("foundation");
});

test("dynamic startup bucket: process_create failures stay in runtime-ownership", () => {
  expect(
    bucketDynamicStartupFailure({
      error: new Error("plain-fork-child-ready: process_create failed during attach publish"),
    }),
  ).toBe("runtime-ownership");
});

test("dynamic startup bucket: missing shared library stays in consumer-runtime", () => {
  expect(
    bucketDynamicStartupFailure({
      error: new Error("error while loading shared libraries: libjli.so: cannot open shared object file"),
    }),
  ).toBe("consumer-runtime");
});

test("dynamic startup bucket: startup timeout stays conservative", () => {
  expect(
    bucketDynamicStartupFailure({
      error: new Error('timeout after 5000ms\nproc_self_exe="/usr/bin/ruby3.3"'),
    }),
  ).toBe("unknown");
});

test("dynamic startup bucket: guest signal and debug-collection traps stay conservative", () => {
  expect(
    bucketDynamicStartupFailure({
      result: {
        exitCode: 139,
        output: "",
        debugInfo:
          'proc_self_exe="/jdk/bin/java"\nsig_deliver=11\ndebug_collect_error="RuntimeError: operation does not support unaligned accesses"',
      },
    }),
  ).toBe("unknown");
});
