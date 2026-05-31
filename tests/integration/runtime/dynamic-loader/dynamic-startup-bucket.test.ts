import { expect, test } from "vite-plus/test";
import {
  bucketDynamicStartupFailure,
  classifyDynamicStartupFailureStatus,
} from "../../../support/dynamic-startup-harness.js";

test("dynamic startup bucket: loader faults stay in foundation", () => {
  expect(
    bucketDynamicStartupFailure({
      error: new Error("ELF inspection failed: PT_INTERP missing interpreter"),
    }),
  ).toBe("foundation");
  expect(
    classifyDynamicStartupFailureStatus({
      error: new Error("ELF inspection failed: PT_INTERP missing interpreter"),
    }),
  ).toBe("foundation-suspected");
});

test("dynamic startup bucket: process_create failures stay in runtime-ownership", () => {
  expect(
    bucketDynamicStartupFailure({
      error: new Error("plain-fork-child-ready: process_create failed during attach publish"),
    }),
  ).toBe("runtime-ownership");
  expect(
    classifyDynamicStartupFailureStatus({
      error: new Error("plain-fork-child-ready: process_create failed during attach publish"),
    }),
  ).toBe("runtime-ownership-suspected");
});

test("dynamic startup bucket: missing shared library stays in consumer-runtime", () => {
  expect(
    bucketDynamicStartupFailure({
      error: new Error("error while loading shared libraries: libexample.so: cannot open shared object file"),
    }),
  ).toBe("consumer-runtime");
  expect(
    classifyDynamicStartupFailureStatus({
      error: new Error("error while loading shared libraries: libexample.so: cannot open shared object file"),
    }),
  ).toBe("consumer-runtime-suspected");
});

test("dynamic startup bucket: startup timeout stays conservative", () => {
  expect(
    bucketDynamicStartupFailure({
      error: new Error('timeout after 5000ms\nproc_self_exe="/usr/bin/guest-runtime"'),
    }),
  ).toBe("unknown");
  expect(
    classifyDynamicStartupFailureStatus({
      error: new Error('timeout after 5000ms\nproc_self_exe="/usr/bin/guest-runtime"'),
    }),
  ).toBe("unclassified");
});

test("dynamic startup bucket: guest signal and debug-collection traps stay conservative", () => {
  expect(
    bucketDynamicStartupFailure({
      result: {
        exitCode: 139,
        output: "",
        debugInfo:
          'proc_self_exe="/usr/bin/guest-runtime"\nsig_deliver=11\ndebug_collect_error="RuntimeError: operation does not support unaligned accesses"',
      },
    }),
  ).toBe("unknown");
  expect(
    classifyDynamicStartupFailureStatus({
      result: {
        exitCode: 139,
        output: "",
        debugInfo:
          'proc_self_exe="/usr/bin/guest-runtime"\nsig_deliver=11\ndebug_collect_error="RuntimeError: operation does not support unaligned accesses"',
      },
    }),
  ).toBe("unclassified");
});
