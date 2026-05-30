import { expect, test } from "vite-plus/test";
import { expectKernelWorkerResponse } from "../../../../src/worker/kernel-rpc/transport.js";

test("kernel-rpc transport returns the typed response when it matches the request", () => {
  const response = expectKernelWorkerResponse({
    type: "stat-result",
    requestId: 1,
    stat: {
      ino: 1,
      mode: 0o040755,
      size: 0n,
      mtimeMs: 0,
    },
  }, "stat-result", "stat");

  expect(response.stat.ino).toBe(1);
});

test("kernel-rpc transport rejects mismatched response ownership", () => {
  expect(() => expectKernelWorkerResponse({
    type: "process-unregistered",
    requestId: 1,
  }, "stat-result", "stat")).toThrow("unexpected kernel-worker response for stat");
});
