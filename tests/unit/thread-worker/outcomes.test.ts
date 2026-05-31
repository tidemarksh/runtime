// Thread-worker tests in this file validate typed scheduler/sync boundary outcomes.
import { expect, test } from "vite-plus/test";
import {
  createThreadWorkerStatusOutcome,
  publishThreadWorkerStatusOutcome,
} from "../../../src/thread-worker/outcomes.js";

test("createThreadWorkerStatusOutcome records scheduler status boundaries", () => {
  expect(createThreadWorkerStatusOutcome(3)).toEqual({
    type: "status",
    status: 3,
    waitInWorker: undefined,
    syncEffects: undefined,
  });
});

test("createThreadWorkerStatusOutcome records blocking and sync-effect outcomes explicitly", () => {
  expect(createThreadWorkerStatusOutcome(20, true, 4)).toEqual({
    type: "status",
    status: 20,
    waitInWorker: true,
    syncEffects: 4,
  });
});

test("publishThreadWorkerStatusOutcome preserves the legacy scheduler callback shape", () => {
  const calls: unknown[][] = [];
  const publish = (...args: unknown[]) => {
    calls.push(args);
  };

  publishThreadWorkerStatusOutcome(publish, createThreadWorkerStatusOutcome(3));
  publishThreadWorkerStatusOutcome(publish, createThreadWorkerStatusOutcome(20, true));
  publishThreadWorkerStatusOutcome(publish, createThreadWorkerStatusOutcome(0, false, 4));

  expect(calls).toEqual([
    [3],
    [20, true],
    [0, false, 4],
  ]);
});
