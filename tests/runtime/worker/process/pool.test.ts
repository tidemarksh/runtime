import { afterEach, expect, test } from "vite-plus/test";
import {
  acquireWorker,
  destroyWorkerPool,
  kernelStackSlotForWorker,
  releaseWorker,
} from "../../../../src/worker/pool.js";
import { FakeWorker } from "../../../support/fake-worker.js";

const originalWorker = globalThis.Worker;

afterEach(() => {
  destroyWorkerPool();
  globalThis.Worker = originalWorker;
});

test("temporary worker allocated beyond pooled capacity still receives a kernel stack slot", () => {
  globalThis.Worker = FakeWorker as unknown as typeof Worker;

  const pooled = [acquireWorker(), acquireWorker(), acquireWorker(), acquireWorker()];
  const pooledSlots = pooled.map((worker) => kernelStackSlotForWorker(worker));

  const temporary = acquireWorker();
  const temporarySlot = kernelStackSlotForWorker(temporary);

  expect(temporarySlot).toBeGreaterThanOrEqual(2);
  expect(pooledSlots).not.toContain(temporarySlot);

  releaseWorker(temporary);
  expect((temporary as unknown as FakeWorker).terminated).toBe(true);
});
