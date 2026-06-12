import { afterEach, expect, test } from "vite-plus/test";
import {
  acquireWorker,
  destroyWorkerPool,
  kernelStackSlotForWorker,
  releaseWorker,
} from "../../../../src/worker/pool.js";
import {
  MAX_KERNEL_STACK_SLOTS,
  THREAD_WORKER_KERNEL_STACK_SLOT_BASE,
} from "../../../../src/wasm/stack.js";
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

test("worker pool can allocate every thread-worker stack slot before exhaustion", () => {
  globalThis.Worker = FakeWorker as unknown as typeof Worker;

  const workers = Array.from(
    { length: MAX_KERNEL_STACK_SLOTS - THREAD_WORKER_KERNEL_STACK_SLOT_BASE },
    () => acquireWorker(),
  );

  expect(new Set(workers.map((worker) => kernelStackSlotForWorker(worker))).size).toBe(workers.length);
  expect(kernelStackSlotForWorker(workers.at(-1)!)).toBe(MAX_KERNEL_STACK_SLOTS - 1);
  expect(() => acquireWorker()).toThrow("exhausted kernel thread-worker stack slots");

  for (const worker of workers) {
    releaseWorker(worker);
  }
});
