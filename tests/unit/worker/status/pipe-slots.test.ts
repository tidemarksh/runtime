import { expect, test, vi } from "vite-plus/test";
import {
  PIPE_GENERATION_OFFSET,
  PIPE_HEAD_OFFSET,
  PIPE_HEADER_BYTES,
  PIPE_READ_OPEN_OFFSET,
  PIPE_SLOT_MODE_CONTROL,
  PIPE_SLOT_MODE_READ,
  PIPE_SLOT_MODE_WRITE,
  PIPE_TAIL_OFFSET,
  PIPE_WRITE_OPEN_OFFSET,
} from "../../../../src/layout/pipe.js";
import {
  applyStatusPipeSlots,
  statusKernelWorkerOwnsPipeState,
} from "../../../../src/worker/message-handler/status/pipe-slots.js";
import { makeHandle, makeKernel } from "./helpers.js";

const SLOT_BYTES = 64;

function makePipeSlot(readOpen: number, writeOpen: number, generation: number) {
  const slot = new Uint8Array(SLOT_BYTES);
  const view = new DataView(slot.buffer);
  view.setUint32(PIPE_HEAD_OFFSET, 0, true);
  view.setUint32(PIPE_TAIL_OFFSET, 0, true);
  view.setInt32(PIPE_READ_OPEN_OFFSET, readOpen, true);
  view.setInt32(PIPE_WRITE_OPEN_OFFSET, writeOpen, true);
  view.setUint32(PIPE_GENERATION_OFFSET, generation, true);
  slot[PIPE_HEADER_BYTES] = 0x61;
  return slot;
}

test("status pipe slots apply fresh control snapshots through the status owner", () => {
  const handle = makeHandle({
    kernel: makeKernel({
      abi_max_pipes: () => 4,
      kernel_pipe_region_bytes: () => SLOT_BYTES * 4,
    }),
  });
  const appendRuntimeLifecycleEvent = vi.fn();

  applyStatusPipeSlots(
    handle as never,
    [{
      pipeSlot: 1,
      slot: makePipeSlot(1, 1, 2),
      mode: PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE | PIPE_SLOT_MODE_CONTROL,
      fresh: true,
    }],
    appendRuntimeLifecycleEvent,
  );

  const view = new DataView(
    (handle.memory as WebAssembly.Memory).buffer,
    (handle.kernelState as { pipeBase: number }).pipeBase + SLOT_BYTES,
    SLOT_BYTES,
  );
  expect(view.getInt32(PIPE_READ_OPEN_OFFSET, true)).toBe(1);
  expect(view.getInt32(PIPE_WRITE_OPEN_OFFSET, true)).toBe(1);
  expect(view.getUint32(PIPE_GENERATION_OFFSET, true)).toBe(2);
  expect(appendRuntimeLifecycleEvent).toHaveBeenCalled();
});

test("status pipe ownership reports kernel-worker owned state only for active requests", () => {
  expect(statusKernelWorkerOwnsPipeState(makeHandle({ kernelWorkerRequest: {} }) as never)).toBe(true);
  expect(statusKernelWorkerOwnsPipeState(makeHandle({ kernelWorkerRequest: undefined }) as never)).toBe(false);
  expect(statusKernelWorkerOwnsPipeState(makeHandle({ pid: 0, kernelWorkerRequest: {} }) as never)).toBe(false);
});
