import { processKernelWorkerRequest, setProcessKernelWorkerRequest } from "../../../../src/worker/process-kernel-worker-request-state.js";
import { expect, test, vi } from "vite-plus/test";
import {
  processHostStdio,
  processStdoutPipe,
  setProcessHostStdio,
  setProcessStderrPipe,
  setProcessStdoutPipe,
} from "../../../../src/worker/process-stdio-state.js";
import { writePipe } from "../../../../src/bridge/pipe.js";
import {
  PIPE_READ_OPEN_OFFSET,
  PIPE_SLOT_BYTES,
  PIPE_WRITE_OPEN_OFFSET,
} from "../../../../src/layout/pipe.js";
import {
  drainProcessOutput,
  settleVisibleProcessOutput,
} from "../../../../src/worker/stdio.js";
import { pendingPipeSlotSnapshotCount } from "../../../../src/worker/pending-pipe-slot-state.js";
import {
  makeHostBoundaryHandle,
  makeHostBoundaryKernel,
} from "./helpers.js";

function setPipeEnds(
  memory: WebAssembly.Memory,
  pipeBase: number,
  pipeSlot: number,
  readOpen: number,
  writeOpen: number,
): void {
  const base = pipeBase + pipeSlot * PIPE_SLOT_BYTES;
  const view = new DataView(memory.buffer);
  view.setInt32(base + PIPE_READ_OPEN_OFFSET, readOpen, true);
  view.setInt32(base + PIPE_WRITE_OPEN_OFFSET, writeOpen, true);
}

function seedPipeOutput(
  memory: WebAssembly.Memory,
  pipeBase: number,
  pipeSlot: number,
  text: string,
): void {
  setPipeEnds(memory, pipeBase, pipeSlot, 1, 1);
  const result = writePipe(memory, pipeBase, pipeSlot, new TextEncoder().encode(text));
  if (result.written !== text.length || result.broken) {
    throw new Error(`failed to seed pipe slot ${pipeSlot}`);
  }
  setPipeEnds(memory, pipeBase, pipeSlot, 1, 0);
}

function makePipeBridgeHandle() {
  const memory = new WebAssembly.Memory({ initial: 8, maximum: 8, shared: true });
  const kernel = makeHostBoundaryKernel();
  kernel.memory = memory;
  kernel.abi_max_pipes = () => 64;
  kernel.kernel_pipe_region_bytes = () => 64 * PIPE_SLOT_BYTES;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => {
    if (fd === 1) return 4;
    if (fd === 2) return 5;
    return 0;
  };
  kernel.kernel_ofd_fd_type = (ofdId: number) => (ofdId === 4 || ofdId === 5 ? kernel.abi_fd_type_pipe() : 0);
  kernel.kernel_ofd_ino = (ofdId: number) => {
    if (ofdId === 4) return 4;
    if (ofdId === 5) return 5;
    return 0;
  };
  kernel.supervisor_stdio_output_settle_kind_visible = () => 1;
  kernel.supervisor_stdio_output_settle_kind_inherited_child = () => 2;
  kernel.supervisor_stdio_pipe_read_progress_action_none = () => 0;
  kernel.supervisor_stdio_pipe_read_progress_action_snapshot = () => 1;
  kernel.supervisor_stdio_output_settle_max_passes = (kind: number) => {
    if (kind === 1) return 4;
    if (kind === 2) return 6;
    return -22;
  };
  kernel.supervisor_stdio_output_settle_stable_passes = (kind: number) => {
    if (kind === 1 || kind === 2) return 2;
    return -22;
  };
  kernel.supervisor_stdio_pipe_read_progress_action = (_vmPtr: number, _pipeSlot: number, drained: number) =>
    drained === 0 ? 0 : 1;

  const worker = { postMessage: vi.fn() } as unknown as Worker;
  const handle = makeHostBoundaryHandle(kernel, worker);
  handle.memory = memory;
  handle.kernelState = {
    ...handle.kernelState,
    pipeBase: 0x1000,
  };
  setProcessStdoutPipe(handle, 4);
  setProcessStderrPipe(handle, 5);
  setProcessHostStdio(handle, {
    ...processHostStdio(handle),
    stdoutPipe: 4,
    stderrPipe: 5,
  });
  return { handle, kernel, memory };
}

test("fd pipe bridge follows supervisor read-progress action while draining stdout bytes", () => {
  const { handle, kernel, memory } = makePipeBridgeHandle();
  const readProgressAction = vi.fn(() => 0);
  kernel.supervisor_stdio_pipe_read_progress_action = readProgressAction;
  const drainedChunks: string[] = [];
  handle.onStdout = (data) => {
    drainedChunks.push(new TextDecoder().decode(data));
  };
  seedPipeOutput(memory, handle.kernelState.pipeBase, processStdoutPipe(handle), "stdout-tail\n");

  expect(drainProcessOutput(handle)).toBe(true);

  expect(readProgressAction).toHaveBeenCalledWith(handle.vmPtr, processStdoutPipe(handle), 1);
  expect(drainedChunks).toEqual(["stdout-tail\n"]);
  expect(pendingPipeSlotSnapshotCount(handle)).toBe(0);
});

test("fd pipe bridge follows supervisor visible-output settle plan", async () => {
  const { handle, kernel, memory } = makePipeBridgeHandle();
  kernel.supervisor_stdio_output_settle_max_passes = vi.fn(() => 1);
  kernel.supervisor_stdio_output_settle_stable_passes = vi.fn(() => 2);
  const drainedChunks: string[] = [];
  handle.onStdout = (data) => {
    drainedChunks.push(new TextDecoder().decode(data));
  };
  setProcessKernelWorkerRequest(handle, vi.fn(async () => ({ type: "pipe-state-synced" } as never)));
  let syncCount = 0;

  await settleVisibleProcessOutput(handle, async () => {
    syncCount += 1;
    if (syncCount === 2) {
      seedPipeOutput(memory, handle.kernelState.pipeBase, processStdoutPipe(handle), "late-out\n");
    }
  });

  expect(kernel.supervisor_stdio_output_settle_max_passes).toHaveBeenCalledWith(1);
  expect(kernel.supervisor_stdio_output_settle_stable_passes).toHaveBeenCalledWith(1);
  expect(syncCount).toBe(1);
  expect(drainedChunks).toEqual([]);
});
