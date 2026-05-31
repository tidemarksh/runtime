// Thread-worker tests in this file validate inbound state application ownership.
import { expect, test, vi } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../src/abi.js";
import { applyThreadWorkerInboundState } from "../../../src/thread-worker/inbound-state.js";

function makeState(): KernelRuntimeState {
  return {
    heapBase: 0x10,
    heapPtr: 0x20,
    ofdTablePtr: 0x40,
    memfsPtr: 0,
    memfsHeaderBytes: 0,
    memfsInodesBytes: 0,
    memfsFirstChildBytes: 0,
    memfsDirEntriesBytes: 0,
    memfsDataBytes: 0,
    memStatePtr: 0,
    epollTablePtr: 0,
    socketTablePtr: 0xc0,
    socketUsed: 1,
    pipeBase: 0x80,
    pipeUsed: 0n,
    netBufBase: 0xe0,
    netBufUsed: 1n,
    sigSlotsPtr: 0,
    processPendingPtr: 0,
    processRuntimeStatePtr: 0,
    timeStatePtr: 0,
  };
}

function makeKernelExports(memory: WebAssembly.Memory): KernelExports {
  return {
    memory,
    abi_max_fds: () => 8,
    abi_max_pipes: () => 4,
    abi_max_sockets: () => 2,
    kernel_ofd_table_bytes: () => 64,
    kernel_pipe_region_bytes: () => 64,
    kernel_socket_table_bytes: () => 16,
    kernel_net_buf_pool_bytes: () => 32,
    process_set_fd_entry: vi.fn(),
    process_record_child_exit: vi.fn(),
    process_record_child_exit_for_parent: vi.fn(),
    debug_clear_guest_write_watch_trace: vi.fn(),
    debug_configure_guest_write_watch: vi.fn(),
    kernel_attach_runtime_state: vi.fn(),
  } as unknown as KernelExports;
}

test("applyThreadWorkerInboundState applies fd, pipe, socket, child, guest write, and watch state", () => {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const bytes = new Uint8Array(memory.buffer);
  const kernelExports = makeKernelExports(memory);
  const kernelState = makeState();

  applyThreadWorkerInboundState({
    kernelExports,
    guestBase: 0x300,
  }, {
    kernelState,
    targetVmPtr: 0x1234,
    ofdSlots: [{ ofdId: 2, slot: Uint8Array.from([1, 2, 3, 4, 5, 6, 7, 8]) }],
    fdEntries: [{ fd: 3, ofdId: 2, fdFlags: 0x40 }],
    socketState: {
      socketTable: Uint8Array.from({ length: 16 }, (_, index) => 0x50 + index),
      netBufSlots: [{ bufferIndex: 1, bytes: Uint8Array.from([9, 8, 7, 6, 5, 4, 3, 2]) }],
      socketUsed: 4,
      netBufUsed: 2n,
    },
    childExitRecords: [
      { childPid: 20, waitStatus: 0x100, parentPid: 10 },
      { childPid: 30, waitStatus: 0x200 },
    ],
    pipeSlots: [{ pipeSlot: 2, slot: Uint8Array.from({ length: 16 }, (_, index) => 0xa0 + index) }],
    guestMemoryWrites: [{ guestPtr: 4, bytes: Uint8Array.from([0xde, 0xad]) }],
    guestWriteWatchRegions: [{ slot: 1, start: 0x10, end: 0x20 }],
  });

  expect([...bytes.slice(0x50, 0x58)]).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
  expect([...bytes.slice(0xa0, 0xb0)]).toEqual(
    Array.from({ length: 16 }, (_, index) => 0xa0 + index),
  );
  expect([...bytes.slice(0xc0, 0xd0)]).toEqual(
    Array.from({ length: 16 }, (_, index) => 0x50 + index),
  );
  expect([...bytes.slice(0xe8, 0xf0)]).toEqual([9, 8, 7, 6, 5, 4, 3, 2]);
  expect([...bytes.slice(0x304, 0x306)]).toEqual([0xde, 0xad]);

  expect(kernelExports.process_set_fd_entry).toHaveBeenCalledWith(0x1234, 3, 2, 0x40);
  expect(kernelExports.process_record_child_exit_for_parent).toHaveBeenCalledWith(10, 20, 0x100);
  expect(kernelExports.process_record_child_exit).toHaveBeenCalledWith(30, 0x200);
  expect(kernelExports.debug_clear_guest_write_watch_trace).toHaveBeenCalledTimes(1);
  expect(kernelExports.debug_configure_guest_write_watch).toHaveBeenCalledWith(1, 0x10n, 0x20n);

  const attachCall = vi.mocked(kernelExports.kernel_attach_runtime_state).mock.calls.at(-1);
  expect(attachCall?.[7]).toBe(5);
  expect(attachCall?.[11]).toBe(3n);
});
