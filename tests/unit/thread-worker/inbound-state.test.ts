// Thread-worker tests in this file validate inbound state application ownership.
import { expect, test, vi } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../src/abi.js";
import {
  applyThreadWorkerInboundState,
  consumeThreadWorkerInboundGuestWriteDebug,
} from "../../../src/thread-worker/inbound-state.js";
import {
  createRuntimeStateBridge,
  publishRuntimeFdAuthorityEpoch,
} from "../../../src/bridge/runtime-state.js";

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
    process_fd_ofd_id: () => 0,
    process_fd_flags: () => 0,
    process_set_fd_entry: vi.fn(),
    process_record_child_exit: vi.fn(),
    process_record_child_exit_for_parent: vi.fn(),
    thread_process_ptr: () => 0x2000,
    debug_clear_guest_write_watch_trace: vi.fn(),
    debug_configure_guest_write_watch: vi.fn(),
    kernel_attach_runtime_state: vi.fn(),
  } as unknown as KernelExports;
}

test("applyThreadWorkerInboundState applies fd, pipe, socket, guest write, and watch state", () => {
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
  expect(kernelExports.debug_clear_guest_write_watch_trace).toHaveBeenCalledTimes(1);
  expect(kernelExports.debug_configure_guest_write_watch).toHaveBeenCalledWith(1, 0x10n, 0x20n);

  const attachCall = vi.mocked(kernelExports.kernel_attach_runtime_state).mock.calls.at(-1);
  expect(attachCall?.[7]).toBe(5);
  expect(attachCall?.[11]).toBe(3n);
});

test("applyThreadWorkerInboundState delegates exited-thread publications to Kernel", () => {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const kernelExports = makeKernelExports(memory);
  const applyPublication = vi.fn(() => 0);
  kernelExports.supervisor_apply_exited_thread_publication = applyPublication;

  applyThreadWorkerInboundState({ kernelExports, guestBase: 0 }, {
    kernelState: makeState(),
    targetVmPtr: 0x1234,
    exitedThreads: [
      { vmPtr: 0x2000, tid: 2 },
      { vmPtr: 0x3000, tid: 3 },
    ],
  });

  expect(applyPublication).toHaveBeenNthCalledWith(1, 0x1234, 0x2000, 2);
  expect(applyPublication).toHaveBeenNthCalledWith(2, 0x1234, 0x3000, 3);
});

test("applyThreadWorkerInboundState preserves a live OFD when Kernel rejects an inbound collision", () => {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const bytes = new Uint8Array(memory.buffer);
  const kernelExports = makeKernelExports(memory);
  const kernelState = makeState();
  const slotBytes = 32;
  const blockedSlot = Uint8Array.from({ length: slotBytes }, (_, index) => 0xa0 + index);
  bytes.set(blockedSlot, kernelState.ofdTablePtr + 2 * slotBytes);
  const decideWrite = vi.fn((ofdId: number) => ofdId === 2 ? 1 : 0);
  Object.assign(kernelExports, {
    kernel_ofd_table_bytes: () => slotBytes * kernelExports.abi_max_fds(),
    supervisor_fd_entry_restore_action_skip: () => 1,
    supervisor_ofd_slot_write_action: decideWrite,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 4 ? 4 : 0,
  });

  const collidingSlot = new Uint8Array(slotBytes);
  const acceptedSlot = new Uint8Array(slotBytes);
  new DataView(acceptedSlot.buffer).setInt32(0, 3, true);

  applyThreadWorkerInboundState({
    kernelExports,
    guestBase: 0,
  }, {
    kernelState,
    targetVmPtr: 0x1234,
    ofdSlots: [
      { ofdId: 2, slot: collidingSlot },
      { ofdId: 3, slot: acceptedSlot },
    ],
    fdEntries: [
      { fd: 4, ofdId: 0, fdFlags: 0 },
      { fd: 5, ofdId: 2, fdFlags: 0 },
      { fd: 6, ofdId: 3, fdFlags: 0 },
    ],
  });

  expect([...bytes.slice(
    kernelState.ofdTablePtr + 2 * slotBytes,
    kernelState.ofdTablePtr + 3 * slotBytes,
  )]).toEqual([...blockedSlot]);
  expect([...bytes.slice(
    kernelState.ofdTablePtr + 3 * slotBytes,
    kernelState.ofdTablePtr + 4 * slotBytes,
  )]).toEqual([...acceptedSlot]);
  expect(kernelExports.process_set_fd_entry).toHaveBeenCalledWith(0x1234, 4, 0, 0);
  expect(kernelExports.process_set_fd_entry).toHaveBeenCalledWith(0x1234, 5, 2, 0);
  expect(kernelExports.process_set_fd_entry).toHaveBeenCalledWith(0x1234, 6, 3, 0);

  expect(decideWrite).toHaveBeenCalledTimes(2);
});

test("applyThreadWorkerInboundState asks Kernel before replacing a live shared OFD slot", () => {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const bytes = new Uint8Array(memory.buffer);
  const kernelExports = makeKernelExports(memory);
  const kernelState = makeState();
  const slotBytes = 32;
  const liveSlot = Uint8Array.from({ length: slotBytes }, (_, index) => 0x80 + index);
  bytes.set(liveSlot, kernelState.ofdTablePtr + 3 * slotBytes);
  const decideWrite = vi.fn(() => 1);
  Object.assign(kernelExports, {
    kernel_ofd_table_bytes: () => slotBytes * kernelExports.abi_max_fds(),
    supervisor_fd_entry_restore_action_skip: () => 1,
    supervisor_ofd_slot_write_action: decideWrite,
  });

  applyThreadWorkerInboundState({
    kernelExports,
    guestBase: 0,
  }, {
    kernelState,
    targetVmPtr: 0x1234,
    ofdSlots: [{ ofdId: 3, slot: new Uint8Array(slotBytes) }],
  });

  expect(decideWrite).toHaveBeenCalledWith(3, 0, 0, 0, 0n);
  expect([...bytes.slice(
    kernelState.ofdTablePtr + 3 * slotBytes,
    kernelState.ofdTablePtr + 4 * slotBytes,
  )]).toEqual([...liveSlot]);
});

test("applyThreadWorkerInboundState preserves a higher live OFD refcount", () => {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const kernelExports = makeKernelExports(memory);
  const kernelState = makeState();
  const slotBytes = 32;
  Object.assign(kernelExports, {
    kernel_ofd_table_bytes: () => slotBytes * kernelExports.abi_max_fds(),
    supervisor_fd_entry_restore_action_skip: () => 2,
    supervisor_ofd_slot_write_action: () => 1,
    supervisor_ofd_fd_type: (ofdId: number) => new DataView(
      memory.buffer,
      kernelState.ofdTablePtr + ofdId * slotBytes,
      slotBytes,
    ).getInt32(0, true),
  });
  const current = new DataView(memory.buffer, kernelState.ofdTablePtr + 3 * slotBytes, slotBytes);
  current.setInt32(0, 7, true);
  current.setInt32(12, 2, true);
  const candidate = new Uint8Array(slotBytes);
  const candidateView = new DataView(candidate.buffer);
  candidateView.setInt32(0, 7, true);
  candidateView.setInt32(12, 1, true);

  applyThreadWorkerInboundState({ kernelExports, guestBase: 0 }, {
    kernelState,
    targetVmPtr: 0x1234,
    ofdSlots: [{ ofdId: 3, slot: candidate }],
  });

  expect(current.getInt32(12, true)).toBe(2);
});

test("applyThreadWorkerInboundState asks Kernel to restore canonical process identity", () => {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const kernelExports = makeKernelExports(memory);
  const restoreCurrentIdentity = vi.fn();
  Object.assign(kernelExports, {
    process_current_pid: () => 2,
    process_current_ppid: () => 1,
    process_restore_current_identity: restoreCurrentIdentity,
  });

  applyThreadWorkerInboundState({
    kernelExports,
    guestBase: 0,
  }, {
    kernelState: makeState(),
    targetVmPtr: 0x1234,
    processIdentity: { processPtr: 0x2000, pid: 6, ppid: 1, nextPid: 7 },
  });

  expect(restoreCurrentIdentity).toHaveBeenCalledWith(0x1234, 6, 1, 7);
});

test("applyThreadWorkerInboundState restores a zeroed process identity", () => {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const kernelExports = makeKernelExports(memory);
  const restoreCurrentIdentity = vi.fn();
  Object.assign(kernelExports, {
    process_current_pid: () => 0,
    process_current_ppid: () => 0,
    process_restore_current_identity: restoreCurrentIdentity,
  });

  applyThreadWorkerInboundState({
    kernelExports,
    guestBase: 0,
  }, {
    kernelState: makeState(),
    targetVmPtr: 0x1234,
    processIdentity: { processPtr: 0x2000, pid: 1, ppid: 0, nextPid: 7 },
  });

  expect(restoreCurrentIdentity).toHaveBeenCalledWith(0x1234, 1, 0, 7);
});

test("applyThreadWorkerInboundState leaves matching process identity unchanged", () => {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const kernelExports = makeKernelExports(memory);
  const restoreCurrentIdentity = vi.fn();
  Object.assign(kernelExports, {
    process_current_pid: () => 6,
    process_current_ppid: () => 1,
    process_restore_current_identity: restoreCurrentIdentity,
  });

  applyThreadWorkerInboundState({
    kernelExports,
    guestBase: 0,
  }, {
    kernelState: makeState(),
    targetVmPtr: 0x1234,
    processIdentity: { processPtr: 0x2000, pid: 6, ppid: 1, nextPid: 7 },
  });

  expect(restoreCurrentIdentity).not.toHaveBeenCalled();
});

test("applyThreadWorkerInboundState rejects stale identity after process ownership changes", () => {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const kernelExports = makeKernelExports(memory);
  const restoreCurrentIdentity = vi.fn();
  Object.assign(kernelExports, {
    thread_process_ptr: () => 0x3000,
    process_current_pid: () => 6,
    process_current_ppid: () => 1,
    process_restore_current_identity: restoreCurrentIdentity,
  });

  applyThreadWorkerInboundState({
    kernelExports,
    guestBase: 0,
  }, {
    kernelState: makeState(),
    targetVmPtr: 0x1234,
    processIdentity: { processPtr: 0x2000, pid: 1, ppid: 0, nextPid: 7 },
    fdEntries: [{ fd: 7, ofdId: 6, fdFlags: 1 }],
  });

  expect(restoreCurrentIdentity).not.toHaveBeenCalled();
  expect(kernelExports.process_set_fd_entry).not.toHaveBeenCalled();
});

test("applyThreadWorkerInboundState records watched inbound guest write overlap", () => {
  consumeThreadWorkerInboundGuestWriteDebug();
  const memory = new WebAssembly.Memory({ initial: 1 });
  const bytes = new Uint8Array(memory.buffer);
  const kernelExports = makeKernelExports(memory);

  applyThreadWorkerInboundState({
    kernelExports,
    guestBase: 0x300,
  }, {
    kernelState: makeState(),
    targetVmPtr: 0x1234,
    guestMemoryWrites: [{ guestPtr: 0x14, bytes: Uint8Array.from([0xde, 0xad, 0xbe, 0xef]) }],
    guestWriteWatchRegions: [{ slot: 1, start: 0x15, end: 0x17 }],
  });

  expect([...bytes.slice(0x314, 0x318)]).toEqual([0xde, 0xad, 0xbe, 0xef]);
  expect(consumeThreadWorkerInboundGuestWriteDebug()).toEqual([
    "thread-inbound-guest-write:slot=1:ptr=0x14:len=4:overlap=0x15..0x17:sample=adbe",
  ]);
  expect(consumeThreadWorkerInboundGuestWriteDebug()).toEqual([]);
});

test("applyThreadWorkerInboundState treats inbound fd entries as canonical across worker generations", () => {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const kernelExports = makeKernelExports(memory);
  const fdState = new Map<number, { ofdId: number; fdFlags: number; generation: number }>([
    [5, { ofdId: 6, fdFlags: 0, generation: 3 }],
  ]);
  Object.assign(kernelExports, {
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdState.get(fd)?.ofdId ?? 0,
    process_fd_flags: (_vmPtr: number, fd: number) => fdState.get(fd)?.fdFlags ?? 0,
    process_fd_generation: (_vmPtr: number, fd: number) => fdState.get(fd)?.generation ?? 0,
    process_set_fd_entry: vi.fn((_vmPtr: number, fd: number, ofdId: number, fdFlags: number) => {
      const current = fdState.get(fd) ?? { ofdId: 0, fdFlags: 0, generation: 0 };
      fdState.set(fd, { ofdId, fdFlags, generation: current.generation + 1 });
    }),
  });

  applyThreadWorkerInboundState({
    kernelExports,
    guestBase: 0,
  }, {
    kernelState: makeState(),
    targetVmPtr: 0x1234,
    fdEntries: [{ fd: 5, ofdId: 0, fdFlags: 0, generation: 2 }],
  });

  expect(fdState.get(5)?.ofdId).toBe(0);
  expect(kernelExports.process_set_fd_entry).toHaveBeenCalledWith(0x1234, 5, 0, 0);

  applyThreadWorkerInboundState({
    kernelExports,
    guestBase: 0,
  }, {
    kernelState: makeState(),
    targetVmPtr: 0x1234,
    fdEntries: [{ fd: 5, ofdId: 0, fdFlags: 0, generation: 4 }],
  });

  expect(fdState.get(5)?.ofdId).toBe(0);
  expect(kernelExports.process_set_fd_entry).toHaveBeenCalledTimes(1);
});

test("applyThreadWorkerInboundState preserves a newer live worker fd from an older inbound snapshot", () => {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const kernelExports = makeKernelExports(memory);
  const fdState = new Map<number, { ofdId: number; fdFlags: number; generation: number }>([
    [3, { ofdId: 4, fdFlags: 0, generation: 3 }],
  ]);
  const restoreAction = vi.fn(() => 2);
  Object.assign(kernelExports, {
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdState.get(fd)?.ofdId ?? 0,
    process_fd_flags: (_vmPtr: number, fd: number) => fdState.get(fd)?.fdFlags ?? 0,
    process_fd_generation: (_vmPtr: number, fd: number) => fdState.get(fd)?.generation ?? 0,
    process_set_fd_entry: vi.fn((_vmPtr: number, fd: number, ofdId: number, fdFlags: number) => {
      const current = fdState.get(fd) ?? { ofdId: 0, fdFlags: 0, generation: 0 };
      fdState.set(fd, { ofdId, fdFlags, generation: current.generation + 1 });
    }),
    supervisor_fd_entry_restore_action: restoreAction,
    supervisor_fd_entry_restore_action_skip: () => 2,
    supervisor_fd_entry_restore_flag_preserve_live_regular_file: () => 1,
    supervisor_fd_entry_restore_flag_preserve_live_epoll_dependent: () => 2,
  });

  applyThreadWorkerInboundState({
    kernelExports,
    guestBase: 0,
  }, {
    kernelState: makeState(),
    targetVmPtr: 0x1234,
    fdEntries: [{ fd: 3, ofdId: 0, fdFlags: 0, generation: 1 }],
  });

  expect(fdState.get(3)?.ofdId).toBe(4);
  expect(kernelExports.process_set_fd_entry).not.toHaveBeenCalled();
  expect(restoreAction).toHaveBeenCalledWith(0x1234, 3, 0, 0xffffffffffffffffn, 3);
});

test("applyThreadWorkerInboundState preserves a newer live pipe fd from an older inbound snapshot", () => {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const kernelExports = makeKernelExports(memory);
  const fdState = new Map<number, { ofdId: number; fdFlags: number; generation: number }>([
    [14, { ofdId: 20, fdFlags: 1, generation: 3 }],
  ]);
  const restoreAction = vi.fn(() => 1);
  const pipeSyncCandidate = vi.fn((ofdId: number) => (ofdId === 20 ? 1 : 0));
  Object.assign(kernelExports, {
    abi_max_fds: () => 32,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdState.get(fd)?.ofdId ?? 0,
    process_fd_flags: (_vmPtr: number, fd: number) => fdState.get(fd)?.fdFlags ?? 0,
    process_fd_generation: (_vmPtr: number, fd: number) => fdState.get(fd)?.generation ?? 0,
    process_set_fd_entry: vi.fn((_vmPtr: number, fd: number, ofdId: number, fdFlags: number) => {
      const current = fdState.get(fd) ?? { ofdId: 0, fdFlags: 0, generation: 0 };
      fdState.set(fd, { ofdId, fdFlags, generation: current.generation + 1 });
    }),
    supervisor_fd_entry_restore_action: restoreAction,
    supervisor_fd_entry_restore_action_skip: () => 2,
    supervisor_fd_entry_restore_flag_preserve_live_regular_file: () => 1,
    supervisor_fd_entry_restore_flag_preserve_live_epoll_dependent: () => 2,
    supervisor_pipe_sync_ofd_candidate: pipeSyncCandidate,
  });

  applyThreadWorkerInboundState({
    kernelExports,
    guestBase: 0,
  }, {
    kernelState: makeState(),
    targetVmPtr: 0x1234,
    fdEntries: [{ fd: 14, ofdId: 0, fdFlags: 0, generation: 1 }],
  });

  expect(fdState.get(14)).toEqual({ ofdId: 20, fdFlags: 1, generation: 3 });
  expect(kernelExports.process_set_fd_entry).not.toHaveBeenCalled();
  expect(pipeSyncCandidate).toHaveBeenCalledWith(20);
  expect(restoreAction).toHaveBeenCalledWith(0x1234, 14, 0, 0xffffffffffffffffn, 3);
});

test("applyThreadWorkerInboundState does not reopen a same-ProcessState fd from a delayed snapshot", () => {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const kernelExports = makeKernelExports(memory);
  const fdState = new Map<number, { ofdId: number; fdFlags: number; generation: number }>([
    [6, { ofdId: 0, fdFlags: 0, generation: 13 }],
  ]);
  Object.assign(kernelExports, {
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdState.get(fd)?.ofdId ?? 0,
    process_fd_flags: (_vmPtr: number, fd: number) => fdState.get(fd)?.fdFlags ?? 0,
    process_fd_generation: (_vmPtr: number, fd: number) => fdState.get(fd)?.generation ?? 0,
    process_set_fd_entry: vi.fn(),
  });

  applyThreadWorkerInboundState({
    kernelExports,
    guestBase: 0,
  }, {
    kernelState: makeState(),
    targetVmPtr: 0x1234,
    fdEntries: [{
      fd: 6,
      ofdId: 8,
      fdFlags: 1,
      generation: 12,
      monotonicGeneration: true,
    }],
  });

  expect(kernelExports.process_set_fd_entry).not.toHaveBeenCalled();
});

test("applyThreadWorkerInboundState rejects an fd payload older than Runtime authority", () => {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const kernelExports = makeKernelExports(memory);
  const runtimeBridge = createRuntimeStateBridge();
  publishRuntimeFdAuthorityEpoch(runtimeBridge, 6);

  applyThreadWorkerInboundState({
    kernelExports,
    guestBase: 0,
    runtimeBridge,
  }, {
    kernelState: makeState(),
    targetVmPtr: 0x1234,
    fdEntries: [{ fd: 6, ofdId: 7, fdFlags: 1, authorityEpoch: 0 }],
  });

  expect(kernelExports.process_set_fd_entry).not.toHaveBeenCalled();
});

test("applyThreadWorkerInboundState applies a newer inbound fd snapshot even when local fd is live", () => {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const kernelExports = makeKernelExports(memory);
  const fdState = new Map<number, { ofdId: number; fdFlags: number; generation: number }>([
    [3, { ofdId: 4, fdFlags: 0, generation: 1 }],
  ]);
  const restoreAction = vi.fn(() => 1);
  Object.assign(kernelExports, {
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdState.get(fd)?.ofdId ?? 0,
    process_fd_flags: (_vmPtr: number, fd: number) => fdState.get(fd)?.fdFlags ?? 0,
    process_fd_generation: (_vmPtr: number, fd: number) => fdState.get(fd)?.generation ?? 0,
    process_set_fd_entry: vi.fn((_vmPtr: number, fd: number, ofdId: number, fdFlags: number) => {
      const current = fdState.get(fd) ?? { ofdId: 0, fdFlags: 0, generation: 0 };
      fdState.set(fd, { ofdId, fdFlags, generation: current.generation + 1 });
    }),
    supervisor_fd_entry_restore_action: restoreAction,
    supervisor_fd_entry_restore_action_skip: () => 2,
    supervisor_fd_entry_restore_flag_preserve_live_regular_file: () => 1,
    supervisor_fd_entry_restore_flag_preserve_live_epoll_dependent: () => 2,
  });

  applyThreadWorkerInboundState({
    kernelExports,
    guestBase: 0,
  }, {
    kernelState: makeState(),
    targetVmPtr: 0x1234,
    fdEntries: [{ fd: 3, ofdId: 0, fdFlags: 0, generation: 3 }],
  });

  expect(fdState.get(3)?.ofdId).toBe(0);
  expect(kernelExports.process_set_fd_entry).toHaveBeenCalledWith(0x1234, 3, 0, 0);
  expect(restoreAction).toHaveBeenCalledWith(0x1234, 3, 0, 0xffffffffffffffffn, 3);
});

test("applyThreadWorkerInboundState applies a canonical generationless fd over a live worker fd", () => {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const kernelExports = makeKernelExports(memory);
  const fdState = new Map<number, { ofdId: number; fdFlags: number; generation: number }>([
    [6, { ofdId: 5, fdFlags: 0, generation: 12 }],
  ]);
  const restoreAction = vi.fn(() => 1);
  Object.assign(kernelExports, {
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdState.get(fd)?.ofdId ?? 0,
    process_fd_flags: (_vmPtr: number, fd: number) => fdState.get(fd)?.fdFlags ?? 0,
    process_fd_generation: (_vmPtr: number, fd: number) => fdState.get(fd)?.generation ?? 0,
    process_set_fd_entry: vi.fn((_vmPtr: number, fd: number, ofdId: number, fdFlags: number) => {
      const current = fdState.get(fd) ?? { ofdId: 0, fdFlags: 0, generation: 0 };
      fdState.set(fd, { ofdId, fdFlags, generation: current.generation + 1 });
    }),
    supervisor_fd_entry_restore_action: restoreAction,
    supervisor_fd_entry_restore_action_skip: () => 2,
    supervisor_fd_entry_restore_flag_preserve_live_regular_file: () => 1,
    supervisor_fd_entry_restore_flag_preserve_live_epoll_dependent: () => 2,
  });

  applyThreadWorkerInboundState({
    kernelExports,
    guestBase: 0,
  }, {
    kernelState: makeState(),
    targetVmPtr: 0x1234,
    fdEntries: [{ fd: 6, ofdId: 7, fdFlags: 0 }],
  });

  expect(fdState.get(6)).toEqual({ ofdId: 7, fdFlags: 0, generation: 13 });
  expect(restoreAction).not.toHaveBeenCalled();
});
