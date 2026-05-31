import { expect, test, vi } from "vite-plus/test";
import {
  snapshotKernelShareOfdSlots,
} from "../../../src/worker/fork-shared.js";
import {
  activeKernelShareOfdIds,
  collectProcessHeldOfds,
  trackForkSharedOfds,
} from "../../../src/worker/ownership/ofd-table.js";
import {
  reconcileKernelSharePipeControl,
  restoreLocalPipeSlots,
  restoreLocalOfdSlots,
  snapshotLocalPipeSlots,
} from "../../../src/worker/kernel-share.js";
import type { ProcessHandle } from "../../../src/worker/types.js";

const FD_TYPE_FILE = 1;
const FD_TYPE_PIPE = 6;
const FD_TYPE_PTY_MASTER = 11;
const FD_TYPE_PTY_SLAVE = 12;

function makeKernelState() {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memStatePtr: 0x240000,
    epollTablePtr: 0x250000,
    socketTablePtr: 0x260000,
    socketUsed: 0,
    pipeBase: 0x270000,
    pipeUsed: 0n,
    netBufBase: 0x280000,
    netBufUsed: 0n,
    sigSlotsPtr: 0xc00000,
    processPendingPtr: 0xc10000,
    processRuntimeStatePtr: 0xc20000,
    timeStatePtr: 0xc30000,
  };
}

type FakeKernel = ProcessHandle["kernel"] & {
  setFdMap(map: Map<number, number>): void;
  setThreadVmPtrs(vmPtrs: number[]): void;
  setBlockedOfdMap(map: Map<number, number>): void;
  retainOfd: ReturnType<typeof vi.fn>;
  releaseOfd: ReturnType<typeof vi.fn>;
};

function seedOfd(
  memory: WebAssembly.Memory,
  ofdTablePtr: number,
  ofdId: number,
  type: number,
  ino: number,
  marker: number,
  hostFd = 0,
): void {
  const view = new DataView(memory.buffer);
  const base = ofdTablePtr + ofdId * 32;
  view.setInt32(base, type, true);
  view.setInt32(base + 4, hostFd, true);
  view.setInt32(base + 16, 1, true);
  view.setUint32(base + 24, ino, true);
  new Uint8Array(memory.buffer)[base + 28] = marker;
}

function ofdMarker(memory: WebAssembly.Memory, ofdTablePtr: number, ofdId: number): number {
  return new Uint8Array(memory.buffer)[ofdTablePtr + ofdId * 32 + 28] ?? 0;
}

function seedPipeSlot(
  memory: WebAssembly.Memory,
  pipeBase: number,
  pipeSlot: number,
  marker: number,
): void {
  const slotBase = pipeBase + pipeSlot * (32 + 65_536);
  const view = new DataView(memory.buffer);
  view.setInt32(slotBase + 8, 1, true);
  view.setInt32(slotBase + 12, 1, true);
  new Uint8Array(memory.buffer)[slotBase + 32] = marker;
}

function pipeMarker(memory: WebAssembly.Memory, pipeBase: number, pipeSlot: number): number {
  return new Uint8Array(memory.buffer)[pipeBase + pipeSlot * (32 + 65_536) + 32] ?? 0;
}

function pipeOpenCounts(memory: WebAssembly.Memory, pipeBase: number, pipeSlot: number): {
  readOpen: number;
  writeOpen: number;
} {
  const view = new DataView(memory.buffer);
  const slotBase = pipeBase + pipeSlot * (32 + 65_536);
  return {
    readOpen: view.getInt32(slotBase + 8, true),
    writeOpen: view.getInt32(slotBase + 12, true),
  };
}

function makeKernel(memory: WebAssembly.Memory): FakeKernel {
  let fdMap = new Map<number, number>();
  let threadVmPtrs = [0x1234];
  let blockedOfdMap = new Map<number, number>();
  const state = makeKernelState();
  const retainOfd = vi.fn((ofdId: number) => {
    const view = new DataView(memory.buffer);
    const base = state.ofdTablePtr + ofdId * 32 + 16;
    view.setInt32(base, view.getInt32(base, true) + 1, true);
  });
  const releaseOfd = vi.fn((ofdId: number) => {
    const view = new DataView(memory.buffer);
    const base = state.ofdTablePtr + ofdId * 32 + 16;
    view.setInt32(base, view.getInt32(base, true) - 1, true);
  });
  return {
    abi_max_fds: () => 32,
    abi_max_threads: () => 4,
    abi_max_pipes: () => 64,
    abi_fd_type_pipe: () => FD_TYPE_PIPE,
    abi_fd_type_pty_master: () => FD_TYPE_PTY_MASTER,
    abi_fd_type_pty_slave: () => FD_TYPE_PTY_SLAVE,
    abi_o_rdonly: () => 0,
    abi_o_wronly: () => 1,
    kernel_pipe_base: () => state.pipeBase,
    kernel_attach_runtime_state: () => {},
    kernel_memfs_header_bytes: () => 32,
    kernel_memfs_inodes_ptr: () => state.memfsPtr + 0x100,
    kernel_memfs_inodes_bytes: () => 16384 * 32,
    kernel_memfs_first_child_ptr: () => state.memfsPtr + 0x200000,
    kernel_memfs_first_child_bytes: () => 16384 * 4,
    kernel_memfs_dir_entries_ptr: () => state.memfsPtr + 0x220000,
    kernel_memfs_dir_entries_bytes: () => 16384 * 264,
    kernel_memfs_data_ptr: () => state.memfsPtr + 0x900000,
    kernel_memfs_data_bytes: () => 256 * 1024,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdMap.get(fd) ?? 0,
    process_thread_vm_ptr: (_vmPtr: number, slot: number) => threadVmPtrs[slot] ?? 0,
    thread_blocked_ofd_id: (vmPtr: number) => blockedOfdMap.get(vmPtr) ?? 0,
    kernel_ofd_fd_type: (ofdId: number) => new DataView(memory.buffer)
      .getInt32(state.ofdTablePtr + ofdId * 32, true),
    kernel_ofd_host_fd: (ofdId: number) => new DataView(memory.buffer)
      .getInt32(state.ofdTablePtr + ofdId * 32 + 4, true),
    kernel_ofd_ino: (ofdId: number) => new DataView(memory.buffer)
      .getUint32(state.ofdTablePtr + ofdId * 32 + 24, true),
    kernel_ofd_status_flags: () => 0,
    kernel_ofd_refcount: (ofdId: number) => new DataView(memory.buffer)
      .getInt32(state.ofdTablePtr + ofdId * 32 + 16, true),
    kernel_pty_input_pipe: (ptyIdx: number) => ptyIdx * 2 + 1,
    kernel_pty_output_pipe: (ptyIdx: number) => ptyIdx * 2 + 2,
    kernel_pipe_region_bytes: () => 64 * (32 + 65_536),
    kernel_retain_ofd: retainOfd,
    kernel_release_ofd: releaseOfd,
    kernel_ofd_table_bytes: () => 32 * 32,
    setFdMap: (map: Map<number, number>) => {
      fdMap = new Map(map);
    },
    setThreadVmPtrs: (vmPtrs: number[]) => {
      threadVmPtrs = [...vmPtrs];
    },
    setBlockedOfdMap: (map: Map<number, number>) => {
      blockedOfdMap = new Map(map);
    },
    retainOfd,
    releaseOfd,
  } as unknown as FakeKernel;
}

function makeHandle(kernel: FakeKernel, memory: WebAssembly.Memory): ProcessHandle {
  return {
    runtimeId: 1,
    pid: 11,
    parentPid: null,
    vmPtr: 0x1234,
    ramSize: 0,
    guestBase: 0,
    worker: {} as Worker,
    memory,
    wasmModule: {} as WebAssembly.Module,
    kernel,
    kernelState: makeKernelState(),
    stdinPipe: 4,
    stdoutPipe: 5,
    stderrPipe: 6,
    hostStdio: {
      stdinPipe: 4,
      stdoutPipe: 5,
      stderrPipe: 6,
      refCount: 1,
    },
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set<number>(),
      ofdSlotSnapshots: new Map(),
      forkSharedSyncPromise: null,
    },
    stdinQueue: [],
    outputBacklog: [],
    debugCommand: "fd-ownership",
    runtimeBridge: {} as ProcessHandle["runtimeBridge"],
    sabPageCache: null,
    workers: new Set(),
    readyWorkers: [],
    runningWorker: null,
    blockedWorkers: new Map(),
    pipeSyncPromise: null,
    pipeSyncQueued: false,
    pipeWaiterWakeQueued: false,
    epollSyncPromise: null,
    epollSyncQueued: false,
    epollWaiterWakeQueued: false,
    ofdSyncPromise: null,
    ofdSyncQueuedIds: new Set(),
    socketSyncPromise: null,
    socketSyncQueued: false,
    socketWaiterWakeQueued: false,
    vforkSnapshot: null,
    vforkWorker: null,
    workerDebugLog: [],
  } as unknown as ProcessHandle;
}

test("collectProcessHeldOfds deduplicates dup aliases and includes blocked worker OFDs", () => {
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const kernel = makeKernel(memory);
  const handle = makeHandle(kernel, memory);
  kernel.setFdMap(new Map([
    [0, 10],
    [3, 11],
    [7, 11],
    [8, 13],
  ]));
  kernel.setThreadVmPtrs([handle.vmPtr, 0x2222]);
  kernel.setBlockedOfdMap(new Map([[0x2222, 12]]));

  expect([...collectProcessHeldOfds(handle)].sort((left, right) => left - right)).toEqual([10, 11, 12, 13]);
});

test("collectProcessHeldOfds skips implausible or unreadable thread slots from corrupted process state", () => {
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const kernel = makeKernel(memory);
  const handle = makeHandle(kernel, memory);
  kernel.setFdMap(new Map([
    [0, 10],
    [3, 11],
  ]));
  kernel.thread_process_thread_count = () => 4;
  kernel.process_thread_vm_ptr = (_vmPtr: number, slot: number) => {
    if (slot === 0) {
      return handle.vmPtr;
    }
    if (slot === 1) {
      return -1;
    }
    if (slot === 2) {
      return memory.buffer.byteLength + 0x1000;
    }
    throw new RangeError("corrupted thread slot");
  };
  kernel.thread_blocked_ofd_id = (vmPtr: number) => (vmPtr === handle.vmPtr ? 12 : 0);

  expect([...collectProcessHeldOfds(handle)].sort((left, right) => left - right)).toEqual([10, 11, 12]);
});

test("trackForkSharedOfds skips hidden stdio pipes and retains non-stdio OFDs only once", () => {
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const kernel = makeKernel(memory);
  const handle = makeHandle(kernel, memory);
  seedOfd(memory, handle.kernelState.ofdTablePtr, 10, FD_TYPE_PIPE, handle.stdoutPipe, 0xaa);
  seedOfd(memory, handle.kernelState.ofdTablePtr, 11, FD_TYPE_FILE, 0, 0xbb);
  seedOfd(memory, handle.kernelState.ofdTablePtr, 13, FD_TYPE_PIPE, 9, 0xcc);
  kernel.setFdMap(new Map([
    [1, 10],
    [3, 11],
    [7, 11],
    [8, 13],
  ]));

  const retained = trackForkSharedOfds(handle, handle.vmPtr, true);

  expect(retained).toEqual([11, 13]);
  expect([...handle.kernelShare.sharedOfdIds].sort((left, right) => left - right)).toEqual([11, 13]);
  expect(kernel.retainOfd).toHaveBeenCalledWith(11);
  expect(kernel.retainOfd).toHaveBeenCalledWith(13);
});

test("activeKernelShareOfdIds ignores stale entries and restoreLocalOfdSlots restores preserved OFD bytes", () => {
  const ownerMemory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const siblingMemory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const ownerKernel = makeKernel(ownerMemory);
  const siblingKernel = makeKernel(siblingMemory);
  const owner = makeHandle(ownerKernel, ownerMemory);
  const sibling = makeHandle(siblingKernel, siblingMemory);
  owner.kernelShare.members.add(owner);
  owner.kernelShare.members.add(sibling);
  sibling.kernelShare = owner.kernelShare;
  owner.kernelShare.sharedOfdIds.add(11);
  owner.kernelShare.sharedOfdIds.add(13);
  owner.kernelShare.sharedOfdIds.add(31);
  ownerKernel.setFdMap(new Map([[3, 13]]));
  siblingKernel.setFdMap(new Map([[4, 11]]));
  seedOfd(siblingMemory, sibling.kernelState.ofdTablePtr, 11, FD_TYPE_FILE, 0, 0xbb);
  seedOfd(ownerMemory, owner.kernelState.ofdTablePtr, 13, FD_TYPE_FILE, 0, 0xcc);

  expect(activeKernelShareOfdIds(owner)).toEqual([11, 13]);

  const preserved = snapshotKernelShareOfdSlots(sibling, [11]);
  new Uint8Array(siblingMemory.buffer).fill(0, sibling.kernelState.ofdTablePtr + 11 * 32, sibling.kernelState.ofdTablePtr + 12 * 32);
  expect(ofdMarker(siblingMemory, sibling.kernelState.ofdTablePtr, 11)).toBe(0);

  restoreLocalOfdSlots(sibling, preserved);

  expect(ofdMarker(siblingMemory, sibling.kernelState.ofdTablePtr, 11)).toBe(0xbb);
});

test("local OFD and pipe snapshots preserve child-private dup-style pipe aliases across scrubbed bytes", () => {
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const kernel = makeKernel(memory);
  const handle = makeHandle(kernel, memory);
  seedOfd(memory, handle.kernelState.ofdTablePtr, 14, FD_TYPE_PIPE, 14, 0xdd);
  seedOfd(memory, handle.kernelState.ofdTablePtr, 15, FD_TYPE_PIPE, 15, 0xee);
  seedPipeSlot(memory, handle.kernelState.pipeBase, 14, 0xa4);
  seedPipeSlot(memory, handle.kernelState.pipeBase, 15, 0xb5);
  kernel.setFdMap(new Map([
    [4, 14],
    [5, 14],
    [6, 15],
  ]));

  const preservedOfds = snapshotKernelShareOfdSlots(handle, [14]);
  const preservedPipes = snapshotLocalPipeSlots(handle, [14]);
  new Uint8Array(memory.buffer).fill(
    0,
    handle.kernelState.ofdTablePtr + 14 * 32,
    handle.kernelState.ofdTablePtr + 16 * 32,
  );
  new Uint8Array(memory.buffer).fill(
    0,
    handle.kernelState.pipeBase + 14 * (32 + 65_536),
    handle.kernelState.pipeBase + 16 * (32 + 65_536),
  );
  new DataView(memory.buffer).setInt32(handle.kernelState.pipeBase + 14 * (32 + 65_536) + 8, 7, true);
  new DataView(memory.buffer).setInt32(handle.kernelState.pipeBase + 14 * (32 + 65_536) + 12, 8, true);

  restoreLocalOfdSlots(handle, preservedOfds);

  expect(ofdMarker(memory, handle.kernelState.ofdTablePtr, 14)).toBe(0xdd);
  expect(ofdMarker(memory, handle.kernelState.ofdTablePtr, 15)).toBe(0);
  expect(pipeMarker(memory, handle.kernelState.pipeBase, 14)).toBe(0);
  expect(pipeMarker(memory, handle.kernelState.pipeBase, 15)).toBe(0);

  restoreLocalPipeSlots(handle, preservedPipes);

  expect(pipeMarker(memory, handle.kernelState.pipeBase, 14)).toBe(0xa4);
  expect(pipeOpenCounts(memory, handle.kernelState.pipeBase, 14)).toEqual({
    readOpen: 7,
    writeOpen: 8,
  });
  expect(pipeMarker(memory, handle.kernelState.pipeBase, 15)).toBe(0);
});

test("local pipe snapshots preserve PTY-backed stdio pipe slots across scrubbed bytes", () => {
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const kernel = makeKernel(memory);
  const handle = makeHandle(kernel, memory);
  const kernelWithPty = kernel as FakeKernel & {
    kernel_ofd_status_flags(ofdId: number): number;
  };
  seedOfd(memory, handle.kernelState.ofdTablePtr, 20, FD_TYPE_PTY_SLAVE, 10, 0xf1);
  seedOfd(memory, handle.kernelState.ofdTablePtr, 21, FD_TYPE_PTY_SLAVE, 10, 0xf2);
  seedPipeSlot(memory, handle.kernelState.pipeBase, 21, 0xa1);
  seedPipeSlot(memory, handle.kernelState.pipeBase, 22, 0xb2);
  kernelWithPty.kernel_ofd_status_flags = (ofdId: number) => {
    if (ofdId === 20) {
      return 0;
    }
    if (ofdId === 21) {
      return 1;
    }
    return 0;
  };

  const preservedPipes = snapshotLocalPipeSlots(handle, [20, 21]);
  new Uint8Array(memory.buffer).fill(
    0,
    handle.kernelState.pipeBase + 21 * (32 + 65_536),
    handle.kernelState.pipeBase + 23 * (32 + 65_536),
  );

  restoreLocalPipeSlots(handle, preservedPipes);

  expect(pipeMarker(memory, handle.kernelState.pipeBase, 21)).toBe(0xa1);
  expect(pipeMarker(memory, handle.kernelState.pipeBase, 22)).toBe(0xb2);
});

test("kernel-share pipe reconcile keeps host stderr reader open for stdio pipes", () => {
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const kernel = makeKernel(memory);
  const handle = makeHandle(kernel, memory);
  seedOfd(memory, handle.kernelState.ofdTablePtr, 3, FD_TYPE_PIPE, handle.stderrPipe, 0xe3, -7);
  seedPipeSlot(memory, handle.kernelState.pipeBase, handle.stderrPipe, 0xa6);
  kernel.setFdMap(new Map([[2, 3]]));
  handle.kernelShare.members.add(handle);

  const view = new DataView(memory.buffer);
  const slotBase = handle.kernelState.pipeBase + handle.stderrPipe * (32 + 65_536);
  view.setInt32(slotBase + 8, 0, true);
  view.setInt32(slotBase + 12, 0, true);

  reconcileKernelSharePipeControl(handle);

  expect(pipeOpenCounts(memory, handle.kernelState.pipeBase, handle.stderrPipe)).toEqual({
    readOpen: 1,
    writeOpen: 1,
  });
});
