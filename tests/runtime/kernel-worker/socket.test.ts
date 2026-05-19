import { expect, test } from "vite-plus/test";
import { readKernelRuntimeState, type KernelExports, type KernelRuntimeState } from "../../../src/abi.js";
import { createKernelWorkerPrimitives } from "../../../src/kernel-worker/primitives.js";
import { createKernelWorkerSharedStateSync } from "../../../src/kernel-worker/shared-state-sync.js";

type TestKernelExports = KernelExports;

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memfsHeaderBytes: 32,
    memfsInodesBytes: 0,
    memfsFirstChildBytes: 0,
    memfsDirEntriesBytes: 0,
    memfsDataBytes: 0,
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

function makeKernelExports(memory: WebAssembly.Memory): TestKernelExports {
  let runtimeState = makeKernelState();
  const exports = {
    memory,
    abi_max_fds: () => 256,
    abi_max_threads: () => 64,
    kernel_heap_base: () => runtimeState.heapBase,
    kernel_heap_ptr: () => runtimeState.heapPtr,
    kernel_ofd_table_ptr: () => runtimeState.ofdTablePtr,
    kernel_ofd_table_bytes: () => 256 * 32,
    kernel_memfs_ptr: () => runtimeState.memfsPtr,
    kernel_memfs_header_bytes: () => runtimeState.memfsHeaderBytes,
    kernel_memfs_inodes_bytes: () => runtimeState.memfsInodesBytes,
    kernel_memfs_first_child_bytes: () => runtimeState.memfsFirstChildBytes,
    kernel_memfs_dir_entries_bytes: () => runtimeState.memfsDirEntriesBytes,
    kernel_memfs_data_bytes: () => runtimeState.memfsDataBytes,
    kernel_mem_state_ptr: () => runtimeState.memStatePtr,
    kernel_epoll_table_ptr: () => runtimeState.epollTablePtr,
    kernel_epoll_table_bytes: () => 64 * 4,
    kernel_epoll_instance_bytes: () => 64 * 16 + 4,
    kernel_socket_table_ptr: () => runtimeState.socketTablePtr,
    kernel_socket_table_bytes: () => 64 * 128,
    kernel_socket_used: () => runtimeState.socketUsed,
    kernel_pipe_base: () => runtimeState.pipeBase,
    kernel_pipe_region_bytes: () => 64 * (32 + 4096),
    kernel_pipe_used: () => runtimeState.pipeUsed,
    kernel_net_buf_base: () => runtimeState.netBufBase,
    kernel_net_buf_pool_bytes: () => 128 * (16 + 8192),
    kernel_net_buf_used: () => runtimeState.netBufUsed,
    kernel_sig_slots_ptr: () => runtimeState.sigSlotsPtr,
    kernel_process_pending_ptr: () => runtimeState.processPendingPtr,
    kernel_process_runtime_state_ptr: () => runtimeState.processRuntimeStatePtr,
    kernel_time_state_ptr: () => runtimeState.timeStatePtr,
    kernel_attach_runtime_state: (
      heapBase: number,
      heapPtr: number,
      ofdTablePtr: number,
      memfsPtr: number,
      memStatePtr: number,
      epollTablePtr: number,
      socketTablePtr: number,
      socketUsed: number,
      pipeBase: number,
      pipeUsed: bigint,
      netBufBase: number,
      netBufUsed: bigint,
      sigSlotsPtr: number,
      processPendingPtr: number,
      processRuntimeStatePtr: number,
      timeStatePtr: number,
    ) => {
      runtimeState = {
        heapBase,
        heapPtr,
        ofdTablePtr,
        memfsPtr,
        memfsHeaderBytes: runtimeState.memfsHeaderBytes,
        memfsInodesBytes: runtimeState.memfsInodesBytes,
        memfsFirstChildBytes: runtimeState.memfsFirstChildBytes,
        memfsDirEntriesBytes: runtimeState.memfsDirEntriesBytes,
        memfsDataBytes: runtimeState.memfsDataBytes,
        memStatePtr,
        epollTablePtr,
        socketTablePtr,
        socketUsed,
        pipeBase,
        pipeUsed,
        netBufBase,
        netBufUsed,
        sigSlotsPtr,
        processPendingPtr,
        processRuntimeStatePtr,
        timeStatePtr,
      };
    },
    process_fd_ofd_id: () => 0,
    process_thread_vm_ptr: (_vmPtr: number, slot: number) => (slot === 0 ? 0x1234 : 0),
    thread_blocked_ofd_id: () => 0,
  };

  return exports as unknown as TestKernelExports;
}

function seedSocketState(
  exports: TestKernelExports,
  memory: WebAssembly.Memory,
  socketUsed: number,
  netBufUsed: bigint,
  socketMarker: number,
  netBufMarker: number,
): void {
  const state = makeKernelState();
  exports.kernel_attach_runtime_state(
    state.heapBase,
    state.heapPtr,
    state.ofdTablePtr,
    state.memfsPtr,
    state.memStatePtr,
    state.epollTablePtr,
    state.socketTablePtr,
    socketUsed,
    state.pipeBase,
    state.pipeUsed,
    state.netBufBase,
    netBufUsed,
    state.sigSlotsPtr,
    state.processPendingPtr,
    state.processRuntimeStatePtr,
    state.timeStatePtr,
  );
  const bytes = new Uint8Array(memory.buffer);
  bytes[state.socketTablePtr + 8] = socketMarker;
  bytes[state.netBufBase + 12] = netBufMarker;
}

function netBufferSlotBytes(exports: TestKernelExports): number {
  return Math.floor(exports.kernel_net_buf_pool_bytes() / 64);
}

function seedNetBuffer(
  exports: TestKernelExports,
  memory: WebAssembly.Memory,
  bufferIndex: number,
  options: { head: number; tail: number; marker: number },
): void {
  const state = makeKernelState();
  const slotBytes = netBufferSlotBytes(exports);
  const base = state.netBufBase + bufferIndex * slotBytes;
  const view = new DataView(memory.buffer);
  view.setUint32(base, options.head, true);
  view.setUint32(base + 4, options.tail, true);
  view.setUint32(base + 8, slotBytes - 16, true);
  new Uint8Array(memory.buffer)[base + 16] = options.marker;
}

function makeSharedStateSync(
  parentExports: TestKernelExports,
  childExports: TestKernelExports,
  canonicalExports: TestKernelExports,
) {
  const processKernels = new Map<number, KernelExports>([
    [1, parentExports],
    [2, childExports],
  ]);
  const getCanonicalProcessVmPtr = (pid: number): number => (pid === 1 || pid === 2 ? 0x1234 : 0);
  const primitives = createKernelWorkerPrimitives({
    getCanonicalExports: () => canonicalExports,
    processKernels,
    getCanonicalProcessVmPtr,
  });

  return createKernelWorkerSharedStateSync({
    ...primitives,
    getCanonicalExports: () => canonicalExports,
    getCanonicalProcessVmPtr,
    processKernels,
    lastProcessPipeSlotModes: new Map(),
    requireProcessKernel: (pid) => {
      const exports = processKernels.get(pid);
      if (!exports) {
        throw new Error(`missing process kernel: ${pid}`);
      }
      return exports;
    },
    copyCanonicalMemfsToProcess: () => {},
    snapshotProcSelfExe: () => null,
    restoreProcSelfExe: () => {},
  });
}

test("kernel-worker socket sync keeps private socket state process-local", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedSocketState(parentExports, parentMemory, 1, 2n, 0xaa, 0xab);
  seedSocketState(childExports, childMemory, 3, 4n, 0xbb, 0xbc);

  sharedStateSync.syncSocketState(2, readKernelRuntimeState(childExports));

  expect(parentExports.kernel_socket_used()).toBe(1);
  expect(parentExports.kernel_net_buf_used()).toBe(2n);
  expect(new Uint8Array(parentMemory.buffer)[makeKernelState().socketTablePtr + 8]).toBe(0xaa);
  expect(new Uint8Array(parentMemory.buffer)[makeKernelState().netBufBase + 12]).toBe(0xab);
  expect(canonicalExports.kernel_socket_used()).toBe(3);
  expect(canonicalExports.kernel_net_buf_used()).toBe(4n);
});

test("kernel-worker socket sync propagates canonical state to shared members only when explicitly requested", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedSocketState(canonicalExports, canonicalMemory, 5, 6n, 0xcc, 0xcd);
  seedSocketState(parentExports, parentMemory, 5, 6n, 0x10, 0x11);
  seedSocketState(childExports, childMemory, 5, 6n, 0x20, 0x21);

  sharedStateSync.syncSocketState(1, readKernelRuntimeState(parentExports));

  expect(new Uint8Array(parentMemory.buffer)[makeKernelState().socketTablePtr + 8]).toBe(0x10);
  expect(new Uint8Array(childMemory.buffer)[makeKernelState().socketTablePtr + 8]).toBe(0x10);
  expect(new Uint8Array(canonicalMemory.buffer)[makeKernelState().socketTablePtr + 8]).toBe(0x10);
});

test("kernel-worker socket sync does not rewind a process-local net buffer consumer head", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedSocketState(parentExports, parentMemory, 1, 1n, 0xaa, 0xab);
  seedSocketState(childExports, childMemory, 1, 1n, 0xbb, 0xbc);
  seedNetBuffer(parentExports, parentMemory, 0, { head: 0, tail: 4096, marker: 0x11 });
  seedNetBuffer(childExports, childMemory, 0, { head: 8192, tail: 8192, marker: 0x22 });

  sharedStateSync.syncSocketState(1, readKernelRuntimeState(parentExports));

  const state = makeKernelState();
  const childSlotBase = state.netBufBase;
  const childView = new DataView(childMemory.buffer);
  expect(childView.getUint32(childSlotBase, true)).toBe(8192);
  expect(childView.getUint32(childSlotBase + 4, true)).toBe(8192);
  expect(new Uint8Array(childMemory.buffer)[childSlotBase + 16]).toBe(0x22);
});
