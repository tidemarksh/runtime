// Kernel-worker tests in this file validate transitional runtime-owned bridge behavior.
import { expect, test } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../src/abi.js";
import { createKernelWorkerPrimitives } from "../../../src/kernel-worker/primitives.js";
import { createKernelWorkerSharedStateSync } from "../../../src/kernel-worker/shared-state-sync.js";

const FD_TYPE_EVENTFD = 9;
const FD_TYPE_TIMERFD = 10;

type TestKernelExports = KernelExports & {
  setFdMap(map: Map<number, number>): void;
};

function makeKernelState(): KernelRuntimeState {
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

function makeKernelExports(memory: WebAssembly.Memory): TestKernelExports {
  let runtimeState = makeKernelState();
  let fdMap = new Map<number, number>();

  const exports = {
    memory,
    abi_max_fds: () => 256,
    abi_max_threads: () => 64,
    abi_fd_type_file: () => 1,
    abi_fd_type_eventfd: () => FD_TYPE_EVENTFD,
    abi_fd_type_timerfd: () => FD_TYPE_TIMERFD,
    supervisor_fd_entry_restore_action_skip: () => 2,
    supervisor_ofd_slot_write_action: () => 1,
    kernel_heap_base: () => runtimeState.heapBase,
    kernel_heap_ptr: () => runtimeState.heapPtr,
    kernel_ofd_table_ptr: () => runtimeState.ofdTablePtr,
    kernel_ofd_table_bytes: () => 256 * 32,
    kernel_memfs_ptr: () => runtimeState.memfsPtr,
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
    kernel_time_state_bytes: () => 4096,
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
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdMap.get(fd) ?? 0,
    process_thread_vm_ptr: (_vmPtr: number, slot: number) => (slot === 0 ? 0x1234 : 0),
    thread_blocked_ofd_id: (_vmPtr: number) => 0,
    supervisor_process_held_ofd_refcount: (_vmPtr: number, ofdId: number) => {
      let refcount = 0;
      for (const heldOfdId of fdMap.values()) {
        if (heldOfdId === ofdId) {
          refcount += 1;
        }
      }
      return refcount;
    },
    kernel_ofd_fd_type: (ofdId: number) => new DataView(memory.buffer)
      .getInt32(runtimeState.ofdTablePtr + ofdId * 32, true),
    kernel_ofd_ino: (ofdId: number) => new DataView(memory.buffer)
      .getUint32(runtimeState.ofdTablePtr + ofdId * 32 + 24, true),
    kernel_ofd_host_fd: (ofdId: number) => new DataView(memory.buffer)
      .getInt32(runtimeState.ofdTablePtr + ofdId * 32 + 4, true),
    kernel_ofd_refcount: (ofdId: number) => new DataView(memory.buffer)
      .getInt32(runtimeState.ofdTablePtr + ofdId * 32 + 16, true),
    kernel_ofd_offset: (ofdId: number) => new DataView(memory.buffer)
      .getBigInt64(runtimeState.ofdTablePtr + ofdId * 32 + 16, true),
    supervisor_ofd_fd_type(this: KernelExports, ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_fd_type(ofdId) : 0;
    },
    supervisor_ofd_host_fd(this: KernelExports, ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_host_fd(ofdId) : -1;
    },
    supervisor_ofd_ino(this: KernelExports, ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_ino(ofdId) : 0;
    },
    supervisor_ofd_refcount(this: KernelExports, ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_refcount(ofdId) : 0;
    },
    supervisor_ofd_offset(this: KernelExports, ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_offset(ofdId) : 0n;
    },
    kernel_retain_ofd: () => {},
    kernel_release_ofd: () => {},
    setFdMap: (map: Map<number, number>) => {
      fdMap = new Map(map);
    },
  };

  return exports as unknown as TestKernelExports;
}

function seedEventfdOfd(memory: WebAssembly.Memory, ofdId: number, marker: number): void {
  const state = makeKernelState();
  const view = new DataView(memory.buffer);
  const base = state.ofdTablePtr + ofdId * 32;
  view.setInt32(base, FD_TYPE_EVENTFD, true);
  view.setInt32(base + 12, 1, true);
  view.setBigInt64(base + 16, BigInt(marker), true);
}

function seedTimerfdOfd(memory: WebAssembly.Memory, ofdId: number, timerfdIdx: number): void {
  const state = makeKernelState();
  const view = new DataView(memory.buffer);
  const base = state.ofdTablePtr + ofdId * 32;
  view.setInt32(base, FD_TYPE_TIMERFD, true);
  view.setInt32(base + 12, 1, true);
  view.setUint32(base + 24, timerfdIdx, true);
}

function eventfdMarker(memory: WebAssembly.Memory, ofdId: number): number {
  const state = makeKernelState();
  return Number(new DataView(memory.buffer).getBigInt64(state.ofdTablePtr + ofdId * 32 + 16, true));
}

function seedTimeStateMarker(memory: WebAssembly.Memory, marker: number): void {
  const state = makeKernelState();
  new DataView(memory.buffer).setUint32(state.timeStatePtr + 64, marker, true);
}

function timeStateMarker(memory: WebAssembly.Memory): number {
  const state = makeKernelState();
  return new DataView(memory.buffer).getUint32(state.timeStatePtr + 64, true);
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

test("kernel-worker eventfd sync keeps private eventfd OFDs process-local", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedEventfdOfd(parentMemory, 7, 0xaa);
  seedEventfdOfd(childMemory, 9, 0xbb);
  parentExports.setFdMap(new Map([[4, 7]]));
  childExports.setFdMap(new Map([[4, 9]]));

  sharedStateSync.syncOfdState(2, [9], makeKernelState());

  expect(eventfdMarker(parentMemory, 7)).toBe(0xaa);
  expect(eventfdMarker(parentMemory, 9)).toBe(0);
  expect(eventfdMarker(canonicalMemory, 9)).toBe(0xbb);
});

test("kernel-worker eventfd sync propagates shared eventfd OFDs by shared OFD id", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedEventfdOfd(parentMemory, 7, 0xaa);
  seedEventfdOfd(childMemory, 7, 0xcc);
  parentExports.setFdMap(new Map([[4, 7]]));
  childExports.setFdMap(new Map([[4, 7]]));

  sharedStateSync.syncOfdState(2, [7], makeKernelState());

  expect(eventfdMarker(parentMemory, 7)).toBe(0xcc);
  expect(eventfdMarker(canonicalMemory, 7)).toBe(0xcc);
});

test("kernel-worker eventfd pull sync preserves canonical eventfd state", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedEventfdOfd(canonicalMemory, 7, 0xcc);
  seedEventfdOfd(parentMemory, 7, 0xaa);
  seedEventfdOfd(childMemory, 7, 0);
  parentExports.setFdMap(new Map([[4, 7]]));
  childExports.setFdMap(new Map([[4, 7]]));

  sharedStateSync.syncOfdState(2, [7], makeKernelState(), "pull");

  expect(eventfdMarker(canonicalMemory, 7)).toBe(0xcc);
  expect(eventfdMarker(parentMemory, 7)).toBe(0xcc);
  expect(eventfdMarker(childMemory, 7)).toBe(0xcc);
});

test("kernel-worker timerfd OFD sync propagates timer backing time state", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedTimerfdOfd(parentMemory, 7, 1);
  seedTimerfdOfd(childMemory, 7, 1);
  seedTimeStateMarker(parentMemory, 0xaa);
  seedTimeStateMarker(childMemory, 0xcc);
  seedTimeStateMarker(canonicalMemory, 0x11);
  parentExports.setFdMap(new Map([[5, 7]]));
  childExports.setFdMap(new Map([[5, 7]]));

  sharedStateSync.syncOfdState(2, [7], makeKernelState());

  expect(timeStateMarker(canonicalMemory)).toBe(0xcc);
  expect(timeStateMarker(parentMemory)).toBe(0xcc);
});
