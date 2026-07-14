// Kernel-worker tests in this file validate shared-state bridge behavior during supervisor migration.
import { expect, test } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../src/abi.js";
import { createKernelWorkerPrimitives } from "../../../src/kernel-worker/primitives.js";
import { createKernelWorkerSharedStateSync } from "../../../src/kernel-worker/shared-state-sync.js";

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

function makeKernelExports(memory: WebAssembly.Memory): KernelExports {
  let runtimeState = makeKernelState();
  const exports = {
    memory,
    abi_max_fds: () => 256,
    abi_max_threads: () => 64,
    abi_fd_type_file: () => 1,
    abi_fd_type_epoll: () => 3,
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
    kernel_socket_used: () => runtimeState.socketUsed,
    kernel_pipe_base: () => runtimeState.pipeBase,
    kernel_pipe_used: () => runtimeState.pipeUsed,
    kernel_net_buf_base: () => runtimeState.netBufBase,
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
    process_fd_ofd_id: (_vmPtr: number, _fd: number) => 0,
    process_thread_vm_ptr: (_vmPtr: number, slot: number) => (slot === 0 ? 0x1234 : 0),
    thread_blocked_ofd_id: (_vmPtr: number) => 0,
    supervisor_process_held_ofd_refcount(this: KernelExports, vmPtr: number, ofdId: number) {
      let refcount = 0;
      for (let fd = 0; fd < this.abi_max_fds(); fd += 1) {
        if (this.process_fd_ofd_id(vmPtr, fd) === ofdId) {
          refcount += 1;
        }
      }
      return refcount;
    },
    supervisor_ofd_fd_type(this: KernelExports, ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_fd_type(ofdId) : 0;
    },
    supervisor_ofd_ino(this: KernelExports, ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_ino(ofdId) : 0;
    },
    supervisor_ofd_host_fd(this: KernelExports, ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_host_fd(ofdId) : -1;
    },
    supervisor_ofd_refcount(this: KernelExports, ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_refcount(ofdId) : 0;
    },
    supervisor_ofd_offset(this: KernelExports, ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_offset(ofdId) : 0n;
    },
    kernel_ofd_fd_type: (ofdId: number) => new DataView(memory.buffer)
      .getInt32(runtimeState.ofdTablePtr + ofdId * 32, true),
    kernel_ofd_ino: (ofdId: number) => new DataView(memory.buffer)
      .getUint32(runtimeState.ofdTablePtr + ofdId * 32 + 24, true),
    kernel_ofd_host_fd: (ofdId: number) => new DataView(memory.buffer)
      .getInt32(runtimeState.ofdTablePtr + ofdId * 32 + 4, true),
    kernel_ofd_refcount: (ofdId: number) => new DataView(memory.buffer)
      .getInt32(runtimeState.ofdTablePtr + ofdId * 32 + 12, true),
    kernel_ofd_offset: (ofdId: number) => new DataView(memory.buffer)
      .getBigInt64(runtimeState.ofdTablePtr + ofdId * 32 + 16, true),
  };
  return exports as unknown as KernelExports;
}

function seedEpollOfd(
  exports: KernelExports,
  memory: WebAssembly.Memory,
  ofdId: number,
  epollIdx: number,
  marker: number,
): void {
  const state = makeKernelState();
  const view = new DataView(memory.buffer);
  view.setInt32(state.ofdTablePtr + ofdId * 32, exports.abi_fd_type_epoll(), true);
  view.setUint32(state.ofdTablePtr + ofdId * 32 + 24, epollIdx, true);
  const epollTable = new Uint32Array(memory.buffer, state.epollTablePtr, exports.kernel_epoll_table_bytes() >>> 2);
  const instancePtr = state.epollTablePtr + 0x1000 + epollIdx * 0x2000;
  epollTable[epollIdx] = instancePtr;
  new Uint8Array(memory.buffer, instancePtr, exports.kernel_epoll_instance_bytes()).fill(marker);
}

function epollMarker(memory: WebAssembly.Memory, epollIdx: number): number {
  const state = makeKernelState();
  const epollTable = new Uint32Array(memory.buffer, state.epollTablePtr, 64);
  const instancePtr = epollTable[epollIdx] ?? 0;
  if (instancePtr === 0) {
    return 0;
  }
  return new Uint8Array(memory.buffer, instancePtr, 1)[0] ?? 0;
}

function makeSharedStateSync(parentExports: KernelExports, childExports: KernelExports, canonicalExports: KernelExports) {
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

test("kernel-worker epoll primitive uses supervisor held epoll list", () => {
  const memory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(memory);
  const processExports = {
    ...makeKernelExports(memory),
    supervisor_process_epoll_held_ofd: (_vmPtr: number, index: number) => (
      index === 0 ? 7 : index === 1 ? 9 : 0
    ),
  } as KernelExports;
  const primitives = createKernelWorkerPrimitives({
    getCanonicalExports: () => canonicalExports,
    processKernels: new Map([[1, processExports]]),
    canonicalProcesses: new Map([[1, { ppid: 0 }]]),
    getCanonicalProcessVmPtr: () => 0x1234,
  });

  expect(primitives.collectProcessHeldEpollOfdIds(processExports, 0x1234)).toEqual([7, 9]);
});

test("kernel-worker epoll sync keeps private epoll instances process-local", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedEpollOfd(parentExports, parentMemory, 7, 0, 0xaa);
  seedEpollOfd(childExports, childMemory, 9, 1, 0xbb);
  parentExports.process_fd_ofd_id = (_vmPtr, fd) => (fd === 4 ? 7 : 0);
  childExports.process_fd_ofd_id = (_vmPtr, fd) => (fd === 4 ? 9 : 0);

  sharedStateSync.syncEpollState(2, makeKernelState());

  const parentTable = new Uint32Array(parentMemory.buffer, makeKernelState().epollTablePtr, 64);
  const canonicalTable = new Uint32Array(canonicalMemory.buffer, makeKernelState().epollTablePtr, 64);
  expect(parentTable[0]).not.toBe(0);
  expect(parentTable[1]).toBe(0);
  expect(epollMarker(parentMemory, 0)).toBe(0xaa);
  expect(canonicalTable[1]).not.toBe(0);
  expect(epollMarker(canonicalMemory, 1)).toBe(0xbb);
});

test("kernel-worker epoll sync propagates shared epoll instances by shared OFD id", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedEpollOfd(parentExports, parentMemory, 7, 0, 0xaa);
  seedEpollOfd(childExports, childMemory, 7, 0, 0xcc);
  parentExports.process_fd_ofd_id = (_vmPtr, fd) => (fd === 4 ? 7 : 0);
  childExports.process_fd_ofd_id = (_vmPtr, fd) => (fd === 4 ? 7 : 0);

  sharedStateSync.syncEpollState(2, makeKernelState());

  expect(epollMarker(parentMemory, 0)).toBe(0xcc);
  expect(epollMarker(canonicalMemory, 0)).toBe(0xcc);
});
