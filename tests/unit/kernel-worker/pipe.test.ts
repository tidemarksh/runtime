// Kernel-worker tests in this file validate transitional runtime-owned bridge behavior.
import { expect, test } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../src/abi.js";
import { createKernelWorkerPrimitives } from "../../../src/kernel-worker/primitives.js";
import { createKernelWorkerSharedStateSync } from "../../../src/kernel-worker/shared-state-sync.js";

const FD_TYPE_PIPE = 6;
const PIPE_BUF_SIZE = 65_536;
const PIPE_SLOT_BYTES = 32 + PIPE_BUF_SIZE;

type TestKernelExports = KernelExports & {
  fdMap(): Map<number, number>;
  setFdMap(map: Map<number, number>): void;
  setHostStdioSlots(slots: { stdin?: number; stdout?: number; stderr?: number }): void;
};

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memfsHeaderBytes: 0x1000,
    memfsInodesBytes: 0x10000,
    memfsFirstChildBytes: 0x4000,
    memfsDirEntriesBytes: 0x10000,
    memfsDataBytes: 0x40000,
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

function pipeSlotBase(state: KernelRuntimeState, pipeSlot: number): number {
  return state.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
}

function makeKernelExports(memory: WebAssembly.Memory): TestKernelExports {
  let runtimeState = makeKernelState();
  let stdinPipeSlot = 0;
  let stdoutPipeSlot = 0;
  let stderrPipeSlot = 0;
  let fdMap = new Map<number, number>();

  const exports = {
    memory,
    abi_max_fds: () => 256,
    abi_max_threads: () => 64,
    abi_max_pipes: () => 64,
    abi_fd_type_file: () => 1,
    abi_fd_type_pipe: () => FD_TYPE_PIPE,
    abi_o_rdonly: () => 0,
    abi_o_wronly: () => 1,
    abi_o_rdwr: () => 2,
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
    kernel_socket_used: () => runtimeState.socketUsed,
    kernel_pipe_base: () => runtimeState.pipeBase,
    kernel_pipe_region_bytes: () => 64 * PIPE_SLOT_BYTES,
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
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdMap.get(fd) ?? 0,
    process_fd_flags: () => 0,
    process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number, _fdFlags: number) => {
      if (ofdId === 0) {
        fdMap.delete(fd);
      } else {
        fdMap.set(fd, ofdId);
      }
    },
    process_thread_vm_ptr: (_vmPtr: number, slot: number) => (slot === 0 ? 0x1234 : 0),
    thread_blocked_ofd_id: (_vmPtr: number) => 0,
    process_stdin_pipe: () => stdinPipeSlot,
    process_stdout_pipe: () => stdoutPipeSlot,
    process_stderr_pipe: () => stderrPipeSlot,
    process_release_host_stdio: () => {
      stdinPipeSlot = 0;
      stdoutPipeSlot = 0;
      stderrPipeSlot = 0;
    },
    kernel_ofd_fd_type: (ofdId: number) => new DataView(memory.buffer)
      .getInt32(runtimeState.ofdTablePtr + ofdId * 32, true),
    kernel_ofd_host_fd: (ofdId: number) => new DataView(memory.buffer)
      .getInt32(runtimeState.ofdTablePtr + ofdId * 32 + 4, true),
    kernel_ofd_status_flags: (ofdId: number) => new DataView(memory.buffer)
      .getInt32(runtimeState.ofdTablePtr + ofdId * 32 + 8, true),
    kernel_ofd_ino: (ofdId: number) => new DataView(memory.buffer)
      .getUint32(runtimeState.ofdTablePtr + ofdId * 32 + 24, true),
    kernel_ofd_refcount: (ofdId: number) => new DataView(memory.buffer)
      .getInt32(runtimeState.ofdTablePtr + ofdId * 32 + 16, true),
    fdMap: () => new Map(fdMap),
    setFdMap: (map: Map<number, number>) => {
      fdMap = new Map(map);
    },
    setHostStdioSlots: (slots: { stdin?: number; stdout?: number; stderr?: number }) => {
      stdinPipeSlot = slots.stdin ?? 0;
      stdoutPipeSlot = slots.stdout ?? 0;
      stderrPipeSlot = slots.stderr ?? 0;
      const view = new DataView(memory.buffer);
      const seedImplicitPipeOfd = (fd: number, ofdId: number, pipeSlot: number, hostFd: number): void => {
        if (pipeSlot <= 0) {
          fdMap.delete(fd);
          return;
        }
        view.setInt32(runtimeState.ofdTablePtr + ofdId * 32, FD_TYPE_PIPE, true);
        view.setInt32(runtimeState.ofdTablePtr + ofdId * 32 + 4, hostFd, true);
        view.setInt32(runtimeState.ofdTablePtr + ofdId * 32 + 8, hostFd >= 0 ? 0 : 1, true);
        view.setInt32(runtimeState.ofdTablePtr + ofdId * 32 + 16, 1, true);
        view.setUint32(runtimeState.ofdTablePtr + ofdId * 32 + 24, pipeSlot, true);
        fdMap.set(fd, ofdId);
      };
      seedImplicitPipeOfd(0, 250, stdinPipeSlot, 10);
      seedImplicitPipeOfd(1, 251, stdoutPipeSlot, -11);
      seedImplicitPipeOfd(2, 252, stderrPipeSlot, -12);
    },
  };

  return exports as unknown as TestKernelExports;
}

function seedPipeOfd(
  exports: KernelExports,
  memory: WebAssembly.Memory,
  ofdId: number,
  pipeSlot: number,
  hostFd: number,
): void {
  const state = makeKernelState();
  const view = new DataView(memory.buffer);
  view.setInt32(state.ofdTablePtr + ofdId * 32, FD_TYPE_PIPE, true);
  view.setInt32(state.ofdTablePtr + ofdId * 32 + 4, hostFd, true);
  view.setInt32(
    state.ofdTablePtr + ofdId * 32 + 8,
    hostFd >= 0 ? 0 : 1,
    true,
  );
  view.setInt32(state.ofdTablePtr + ofdId * 32 + 16, 1, true);
  view.setUint32(state.ofdTablePtr + ofdId * 32 + 24, pipeSlot, true);
  if (exports.kernel_ofd_fd_type(ofdId) !== FD_TYPE_PIPE) {
    throw new Error(`failed to seed pipe OFD ${ofdId}`);
  }
}

function snapshotOfdSlot(memory: WebAssembly.Memory, ofdId: number): Uint8Array {
  const state = makeKernelState();
  const start = state.ofdTablePtr + ofdId * 32;
  return new Uint8Array(memory.buffer, start, 32).slice();
}

function seedPipeSlot(
  memory: WebAssembly.Memory,
  pipeSlot: number,
  marker: number,
  readOpen: number,
  writeOpen: number,
): void {
  seedPipeSlotState(memory, pipeSlot, { head: 0, tail: 1, marker, readOpen, writeOpen });
}

function seedPipeSlotState(
  memory: WebAssembly.Memory,
  pipeSlot: number,
  options: {
    head?: number;
    tail?: number;
    marker: number;
    readOpen: number;
    writeOpen: number;
  },
): void {
  const state = makeKernelState();
  const view = new DataView(memory.buffer);
  const base = pipeSlotBase(state, pipeSlot);
  view.setUint32(base, options.head ?? 0, true);
  view.setUint32(base + 4, options.tail ?? 1, true);
  view.setInt32(base + 8, options.readOpen, true);
  view.setInt32(base + 12, options.writeOpen, true);
  new Uint8Array(memory.buffer)[base + 32] = options.marker;
}

function pipeMarker(memory: WebAssembly.Memory, pipeSlot: number): number {
  const state = makeKernelState();
  return new Uint8Array(memory.buffer)[pipeSlotBase(state, pipeSlot) + 32] ?? 0;
}

function pipeTail(memory: WebAssembly.Memory, pipeSlot: number): number {
  const state = makeKernelState();
  return new DataView(memory.buffer).getUint32(pipeSlotBase(state, pipeSlot) + 4, true);
}

function pipeHead(memory: WebAssembly.Memory, pipeSlot: number): number {
  const state = makeKernelState();
  return new DataView(memory.buffer).getUint32(pipeSlotBase(state, pipeSlot), true);
}

function pipeReadOpen(memory: WebAssembly.Memory, pipeSlot: number): number {
  const state = makeKernelState();
  return new DataView(memory.buffer).getInt32(pipeSlotBase(state, pipeSlot) + 8, true);
}

function pipeWriteOpen(memory: WebAssembly.Memory, pipeSlot: number): number {
  const state = makeKernelState();
  return new DataView(memory.buffer).getInt32(pipeSlotBase(state, pipeSlot) + 12, true);
}

function makeSharedStateSync(
  parentExports: TestKernelExports,
  childExports: TestKernelExports,
  canonicalExports: TestKernelExports,
  canonicalProcesses = new Map<number, { ppid: number }>([
    [1, { ppid: 0 }],
    [2, { ppid: 0 }],
  ]),
) {
  const processKernels = new Map<number, KernelExports>([
    [1, parentExports],
    [2, childExports],
  ]);
  const getCanonicalProcessVmPtr = (pid: number): number => (pid === 1 || pid === 2 ? 0x1234 : 0);
  const primitives = createKernelWorkerPrimitives({
    getCanonicalExports: () => canonicalExports,
    processKernels,
    canonicalProcesses,
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

test("kernel-worker releaseHostStdio keeps sibling private host stdio pipe counts intact", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  parentExports.setHostStdioSlots({ stdout: 4 });
  childExports.setHostStdioSlots({ stdout: 5 });
  seedPipeSlot(parentMemory, 4, 0xaa, 1, 0);
  seedPipeSlot(childMemory, 5, 0xbb, 1, 0);

  sharedStateSync.releaseHostStdio(2, 0x1234, makeKernelState());

  expect(pipeReadOpen(parentMemory, 4)).toBe(1);
  expect(pipeReadOpen(childMemory, 5)).toBe(0);
  expect(pipeReadOpen(canonicalMemory, 4)).toBe(1);
});

test("kernel-worker pipe sync keeps host stdout readable for a spawned process with a parent pid", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(
    parentExports,
    childExports,
    canonicalExports,
    new Map([
      [1, { ppid: 0 }],
      [2, { ppid: 1 }],
    ]),
  );

  childExports.setHostStdioSlots({ stdout: 4 });
  seedPipeSlotState(childMemory, 4, {
    head: 0,
    tail: 0,
    marker: 0xcc,
    readOpen: 1,
    writeOpen: 1,
  });

  sharedStateSync.syncPipeState(2, makeKernelState());

  expect(pipeReadOpen(canonicalMemory, 4)).toBe(1);
  expect(pipeWriteOpen(canonicalMemory, 4)).toBe(1);
  expect(pipeReadOpen(childMemory, 4)).toBe(1);
  expect(pipeWriteOpen(childMemory, 4)).toBe(1);
});

test("kernel-worker pipe sync keeps host output readable after guest fd remapping", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  parentExports.setHostStdioSlots({ stderr: 5 });
  parentExports.setFdMap(new Map());
  seedPipeSlotState(parentMemory, 5, {
    head: 0,
    tail: 0,
    marker: 0xdd,
    readOpen: 1,
    writeOpen: 0,
  });

  sharedStateSync.syncPipeState(1, makeKernelState());

  expect(pipeReadOpen(canonicalMemory, 5)).toBe(1);
  expect(pipeReadOpen(parentMemory, 5)).toBe(1);
});

test("kernel-worker pipe sync propagates shared pipe slots by shared OFD ids", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedPipeOfd(parentExports, parentMemory, 7, 6, 10);
  seedPipeOfd(parentExports, parentMemory, 8, 6, -11);
  seedPipeOfd(childExports, childMemory, 7, 6, 10);
  seedPipeOfd(childExports, childMemory, 8, 6, -11);
  parentExports.setFdMap(new Map([[4, 7], [5, 8]]));
  childExports.setFdMap(new Map([[4, 7], [5, 8]]));
  seedPipeSlot(parentMemory, 6, 0xaa, 1, 1);
  seedPipeSlot(childMemory, 6, 0xcc, 1, 1);

  sharedStateSync.syncPipeState(2, makeKernelState());

  expect(pipeMarker(parentMemory, 6)).toBe(0xcc);
  expect(pipeMarker(canonicalMemory, 6)).toBe(0xcc);
});

test("kernel-worker pipe sync counts shared pipe endpoints by process-held OFD, not fd aliases", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedPipeOfd(parentExports, parentMemory, 7, 6, 10);
  seedPipeOfd(parentExports, parentMemory, 8, 6, -11);
  seedPipeOfd(childExports, childMemory, 7, 6, 10);
  seedPipeOfd(childExports, childMemory, 8, 6, -11);
  parentExports.setFdMap(new Map([[4, 7], [5, 8], [6, 7], [7, 8]]));
  childExports.setFdMap(new Map([[4, 7], [5, 8], [6, 7], [7, 8]]));
  seedPipeSlot(parentMemory, 6, 0xaa, 4, 4);
  seedPipeSlot(childMemory, 6, 0xcc, 4, 4);

  sharedStateSync.syncPipeState(2, makeKernelState());

  expect(pipeReadOpen(canonicalMemory, 6)).toBe(2);
  expect(pipeWriteOpen(canonicalMemory, 6)).toBe(2);
  expect(pipeReadOpen(parentMemory, 6)).toBe(2);
  expect(pipeWriteOpen(parentMemory, 6)).toBe(2);
  expect(pipeReadOpen(childMemory, 6)).toBe(2);
  expect(pipeWriteOpen(childMemory, 6)).toBe(2);
});

test("kernel-worker pipe sync keeps live fd table authoritative before recomputing pipe readers", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedPipeOfd(parentExports, parentMemory, 7, 6, 10);
  seedPipeOfd(childExports, childMemory, 8, 6, -11);
  childExports.setFdMap(new Map([[1, 8]]));
  seedPipeSlot(parentMemory, 6, 0xaa, 1, 1);
  seedPipeSlot(childMemory, 6, 0xcc, 1, 1);

  sharedStateSync.syncPipeState(
    1,
    makeKernelState(),
    [{ fd: 4, ofdId: 7, fdFlags: 0 }],
    [{ ofdId: 7, slot: snapshotOfdSlot(parentMemory, 7) }],
  );

  expect(parentExports.fdMap().has(4)).toBe(false);
  expect(pipeReadOpen(childMemory, 6)).toBe(0);
  expect(pipeWriteOpen(childMemory, 6)).toBe(1);
});

test("kernel-worker pipe sync can seed an unpublished child fd table from snapshots", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedPipeOfd(childExports, childMemory, 8, 6, -11);
  seedPipeSlot(childMemory, 6, 0xcc, 0, 1);

  sharedStateSync.syncPipeState(
    2,
    makeKernelState(),
    [{ fd: 1, ofdId: 8, fdFlags: 0 }],
    [{ ofdId: 8, slot: snapshotOfdSlot(childMemory, 8) }],
    undefined,
    0x1234,
    true,
  );

  expect(childExports.fdMap().get(1)).toBe(8);
  expect(pipeWriteOpen(canonicalMemory, 6)).toBe(1);
  expect(pipeWriteOpen(childMemory, 6)).toBe(1);
});

test("fork-shared OFD sync preserves parent reader while method child owns writer", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedPipeOfd(parentExports, parentMemory, 7, 6, 10);
  seedPipeOfd(childExports, childMemory, 8, 6, -11);
  parentExports.setFdMap(new Map([[4, 7]]));
  childExports.setFdMap(new Map([[5, 8]]));
  seedPipeSlotState(parentMemory, 6, {
    head: 0,
    tail: 0,
    marker: 0xaa,
    readOpen: 1,
    writeOpen: 1,
  });
  seedPipeSlotState(childMemory, 6, {
    head: 0,
    tail: 1,
    marker: 0xcc,
    readOpen: 1,
    writeOpen: 1,
  });

  sharedStateSync.syncForkSharedState(
    2,
    0x1234,
    [1],
    [{ pid: 1, vmPtr: 0x1234, kernelState: makeKernelState() }],
    [8],
    [{ ofdId: 8, slot: snapshotOfdSlot(childMemory, 8) }],
    [{ fd: 5, ofdId: 8, fdFlags: 0 }],
    false,
    false,
    false,
    makeKernelState(),
  );

  expect(pipeReadOpen(canonicalMemory, 6)).toBe(1);
  expect(pipeWriteOpen(canonicalMemory, 6)).toBe(1);
  expect(pipeReadOpen(parentMemory, 6)).toBe(1);
  expect(pipeWriteOpen(parentMemory, 6)).toBe(1);
  expect(pipeReadOpen(childMemory, 6)).toBe(1);
  expect(pipeWriteOpen(childMemory, 6)).toBe(1);
});

test("kernel-worker pipe sync ignores fd snapshots while applying matching OFD snapshots", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedPipeOfd(parentExports, parentMemory, 7, 6, 10);
  seedPipeOfd(parentExports, parentMemory, 8, 6, -11);
  parentExports.setFdMap(new Map([[5, 7]]));

  sharedStateSync.syncPipeState(
    1,
    makeKernelState(),
    [
      { fd: 5, ofdId: 10, fdFlags: 0 },
      { fd: 6, ofdId: 8, fdFlags: 1 },
      { fd: 7, ofdId: 11, fdFlags: 0 },
    ],
    [{ ofdId: 8, slot: snapshotOfdSlot(parentMemory, 8) }],
  );

  expect(parentExports.fdMap().get(5)).toBe(7);
  expect(parentExports.fdMap().has(6)).toBe(false);
  expect(parentExports.fdMap().has(7)).toBe(false);
});

test("kernel-worker pipe sync keeps private stdio pipe slots process-local", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  parentExports.setHostStdioSlots({ stdout: 4 });
  childExports.setHostStdioSlots({ stdout: 5 });
  seedPipeSlot(parentMemory, 4, 0xaa, 1, 0);
  seedPipeSlot(childMemory, 5, 0xbb, 1, 0);

  sharedStateSync.syncPipeState(2, makeKernelState());

  expect(pipeMarker(parentMemory, 4)).toBe(0xaa);
  expect(pipeMarker(parentMemory, 5)).toBe(0);
  expect(pipeMarker(canonicalMemory, 5)).toBe(0xbb);
});

test("kernel-worker pipe sync refreshes shared write-only pipe slots for sibling writers", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedPipeOfd(parentExports, parentMemory, 8, 6, -11);
  seedPipeOfd(childExports, childMemory, 8, 6, -11);
  parentExports.setFdMap(new Map([[5, 8]]));
  childExports.setFdMap(new Map([[5, 8]]));
  seedPipeSlotState(parentMemory, 6, {
    head: 0,
    tail: 1,
    marker: 0xaa,
    readOpen: 1,
    writeOpen: 1,
  });
  seedPipeSlotState(childMemory, 6, {
    head: 0,
    tail: 3,
    marker: 0xcc,
    readOpen: 1,
    writeOpen: 1,
  });

  sharedStateSync.syncPipeState(2, makeKernelState());

  expect(pipeMarker(parentMemory, 6)).toBe(0xcc);
  expect(pipeTail(parentMemory, 6)).toBe(3);
  expect(pipeMarker(canonicalMemory, 6)).toBe(0xcc);
  expect(pipeTail(canonicalMemory, 6)).toBe(3);
});

test("kernel-worker pipe sync propagates shared writer tail across ring wrap", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedPipeOfd(parentExports, parentMemory, 8, 6, -11);
  seedPipeOfd(childExports, childMemory, 8, 6, -11);
  parentExports.setFdMap(new Map([[5, 8]]));
  childExports.setFdMap(new Map([[5, 8]]));
  seedPipeSlotState(parentMemory, 6, {
    head: 0,
    tail: 65_520,
    marker: 0xaa,
    readOpen: 1,
    writeOpen: 1,
  });
  seedPipeSlotState(childMemory, 6, {
    head: 0,
    tail: 20,
    marker: 0xcc,
    readOpen: 1,
    writeOpen: 1,
  });

  sharedStateSync.syncPipeState(2, makeKernelState());

  expect(pipeMarker(parentMemory, 6)).toBe(0xcc);
  expect(pipeTail(parentMemory, 6)).toBe(20);
  expect(pipeMarker(canonicalMemory, 6)).toBe(0xcc);
  expect(pipeTail(canonicalMemory, 6)).toBe(20);
});

test("kernel-worker pipe sync does not let a stale writer clobber canonical output", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedPipeOfd(parentExports, parentMemory, 8, 6, -11);
  seedPipeOfd(childExports, childMemory, 8, 6, -11);
  parentExports.setFdMap(new Map([[5, 8]]));
  childExports.setFdMap(new Map([[5, 8]]));

  seedPipeSlotState(canonicalMemory, 6, {
    head: 0,
    tail: 3,
    marker: 0xcc,
    readOpen: 1,
    writeOpen: 1,
  });
  seedPipeSlotState(parentMemory, 6, {
    head: 0,
    tail: 1,
    marker: 0xaa,
    readOpen: 1,
    writeOpen: 1,
  });

  sharedStateSync.syncPipeState(1, makeKernelState());

  expect(pipeMarker(canonicalMemory, 6)).toBe(0xcc);
  expect(pipeTail(canonicalMemory, 6)).toBe(3);
  expect(pipeMarker(parentMemory, 6)).toBe(0xcc);
  expect(pipeTail(parentMemory, 6)).toBe(3);
});

test("kernel-worker pipe sync does not promote reader-only stale tail as writer output", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedPipeOfd(parentExports, parentMemory, 7, 6, 10);
  parentExports.setFdMap(new Map([[4, 7]]));

  seedPipeSlotState(canonicalMemory, 6, {
    head: 0,
    tail: 3,
    marker: 0xcc,
    readOpen: 1,
    writeOpen: 1,
  });
  seedPipeSlotState(parentMemory, 6, {
    head: 0,
    tail: 5,
    marker: 0xaa,
    readOpen: 1,
    writeOpen: 1,
  });

  sharedStateSync.syncPipeState(1, makeKernelState());

  expect(pipeMarker(canonicalMemory, 6)).toBe(0xcc);
  expect(pipeTail(canonicalMemory, 6)).toBe(3);
});

test("kernel-worker pipe sync does not promote writer-only stale head as reader progress", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedPipeOfd(parentExports, parentMemory, 8, 6, -11);
  parentExports.setFdMap(new Map([[5, 8]]));

  seedPipeSlotState(canonicalMemory, 6, {
    head: 2,
    tail: 8,
    marker: 0xcc,
    readOpen: 1,
    writeOpen: 1,
  });
  seedPipeSlotState(parentMemory, 6, {
    head: 6,
    tail: 8,
    marker: 0xaa,
    readOpen: 1,
    writeOpen: 1,
  });

  sharedStateSync.syncPipeState(1, makeKernelState());

  expect(pipeHead(canonicalMemory, 6)).toBe(2);
  expect(pipeTail(canonicalMemory, 6)).toBe(8);
});

test("kernel-worker pipe sync does not reset canonical data from a stale zeroed reader slot", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  seedPipeOfd(parentExports, parentMemory, 9, 6, 10);
  parentExports.setFdMap(new Map([[4, 9]]));

  seedPipeSlotState(canonicalMemory, 6, {
    head: 0,
    tail: 3,
    marker: 0xcc,
    readOpen: 1,
    writeOpen: 1,
  });
  seedPipeSlotState(parentMemory, 6, {
    head: 0,
    tail: 0,
    marker: 0xaa,
    readOpen: 1,
    writeOpen: 1,
  });

  sharedStateSync.syncPipeState(1, makeKernelState());

  expect(pipeMarker(canonicalMemory, 6)).toBe(0xcc);
  expect(pipeTail(canonicalMemory, 6)).toBe(3);
  expect(pipeMarker(parentMemory, 6)).toBe(0xcc);
  expect(pipeTail(parentMemory, 6)).toBe(3);
});

test("kernel-worker pipe snapshot apply does not full-copy stale reader slots", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const targetMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const targetExports = makeKernelExports(targetMemory);
  const primitives = createKernelWorkerPrimitives({
    getCanonicalExports: () => canonicalExports,
    processKernels: new Map([[1, targetExports]]),
    canonicalProcesses: new Map([[1, { ppid: 0 }]]),
    getCanonicalProcessVmPtr: (pid) => (pid === 1 ? 0x1234 : 0),
  });

  seedPipeSlotState(targetMemory, 6, {
    head: 9,
    tail: 9,
    marker: 0x7a,
    readOpen: 1,
    writeOpen: 1,
  });
  const staleReaderSlot = new Uint8Array(PIPE_SLOT_BYTES);
  const staleReaderView = new DataView(staleReaderSlot.buffer);
  staleReaderView.setUint32(0, 0, true);
  staleReaderView.setUint32(4, 9, true);
  staleReaderView.setInt32(8, 1, true);
  staleReaderView.setInt32(12, 1, true);
  staleReaderSlot[32] = 0x99;

  primitives.applySelectedPipeSlotSnapshots(
    targetExports,
    makeKernelState(),
    [{ pipeSlot: 6, slot: staleReaderSlot }],
  );

  expect(pipeHead(targetMemory, 6)).toBe(9);
  expect(pipeTail(targetMemory, 6)).toBe(9);
  expect(pipeMarker(targetMemory, 6)).toBe(0x7a);
});

test("kernel-worker pipe snapshot apply copies only forward writer tail delta", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const targetMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const targetExports = makeKernelExports(targetMemory);
  const primitives = createKernelWorkerPrimitives({
    getCanonicalExports: () => canonicalExports,
    processKernels: new Map([[1, targetExports]]),
    canonicalProcesses: new Map([[1, { ppid: 0 }]]),
    getCanonicalProcessVmPtr: (pid) => (pid === 1 ? 0x1234 : 0),
  });

  const state = makeKernelState();
  const base = pipeSlotBase(state, 6);
  seedPipeSlotState(targetMemory, 6, {
    head: 0,
    tail: 8,
    marker: 0x11,
    readOpen: 1,
    writeOpen: 1,
  });
  const targetBytes = new Uint8Array(targetMemory.buffer);
  targetBytes[base + 32 + 2] = 0xc2;

  const writerSlot = new Uint8Array(PIPE_SLOT_BYTES);
  const writerView = new DataView(writerSlot.buffer);
  writerView.setUint32(0, 0, true);
  writerView.setUint32(4, 16, true);
  writerView.setInt32(8, 1, true);
  writerView.setInt32(12, 1, true);
  writerSlot[32 + 2] = 0xd2;
  writerSlot[32 + 12] = 0xd0;

  primitives.applySelectedPipeSlotSnapshots(
    targetExports,
    state,
    [{ pipeSlot: 6, slot: writerSlot }],
  );

  expect(pipeHead(targetMemory, 6)).toBe(0);
  expect(pipeTail(targetMemory, 6)).toBe(16);
  expect(targetBytes[base + 32 + 2]).toBe(0xc2);
  expect(targetBytes[base + 32 + 12]).toBe(0xd0);
});

test("kernel-worker pipe snapshot apply copies wrapped writer tail delta in two spans", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const targetMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const targetExports = makeKernelExports(targetMemory);
  const primitives = createKernelWorkerPrimitives({
    getCanonicalExports: () => canonicalExports,
    processKernels: new Map([[1, targetExports]]),
    canonicalProcesses: new Map([[1, { ppid: 0 }]]),
    getCanonicalProcessVmPtr: (pid) => (pid === 1 ? 0x1234 : 0),
  });

  const state = makeKernelState();
  const base = pipeSlotBase(state, 6);
  const targetTail = PIPE_BUF_SIZE - 8;
  seedPipeSlotState(targetMemory, 6, {
    head: 0,
    tail: targetTail,
    marker: 0x11,
    readOpen: 1,
    writeOpen: 1,
  });
  const targetBytes = new Uint8Array(targetMemory.buffer);
  targetBytes[base + 32 + 512] = 0xc5;

  const writerSlot = new Uint8Array(PIPE_SLOT_BYTES);
  const writerView = new DataView(writerSlot.buffer);
  writerView.setUint32(0, 0, true);
  writerView.setUint32(4, 8, true);
  writerView.setInt32(8, 1, true);
  writerView.setInt32(12, 1, true);
  writerSlot[32 + PIPE_BUF_SIZE - 4] = 0xd1;
  writerSlot[32 + 4] = 0xd2;
  writerSlot[32 + 512] = 0xd5;

  primitives.applySelectedPipeSlotSnapshots(
    targetExports,
    state,
    [{ pipeSlot: 6, slot: writerSlot }],
  );

  expect(pipeHead(targetMemory, 6)).toBe(0);
  expect(pipeTail(targetMemory, 6)).toBe(8);
  expect(targetBytes[base + 32 + PIPE_BUF_SIZE - 4]).toBe(0xd1);
  expect(targetBytes[base + 32 + 4]).toBe(0xd2);
  expect(targetBytes[base + 32 + 512]).toBe(0xc5);
});

test("kernel-worker pipe snapshot apply preserves full-drain reader progress", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const targetMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const targetExports = makeKernelExports(targetMemory);
  const primitives = createKernelWorkerPrimitives({
    getCanonicalExports: () => canonicalExports,
    processKernels: new Map([[1, targetExports]]),
    canonicalProcesses: new Map([[1, { ppid: 0 }]]),
    getCanonicalProcessVmPtr: (pid) => (pid === 1 ? 0x1234 : 0),
  });

  seedPipeSlotState(targetMemory, 6, {
    head: 0,
    tail: 0,
    marker: 0x7a,
    readOpen: 1,
    writeOpen: 1,
  });
  const drainedReaderSlot = new Uint8Array(PIPE_SLOT_BYTES);
  const drainedReaderView = new DataView(drainedReaderSlot.buffer);
  drainedReaderView.setUint32(0, 12, true);
  drainedReaderView.setUint32(4, 12, true);
  drainedReaderView.setInt32(8, 1, true);
  drainedReaderView.setInt32(12, 1, true);
  drainedReaderSlot[32] = 0x99;

  primitives.applySelectedPipeSlotSnapshots(
    targetExports,
    makeKernelState(),
    [{ pipeSlot: 6, slot: drainedReaderSlot, mode: 1 }],
  );

  expect(pipeHead(targetMemory, 6)).toBe(12);
  expect(pipeTail(targetMemory, 6)).toBe(12);
  expect(pipeMarker(targetMemory, 6)).toBe(0x7a);
});

test("kernel-worker pipe sync does not let a stale private stdout slot clobber canonical PTY output", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  parentExports.setHostStdioSlots({ stdout: 4, stderr: 4 });
  childExports.setHostStdioSlots({ stdout: 4, stderr: 4 });

  seedPipeSlotState(childMemory, 4, {
    head: 0,
    tail: 3,
    marker: 0xcc,
    readOpen: 1,
    writeOpen: 1,
  });
  seedPipeSlotState(parentMemory, 4, {
    head: 0,
    tail: 1,
    marker: 0xaa,
    readOpen: 1,
    writeOpen: 1,
  });

  sharedStateSync.syncPipeState(2, makeKernelState());
  sharedStateSync.syncPipeState(1, makeKernelState());

  expect(pipeMarker(canonicalMemory, 4)).toBe(0xcc);
  expect(pipeTail(canonicalMemory, 4)).toBe(3);
  expect(pipeMarker(parentMemory, 4)).toBe(0xcc);
  expect(pipeTail(parentMemory, 4)).toBe(3);
});

test("kernel-worker pipe sync advances private stdout read head from canonical PTY output", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  parentExports.setHostStdioSlots({ stdout: 4, stderr: 4 });
  childExports.setHostStdioSlots({ stdout: 4, stderr: 4 });

  seedPipeSlotState(childMemory, 4, {
    head: 3,
    tail: 3,
    marker: 0xcc,
    readOpen: 1,
    writeOpen: 1,
  });
  seedPipeSlotState(parentMemory, 4, {
    head: 0,
    tail: 1,
    marker: 0xaa,
    readOpen: 1,
    writeOpen: 1,
  });

  sharedStateSync.syncPipeState(2, makeKernelState());
  sharedStateSync.syncPipeState(1, makeKernelState());

  expect(pipeHead(canonicalMemory, 4)).toBe(3);
  expect(pipeHead(parentMemory, 4)).toBe(3);
  expect(pipeTail(parentMemory, 4)).toBe(3);
});

test("kernel-worker pipe sync propagates closed-child PTY output when local tail is ahead", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  canonicalExports.setHostStdioSlots({ stdout: 4, stderr: 4 });
  parentExports.setHostStdioSlots({ stdout: 4, stderr: 4 });
  childExports.setHostStdioSlots({ stdout: 4, stderr: 4 });

  seedPipeSlotState(canonicalMemory, 4, {
    head: 0,
    tail: 1,
    marker: 0xaa,
    readOpen: 1,
    writeOpen: 1,
  });
  seedPipeSlotState(childMemory, 4, {
    head: 0,
    tail: 3,
    marker: 0xcc,
    readOpen: 1,
    writeOpen: 1,
  });

  sharedStateSync.syncPipeState(2, makeKernelState());
  sharedStateSync.syncPipeState(1, makeKernelState());

  expect(pipeMarker(canonicalMemory, 4)).toBe(0xcc);
  expect(pipeTail(canonicalMemory, 4)).toBe(3);
  expect(pipeMarker(parentMemory, 4)).toBe(0xcc);
  expect(pipeTail(parentMemory, 4)).toBe(3);
});

test("kernel-worker pipe sync immediately refreshes the parent's shared PTY stdout slot from child output", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  parentExports.setHostStdioSlots({ stdout: 4, stderr: 4 });
  childExports.setHostStdioSlots({ stdout: 4, stderr: 4 });

  seedPipeSlotState(parentMemory, 4, {
    head: 7,
    tail: 7,
    marker: 0xaa,
    readOpen: 1,
    writeOpen: 1,
  });
  seedPipeSlotState(childMemory, 4, {
    head: 0,
    tail: 82,
    marker: 0xcc,
    readOpen: 1,
    writeOpen: 1,
  });

  sharedStateSync.syncPipeState(2, makeKernelState());

  const state = makeKernelState();
  const view = new DataView(parentMemory.buffer);
  const base = pipeSlotBase(state, 4);
  expect(view.getUint32(base, true)).toBe(7);
  expect(view.getUint32(base + 4, true)).toBe(82);
  expect(pipeMarker(parentMemory, 4)).toBe(0xcc);
});

test("kernel-worker canonical PTY refresh preserves the parent's local read head on private stdout", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const childMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const childExports = makeKernelExports(childMemory);
  const sharedStateSync = makeSharedStateSync(parentExports, childExports, canonicalExports);

  parentExports.setHostStdioSlots({ stdout: 4, stderr: 4 });
  childExports.setHostStdioSlots({ stdout: 4, stderr: 4 });

  seedPipeSlotState(parentMemory, 4, {
    head: 7,
    tail: 7,
    marker: 0xaa,
    readOpen: 1,
    writeOpen: 1,
  });
  seedPipeSlotState(canonicalMemory, 4, {
    head: 0,
    tail: 82,
    marker: 0xcc,
    readOpen: 1,
    writeOpen: 1,
  });

  sharedStateSync.syncPipeState(1, makeKernelState());

  const state = makeKernelState();
  const view = new DataView(parentMemory.buffer);
  const base = pipeSlotBase(state, 4);
  expect(view.getUint32(base, true)).toBe(7);
  expect(view.getUint32(base + 4, true)).toBe(82);
  expect(pipeMarker(parentMemory, 4)).toBe(0xcc);
});

test("kernel-worker canonical PTY refresh preserves the process-local read head on private stdin", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const processKernels = new Map<number, KernelExports>([[1, parentExports]]);
  const primitives = createKernelWorkerPrimitives({
    getCanonicalExports: () => canonicalExports,
    processKernels,
    canonicalProcesses: new Map([[1, { ppid: 0 }]]),
    getCanonicalProcessVmPtr: (pid) => (pid === 1 ? 0x1234 : 0),
  });

  parentExports.setHostStdioSlots({ stdin: 4 });

  seedPipeSlotState(parentMemory, 4, {
    head: 7,
    tail: 12,
    marker: 0xaa,
    readOpen: 1,
    writeOpen: 1,
  });
  seedPipeSlotState(canonicalMemory, 4, {
    head: 0,
    tail: 12,
    marker: 0xcc,
    readOpen: 1,
    writeOpen: 1,
  });

  primitives.mergePipeSlotsFromCanonical(
    canonicalExports,
    makeKernelState(),
    parentExports,
    makeKernelState(),
    0x1234,
    [4],
  );

  expect(pipeHead(parentMemory, 4)).toBe(7);
  expect(pipeTail(parentMemory, 4)).toBe(12);
  expect(pipeMarker(parentMemory, 4)).toBe(0xaa);
});

test("kernel-worker canonical PTY refresh advances private stdin read head when canonical is newer", () => {
  const canonicalMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const parentMemory = new WebAssembly.Memory({ initial: 256, maximum: 512, shared: true });
  const canonicalExports = makeKernelExports(canonicalMemory);
  const parentExports = makeKernelExports(parentMemory);
  const processKernels = new Map<number, KernelExports>([[1, parentExports]]);
  const primitives = createKernelWorkerPrimitives({
    getCanonicalExports: () => canonicalExports,
    processKernels,
    canonicalProcesses: new Map([[1, { ppid: 0 }]]),
    getCanonicalProcessVmPtr: (pid) => (pid === 1 ? 0x1234 : 0),
  });

  parentExports.setHostStdioSlots({ stdin: 4 });

  seedPipeSlotState(parentMemory, 4, {
    head: 3,
    tail: 12,
    marker: 0xaa,
    readOpen: 1,
    writeOpen: 1,
  });
  seedPipeSlotState(canonicalMemory, 4, {
    head: 7,
    tail: 12,
    marker: 0xcc,
    readOpen: 1,
    writeOpen: 1,
  });

  primitives.mergePipeSlotsFromCanonical(
    canonicalExports,
    makeKernelState(),
    parentExports,
    makeKernelState(),
    0x1234,
    [4],
  );

  expect(pipeHead(parentMemory, 4)).toBe(7);
  expect(pipeTail(parentMemory, 4)).toBe(12);
  expect(pipeMarker(parentMemory, 4)).toBe(0xaa);
});
