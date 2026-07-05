import { expect, test, vi } from "vite-plus/test";
import {
  processStderrPipe,
  processStdinPipe,
  processStdoutPipe,
} from "../../../src/worker/process-stdio-state.js";
import {
  snapshotKernelShareOfdSlots,
} from "../../../src/worker/fork-shared.js";
import {
  activeKernelShareOfdIds,
  collectHeldEpollDependentSyncOfdIds,
  collectHeldEpollOfdIds,
  collectProcessHeldOfdRefcounts,
  collectProcessHeldOfds,
  collectProcessSyncDependentOfdIds,
  processEpollDependsOnOfd,
  trackForkSharedOfds,
} from "../../../src/worker/ownership/ofd-table.js";
import {
  addKernelShareMember,
  addKernelShareSharedOfdId,
  kernelShareHasSharedOfdId,
  kernelShareMembers,
  kernelShareSharedOfdIdList,
  processKernelShareGroup,
  reconcileKernelSharePipeControl,
  restoreLocalPipeSlots,
  restoreLocalOfdSlots,
  snapshotLocalPipeSlots,
  setProcessKernelShareGroup,
} from "../../../src/worker/kernel-share.js";
import { setProcessDebugCommand } from "../../../src/worker/process-debug-state.js";
import { refreshPipeControlFromKernelShare } from "../../../src/worker/ownership/pipe-control.js";
import type { ProcessHandle } from "../../../src/worker/types.js";
import { seedProcessHandleStateFixture } from "./support/process-handle-state-fixture.js";

const FD_TYPE_FILE = 1;
const FD_TYPE_PIPE = 6;
const FD_TYPE_EVENTFD = 9;
const FD_TYPE_TIMERFD = 10;
const FD_TYPE_PTY_MASTER = 11;
const FD_TYPE_PTY_SLAVE = 12;
const PIPE_SLOT_MODE_READ = 1;
const PIPE_SLOT_MODE_WRITE = 2;

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
  setPrivatePipeMask(mask: bigint): void;
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
  let privatePipeMask = 0n;
  const state = makeKernelState();
  const pipeBit = (pipeSlot: number): bigint => (
    pipeSlot > 0 && pipeSlot < 64 ? 1n << BigInt(pipeSlot) : 0n
  );
  const pipeModesForOfd = (ofdId: number): Array<{ pipeSlot: number; mode: number }> => {
    if (ofdId <= 0 || ofdId >= 32) {
      return [];
    }
    const view = new DataView(memory.buffer);
    const ofdBase = state.ofdTablePtr + ofdId * 32;
    const fdType = view.getInt32(ofdBase, true);
    const ino = view.getUint32(ofdBase + 24, true);
    const accessMode = view.getInt32(ofdBase + 8, true) & 0x3;
    if (fdType === FD_TYPE_PIPE) {
      const pipeSlot = ino;
      if (pipeSlot <= 0 || pipeSlot >= 64) {
        return [];
      }
      return [{
        pipeSlot,
        mode: view.getInt32(ofdBase + 4, true) >= 0 ? PIPE_SLOT_MODE_READ : PIPE_SLOT_MODE_WRITE,
      }];
    }
    if (fdType !== FD_TYPE_PTY_SLAVE && fdType !== FD_TYPE_PTY_MASTER) {
      return [];
    }
    const inputPipe = ino * 2 + 1;
    const outputPipe = ino * 2 + 2;
    const modes: Array<{ pipeSlot: number; mode: number }> = [];
    if (fdType === FD_TYPE_PTY_SLAVE) {
      if (accessMode !== 1) {
        modes.push({ pipeSlot: inputPipe, mode: PIPE_SLOT_MODE_READ });
      }
      if (accessMode !== 0) {
        modes.push({ pipeSlot: outputPipe, mode: PIPE_SLOT_MODE_WRITE });
      }
    } else {
      if (accessMode !== 1) {
        modes.push({ pipeSlot: outputPipe, mode: PIPE_SLOT_MODE_READ });
      }
      if (accessMode !== 0) {
        modes.push({ pipeSlot: inputPipe, mode: PIPE_SLOT_MODE_WRITE });
      }
    }
    return modes.filter(({ pipeSlot }) => pipeSlot > 0 && pipeSlot < 64);
  };
  const pipeModeForOfd = (ofdId: number): { pipeSlot: number; mode: number } | null => {
    return pipeModesForOfd(ofdId)[0] ?? null;
  };
  const heldPipeSlotMask = (): bigint => {
    let mask = 0n;
    for (let ofdId = 1; ofdId < 32; ofdId += 1) {
      for (const pipeMode of pipeModesForOfd(ofdId)) {
        const bit = pipeBit(pipeMode.pipeSlot);
        if (bit !== 0n && (privatePipeMask & bit) === 0n && (fdMapHasOfd(ofdId) || blockedMapHasOfd(ofdId))) {
          mask |= bit;
        }
      }
    }
    return mask;
  };
  const pipeSlotMode = (pipeSlot: number): number => {
    if ((privatePipeMask & pipeBit(pipeSlot)) !== 0n) {
      return 0;
    }
    let mode = 0;
    for (let ofdId = 1; ofdId < 32; ofdId += 1) {
      if (!fdMapHasOfd(ofdId) && !blockedMapHasOfd(ofdId)) {
        continue;
      }
      for (const pipeMode of pipeModesForOfd(ofdId)) {
        if (pipeMode.pipeSlot === pipeSlot) {
          mode |= pipeMode.mode;
        }
      }
    }
    return mode;
  };
  const pipeOpenCount = (pipeSlot: number, requestedMode: number): number => {
    let count = 0;
    for (let ofdId = 1; ofdId < 32; ofdId += 1) {
      if (!fdMapHasOfd(ofdId) && !blockedMapHasOfd(ofdId)) {
        continue;
      }
      for (const pipeMode of pipeModesForOfd(ofdId)) {
        if (pipeMode.pipeSlot === pipeSlot && (pipeMode.mode & requestedMode) !== 0) {
          count += 1;
          break;
        }
      }
    }
    return count;
  };
  const fdMapHasOfd = (ofdId: number): boolean => {
    for (const heldOfdId of fdMap.values()) {
      if (heldOfdId === ofdId) {
        return true;
      }
    }
    return false;
  };
  const blockedMapHasOfd = (ofdId: number): boolean => {
    for (const blockedOfdId of blockedOfdMap.values()) {
      if (blockedOfdId === ofdId) {
        return true;
      }
    }
    return false;
  };
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
    abi_fd_type_file: () => FD_TYPE_FILE,
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
    process_fd_flags: () => 0,
    supervisor_process_ofd_for_fd(this: ProcessHandle["kernel"], vmPtr: number, fd: number) {
      const ofdId = this.process_fd_ofd_id(vmPtr, fd);
      return ofdId > 0 && ofdId < this.abi_max_fds() ? ofdId : 0;
    },
    supervisor_process_fd_flags_for_fd(this: ProcessHandle["kernel"], vmPtr: number, fd: number) {
      return fd >= 0 && fd < this.abi_max_fds() ? this.process_fd_flags(vmPtr, fd) : 0;
    },
    supervisor_process_pipe_slot_for_fd(this: ProcessHandle["kernel"], vmPtr: number, fd: number) {
      const ofdId = this.supervisor_process_ofd_for_fd?.(vmPtr, fd) ?? 0;
      if (ofdId <= 0 || ofdId >= this.abi_max_fds()) {
        return 0;
      }
      const fdType = this.kernel_ofd_fd_type(ofdId);
      if (fdType === this.abi_fd_type_pipe()) {
        const pipeSlot = this.kernel_ofd_ino(ofdId);
        return pipeSlot > 0 && pipeSlot < this.abi_max_pipes() ? pipeSlot : 0;
      }
      const ptySlaveType = this.abi_fd_type_pty_slave?.();
      const ptyMasterType = this.abi_fd_type_pty_master?.();
      if (fdType !== ptySlaveType && fdType !== ptyMasterType) {
        return 0;
      }
      const ptyIdx = this.kernel_ofd_ino(ofdId);
      const accessMode = (this.kernel_ofd_status_flags?.(ofdId) ?? 0) & 0x3;
      const fdWritesToStdioOutput = fd === 1 || fd === 2 || accessMode === 1 || accessMode === 2;
      const pipeSlot = fdType === ptySlaveType
        ? fdWritesToStdioOutput
          ? (this.kernel_pty_output_pipe?.(ptyIdx) ?? 0)
          : (this.kernel_pty_input_pipe?.(ptyIdx) ?? 0)
        : fdWritesToStdioOutput
          ? (this.kernel_pty_input_pipe?.(ptyIdx) ?? 0)
          : (this.kernel_pty_output_pipe?.(ptyIdx) ?? 0);
      return pipeSlot > 0 && pipeSlot < this.abi_max_pipes() ? pipeSlot : 0;
    },
    supervisor_process_pipe_ofd_for_fd(this: ProcessHandle["kernel"], vmPtr: number, fd: number) {
      const ofdId = this.supervisor_process_ofd_for_fd?.(vmPtr, fd) ?? 0;
      return ofdId > 0 && this.supervisor_process_pipe_slot_for_fd?.(vmPtr, fd) !== 0 ? ofdId : 0;
    },
    supervisor_process_pipe_host_fd_for_fd(this: ProcessHandle["kernel"], vmPtr: number, fd: number) {
      const ofdId = this.supervisor_process_pipe_ofd_for_fd?.(vmPtr, fd) ?? 0;
      return ofdId > 0 ? this.kernel_ofd_host_fd(ofdId) : -1;
    },
    process_thread_vm_ptr: (_vmPtr: number, slot: number) => threadVmPtrs[slot] ?? 0,
    thread_blocked_ofd_id: (vmPtr: number) => blockedOfdMap.get(vmPtr) ?? 0,
    supervisor_process_held_ofd_refcount: (_vmPtr: number, ofdId: number) => {
      let refcount = 0;
      for (const heldOfdId of fdMap.values()) {
        if (heldOfdId === ofdId) {
          refcount += 1;
        }
      }
      for (const threadVmPtr of threadVmPtrs) {
        if ((blockedOfdMap.get(threadVmPtr) ?? 0) === ofdId) {
          refcount += 1;
        }
      }
      return refcount;
    },
    supervisor_fd_entry_restore_action_apply: () => 1,
    supervisor_fd_entry_restore_action_skip: () => 2,
    supervisor_fork_shared_held_ofd_track_flag_shared: () => 1,
    supervisor_fork_shared_held_ofd_track_flag_host_stdio_pipe: () => 1 << 1,
    supervisor_fork_shared_held_ofd_track_flag_stdio_fd: () => 1 << 2,
    supervisor_fork_shared_held_ofd_track_flag_fd_entry_source: () => 1 << 3,
    supervisor_fork_shared_held_ofd_track_action: (ofdId: number, flags: number) => {
      if (ofdId <= 0 || ofdId >= 32) {
        return 0;
      }
      if ((flags & (1 << 1)) !== 0) {
        return 0;
      }
      if ((flags & (1 | (1 << 2) | (1 << 3))) !== 0) {
        return 1;
      }
      const type = new DataView(memory.buffer).getInt32(state.ofdTablePtr + ofdId * 32, true);
      return type === FD_TYPE_FILE ? 0 : 1;
    },
    supervisor_ofd_slot_write_action: (
      ofdId: number,
      candidateType: number,
      candidateHostFd: number,
      candidateIno: number,
      candidateOffset: bigint | number,
    ) => {
      if (ofdId <= 0 || ofdId >= 32) {
        return 2;
      }
      const view = new DataView(memory.buffer);
      const base = state.ofdTablePtr + ofdId * 32;
      const currentType = view.getInt32(base, true);
      const currentRefcount = view.getInt32(base + 16, true);
      if (currentType === 0 || currentRefcount <= 0) {
        return 1;
      }
      if (candidateType === 0 || candidateType !== currentType) {
        return 2;
      }
      if (
        candidateHostFd !== view.getInt32(base + 4, true)
        || candidateIno !== view.getUint32(base + 24, true)
      ) {
        return 2;
      }
      const candidate = typeof candidateOffset === "bigint" ? candidateOffset : BigInt(Math.trunc(candidateOffset));
      if (currentType === FD_TYPE_FILE && candidate < view.getBigInt64(base + 8, true)) {
        return 2;
      }
      return 1;
    },
    supervisor_process_private_pipe_slot_mask: () => privatePipeMask,
    supervisor_process_private_pipe_source_mode: (_vmPtr: number, pipeSlot: number) => (
      (privatePipeMask & pipeBit(pipeSlot)) !== 0n
        ? PIPE_SLOT_MODE_READ | PIPE_SLOT_MODE_WRITE
        : 0
    ),
    supervisor_process_private_pipe_target_mode: (_vmPtr: number, pipeSlot: number) => (
      (privatePipeMask & pipeBit(pipeSlot)) !== 0n ? PIPE_SLOT_MODE_READ : 0
    ),
    supervisor_process_held_pipe_slot_mask: () => heldPipeSlotMask(),
    supervisor_process_pipe_used_mask: () => 1n | privatePipeMask | heldPipeSlotMask(),
    supervisor_process_pipe_slot_mode: (_vmPtr: number, pipeSlot: number) => pipeSlotMode(pipeSlot),
    supervisor_ofd_pipe_generation_slot: (ofdId: number) => {
      if (ofdId <= 0 || ofdId >= 32) {
        return 0;
      }
      const fdType = new DataView(memory.buffer).getInt32(state.ofdTablePtr + ofdId * 32, true);
      if (fdType !== FD_TYPE_PIPE) {
        return 0;
      }
      const pipeMode = pipeModeForOfd(ofdId);
      return pipeMode === null ? 0 : pipeMode.pipeSlot;
    },
    supervisor_ofd_pipe_slot_mask: (ofdId: number) => (
      pipeModesForOfd(ofdId).reduce((mask, { pipeSlot }) => mask | pipeBit(pipeSlot), 0n)
    ),
    supervisor_ofd_pipe_slot_mode: (ofdId: number, pipeSlot: number) => (
      pipeModesForOfd(ofdId).reduce(
        (mode, candidate) => candidate.pipeSlot === pipeSlot ? mode | candidate.mode : mode,
        0,
      )
    ),
    supervisor_process_pipe_read_open_count: (_vmPtr: number, pipeSlot: number) => (
      pipeOpenCount(pipeSlot, PIPE_SLOT_MODE_READ)
    ),
    supervisor_process_pipe_write_open_count: (_vmPtr: number, pipeSlot: number) => (
      pipeOpenCount(pipeSlot, PIPE_SLOT_MODE_WRITE)
    ),
    supervisor_pipe_reconcile_control: (pipeSlot: number, readOpen: number, writeOpen: number) => {
      if (pipeSlot <= 0 || pipeSlot >= 64 || readOpen < 0 || writeOpen < 0) {
        return 0;
      }
      const slotBase = state.pipeBase + pipeSlot * (32 + 65_536);
      const view = new DataView(memory.buffer);
      view.setInt32(slotBase + 8, Math.trunc(readOpen), true);
      view.setInt32(slotBase + 12, Math.trunc(writeOpen), true);
      return 1;
    },
    kernel_ofd_fd_type: (ofdId: number) => new DataView(memory.buffer)
      .getInt32(state.ofdTablePtr + ofdId * 32, true),
    kernel_ofd_host_fd: (ofdId: number) => new DataView(memory.buffer)
      .getInt32(state.ofdTablePtr + ofdId * 32 + 4, true),
    kernel_ofd_ino: (ofdId: number) => new DataView(memory.buffer)
      .getUint32(state.ofdTablePtr + ofdId * 32 + 24, true),
    kernel_ofd_status_flags: () => 0,
    kernel_ofd_refcount: (ofdId: number) => new DataView(memory.buffer)
      .getInt32(state.ofdTablePtr + ofdId * 32 + 16, true),
    kernel_ofd_offset: (ofdId: number) => new DataView(memory.buffer)
      .getBigInt64(state.ofdTablePtr + ofdId * 32 + 8, true),
    supervisor_ofd_fd_type(this: ProcessHandle["kernel"], ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_fd_type(ofdId) : 0;
    },
    supervisor_ofd_host_fd(this: ProcessHandle["kernel"], ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_host_fd(ofdId) : -1;
    },
    supervisor_ofd_status_flags(this: ProcessHandle["kernel"], ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_status_flags?.(ofdId) ?? 0 : 0;
    },
    supervisor_ofd_ino(this: ProcessHandle["kernel"], ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_ino(ofdId) : 0;
    },
    supervisor_ofd_refcount(this: ProcessHandle["kernel"], ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_refcount(ofdId) : 0;
    },
    supervisor_ofd_reconcile_refcount(this: ProcessHandle["kernel"], ofdId: number, expectedRefcount: number) {
      if (ofdId <= 0 || ofdId >= this.abi_max_fds()) {
        return 0;
      }
      let currentRefcount = this.kernel_ofd_refcount(ofdId);
      const expected = Math.trunc(expectedRefcount);
      if (expected < 0) {
        return currentRefcount;
      }
      while (currentRefcount < expected) {
        retainOfd(ofdId);
        currentRefcount += 1;
      }
      while (currentRefcount > expected) {
        releaseOfd(ofdId);
        currentRefcount -= 1;
      }
      return this.kernel_ofd_refcount(ofdId);
    },
    supervisor_ofd_offset(this: ProcessHandle["kernel"], ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_offset(ofdId) : 0n;
    },
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
    setPrivatePipeMask: (mask: bigint) => {
      privatePipeMask = mask;
    },
    retainOfd,
    releaseOfd,
  } as unknown as FakeKernel;
}

function makeHandle(kernel: FakeKernel, memory: WebAssembly.Memory): ProcessHandle {
  const handle = {
    runtimeId: 1,
    pid: 11,
    parentPid: null,
    vmPtr: 0x1234,
    guestMemoryPolicyLimitBytes: 0,
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
    },
    stdinQueue: [],
    outputBacklog: [],
    runtimeBridge: {} as ProcessHandle["runtimeBridge"],
    sabPageCache: null,
    workers: new Set(),
    vforkReadyWorkers: [],
    runningWorker: null,
    hostBlockedWorkers: new Map(),
    vforkSnapshot: null,
    vforkWorker: null,
    workerDebugLog: [],
  } as unknown as ProcessHandle;
  setProcessDebugCommand(handle, "fd-ownership");
  seedProcessHandleStateFixture(handle);
  kernel.setPrivatePipeMask(
    (1n << BigInt(processStdinPipe(handle)))
      | (1n << BigInt(processStdoutPipe(handle)))
      | (1n << BigInt(processStderrPipe(handle))),
  );
  return handle;
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

test("fd ownership helpers use supervisor process authority when available", () => {
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const kernel = makeKernel(memory);
  const handle = makeHandle(kernel, memory);
  seedOfd(memory, handle.kernelState.ofdTablePtr, 7, FD_TYPE_FILE, 0, 0x71);
  seedOfd(memory, handle.kernelState.ofdTablePtr, 9, FD_TYPE_PIPE, 8, 0x91);
  seedOfd(memory, handle.kernelState.ofdTablePtr, 10, FD_TYPE_EVENTFD, 0, 0xa1);
  seedOfd(memory, handle.kernelState.ofdTablePtr, 12, FD_TYPE_TIMERFD, 0, 0xc1);
  kernel.setFdMap(new Map([[3, 11]]));
  kernel.supervisor_process_held_ofd_refcount = (_vmPtr, ofdId) => (
    ofdId === 7 ? 2 : ofdId === 9 ? 1 : 0
  );
  kernel.supervisor_process_epoll_held_ofd = (_vmPtr, index) => (index === 0 ? 7 : 0);
  kernel.supervisor_process_epoll_dependent_ofd = (_vmPtr, index) => (index === 0 ? 9 : 0);
  kernel.supervisor_process_sync_dependent_ofd = (_vmPtr, index) => (
    index === 0 ? 10 : index === 1 ? 12 : 0
  );
  kernel.supervisor_process_epoll_dependent_sync_ofd = (_vmPtr, index) => (index === 0 ? 9 : 0);
  kernel.supervisor_process_epoll_depends_on_ofd = (_vmPtr, ofdId) => (ofdId === 9 ? 1 : 0);

  expect([...collectProcessHeldOfds(handle)].sort((left, right) => left - right)).toEqual([7, 9]);
  const heldRefcounts = collectProcessHeldOfdRefcounts(handle);
  expect(heldRefcounts[7]).toBe(2);
  expect(heldRefcounts[9]).toBe(1);
  expect(collectHeldEpollOfdIds(handle)).toEqual([7]);
  expect(collectProcessSyncDependentOfdIds(handle)).toEqual([10, 12]);
  expect(collectHeldEpollDependentSyncOfdIds(handle)).toEqual([9]);
  expect(processEpollDependsOnOfd(handle, 9)).toBe(true);
  expect(processEpollDependsOnOfd(handle, 10)).toBe(false);
});

test("trackForkSharedOfds skips hidden stdio pipes and retains non-stdio OFDs only once", () => {
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const kernel = makeKernel(memory);
  const handle = makeHandle(kernel, memory);
  seedOfd(memory, handle.kernelState.ofdTablePtr, 10, FD_TYPE_PIPE, processStdoutPipe(handle), 0xaa);
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
  expect(kernelShareSharedOfdIdList(handle).sort((left, right) => left - right)).toEqual([11, 13]);
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
  addKernelShareMember(owner);
  addKernelShareMember(owner, sibling);
  setProcessKernelShareGroup(sibling, processKernelShareGroup(owner));
  addKernelShareSharedOfdId(owner, 11);
  addKernelShareSharedOfdId(owner, 13);
  addKernelShareSharedOfdId(owner, 31);
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
  new DataView(memory.buffer).setInt32(handle.kernelState.ofdTablePtr + 21 * 32 + 8, 1, true);
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

test("kernel-share pipe reconcile applies supervisor host stdio pipe counts", () => {
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const kernel = makeKernel(memory);
  const handle = makeHandle(kernel, memory);
  seedOfd(memory, handle.kernelState.ofdTablePtr, 3, FD_TYPE_PIPE, processStderrPipe(handle), 0xe3, -7);
  seedPipeSlot(memory, handle.kernelState.pipeBase, processStderrPipe(handle), 0xa6);
  kernel.setFdMap(new Map([[2, 3]]));
  kernel.supervisor_process_pipe_used_mask = () => 1n | (1n << BigInt(processStderrPipe(handle)));
  kernel.supervisor_process_pipe_read_open_count = (_vmPtr, slot) => slot === processStderrPipe(handle) ? 1 : 0;
  kernel.supervisor_process_pipe_write_open_count = (_vmPtr, slot) => slot === processStderrPipe(handle) ? 1 : 0;
  addKernelShareMember(handle);

  const view = new DataView(memory.buffer);
  const slotBase = handle.kernelState.pipeBase + processStderrPipe(handle) * (32 + 65_536);
  view.setInt32(slotBase + 8, 0, true);
  view.setInt32(slotBase + 12, 0, true);

  reconcileKernelSharePipeControl(handle);

  expect(pipeOpenCounts(memory, handle.kernelState.pipeBase, processStderrPipe(handle))).toEqual({
    readOpen: 1,
    writeOpen: 1,
  });
});

test("kernel-share pipe reconcile does not infer host stdio counts over supervisor values", () => {
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const kernel = makeKernel(memory);
  const handle = makeHandle(kernel, memory);
  seedOfd(memory, handle.kernelState.ofdTablePtr, 3, FD_TYPE_PIPE, processStderrPipe(handle), 0xe3, -7);
  seedPipeSlot(memory, handle.kernelState.pipeBase, processStderrPipe(handle), 0xa6);
  kernel.setFdMap(new Map([[2, 3]]));
  kernel.supervisor_process_pipe_used_mask = () => 1n | (1n << BigInt(processStderrPipe(handle)));
  kernel.supervisor_process_pipe_read_open_count = () => 0;
  kernel.supervisor_process_pipe_write_open_count = () => 0;
  addKernelShareMember(handle);

  const view = new DataView(memory.buffer);
  const slotBase = handle.kernelState.pipeBase + processStderrPipe(handle) * (32 + 65_536);
  view.setInt32(slotBase + 8, 1, true);
  view.setInt32(slotBase + 12, 1, true);

  reconcileKernelSharePipeControl(handle);

  expect(pipeOpenCounts(memory, handle.kernelState.pipeBase, processStderrPipe(handle))).toEqual({
    readOpen: 0,
    writeOpen: 0,
  });
});

test("kernel-share pipe reconcile uses supervisor endpoint counts when available", () => {
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const kernel = makeKernel(memory);
  const handle = makeHandle(kernel, memory);
  const pipeSlot = 12;
  seedPipeSlot(memory, handle.kernelState.pipeBase, pipeSlot, 0xb7);
  kernel.setFdMap(new Map([[3, 18]]));
  kernel.supervisor_process_pipe_used_mask = () => 1n | (1n << BigInt(pipeSlot));
  kernel.supervisor_process_pipe_read_open_count = (_vmPtr, slot) => (slot === pipeSlot ? 2 : 0);
  kernel.supervisor_process_pipe_write_open_count = (_vmPtr, slot) => (slot === pipeSlot ? 1 : 0);
  addKernelShareMember(handle);

  reconcileKernelSharePipeControl(handle);

  expect(pipeOpenCounts(memory, handle.kernelState.pipeBase, pipeSlot)).toEqual({
    readOpen: 2,
    writeOpen: 1,
  });
  expect(handle.kernelState.pipeUsed & (1n << BigInt(pipeSlot))).not.toBe(0n);
});

test("pipe-control refresh uses supervisor endpoint counts when available", () => {
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const kernel = makeKernel(memory);
  const handle = makeHandle(kernel, memory);
  const pipeSlot = 13;
  seedPipeSlot(memory, handle.kernelState.pipeBase, pipeSlot, 0xb8);
  kernel.setFdMap(new Map([[3, 18]]));
  kernel.supervisor_process_pipe_read_open_count = (_vmPtr, slot) => (slot === pipeSlot ? 1 : 0);
  kernel.supervisor_process_pipe_write_open_count = (_vmPtr, slot) => (slot === pipeSlot ? 2 : 0);
  addKernelShareMember(handle);

  refreshPipeControlFromKernelShare(handle);

  expect(pipeOpenCounts(memory, handle.kernelState.pipeBase, pipeSlot)).toEqual({
    readOpen: 1,
    writeOpen: 2,
  });
});

test("kernel-share pipe reconcile does not resurrect closed normal pipe writers", () => {
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const kernel = makeKernel(memory);
  const handle = makeHandle(kernel, memory);
  const pipeSlot = 9;
  seedOfd(memory, handle.kernelState.ofdTablePtr, 18, FD_TYPE_PIPE, pipeSlot, 0xa8, 6);
  seedOfd(memory, handle.kernelState.ofdTablePtr, 19, FD_TYPE_PIPE, pipeSlot, 0xa9, -7);
  seedPipeSlot(memory, handle.kernelState.pipeBase, pipeSlot, 0xb9);
  kernel.setFdMap(new Map([
    [3, 18],
  ]));
  addKernelShareMember(handle);

  const view = new DataView(memory.buffer);
  const slotBase = handle.kernelState.pipeBase + pipeSlot * (32 + 65_536);
  view.setInt32(slotBase + 8, 1, true);
  view.setInt32(slotBase + 12, 0, true);

  reconcileKernelSharePipeControl(handle);

  expect(pipeOpenCounts(memory, handle.kernelState.pipeBase, pipeSlot)).toEqual({
    readOpen: 1,
    writeOpen: 0,
  });
});
