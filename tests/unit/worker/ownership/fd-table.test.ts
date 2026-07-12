import { expect, test } from "vite-plus/test";
import {
  applyThreadFdEntries,
  snapshotAuthoritativeProcessFdEntriesForThread,
  snapshotProcessFdEntriesForThread,
} from "../../../../src/worker/ownership/fd-table.js";
import type { KernelExports, KernelRuntimeState } from "../../../../src/abi.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";
import { makeSupervisorAbiStubs } from "../support/supervisor-abi.js";
import { createRuntimeStateBridge } from "../../../../src/bridge/runtime-state.js";

function makeHandle(options: {
  ownerPid: number;
  threadPid: number;
  ownerFd9Ofd: number;
  threadFd9Ofd: number;
}): ProcessHandle {
  return {
    pid: options.ownerPid,
    vmPtr: 0x1234,
    runtimeBridge: createRuntimeStateBridge(),
    kernel: {
      abi_max_fds: () => 16,
      process_current_pid: (vmPtr: number) => {
        if (vmPtr === 0x1234) {
          return options.ownerPid;
        }
        if (vmPtr === 0x5678) {
          return options.threadPid;
        }
        return 0;
      },
      process_fd_ofd_id: (vmPtr: number, fd: number) => {
        if (fd !== 9) {
          return 0;
        }
        return vmPtr === 0x1234 ? options.ownerFd9Ofd : options.threadFd9Ofd;
      },
      process_fd_flags: (_vmPtr: number, fd: number) => (fd === 9 ? 1 : 0),
      process_fd_generation: () => 9,
      supervisor_process_ofd_for_fd(this: KernelExports, vmPtr: number, fd: number) {
        const ofdId = this.process_fd_ofd_id(vmPtr, fd);
        return ofdId > 0 && ofdId < this.abi_max_fds() ? ofdId : 0;
      },
      supervisor_process_fd_flags_for_fd(this: KernelExports, vmPtr: number, fd: number) {
        return fd >= 0 && fd < this.abi_max_fds() ? this.process_fd_flags(vmPtr, fd) : 0;
      },
    },
  } as unknown as ProcessHandle;
}

test("snapshotProcessFdEntriesForThread uses the owner fd table for same-process threads", () => {
  const snapshot = snapshotProcessFdEntriesForThread(makeHandle({
    ownerPid: 7,
    threadPid: 7,
    ownerFd9Ofd: 10,
    threadFd9Ofd: 0,
  }), 0x5678);

  expect(snapshot?.find((entry) => entry.fd === 9)).toEqual({
    fd: 9,
    ofdId: 10,
    fdFlags: 1,
    authorityEpoch: 0,
    generation: 9,
  });
});

test("snapshotProcessFdEntriesForThread keeps the target fd table for a different process", () => {
  const snapshot = snapshotProcessFdEntriesForThread(makeHandle({
    ownerPid: 7,
    threadPid: 8,
    ownerFd9Ofd: 10,
    threadFd9Ofd: 0,
  }), 0x5678);

  expect(snapshot?.find((entry) => entry.fd === 9)).toEqual({
    fd: 9,
    ofdId: 0,
    fdFlags: 1,
    authorityEpoch: 0,
    generation: 9,
  });
});

test("snapshotAuthoritativeProcessFdEntriesForThread strips generations for init hydration", () => {
  const snapshot = snapshotAuthoritativeProcessFdEntriesForThread(makeHandle({
    ownerPid: 7,
    threadPid: 7,
    ownerFd9Ofd: 10,
    threadFd9Ofd: 0,
  }), 0x5678);

  expect(snapshot?.find((entry) => entry.fd === 9)).toEqual({
    fd: 9,
    ofdId: 10,
    fdFlags: 1,
    authorityEpoch: 0,
  });
});

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memfsHeaderBytes: 0,
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

test("applyThreadFdEntries preserves epoll watched pipe and eventfd fds on non-clearing snapshots", () => {
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const kernelState = makeKernelState();
  const epollInstancePtr = kernelState.epollTablePtr + 0x1000;
  const view = new DataView(memory.buffer);
  view.setUint32(kernelState.epollTablePtr, epollInstancePtr, true);
  view.setUint32(epollInstancePtr + 4, 12, true);
  view.setUint32(epollInstancePtr + 32 + 4, 14, true);
  view.setUint32(epollInstancePtr + 64 * 16, 2, true);
  const fdMap = new Map<number, number>([
    [4, 10],
    [11, 12],
    [13, 14],
  ]);
  let kernel: Partial<KernelExports>;
  kernel = {
    ...makeSupervisorAbiStubs({
      memory,
      currentExports: () => kernel,
    }),
    abi_max_fds: () => 32,
    abi_max_threads: () => 4,
    abi_fd_type_epoll: () => 7,
    abi_fd_type_pipe: () => 6,
    abi_fd_type_eventfd: () => 9,
    abi_fd_type_file: () => 3,
    abi_sys_close: () => 57n,
    abi_sys_close_range: () => 436n,
    abi_sys_execve: () => 221n,
    kernel_epoll_table_ptr: () => kernelState.epollTablePtr,
    kernel_epoll_table_bytes: () => 64 * 4,
    kernel_epoll_instance_bytes: () => 64 * 16 + 4,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdMap.get(fd) ?? 0,
    process_fd_flags: () => 0,
    process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number) => {
      fdMap.set(fd, ofdId);
    },
    process_thread_vm_ptr: () => 0,
    thread_blocked_ofd_id: () => 0,
    kernel_ofd_fd_type: (ofdId: number) => {
      if (ofdId === 10) {
        return 7;
      }
      if (ofdId === 12) {
        return 6;
      }
      if (ofdId === 14) {
        return 9;
      }
      return 0;
    },
    kernel_ofd_ino: (ofdId: number) => (ofdId === 10 ? 0 : 0),
  };
  const handle = {
    pid: 1,
    vmPtr: 0x1234,
    memory,
    kernelState,
    hostBlockedWorkers: new Map(),
    kernel,
  } as unknown as ProcessHandle;

  applyThreadFdEntries(handle, 0x1234, [{ fd: 11, ofdId: 0, fdFlags: 0 }], {
    preserveLiveEpollDependentDescriptors: true,
  });
  applyThreadFdEntries(handle, 0x1234, [{ fd: 13, ofdId: 0, fdFlags: 0 }], {
    preserveLiveEpollDependentDescriptors: true,
  });

  expect(fdMap.get(11)).toBe(12);
  expect(fdMap.get(13)).toBe(14);
});

test("applyThreadFdEntries rejects older fd snapshots by slot generation", () => {
  const memory = new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
  const kernelState = makeKernelState();
  const fdMap = new Map<number, number>([[5, 6]]);
  const fdGenerations = new Map<number, number>([[5, 3]]);
  const setCalls: Array<{ fd: number; ofdId: number; fdFlags: number }> = [];
  let kernel: Partial<KernelExports>;
  kernel = {
    ...makeSupervisorAbiStubs({
      memory,
      currentExports: () => kernel,
    }),
    abi_max_fds: () => 32,
    abi_max_threads: () => 4,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdMap.get(fd) ?? 0,
    process_fd_flags: () => 0,
    process_fd_generation: (_vmPtr: number, fd: number) => fdGenerations.get(fd) ?? 0,
    process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number, fdFlags: number) => {
      setCalls.push({ fd, ofdId, fdFlags });
      fdMap.set(fd, ofdId);
      fdGenerations.set(fd, (fdGenerations.get(fd) ?? 0) + 1);
    },
    supervisor_process_ofd_for_fd(this: KernelExports, vmPtr: number, fd: number) {
      const ofdId = this.process_fd_ofd_id(vmPtr, fd);
      return ofdId > 0 && ofdId < this.abi_max_fds() ? ofdId : 0;
    },
    supervisor_process_fd_flags_for_fd: () => 0,
  };
  const handle = {
    pid: 1,
    vmPtr: 0x1234,
    memory,
    kernelState,
    hostBlockedWorkers: new Map(),
    kernel,
  } as unknown as ProcessHandle;

  applyThreadFdEntries(handle, 0x1234, [{ fd: 5, ofdId: 6, fdFlags: 0, generation: 3 }]);
  expect(setCalls).toEqual([]);
  expect(fdGenerations.get(5)).toBe(3);

  applyThreadFdEntries(handle, 0x1234, [{ fd: 5, ofdId: 0, fdFlags: 0, generation: 2 }]);
  expect(fdMap.get(5)).toBe(6);
  expect(setCalls).toEqual([]);

  applyThreadFdEntries(handle, 0x1234, [{ fd: 5, ofdId: 0, fdFlags: 0, generation: 4 }]);
  expect(fdMap.get(5)).toBe(0);
  expect(setCalls).toEqual([{ fd: 5, ofdId: 0, fdFlags: 0 }]);
});
