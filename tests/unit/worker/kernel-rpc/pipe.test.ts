import { expect, test } from "vite-plus/test";
import { activeChildMayHoldBlockedPipeWriter } from "../../../../src/worker/kernel-rpc/pipe.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

function makeHandle(options: {
  pid: number;
  parentPid?: number | null;
  pipeSlot: number;
  reader?: boolean;
  writer?: boolean;
  stdioOwner?: ProcessHandle | null;
}): ProcessHandle {
  const ofdId = options.reader ? 1 : options.writer ? 2 : 0;
  return {
    pid: options.pid,
    parentPid: options.parentPid ?? null,
    vmPtr: 0x1000,
    exitInProgress: false,
    workers: new Set([{} as Worker]),
    stdioOwner: options.stdioOwner ?? null,
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set(),
    },
    kernel: {
      abi_max_fds: () => 8,
      abi_max_pipes: () => 8,
      abi_fd_type_pipe: () => 2,
      thread_blocked_ofd_id: () => options.reader ? 1 : 0,
      thread_poll_wait_first_pipe_revents: () => 0,
      thread_blocked_fd: () => 0,
      thread_a0: () => 0n,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 0 ? ofdId : 0,
      kernel_ofd_fd_type: (candidateOfdId: number) => candidateOfdId === 1 || candidateOfdId === 2 ? 2 : 0,
      kernel_ofd_host_fd: (candidateOfdId: number) => candidateOfdId === 1 ? 0 : -1,
      kernel_ofd_ino: () => options.pipeSlot,
    },
  } as unknown as ProcessHandle;
}

test("kernel-rpc pipe ownership detects an active child writer for a blocked parent reader", () => {
  const parent = makeHandle({ pid: 1, pipeSlot: 3, reader: true });
  const child = makeHandle({ pid: 2, parentPid: 1, pipeSlot: 3, writer: true });
  const processHandles = new Map([[1, parent], [2, child]]);

  expect(activeChildMayHoldBlockedPipeWriter({ processHandles }, parent, parent.vmPtr)).toBe(true);
});

test("kernel-rpc pipe ownership ignores stale or unrelated child writers", () => {
  const parent = makeHandle({ pid: 1, pipeSlot: 3, reader: true });
  const child = makeHandle({ pid: 2, parentPid: 99, pipeSlot: 3, writer: true });
  const processHandles = new Map([[1, parent], [2, child]]);

  expect(activeChildMayHoldBlockedPipeWriter({ processHandles }, parent, parent.vmPtr)).toBe(false);
});
