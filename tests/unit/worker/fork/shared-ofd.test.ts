import { expect, test } from "vite-plus/test";
import {
  reconcileInheritedSharedOfdsAfterVforkExecve,
  trackVforkExecveChildSharedOfds,
} from "../../../../src/worker/fork/shared-ofd.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

function makeHandle(
  heldOfdIds: readonly number[],
  sharedOfdIds: Iterable<number> = [],
  regularFileOfdIds: Iterable<number> = [],
): ProcessHandle {
  const regularFileOfdIdSet = new Set(regularFileOfdIds);
  return {
    vmPtr: 0x1000,
    blockedWorkers: new Map(),
    kernelShare: {
      members: new Set<ProcessHandle>(),
      sharedOfdIds: new Set(sharedOfdIds),
    },
    kernel: {
      abi_max_fds: () => 16,
      abi_max_threads: () => 0,
      abi_fd_type_file: () => 1,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => heldOfdIds[fd] ?? 0,
      process_thread_vm_ptr: () => 0,
      kernel_ofd_fd_type: (ofdId: number) => regularFileOfdIdSet.has(ofdId) ? 1 : 6,
    },
  } as unknown as ProcessHandle;
}

test("vfork execve inherited shared OFDs stay shared only while another member still holds them", () => {
  const parent = makeHandle([], [4, 5]);
  const sibling = makeHandle([0, 4]);
  parent.kernelShare.members.add(sibling);

  reconcileInheritedSharedOfdsAfterVforkExecve(parent, [4, 5]);

  expect(parent.kernelShare.sharedOfdIds.has(4)).toBe(true);
  expect(parent.kernelShare.sharedOfdIds.has(5)).toBe(false);
});

test("vfork execve child tracking records only exec-inherited fd entries", () => {
  const child = makeHandle([0, 7, 0, 9, 11], [7, 9, 11, 12], [11, 12]);

  trackVforkExecveChildSharedOfds(child, [
    { fd: 1, ofdId: 7, fdFlags: 0 },
    { fd: 3, ofdId: 9, fdFlags: 0 },
  ]);

  expect([...child.kernelShare.sharedOfdIds].sort((left, right) => left - right)).toEqual([7, 9]);
});
