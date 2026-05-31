import { expect, test } from "vite-plus/test";
import {
  reconcileInheritedSharedOfdsAfterVforkExecve,
  trackVforkExecveChildSharedOfds,
} from "../../../../src/worker/fork/shared-ofd.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

function makeHandle(
  heldOfdIds: readonly number[],
  sharedOfdIds: Iterable<number> = [],
): ProcessHandle {
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
      process_fd_ofd_id: (_vmPtr: number, fd: number) => heldOfdIds[fd] ?? 0,
      process_thread_vm_ptr: () => 0,
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

test("vfork execve child tracking records exec fd entries and live held OFDs", () => {
  const child = makeHandle([0, 7, 0, 9]);

  trackVforkExecveChildSharedOfds(child, [
    { fd: 1, ofdId: 7, fdFlags: 0 },
    { fd: 3, ofdId: 9, fdFlags: 0 },
  ]);

  expect([...child.kernelShare.sharedOfdIds].sort((left, right) => left - right)).toEqual([7, 9]);
});
