import { addKernelShareMember, addKernelShareSharedOfdId, kernelShareHasSharedOfdId, kernelShareMembers, kernelShareSharedOfdIdList, processKernelShareGroup, setProcessKernelShareGroup } from "../../../../src/worker/kernel-share.js";
import { expect, test } from "vite-plus/test";
import {
  reconcileInheritedSharedOfdsAfterVforkExecve,
  trackVforkExecveChildSharedOfds,
} from "../../../../src/worker/fork/shared-ofd.js";
import {
  pruneStaleKernelShareOfdIds,
  pruneUnheldKernelShareOfdIds,
  reconcileForkSharedOfdRefcounts,
  syncKernelShareOfdReservations,
} from "../../../../src/worker/fork-shared.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

function makeHandle(
  heldOfdIds: readonly number[],
  sharedOfdIds: Iterable<number> = [],
  regularFileOfdIds: Iterable<number> = [],
): ProcessHandle {
  const regularFileOfdIdSet = new Set(regularFileOfdIds);
  return {
    vmPtr: 0x1000,
    hostBlockedWorkers: new Map(),
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
      supervisor_process_held_ofd_refcount: (_vmPtr: number, ofdId: number) => (
        heldOfdIds.filter((heldOfdId) => heldOfdId === ofdId).length
      ),
      kernel_ofd_fd_type: (ofdId: number) => regularFileOfdIdSet.has(ofdId) ? 1 : 6,
    },
  } as unknown as ProcessHandle;
}

test("vfork execve inherited shared OFDs stay shared only while another member still holds them", () => {
  const parent = makeHandle([], [4, 5]);
  const sibling = makeHandle([0, 4]);
  addKernelShareMember(parent, sibling);

  reconcileInheritedSharedOfdsAfterVforkExecve(parent, [4, 5]);

  expect(kernelShareHasSharedOfdId(parent, 4)).toBe(true);
  expect(kernelShareHasSharedOfdId(parent, 5)).toBe(false);
});

test("vfork execve child tracking records only exec-inherited fd entries", () => {
  const child = makeHandle([0, 7, 0, 9, 11], [7, 9, 11, 12], [11, 12]);

  trackVforkExecveChildSharedOfds(child, [
    { fd: 1, ofdId: 7, fdFlags: 0 },
    { fd: 3, ofdId: 9, fdFlags: 0 },
  ]);

  expect(kernelShareSharedOfdIdList(child).sort((left, right) => left - right)).toEqual([7, 9]);
});

test("vfork execve reconciles an OFD aliased by parent and child descriptors", () => {
  const parent = makeHandle([0, 0, 0, 0, 0, 0, 8], [8]);
  const child = makeHandle([8], [8]);
  const reconciled: Array<[number, number]> = [];
  parent.kernel.supervisor_ofd_reconcile_refcount = (ofdId: number, refcount: number) => {
    reconciled.push([ofdId, refcount]);
    return refcount;
  };
  addKernelShareMember(parent);
  addKernelShareMember(parent, child);

  reconcileForkSharedOfdRefcounts(parent, [8]);

  expect(reconciled).toEqual([[8, 2]]);
});

test("stale shared OFDs are reconciled before their reservations are released", () => {
  const parent = makeHandle([], [8]);
  const child = makeHandle([], [8]);
  const operations: string[] = [];
  for (const [name, handle] of [["parent", parent], ["child", child]] as const) {
    handle.kernel.supervisor_ofd_reconcile_refcount = (ofdId: number, refcount: number) => {
      operations.push(`${name}:reconcile:${ofdId}:${refcount}`);
      return refcount;
    };
    handle.kernel.kernel_unreserve_ofd = (ofdId: number) => {
      operations.push(`${name}:unreserve:${ofdId}`);
    };
  }
  setProcessKernelShareGroup(child, processKernelShareGroup(parent));
  addKernelShareSharedOfdId(parent, 8);
  addKernelShareMember(parent);
  addKernelShareMember(parent, child);

  pruneStaleKernelShareOfdIds(parent, [8]);

  expect(operations).toEqual([
    "parent:reconcile:8:0",
    "parent:unreserve:8",
    "child:reconcile:8:0",
    "child:unreserve:8",
  ]);
  expect(kernelShareHasSharedOfdId(parent, 8)).toBe(false);
});

test("stale shared OFD pruning preserves an OFD observed by the active process snapshot", () => {
  const parent = makeHandle([], [8]);
  const operations: string[] = [];
  parent.kernel.supervisor_ofd_reconcile_refcount = (ofdId: number, refcount: number) => {
    operations.push(`reconcile:${ofdId}:${refcount}`);
    return refcount;
  };
  parent.kernel.kernel_unreserve_ofd = (ofdId: number) => {
    operations.push(`unreserve:${ofdId}`);
  };
  addKernelShareSharedOfdId(parent, 8);
  addKernelShareMember(parent);

  pruneStaleKernelShareOfdIds(parent, [8], [8]);

  expect(operations).toEqual([]);
  expect(kernelShareHasSharedOfdId(parent, 8)).toBe(true);
});

test("kernel-share allocation reservations cover every OFD held by a member", () => {
  const parent = makeHandle([0, 4], [8]);
  const child = makeHandle([0, 7], [8]);
  const operations: string[] = [];
  for (const [name, handle] of [["parent", parent], ["child", child]] as const) {
    handle.kernel.kernel_reserve_ofd = (ofdId: number) => {
      operations.push(`${name}:reserve:${ofdId}`);
    };
  }
  setProcessKernelShareGroup(child, processKernelShareGroup(parent));
  addKernelShareSharedOfdId(parent, 8);
  addKernelShareMember(parent);
  addKernelShareMember(parent, child);

  syncKernelShareOfdReservations(parent);

  expect(operations).toEqual([
    "parent:reserve:8",
    "parent:reserve:4",
    "parent:reserve:7",
    "child:reserve:8",
    "child:reserve:4",
    "child:reserve:7",
  ]);
});

test("kernel-share reservations preserve OFDs observed through the active process snapshot", () => {
  const heldOfdIds = [0, 7];
  const parent = makeHandle(heldOfdIds);
  const operations: string[] = [];
  parent.kernel.kernel_reserve_ofd = (ofdId: number) => {
    operations.push(`reserve:${ofdId}`);
  };
  parent.kernel.kernel_unreserve_ofd = (ofdId: number) => {
    operations.push(`unreserve:${ofdId}`);
  };
  addKernelShareMember(parent);

  syncKernelShareOfdReservations(parent);
  heldOfdIds[1] = 0;
  syncKernelShareOfdReservations(parent, [7]);
  syncKernelShareOfdReservations(parent);

  expect(operations).toEqual([
    "reserve:7",
    "reserve:7",
    "unreserve:7",
  ]);
});

test("detached child-only shared OFDs release reservations from remaining members", () => {
  const parent = makeHandle([], [8, 9]);
  const child = makeHandle([8], [8, 9]);
  const sibling = makeHandle([9], [8, 9]);
  const operations: string[] = [];
  for (const [name, handle] of [["parent", parent], ["sibling", sibling]] as const) {
    handle.kernel.supervisor_ofd_reconcile_refcount = (ofdId: number, refcount: number) => {
      operations.push(`${name}:reconcile:${ofdId}:${refcount}`);
      return refcount;
    };
    handle.kernel.kernel_unreserve_ofd = (ofdId: number) => {
      operations.push(`${name}:unreserve:${ofdId}`);
    };
  }
  setProcessKernelShareGroup(child, processKernelShareGroup(parent));
  setProcessKernelShareGroup(sibling, processKernelShareGroup(parent));
  addKernelShareSharedOfdId(parent, 8);
  addKernelShareSharedOfdId(parent, 9);
  addKernelShareMember(parent);
  addKernelShareMember(parent, child);
  addKernelShareMember(parent, sibling);
  kernelShareMembers(parent).delete(child);

  pruneUnheldKernelShareOfdIds(parent);

  expect(operations).toEqual([
    "parent:reconcile:8:0",
    "parent:unreserve:8",
    "sibling:reconcile:8:0",
    "sibling:unreserve:8",
  ]);
  expect(kernelShareSharedOfdIdList(parent)).toEqual([9]);
});
