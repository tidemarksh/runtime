// Kernel-worker tests in this file validate shared-state bridge behavior during supervisor migration.
import { expect, test, vi } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../src/abi.js";
import {
  collectProcessHeldOfdRefcounts,
  createKernelWorkerOfdPrimitives,
} from "../../../src/kernel-worker/primitives/ofd.js";
import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_OFFSET_OFFSET,
  OFD_REFCOUNT_OFFSET,
  OFD_TYPE_OFFSET,
} from "../../../src/layout/ofd.js";

function makeMemory(): WebAssembly.Memory {
  return new WebAssembly.Memory({ initial: 64, maximum: 64, shared: true });
}

function makeKernelExports(memory: WebAssembly.Memory) {
  let fdMap = new Map<number, number>();
  let threadVmPtrs = [0x1234];
  let blockedOfdMap = new Map<number, number>();
  let processThreadCount = 1;
  const retainOfd = vi.fn((ofdId: number) => {
    const view = new DataView(memory.buffer);
    const base = 0x220000 + ofdId * 32 + 16;
    view.setInt32(base, view.getInt32(base, true) + 1, true);
  });
  const releaseOfd = vi.fn((ofdId: number) => {
    const view = new DataView(memory.buffer);
    const base = 0x220000 + ofdId * 32 + 16;
    view.setInt32(base, view.getInt32(base, true) - 1, true);
  });
  const exports = {
    memory,
    abi_max_fds: () => 32,
    abi_max_threads: () => 4,
    thread_process_thread_count: () => processThreadCount,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fdMap.get(fd) ?? 0,
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
    kernel_ofd_table_bytes: () => 32 * 32,
    kernel_ofd_refcount: (ofdId: number) => new DataView(memory.buffer).getInt32(0x220000 + ofdId * 32 + 16, true),
    supervisor_ofd_refcount(this: KernelExports, ofdId: number) {
      return ofdId > 0 && ofdId < this.abi_max_fds() ? this.kernel_ofd_refcount(ofdId) : 0;
    },
    supervisor_ofd_reconcile_refcount(this: KernelExports, ofdId: number, expectedRefcount: number) {
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
    kernel_retain_ofd: retainOfd,
    kernel_release_ofd: releaseOfd,
    setFdMap(map: Map<number, number>) {
      fdMap = new Map(map);
    },
    setThreadVmPtrs(vmPtrs: number[]) {
      threadVmPtrs = [...vmPtrs];
    },
    setBlockedOfdMap(map: Map<number, number>) {
      blockedOfdMap = new Map(map);
    },
    setProcessThreadCount(nextCount: number) {
      processThreadCount = nextCount;
    },
    retainOfd,
    releaseOfd,
  };
  return exports as unknown as KernelExports & {
    setFdMap(map: Map<number, number>): void;
    setThreadVmPtrs(vmPtrs: number[]): void;
    setBlockedOfdMap(map: Map<number, number>): void;
    setProcessThreadCount(nextCount: number): void;
    retainOfd: ReturnType<typeof vi.fn>;
    releaseOfd: ReturnType<typeof vi.fn>;
  };
}

function seedRefcount(memory: WebAssembly.Memory, ofdId: number, refcount: number): void {
  new DataView(memory.buffer).setInt32(0x220000 + ofdId * 32 + 16, refcount, true);
}

test("collectProcessHeldOfdRefcounts uses supervisor process authority when available", () => {
  const memory = makeMemory();
  const processExports = {
    ...makeKernelExports(memory),
    supervisor_process_held_ofd_refcount: (_vmPtr: number, ofdId: number) => (
      ofdId === 7 ? 2 : ofdId === 8 ? 1 : 0
    ),
  } as KernelExports;

  const held = collectProcessHeldOfdRefcounts(processExports, 0x1234);

  expect(held[7]).toBe(2);
  expect(held[8]).toBe(1);
});

test("selected OFD transport preserves a live target refcount", () => {
  const sourceMemory = makeMemory();
  const targetMemory = makeMemory();
  const tablePtr = 0x220000;
  const slotBytes = 32;
  const ofdId = 7;
  const slotOffset = tablePtr + ofdId * slotBytes;
  const writeSlot = (memory: WebAssembly.Memory, refcount: number, offset: bigint): void => {
    const view = new DataView(memory.buffer, slotOffset, slotBytes);
    view.setInt32(OFD_TYPE_OFFSET, 3, true);
    view.setInt32(OFD_HOST_FD_OFFSET, -1, true);
    view.setInt32(OFD_REFCOUNT_OFFSET, refcount, true);
    view.setBigInt64(OFD_OFFSET_OFFSET, offset, true);
    view.setUint32(OFD_INO_OFFSET, 45, true);
  };
  writeSlot(sourceMemory, 0, 64n);
  writeSlot(targetMemory, 2, 32n);

  const decideWrite = vi.fn(() => 1);
  const makeTransportExports = (memory: WebAssembly.Memory) => ({
    ...makeKernelExports(memory),
    supervisor_ofd_slot_write_action: decideWrite,
    supervisor_fd_entry_restore_action_skip: () => 2,
  }) as unknown as KernelExports;
  const source = makeTransportExports(sourceMemory);
  const target = makeTransportExports(targetMemory);
  const state = { ofdTablePtr: tablePtr } as KernelRuntimeState;
  const primitives = createKernelWorkerOfdPrimitives({
    getCanonicalExports: () => target,
    processKernels: new Map(),
  });

  primitives.copySelectedOfdState(source, state, target, state, [ofdId]);
  let applied = new DataView(targetMemory.buffer, slotOffset, slotBytes);
  expect(applied.getInt32(OFD_REFCOUNT_OFFSET, true)).toBe(2);
  expect(applied.getBigInt64(OFD_OFFSET_OFFSET, true)).toBe(64n);

  const snapshot = new Uint8Array(sourceMemory.buffer, slotOffset, slotBytes).slice();
  new DataView(snapshot.buffer).setBigInt64(OFD_OFFSET_OFFSET, 96n, true);
  primitives.applySelectedOfdSnapshots(target, state, [{ ofdId, slot: snapshot }]);
  applied = new DataView(targetMemory.buffer, slotOffset, slotBytes);
  expect(applied.getInt32(OFD_REFCOUNT_OFFSET, true)).toBe(2);
  expect(applied.getBigInt64(OFD_OFFSET_OFFSET, true)).toBe(96n);
  expect(decideWrite).toHaveBeenCalled();
});

test("reconcileCanonicalOfdRefcounts does not last-close an OFD while one member still holds it", () => {
  const canonicalMemory = makeMemory();
  const parentMemory = makeMemory();
  const childMemory = makeMemory();
  const canonical = makeKernelExports(canonicalMemory);
  const parent = makeKernelExports(parentMemory);
  const child = makeKernelExports(childMemory);
  seedRefcount(canonicalMemory, 7, 2);
  parent.setFdMap(new Map([[4, 7]]));
  child.setFdMap(new Map());

  const primitives = createKernelWorkerOfdPrimitives({
    getCanonicalExports: () => canonical,
    processKernels: new Map<number, KernelExports>([
      [1, parent],
      [2, child],
    ]),
  });

  primitives.reconcileCanonicalOfdRefcounts(
    [1, 2],
    new Map([
      [1, 0x1234],
      [2, 0x1234],
    ]),
    [7],
  );

  expect(canonical.releaseOfd).toHaveBeenCalledTimes(1);
  expect(canonical.kernel_ofd_refcount(7)).toBe(1);
});

test("reconcileCanonicalOfdRefcounts counts duplicate fd aliases as OFD references", () => {
  const canonicalMemory = makeMemory();
  const parentMemory = makeMemory();
  const canonical = makeKernelExports(canonicalMemory);
  const parent = makeKernelExports(parentMemory);
  seedRefcount(canonicalMemory, 7, 1);
  parent.setFdMap(new Map([
    [4, 7],
    [5, 7],
  ]));

  const primitives = createKernelWorkerOfdPrimitives({
    getCanonicalExports: () => canonical,
    processKernels: new Map<number, KernelExports>([[1, parent]]),
  });

  primitives.reconcileCanonicalOfdRefcounts(
    [1],
    new Map([[1, 0x1234]]),
    [7],
  );

  expect(canonical.retainOfd).toHaveBeenCalledWith(7);
  expect(canonical.releaseOfd).not.toHaveBeenCalled();
  expect(canonical.kernel_ofd_refcount(7)).toBe(2);
});

test("reconcileCanonicalOfdRefcounts counts registered members outside the current sync targets", () => {
  const canonicalMemory = makeMemory();
  const parentMemory = makeMemory();
  const childMemory = makeMemory();
  const canonical = makeKernelExports(canonicalMemory);
  const parent = makeKernelExports(parentMemory);
  const child = makeKernelExports(childMemory);
  seedRefcount(canonicalMemory, 7, 1);
  parent.setFdMap(new Map([[7, 7]]));
  child.setFdMap(new Map([[0, 7]]));

  const primitives = createKernelWorkerOfdPrimitives({
    getCanonicalExports: () => canonical,
    getCanonicalProcessVmPtr: (pid) => (pid === 2 ? 0x5678 : 0),
    processKernels: new Map<number, KernelExports>([
      [1, parent],
      [2, child],
    ]),
  });

  primitives.reconcileCanonicalOfdRefcounts(
    [1],
    new Map([[1, 0x1234]]),
    [7],
  );

  expect(canonical.retainOfd).toHaveBeenCalledWith(7);
  expect(canonical.kernel_ofd_refcount(7)).toBe(2);
});

test("reconcileCanonicalOfdRefcounts resolves a registered member's live FD authority VM", () => {
  const canonicalMemory = makeMemory();
  const parentMemory = makeMemory();
  const childMemory = makeMemory();
  const canonical = makeKernelExports(canonicalMemory);
  const parent = makeKernelExports(parentMemory);
  const childBase = makeKernelExports(childMemory);
  const staleVmPtr = 0x5678;
  const liveVmPtr = 0x9abc;
  const child = {
    ...childBase,
    supervisor_process_fd_authority_vm_ptr: (vmPtr: number, pid: number) => (
      vmPtr === staleVmPtr && pid === 2 ? liveVmPtr : 0
    ),
    supervisor_process_held_ofd_refcount: (vmPtr: number, ofdId: number) => (
      vmPtr === liveVmPtr && ofdId === 7 ? 1 : 0
    ),
  } as KernelExports;
  seedRefcount(canonicalMemory, 7, 1);
  parent.setFdMap(new Map([[7, 7]]));

  const primitives = createKernelWorkerOfdPrimitives({
    getCanonicalExports: () => canonical,
    getCanonicalProcessVmPtr: (pid) => (pid === 2 ? staleVmPtr : 0),
    processKernels: new Map<number, KernelExports>([
      [1, parent],
      [2, child],
    ]),
  });

  primitives.reconcileCanonicalOfdRefcounts(
    [1],
    new Map([[1, 0x1234]]),
    [7],
  );

  expect(canonical.retainOfd).toHaveBeenCalledWith(7);
  expect(canonical.kernel_ofd_refcount(7)).toBe(2);
});

test("reconcileCanonicalOfdRefcounts does not last-close an eventfd while another member still holds it", () => {
  const canonicalMemory = makeMemory();
  const parentMemory = makeMemory();
  const childMemory = makeMemory();
  const canonical = makeKernelExports(canonicalMemory);
  const parent = makeKernelExports(parentMemory);
  const child = makeKernelExports(childMemory);
  seedRefcount(canonicalMemory, 9, 2);
  parent.setFdMap(new Map([[4, 9]]));
  child.setFdMap(new Map());

  const primitives = createKernelWorkerOfdPrimitives({
    getCanonicalExports: () => canonical,
    processKernels: new Map<number, KernelExports>([
      [1, parent],
      [2, child],
    ]),
  });

  primitives.reconcileCanonicalOfdRefcounts(
    [1, 2],
    new Map([
      [1, 0x1234],
      [2, 0x1234],
    ]),
    [9],
  );

  expect(canonical.releaseOfd).toHaveBeenCalledTimes(1);
  expect(canonical.releaseOfd).toHaveBeenCalledWith(9);
  expect(canonical.kernel_ofd_refcount(9)).toBe(1);
});

test("reconcileCanonicalOfdRefcounts waits until the last pipe holder disappears before releasing the OFD", () => {
  const canonicalMemory = makeMemory();
  const parentMemory = makeMemory();
  const childMemory = makeMemory();
  const canonical = makeKernelExports(canonicalMemory);
  const parent = makeKernelExports(parentMemory);
  const child = makeKernelExports(childMemory);
  seedRefcount(canonicalMemory, 10, 2);
  parent.setFdMap(new Map([[1, 10]]));
  child.setFdMap(new Map([[2, 10]]));

  const primitives = createKernelWorkerOfdPrimitives({
    getCanonicalExports: () => canonical,
    processKernels: new Map<number, KernelExports>([
      [1, parent],
      [2, child],
    ]),
  });

  primitives.reconcileCanonicalOfdRefcounts(
    [1, 2],
    new Map([
      [1, 0x1234],
      [2, 0x1234],
    ]),
    [10],
  );

  expect(canonical.releaseOfd).not.toHaveBeenCalled();
  expect(canonical.kernel_ofd_refcount(10)).toBe(2);

  child.setFdMap(new Map());
  primitives.reconcileCanonicalOfdRefcounts(
    [1, 2],
    new Map([
      [1, 0x1234],
      [2, 0x1234],
    ]),
    [10],
  );

  expect(canonical.releaseOfd).toHaveBeenCalledTimes(1);
  expect(canonical.releaseOfd).toHaveBeenCalledWith(10);
  expect(canonical.kernel_ofd_refcount(10)).toBe(1);
});

test("reconcileCanonicalOfdRefcounts waits until the last socket holder disappears before releasing the OFD", () => {
  const canonicalMemory = makeMemory();
  const parentMemory = makeMemory();
  const childMemory = makeMemory();
  const canonical = makeKernelExports(canonicalMemory);
  const parent = makeKernelExports(parentMemory);
  const child = makeKernelExports(childMemory);
  seedRefcount(canonicalMemory, 11, 2);
  parent.setFdMap(new Map([[4, 11]]));
  child.setFdMap(new Map([[5, 11]]));

  const primitives = createKernelWorkerOfdPrimitives({
    getCanonicalExports: () => canonical,
    processKernels: new Map<number, KernelExports>([
      [1, parent],
      [2, child],
    ]),
  });

  primitives.reconcileCanonicalOfdRefcounts(
    [1, 2],
    new Map([
      [1, 0x1234],
      [2, 0x1234],
    ]),
    [11],
  );

  expect(canonical.releaseOfd).not.toHaveBeenCalled();
  expect(canonical.kernel_ofd_refcount(11)).toBe(2);

  child.setFdMap(new Map());
  primitives.reconcileCanonicalOfdRefcounts(
    [1, 2],
    new Map([
      [1, 0x1234],
      [2, 0x1234],
    ]),
    [11],
  );

  expect(canonical.releaseOfd).toHaveBeenCalledTimes(1);
  expect(canonical.releaseOfd).toHaveBeenCalledWith(11);
  expect(canonical.kernel_ofd_refcount(11)).toBe(1);
});

test("reconcileCanonicalOfdRefcounts treats blocked threads as live OFD holders until they unblock", () => {
  const canonicalMemory = makeMemory();
  const parentMemory = makeMemory();
  const canonical = makeKernelExports(canonicalMemory);
  const parent = makeKernelExports(parentMemory);
  seedRefcount(canonicalMemory, 12, 1);
  parent.setFdMap(new Map());
  parent.setThreadVmPtrs([0x1234, 0x2222]);
  parent.setBlockedOfdMap(new Map([[0x2222, 12]]));

  const primitives = createKernelWorkerOfdPrimitives({
    getCanonicalExports: () => canonical,
    processKernels: new Map<number, KernelExports>([[1, parent]]),
  });

  primitives.reconcileCanonicalOfdRefcounts(
    [1],
    new Map([[1, 0x1234]]),
    [12],
  );

  expect(canonical.releaseOfd).not.toHaveBeenCalled();
  expect(canonical.kernel_ofd_refcount(12)).toBe(1);

  parent.setBlockedOfdMap(new Map());
  primitives.reconcileCanonicalOfdRefcounts(
    [1],
    new Map([[1, 0x1234]]),
    [12],
  );

  expect(canonical.releaseOfd).toHaveBeenCalledTimes(1);
  expect(canonical.releaseOfd).toHaveBeenCalledWith(12);
  expect(canonical.kernel_ofd_refcount(12)).toBe(0);
});
