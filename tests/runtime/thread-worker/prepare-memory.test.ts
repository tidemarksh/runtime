import { expect, test } from "vite-plus/test";
import {
  restoreThreadWorkerPrepareMemory,
  snapshotThreadWorkerPrepareMemory,
} from "../../../src/thread-worker/prepare-memory.js";
import type { KernelRuntimeState } from "../../../src/abi.js";

function makeState(): KernelRuntimeState {
  return {
    heapBase: 0x1000,
    heapPtr: 0x1800,
    ofdTablePtr: 0x3000,
    memfsPtr: 0x4000,
    memfsHeaderBytes: 0x20,
    memfsInodesBytes: 0x40,
    memfsFirstChildBytes: 0x8,
    memfsDirEntriesBytes: 0x20,
    memfsDataBytes: 0x200,
    memStatePtr: 0x1000,
    epollTablePtr: 0x5000,
    socketTablePtr: 0x6000,
    socketUsed: 0,
    pipeBase: 0x7000,
    pipeUsed: 0n,
    netBufBase: 0x8000,
    netBufUsed: 0n,
    sigSlotsPtr: 0x9000,
    processPendingPtr: 0xa000,
    processRuntimeStatePtr: 0x1028,
    timeStatePtr: 0xb000,
  };
}

test("thread-worker prepare snapshot preserves memState bytes at heapBase and memfs header bytes above heapBase", () => {
  const memory = new WebAssembly.Memory({ initial: 2, maximum: 2, shared: true });
  const state = makeState();
  const bytes = new Uint8Array(memory.buffer);
  const view = new DataView(memory.buffer);
  const memfsInodesPtr = 0x5000;
  const memfsFirstChildPtr = 0x5040;
  const memfsDirEntriesPtr = 0x5048;
  const memfsDataPtr = 0xc000;

  for (let i = 0; i < state.heapBase; i += 1) {
    bytes[i] = (i * 3) & 0xff;
  }
  for (let i = 0; i < 0x28; i += 1) {
    bytes[state.memStatePtr + i] = 0xa0 + i;
  }
  for (let i = 0; i < 0x20; i += 1) {
    bytes[state.memfsPtr + i] = 0xc0 + i;
  }
  view.setUint32(state.memfsPtr + 0, 2, true);
  view.setUint32(state.memfsPtr + 4, 1, true);
  view.setUint32(state.memfsPtr + 8, 0x40, true);
  view.setUint32(state.memfsPtr + 12, state.memfsDataBytes, true);
  view.setUint32(state.memfsPtr + 16, memfsInodesPtr, true);
  view.setUint32(state.memfsPtr + 20, memfsFirstChildPtr, true);
  view.setUint32(state.memfsPtr + 24, memfsDirEntriesPtr, true);
  view.setUint32(state.memfsPtr + 28, memfsDataPtr, true);
  const inodeBase = memfsInodesPtr + 32;
  view.setUint32(inodeBase + 0, 0o100644, true);
  view.setBigUint64(inodeBase + 16, 0x40n, true);
  view.setUint32(inodeBase + 24, 0, true);
  view.setUint32(inodeBase + 28, 0x40, true);
  const expectedMemfsHeader = Array.from(bytes.slice(state.memfsPtr, state.memfsPtr + 0x20));
  bytes[state.memfsPtr + 0x20] = 0xee;
  for (let i = 0; i < 0x40; i += 1) {
    bytes[memfsDataPtr + i] = 0xd0 + i;
  }
  bytes[memfsDataPtr + 0x40] = 0xaa;

  const snapshot = snapshotThreadWorkerPrepareMemory(
    memory,
    {
      kernel_memfs_header_bytes: () => 0x20,
      kernel_memfs_data_ptr: () => memfsDataPtr,
      memfs_data_used: () => 0x40,
    } as never,
    state,
    state.heapBase,
  );

  bytes.fill(0, 0, state.heapBase);
  bytes.fill(0, state.memStatePtr, state.memStatePtr + 0x28);
  bytes.fill(0, state.memfsPtr, state.memfsPtr + 0x20);
  bytes[state.memfsPtr + 0x20] = 0x55;
  bytes.fill(0, memfsDataPtr, memfsDataPtr + 0x40);
  bytes[memfsDataPtr + 0x40] = 0x33;

  restoreThreadWorkerPrepareMemory(memory, snapshot);

  expect(bytes[0]).toBe(0);
  expect(bytes[0x3ff]).toBe((0x3ff * 3) & 0xff);
  expect(Array.from(bytes.slice(state.memStatePtr, state.memStatePtr + 0x28))).toEqual(
    Array.from({ length: 0x28 }, (_value, index) => 0xa0 + index),
  );
  expect(Array.from(bytes.slice(state.memfsPtr, state.memfsPtr + 0x20))).toEqual(expectedMemfsHeader);
  expect(bytes[state.memfsPtr + 0x20]).toBe(0x55);
  expect(Array.from(bytes.slice(memfsDataPtr, memfsDataPtr + 0x40))).toEqual(
    Array.from({ length: 0x40 }, (_value, index) => (0xd0 + index) & 0xff),
  );
  expect(bytes[memfsDataPtr + 0x40]).toBe(0x33);
});

test("thread-worker prepare snapshot preserves referenced memfs file data beyond data_used without copying the full data cap", () => {
  const memory = new WebAssembly.Memory({ initial: 2, maximum: 2, shared: true });
  const state = makeState();
  const bytes = new Uint8Array(memory.buffer);
  const view = new DataView(memory.buffer);
  const memfsInodesPtr = 0x5000;
  const memfsFirstChildPtr = 0x5040;
  const memfsDirEntriesPtr = 0x5048;
  const memfsDataPtr = 0x6000;

  view.setUint32(state.memfsPtr + 0, 2, true);
  view.setUint32(state.memfsPtr + 4, 1, true);
  view.setUint32(state.memfsPtr + 8, 0x40, true);
  view.setUint32(state.memfsPtr + 12, state.memfsDataBytes, true);
  view.setUint32(state.memfsPtr + 16, memfsInodesPtr, true);
  view.setUint32(state.memfsPtr + 20, memfsFirstChildPtr, true);
  view.setUint32(state.memfsPtr + 24, memfsDirEntriesPtr, true);
  view.setUint32(state.memfsPtr + 28, memfsDataPtr, true);

  const inodeBase = memfsInodesPtr + 32;
  view.setUint32(inodeBase + 0, 0o100644, true);
  view.setBigUint64(inodeBase + 16, 0x80n, true);
  view.setUint32(inodeBase + 24, 0x100, true);
  view.setUint32(inodeBase + 28, 0x100, true);

  bytes[memfsDataPtr + 0x40] = 0xaa;
  bytes[memfsDataPtr + 0x120] = 0xbb;
  bytes[memfsDataPtr + 0x190] = 0xcc;

  const snapshot = snapshotThreadWorkerPrepareMemory(
    memory,
    {
      kernel_memfs_header_bytes: () => state.memfsHeaderBytes,
      kernel_memfs_inodes_bytes: () => state.memfsInodesBytes,
      kernel_memfs_first_child_bytes: () => state.memfsFirstChildBytes,
      kernel_memfs_dir_entries_bytes: () => state.memfsDirEntriesBytes,
      kernel_memfs_data_ptr: () => memfsDataPtr,
      kernel_memfs_data_bytes: () => state.memfsDataBytes,
      memfs_data_used: () => 0x40,
    } as never,
    state,
    state.heapBase,
  );

  bytes.fill(0, memfsDataPtr, memfsDataPtr + state.memfsDataBytes);

  restoreThreadWorkerPrepareMemory(memory, snapshot);

  expect(bytes[memfsDataPtr + 0x40]).toBe(0xaa);
  expect(bytes[memfsDataPtr + 0x120]).toBe(0xbb);
  expect(bytes[memfsDataPtr + 0x190]).toBe(0);
});

test("thread-worker prepare snapshot can preserve explicit ranges without copying the whole prefix", () => {
  const memory = new WebAssembly.Memory({ initial: 3, maximum: 3, shared: true });
  const bytes = new Uint8Array(memory.buffer);
  const firstRangeStart = 0x4000;
  const firstRangeEnd = 0x4400;
  const secondRangeStart = 0x18000;
  const secondRangeEnd = 0x18800;

  bytes.fill(0x11, 0, firstRangeStart);
  bytes.fill(0x22, firstRangeStart, firstRangeEnd);
  bytes.fill(0x33, firstRangeEnd, secondRangeStart);
  bytes.fill(0x44, secondRangeStart, secondRangeEnd);
  bytes.fill(0x55, secondRangeEnd);

  const snapshot = snapshotThreadWorkerPrepareMemory(
    memory,
    null,
    makeState(),
    0,
    {
      preserveRanges: [
        { start: secondRangeStart, end: secondRangeEnd },
        { start: firstRangeStart, end: firstRangeEnd },
      ],
    },
  );

  bytes.fill(0);

  restoreThreadWorkerPrepareMemory(memory, snapshot);

  expect(bytes[firstRangeStart - 1]).toBe(0);
  expect(bytes[firstRangeStart]).toBe(0x22);
  expect(bytes[firstRangeEnd - 1]).toBe(0x22);
  expect(bytes[firstRangeEnd]).toBe(0);
  expect(bytes[secondRangeStart - 1]).toBe(0);
  expect(bytes[secondRangeStart]).toBe(0x44);
  expect(bytes[secondRangeEnd - 1]).toBe(0x44);
  expect(bytes[secondRangeEnd]).toBe(0);
});
