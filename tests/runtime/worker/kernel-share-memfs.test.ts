import { expect, test } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../src/abi.js";
import { syncMemfsState } from "../../../src/worker/kernel-share/memfs.js";
import type { KernelSyncHandle } from "../../../src/worker/types.js";

function writeMemfsHeader(
  memory: WebAssembly.Memory,
  ptr: number,
  layout: {
    inodeCount?: number;
    dirEntryCount?: number;
    dataUsed?: number;
    dataCap: number;
    inodesPtr: number;
    firstChildPtr: number;
    dirEntriesPtr: number;
    dataPtr: number;
  },
): void {
  const view = new DataView(memory.buffer, ptr, 32);
  view.setUint32(0, layout.inodeCount ?? 1, true);
  view.setUint32(4, layout.dirEntryCount ?? 1, true);
  view.setUint32(8, layout.dataUsed ?? layout.dataCap, true);
  view.setUint32(12, layout.dataCap, true);
  view.setUint32(16, layout.inodesPtr, true);
  view.setUint32(20, layout.firstChildPtr, true);
  view.setUint32(24, layout.dirEntriesPtr, true);
  view.setUint32(28, layout.dataPtr, true);
}

function createHandle(
  memory: WebAssembly.Memory,
  runtimeState: KernelRuntimeState,
  layout: {
    inodesBytes: number;
    firstChildBytes: number;
    dirEntriesBytes: number;
    headerBytes?: number;
  },
): KernelSyncHandle {
  return {
    memory,
    pid: 1,
    kernelWorkerRequest: undefined,
    kernelState: runtimeState,
    kernel: {
      memory,
      kernel_memfs_inodes_bytes: () => layout.inodesBytes,
      kernel_memfs_first_child_bytes: () => layout.firstChildBytes,
      kernel_memfs_dir_entries_bytes: () => layout.dirEntriesBytes,
      kernel_memfs_header_bytes: () => layout.headerBytes ?? 32,
    } as unknown as KernelExports,
  };
}

function createRuntimeState(memfsPtr: number): KernelRuntimeState {
  return {
    heapBase: 0,
    heapPtr: 0,
    ofdTablePtr: 0,
    memfsPtr,
    memStatePtr: 0,
    epollTablePtr: 0,
    socketTablePtr: 0,
    socketUsed: 0,
    pipeBase: 0,
    pipeUsed: 0n,
    netBufBase: 0,
    netBufUsed: 0n,
    sigSlotsPtr: 0,
    processPendingPtr: 0,
    processRuntimeStatePtr: 0,
    timeStatePtr: 0,
  };
}

test("syncMemfsState copies referenced file data even when dataUsed is stale", () => {
  const memoryPages = { initial: 1, maximum: 4, shared: true } as const;
  const sourceMemory = new WebAssembly.Memory(memoryPages);
  const targetMemory = new WebAssembly.Memory(memoryPages);

  const sourceState = createRuntimeState(0x80);
  const targetState = createRuntimeState(0x180);

  writeMemfsHeader(sourceMemory, sourceState.memfsPtr, {
    inodeCount: 2,
    dataUsed: 8,
    dataCap: 64,
    inodesPtr: 0x200,
    firstChildPtr: 0x280,
    dirEntriesPtr: 0x2a0,
    dataPtr: 0x300,
  });
  writeMemfsHeader(targetMemory, targetState.memfsPtr, {
    inodeCount: 2,
    dataUsed: 0,
    dataCap: 64,
    inodesPtr: 0x400,
    firstChildPtr: 0x480,
    dirEntriesPtr: 0x4a0,
    dataPtr: 0x500,
  });

  const sourceInodeView = new DataView(sourceMemory.buffer);
  const sourceInodeBase = 0x200 + 32;
  sourceInodeView.setUint32(sourceInodeBase + 0, 0o100755, true);
  sourceInodeView.setBigUint64(sourceInodeBase + 16, 16n, true);
  sourceInodeView.setUint32(sourceInodeBase + 24, 32, true);
  sourceInodeView.setUint32(sourceInodeBase + 28, 16, true);
  new Uint8Array(sourceMemory.buffer).set(new TextEncoder().encode("0123456789abcdef"), 0x300 + 32);
  new Uint8Array(targetMemory.buffer, 0x500 + 48, 16).fill(0xcc);

  const source = createHandle(sourceMemory, sourceState, {
    inodesBytes: 64,
    firstChildBytes: 8,
    dirEntriesBytes: 32,
  });
  const target = createHandle(targetMemory, targetState, {
    inodesBytes: 64,
    firstChildBytes: 8,
    dirEntriesBytes: 32,
  });

  syncMemfsState(source, target);

  expect(new TextDecoder().decode(new Uint8Array(targetMemory.buffer, 0x500 + 32, 16))).toBe("0123456789abcdef");
  expect(new Uint8Array(targetMemory.buffer, 0x500 + 48, 16)).toEqual(new Uint8Array(16));
  const targetHeader = new DataView(targetMemory.buffer, targetState.memfsPtr, 32);
  expect(targetHeader.getUint32(8, true)).toBe(48);
  expect(targetHeader.getUint32(12, true)).toBe(64);
  expect(targetHeader.getUint32(16, true)).toBe(0x400);
  expect(targetHeader.getUint32(28, true)).toBe(0x500);
});
