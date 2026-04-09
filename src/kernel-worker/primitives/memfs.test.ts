import { describe, expect, it } from "vitest";
import type { KernelExports, KernelRuntimeState } from "../../abi.js";
import { readMemfsLayout } from "../common.js";
import { createKernelWorkerMemfsPrimitives } from "./memfs.js";

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

function createFakeExports(
  memory: WebAssembly.Memory,
  layout: {
    inodesBytes: number;
    firstChildBytes: number;
    dirEntriesBytes: number;
    dataBytes: number;
    headerBytes?: number;
    dataPtr: number;
  },
): KernelExports {
  return {
    memory,
    kernel_memfs_inodes_bytes: () => layout.inodesBytes,
    kernel_memfs_first_child_bytes: () => layout.firstChildBytes,
    kernel_memfs_dir_entries_bytes: () => layout.dirEntriesBytes,
    kernel_memfs_data_bytes: () => layout.dataBytes,
    kernel_memfs_data_ptr: () => layout.dataPtr,
    kernel_memfs_header_bytes: () => layout.headerBytes ?? 32,
  } as unknown as KernelExports;
}

describe("createKernelWorkerMemfsPrimitives", () => {
  it("copies into the target memfs layout instead of source offsets", () => {
    const memoryPages = { initial: 1, maximum: 4, shared: true } as const;
    const sourceMemory = new WebAssembly.Memory(memoryPages);
    const targetMemory = new WebAssembly.Memory(memoryPages);
    const primitives = createKernelWorkerMemfsPrimitives();

    const sourceState: KernelRuntimeState = {
      heapBase: 0,
      heapPtr: 0,
      ofdTablePtr: 0,
      memfsPtr: 0x80,
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
    const targetState: KernelRuntimeState = {
      ...sourceState,
      memfsPtr: 0x180,
      memStatePtr: 0x320,
    };

    writeMemfsHeader(sourceMemory, sourceState.memfsPtr, {
      dataCap: 16,
      inodesPtr: 0x200,
      firstChildPtr: 0x240,
      dirEntriesPtr: 0x280,
      dataPtr: 0x2c0,
    });
    writeMemfsHeader(targetMemory, targetState.memfsPtr, {
      dataCap: 16,
      inodesPtr: 0x400,
      firstChildPtr: 0x440,
      dirEntriesPtr: 0x480,
      dataPtr: 0x4c0,
    });

    new Uint8Array(sourceMemory.buffer).set(new TextEncoder().encode("/bin/program\0"), 0x2c0);

    const targetBytes = new Uint8Array(targetMemory.buffer);
    targetBytes.fill(0xaa, 0x2c0, 0x2c0 + 16);
    targetBytes.fill(0xbb, 0x320, 0x320 + 16);

    const sourceExports = createFakeExports(sourceMemory, {
      inodesBytes: 16,
      firstChildBytes: 16,
      dirEntriesBytes: 16,
      dataBytes: 16,
      dataPtr: 0x2c0,
    });
    const targetExports = createFakeExports(targetMemory, {
      inodesBytes: 16,
      firstChildBytes: 16,
      dirEntriesBytes: 16,
      dataBytes: 16,
      dataPtr: 0x4c0,
    });

    primitives.copyMemfsState(sourceExports, sourceState, targetExports, targetState);

    const copied = new TextDecoder().decode(new Uint8Array(targetMemory.buffer, 0x4c0, 13));
    expect(copied).toBe("/bin/program\0");
    expect(new Uint8Array(targetMemory.buffer, 0x2c0, 16)).toEqual(new Uint8Array(16).fill(0xaa));
    expect(new Uint8Array(targetMemory.buffer, 0x320, 16)).toEqual(new Uint8Array(16).fill(0xbb));
    expect(readMemfsLayout(targetMemory, targetState.memfsPtr).dataPtr).toBe(0x4c0);
  });

  it("clamps full data copy to the target memfs capacity", () => {
    const memoryPages = { initial: 1, maximum: 4, shared: true } as const;
    const sourceMemory = new WebAssembly.Memory(memoryPages);
    const targetMemory = new WebAssembly.Memory(memoryPages);
    const primitives = createKernelWorkerMemfsPrimitives();

    const sourceState: KernelRuntimeState = {
      heapBase: 0,
      heapPtr: 0,
      ofdTablePtr: 0,
      memfsPtr: 0x80,
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
    const targetState: KernelRuntimeState = {
      ...sourceState,
      memfsPtr: 0x180,
    };

    writeMemfsHeader(sourceMemory, sourceState.memfsPtr, {
      dataUsed: 32,
      dataCap: 64,
      inodesPtr: 0x200,
      firstChildPtr: 0x240,
      dirEntriesPtr: 0x280,
      dataPtr: 0x2c0,
    });
    writeMemfsHeader(targetMemory, targetState.memfsPtr, {
      dataUsed: 0,
      dataCap: 16,
      inodesPtr: 0x400,
      firstChildPtr: 0x440,
      dirEntriesPtr: 0x480,
      dataPtr: 0x4c0,
    });

    const sourceBytes = new Uint8Array(sourceMemory.buffer, 0x2c0, 64);
    for (let index = 0; index < sourceBytes.length; index += 1) {
      sourceBytes[index] = index + 1;
    }

    new Uint8Array(targetMemory.buffer, 0x4d0, 16).fill(0xcc);

    const sourceExports = createFakeExports(sourceMemory, {
      inodesBytes: 16,
      firstChildBytes: 16,
      dirEntriesBytes: 16,
      dataBytes: 64,
      dataPtr: 0x2c0,
    });
    const targetExports = createFakeExports(targetMemory, {
      inodesBytes: 16,
      firstChildBytes: 16,
      dirEntriesBytes: 16,
      dataBytes: 16,
      dataPtr: 0x4c0,
    });

    primitives.copyMemfsState(sourceExports, sourceState, targetExports, targetState);

    expect(new Uint8Array(targetMemory.buffer, 0x4c0, 16)).toEqual(sourceBytes.subarray(0, 16));
    expect(new Uint8Array(targetMemory.buffer, 0x4d0, 16)).toEqual(new Uint8Array(16).fill(0xcc));
    expect(readMemfsLayout(targetMemory, targetState.memfsPtr).dataUsed).toBe(16);
  });
});
