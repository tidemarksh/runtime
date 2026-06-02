// Kernel-worker tests in this file validate transitional runtime-owned bridge behavior.
import { describe, expect, it } from "vitest";
import type { KernelExports, KernelRuntimeState } from "../../../../src/abi.js";
import { readMemfsLayout } from "../../../../src/kernel-worker/common.js";
import { createKernelWorkerMemfsPrimitives } from "../../../../src/kernel-worker/primitives/memfs.js";

function writeMemfsHeader(
  memory: WebAssembly.Memory,
  ptr: number,
  layout: {
    headerBytes?: number;
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
  const headerBytes = layout.headerBytes ?? 32;
  const view = new DataView(memory.buffer, ptr, headerBytes);
  view.setUint32(0, layout.inodeCount ?? 1, true);
  view.setUint32(4, layout.dirEntryCount ?? 1, true);
  view.setUint32(8, layout.dataUsed ?? layout.dataCap, true);
  view.setUint32(12, layout.dataCap, true);
  if (headerBytes >= 48) {
    view.setBigUint64(16, BigInt(layout.inodesPtr), true);
    view.setBigUint64(24, BigInt(layout.firstChildPtr), true);
    view.setBigUint64(32, BigInt(layout.dirEntriesPtr), true);
    view.setBigUint64(40, BigInt(layout.dataPtr), true);
    return;
  }
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
    kernel_attach_runtime_state: () => {},
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

  it("preserves Memory64 memfs header pointer slots when copying state", () => {
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
      memfsHeaderBytes: 48,
    };
    const targetState: KernelRuntimeState = {
      ...sourceState,
      memfsPtr: 0x180,
      memStatePtr: 0x320,
    };

    writeMemfsHeader(sourceMemory, sourceState.memfsPtr, {
      headerBytes: 48,
      inodeCount: 3,
      dirEntryCount: 2,
      dataUsed: 16,
      dataCap: 32,
      inodesPtr: 0x200,
      firstChildPtr: 0x240,
      dirEntriesPtr: 0x280,
      dataPtr: 0x2c0,
    });
    writeMemfsHeader(targetMemory, targetState.memfsPtr, {
      headerBytes: 48,
      inodeCount: 1,
      dirEntryCount: 1,
      dataUsed: 8,
      dataCap: 32,
      inodesPtr: 0x400,
      firstChildPtr: 0x440,
      dirEntriesPtr: 0x480,
      dataPtr: 0x4c0,
    });

    const sourceExports = createFakeExports(sourceMemory, {
      headerBytes: 48,
      inodesBytes: 16,
      firstChildBytes: 16,
      dirEntriesBytes: 16,
      dataBytes: 32,
      dataPtr: 0x2c0,
    });
    const targetExports = createFakeExports(targetMemory, {
      headerBytes: 48,
      inodesBytes: 16,
      firstChildBytes: 16,
      dirEntriesBytes: 16,
      dataBytes: 32,
      dataPtr: 0x4c0,
    });

    primitives.copyMemfsState(sourceExports, sourceState, targetExports, targetState);

    const header = new DataView(targetMemory.buffer, targetState.memfsPtr, 48);
    expect(header.getUint32(0, true)).toBe(3);
    expect(header.getUint32(4, true)).toBe(2);
    expect(header.getUint32(8, true)).toBe(16);
    expect(header.getBigUint64(16, true)).toBe(0x400n);
    expect(header.getBigUint64(24, true)).toBe(0x440n);
    expect(header.getBigUint64(32, true)).toBe(0x480n);
    expect(header.getBigUint64(40, true)).toBe(0x4c0n);
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

  it("copies referenced regular-file data even when the source dataUsed header is stale", () => {
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

    const sourceExports = createFakeExports(sourceMemory, {
      inodesBytes: 64,
      firstChildBytes: 8,
      dirEntriesBytes: 32,
      dataBytes: 64,
      dataPtr: 0x300,
    });
    const targetExports = createFakeExports(targetMemory, {
      inodesBytes: 64,
      firstChildBytes: 8,
      dirEntriesBytes: 32,
      dataBytes: 64,
      dataPtr: 0x500,
    });

    primitives.copyMemfsState(sourceExports, sourceState, targetExports, targetState);

    expect(new TextDecoder().decode(new Uint8Array(targetMemory.buffer, 0x500 + 32, 16))).toBe("0123456789abcdef");
    expect(readMemfsLayout(targetMemory, targetState.memfsPtr).dataUsed).toBe(48);
  });

  it("metadata-only copy preserves symlink targets while dropping regular file data", () => {
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
      inodeCount: 3,
      dataUsed: 48,
      dataCap: 64,
      inodesPtr: 0x200,
      firstChildPtr: 0x280,
      dirEntriesPtr: 0x2a0,
      dataPtr: 0x300,
    });
    writeMemfsHeader(targetMemory, targetState.memfsPtr, {
      inodeCount: 3,
      dataUsed: 0,
      dataCap: 64,
      inodesPtr: 0x400,
      firstChildPtr: 0x480,
      dirEntriesPtr: 0x4a0,
      dataPtr: 0x500,
    });

    const sourceInodeView = new DataView(sourceMemory.buffer);
    const regularInodeBase = 0x200 + 32;
    sourceInodeView.setUint32(regularInodeBase + 0, 0o100644, true);
    sourceInodeView.setBigUint64(regularInodeBase + 16, 4n, true);
    sourceInodeView.setUint32(regularInodeBase + 24, 16, true);
    sourceInodeView.setUint32(regularInodeBase + 28, 4, true);

    const symlinkInodeBase = 0x200 + 64;
    const symlinkTarget = new TextEncoder().encode("usr/lib");
    sourceInodeView.setUint32(symlinkInodeBase + 0, 0o120777, true);
    sourceInodeView.setBigUint64(symlinkInodeBase + 16, BigInt(symlinkTarget.length), true);
    sourceInodeView.setUint32(symlinkInodeBase + 24, 32, true);
    sourceInodeView.setUint32(symlinkInodeBase + 28, symlinkTarget.length, true);

    new Uint8Array(sourceMemory.buffer).set(new TextEncoder().encode("file"), 0x300 + 16);
    new Uint8Array(sourceMemory.buffer).set(symlinkTarget, 0x300 + 32);

    const sourceExports = createFakeExports(sourceMemory, {
      inodesBytes: 96,
      firstChildBytes: 12,
      dirEntriesBytes: 32,
      dataBytes: 64,
      dataPtr: 0x300,
    });
    const targetExports = createFakeExports(targetMemory, {
      inodesBytes: 96,
      firstChildBytes: 12,
      dirEntriesBytes: 32,
      dataBytes: 64,
      dataPtr: 0x500,
    });

    primitives.copyMemfsState(sourceExports, sourceState, targetExports, targetState, {
      includeData: false,
    });

    const targetInodeView = new DataView(targetMemory.buffer);
    const targetRegularInodeBase = 0x400 + 32;
    const targetSymlinkInodeBase = 0x400 + 64;
    const symlinkDataOffset = targetInodeView.getUint32(targetSymlinkInodeBase + 24, true);
    const symlinkDataCap = targetInodeView.getUint32(targetSymlinkInodeBase + 28, true);

    expect(targetInodeView.getUint32(targetRegularInodeBase + 24, true)).toBe(0);
    expect(targetInodeView.getUint32(targetRegularInodeBase + 28, true)).toBe(0);
    expect(symlinkDataOffset).toBe(8);
    expect(symlinkDataCap).toBe(symlinkTarget.length);
    expect(new TextDecoder().decode(new Uint8Array(targetMemory.buffer, 0x500 + symlinkDataOffset, symlinkTarget.length))).toBe("usr/lib");
    expect(readMemfsLayout(targetMemory, targetState.memfsPtr).dataUsed).toBe(8 + symlinkTarget.length);
  });
});
