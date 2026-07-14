// Kernel-worker tests in this file validate transitional runtime-owned bridge behavior.
import { describe, expect, it, vi } from "vitest";
import type { KernelExports, KernelRuntimeState } from "../../../../src/abi.js";
import { readMemfsLayout } from "../../../../src/kernel-worker/common.js";
import { createKernelWorkerMemfsPrimitives } from "../../../../src/kernel-worker/primitives/memfs.js";
import { writeMemfsHeaderPointer } from "../../../../src/layout/memfs.js";

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
    kernel_attach_runtime_state: () => {},
    kernel_memfs_inodes_bytes: () => layout.inodesBytes,
    kernel_memfs_first_child_bytes: () => layout.firstChildBytes,
    kernel_memfs_dir_entries_bytes: () => layout.dirEntriesBytes,
    kernel_memfs_data_bytes: () => layout.dataBytes,
    kernel_memfs_data_ptr: () => layout.dataPtr,
    kernel_memfs_header_bytes: () => layout.headerBytes ?? 32,
  } as unknown as KernelExports;
}

describe("createKernelWorkerMemfsPrimitives", () => {
  it("reads wasm64 MemFS header pointers at eight-byte offsets", () => {
    const memory = new WebAssembly.Memory({ initial: 1, maximum: 4, shared: true });
    const headerPtr = 0x80;
    const headerBytes = 48;
    const view = new DataView(memory.buffer, headerPtr, headerBytes);
    view.setUint32(0, 17, true);
    view.setUint32(4, 13, true);
    view.setUint32(8, 8, true);
    view.setUint32(12, 4096, true);
    writeMemfsHeaderPointer(view, headerBytes, 0, 0x1000);
    writeMemfsHeaderPointer(view, headerBytes, 1, 0x2000);
    writeMemfsHeaderPointer(view, headerBytes, 2, 0x3000);
    writeMemfsHeaderPointer(view, headerBytes, 3, 0x4000);

    expect(readMemfsLayout(memory, headerPtr, { kernel_memfs_header_bytes: () => headerBytes })).toEqual({
      inodeCount: 17,
      dirEntryCount: 13,
      dataUsed: 8,
      dataCap: 4096,
      inodesPtr: 0x1000,
      firstChildPtr: 0x2000,
      dirEntriesPtr: 0x3000,
      dataPtr: 0x4000,
    });
  });

  it("reattaches the target runtime state after replacing MemFS tables", () => {
    const memoryPages = { initial: 1, maximum: 4, shared: true } as const;
    const sourceMemory = new WebAssembly.Memory(memoryPages);
    const targetMemory = new WebAssembly.Memory(memoryPages);
    const sourceState = { memfsPtr: 0x80 } as KernelRuntimeState;
    const targetState = {
      heapBase: 1,
      heapPtr: 2,
      ofdTablePtr: 3,
      memfsPtr: 0x180,
      memStatePtr: 4,
      epollTablePtr: 5,
      socketStatePtr: 6,
      socketUsed: 7,
      pipeBase: 8,
      pipeUsed: 9n,
      netBufBase: 10,
      netBufUsed: 11n,
      sigSlotsPtr: 12,
      processPendingPtr: 13,
      processRuntimeStatePtr: 14,
      timeStatePtr: 15,
    } as KernelRuntimeState;
    writeMemfsHeader(sourceMemory, sourceState.memfsPtr, {
      inodeCount: 1,
      dirEntryCount: 1,
      dataUsed: 8,
      dataCap: 16,
      inodesPtr: 0x200,
      firstChildPtr: 0x300,
      dirEntriesPtr: 0x340,
      dataPtr: 0x440,
    });
    writeMemfsHeader(targetMemory, targetState.memfsPtr, {
      inodeCount: 1,
      dirEntryCount: 1,
      dataUsed: 8,
      dataCap: 16,
      inodesPtr: 0x500,
      firstChildPtr: 0x600,
      dirEntriesPtr: 0x640,
      dataPtr: 0x740,
    });
    const sourceExports = createFakeExports(sourceMemory, {
      inodesBytes: 128,
      firstChildBytes: 16,
      dirEntriesBytes: 64,
      dataBytes: 16,
      dataPtr: 0x440,
    });
    const targetExports = createFakeExports(targetMemory, {
      inodesBytes: 128,
      firstChildBytes: 16,
      dirEntriesBytes: 64,
      dataBytes: 16,
      dataPtr: 0x740,
    });
    const attach = vi.fn();
    targetExports.kernel_attach_runtime_state = attach;

    createKernelWorkerMemfsPrimitives().copyMemfsState(
      sourceExports,
      sourceState,
      targetExports,
      targetState,
    );

    expect(attach).toHaveBeenCalledOnce();
    expect(attach.mock.calls[0]?.[3]).toBe(targetState.memfsPtr);
  });

  it("copies only active metadata entries and clears a stale target tail", () => {
    const memoryPages = { initial: 1, maximum: 4, shared: true } as const;
    const sourceMemory = new WebAssembly.Memory(memoryPages);
    const targetMemory = new WebAssembly.Memory(memoryPages);
    const primitives = createKernelWorkerMemfsPrimitives();
    const sourceState = { memfsPtr: 0x80 } as KernelRuntimeState;
    const targetState = { memfsPtr: 0x180 } as KernelRuntimeState;

    writeMemfsHeader(sourceMemory, sourceState.memfsPtr, {
      inodeCount: 2,
      dirEntryCount: 2,
      dataUsed: 8,
      dataCap: 16,
      inodesPtr: 0x200,
      firstChildPtr: 0x300,
      dirEntriesPtr: 0x340,
      dataPtr: 0x440,
    });
    writeMemfsHeader(targetMemory, targetState.memfsPtr, {
      inodeCount: 3,
      dirEntryCount: 3,
      dataUsed: 8,
      dataCap: 16,
      inodesPtr: 0x500,
      firstChildPtr: 0x600,
      dirEntriesPtr: 0x640,
      dataPtr: 0x740,
    });

    new Uint8Array(sourceMemory.buffer, 0x200, 64).fill(0x11);
    new Uint8Array(sourceMemory.buffer, 0x300, 8).fill(0x22);
    new Uint8Array(sourceMemory.buffer, 0x340, 32).fill(0x33);
    new Uint8Array(targetMemory.buffer, 0x500, 128).fill(0xcc);
    new Uint8Array(targetMemory.buffer, 0x600, 16).fill(0xcc);
    new Uint8Array(targetMemory.buffer, 0x640, 64).fill(0xcc);

    const sourceExports = createFakeExports(sourceMemory, {
      inodesBytes: 128,
      firstChildBytes: 16,
      dirEntriesBytes: 64,
      dataBytes: 16,
      dataPtr: 0x440,
    });
    const targetExports = createFakeExports(targetMemory, {
      inodesBytes: 128,
      firstChildBytes: 16,
      dirEntriesBytes: 64,
      dataBytes: 16,
      dataPtr: 0x740,
    });

    primitives.copyMemfsState(sourceExports, sourceState, targetExports, targetState);

    expect(new Uint8Array(targetMemory.buffer, 0x500, 64)).toEqual(new Uint8Array(64).fill(0x11));
    expect(new Uint8Array(targetMemory.buffer, 0x500 + 64, 32)).toEqual(new Uint8Array(32));
    expect(new Uint8Array(targetMemory.buffer, 0x500 + 96, 32)).toEqual(new Uint8Array(32).fill(0xcc));
    expect(new Uint8Array(targetMemory.buffer, 0x600, 8)).toEqual(new Uint8Array(8).fill(0x22));
    expect(new Uint8Array(targetMemory.buffer, 0x600 + 8, 4)).toEqual(new Uint8Array(4));
    expect(new Uint8Array(targetMemory.buffer, 0x600 + 12, 4)).toEqual(new Uint8Array(4).fill(0xcc));
    expect(new Uint8Array(targetMemory.buffer, 0x640, 32)).toEqual(new Uint8Array(32).fill(0x33));
    expect(new Uint8Array(targetMemory.buffer, 0x640 + 32, 16)).toEqual(new Uint8Array(16));
    expect(new Uint8Array(targetMemory.buffer, 0x640 + 48, 16)).toEqual(new Uint8Array(16).fill(0xcc));
  });

  it("copies metadata into the target memfs layout without regular inline data", () => {
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

    expect(new Uint8Array(targetMemory.buffer, 0x4c0, 13)).toEqual(new Uint8Array(13));
    expect(new Uint8Array(targetMemory.buffer, 0x2c0, 16)).toEqual(new Uint8Array(16).fill(0xaa));
    expect(new Uint8Array(targetMemory.buffer, 0x320, 16)).toEqual(new Uint8Array(16).fill(0xbb));
    expect(readMemfsLayout(targetMemory, targetState.memfsPtr).dataPtr).toBe(0x4c0);
    expect(readMemfsLayout(targetMemory, targetState.memfsPtr).dataUsed).toBe(8);
  });

  it("clears target data instead of copying regular inline data", () => {
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

    expect(new Uint8Array(targetMemory.buffer, 0x4c0, 16)).toEqual(new Uint8Array(16));
    expect(new Uint8Array(targetMemory.buffer, 0x4d0, 16)).toEqual(new Uint8Array(16).fill(0xcc));
    expect(readMemfsLayout(targetMemory, targetState.memfsPtr).dataUsed).toBe(8);
  });

  it("drops referenced regular-file data even when the source dataUsed header is stale", () => {
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

    const targetInodeView = new DataView(targetMemory.buffer);
    const targetInodeBase = 0x400 + 32;
    expect(new Uint8Array(targetMemory.buffer, 0x500 + 32, 16)).toEqual(new Uint8Array(16));
    expect(targetInodeView.getUint32(targetInodeBase + 24, true)).toBe(0);
    expect(targetInodeView.getUint32(targetInodeBase + 28, true)).toBe(0);
    expect(readMemfsLayout(targetMemory, targetState.memfsPtr).dataUsed).toBe(8);
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
      dataUsed: 16,
      dataCap: 64,
      inodesPtr: 0x400,
      firstChildPtr: 0x480,
      dirEntriesPtr: 0x4a0,
      dataPtr: 0x500,
    });
    new Uint8Array(targetMemory.buffer, 0x500, 64).fill(0xcc);

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
      dirEntriesBytes: 36,
      dataBytes: 64,
      dataPtr: 0x300,
    });
    const targetExports = createFakeExports(targetMemory, {
      inodesBytes: 96,
      firstChildBytes: 12,
      dirEntriesBytes: 36,
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
    expect(new Uint8Array(targetMemory.buffer)[0x500 + 32]).toBe(0xcc);
  });

  it("includeData copy preserves inline regular file data", () => {
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
    const procCpuinfo = new TextEncoder().encode("processor\t: 0\nmmu\t\t: sv48\n");
    const cpuinfoInodeBase = 0x200 + 32;
    sourceInodeView.setUint32(cpuinfoInodeBase + 0, 0o100444, true);
    sourceInodeView.setBigUint64(cpuinfoInodeBase + 16, BigInt(procCpuinfo.length), true);
    sourceInodeView.setUint32(cpuinfoInodeBase + 24, 16, true);
    sourceInodeView.setUint32(cpuinfoInodeBase + 28, procCpuinfo.length, true);

    const symlinkTarget = new TextEncoder().encode("/bin/java");
    const symlinkInodeBase = 0x200 + 64;
    sourceInodeView.setUint32(symlinkInodeBase + 0, 0o120777, true);
    sourceInodeView.setBigUint64(symlinkInodeBase + 16, BigInt(symlinkTarget.length), true);
    sourceInodeView.setUint32(symlinkInodeBase + 24, 48, true);
    sourceInodeView.setUint32(symlinkInodeBase + 28, symlinkTarget.length, true);

    new Uint8Array(sourceMemory.buffer).set(procCpuinfo, 0x300 + 16);
    new Uint8Array(sourceMemory.buffer).set(symlinkTarget, 0x300 + 48);

    const sourceExports = createFakeExports(sourceMemory, {
      inodesBytes: 96,
      firstChildBytes: 12,
      dirEntriesBytes: 36,
      dataBytes: 64,
      dataPtr: 0x300,
    });
    const targetExports = createFakeExports(targetMemory, {
      inodesBytes: 96,
      firstChildBytes: 12,
      dirEntriesBytes: 36,
      dataBytes: 64,
      dataPtr: 0x500,
    });

    primitives.copyMemfsState(sourceExports, sourceState, targetExports, targetState, {
      includeData: true,
    });

    const targetInodeView = new DataView(targetMemory.buffer);
    const targetCpuinfoInodeBase = 0x400 + 32;
    const targetSymlinkInodeBase = 0x400 + 64;
    const cpuinfoDataOffset = targetInodeView.getUint32(targetCpuinfoInodeBase + 24, true);
    const cpuinfoDataCap = targetInodeView.getUint32(targetCpuinfoInodeBase + 28, true);
    const symlinkDataOffset = targetInodeView.getUint32(targetSymlinkInodeBase + 24, true);
    const symlinkDataCap = targetInodeView.getUint32(targetSymlinkInodeBase + 28, true);

    expect(cpuinfoDataOffset).toBe(8);
    expect(cpuinfoDataCap).toBe(procCpuinfo.length);
    expect(new TextDecoder().decode(new Uint8Array(targetMemory.buffer, 0x500 + cpuinfoDataOffset, procCpuinfo.length)))
      .toBe("processor\t: 0\nmmu\t\t: sv48\n");
    expect(symlinkDataOffset).toBe(8 + procCpuinfo.length);
    expect(symlinkDataCap).toBe(symlinkTarget.length);
    expect(new TextDecoder().decode(new Uint8Array(targetMemory.buffer, 0x500 + symlinkDataOffset, symlinkTarget.length)))
      .toBe("/bin/java");
    expect(readMemfsLayout(targetMemory, targetState.memfsPtr).dataUsed)
      .toBe(8 + procCpuinfo.length + symlinkTarget.length);
  });
});
