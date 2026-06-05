// Kernel-worker tests in this file validate transitional runtime-owned bridge behavior.
import { expect, test, vi } from "vite-plus/test";
import type { KernelRuntimeState } from "../../../src/abi.js";
import type { KernelWorkerOutbound } from "../../../src/messages.js";
import { createKernelWorkerMessageHandler } from "../../../src/kernel-worker/message-handler.js";
import { KERNEL_WORKER_FS_STATE_OWNERSHIP } from "../../../src/kernel-worker/fs-state.js";
import { KERNEL_WORKER_PRIMITIVES_OWNERSHIP } from "../../../src/kernel-worker/primitives.js";
import { KERNEL_WORKER_PROCESS_CONTROL_OWNERSHIP } from "../../../src/kernel-worker/process-control.js";
import { KERNEL_WORKER_SHARED_STATE_SYNC_OWNERSHIP } from "../../../src/kernel-worker/shared-state-sync.js";

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memfsHeaderBytes: 0x1000,
    memfsInodesBytes: 0x2000,
    memfsFirstChildBytes: 0x1000,
    memfsDirEntriesBytes: 0x2000,
    memfsDataBytes: 0x4000,
    memStatePtr: 0x240000,
    epollTablePtr: 0x250000,
    socketTablePtr: 0x260000,
    socketUsed: 0,
    pipeBase: 0x270000,
    pipeUsed: 0n,
    netBufBase: 0x280000,
    netBufUsed: 0n,
    sigSlotsPtr: 0xc00000,
    processPendingPtr: 0xc10000,
    processRuntimeStatePtr: 0xc20000,
    timeStatePtr: 0xc30000,
  };
}

function makeDeps(posted: KernelWorkerOutbound[]) {
  const state = makeKernelState();
  return {
    postResponse: (msg: KernelWorkerOutbound) => posted.push(msg),
    initKernel: vi.fn(async () => state),
    writeFile: vi.fn(),
    writeFiles: vi.fn(),
    applyFileLayer: vi.fn(),
    symlink: vi.fn(),
    readFile: vi.fn(() => new Uint8Array([1, 2, 3])),
    readlink: vi.fn(() => new Uint8Array([4, 5, 6])),
    mkdir: vi.fn(),
    statPath: vi.fn(() => ({ ino: 1, mode: 0o100644, nlink: 1, uid: 0, gid: 0, size: 3 })),
    readdir: vi.fn(() => ["a"]),
    snapshotCanonicalFs: vi.fn(() => []),
    loadFsSnapshot: vi.fn(),
    registerProcess: vi.fn(async () => state),
    registerHostFileBackingSource: vi.fn(async () => {}),
    registerContentAddressedBackingSource: vi.fn(async () => {}),
    unregisterProcess: vi.fn(),
    processFs: vi.fn(() => ({
      kernelState: state,
      result: 0n,
      memfsMutated: false,
      ofdStateChanged: false,
    })),
    recordChildExit: vi.fn(() => state),
    resumeConnect: vi.fn(() => state),
    resumeWait4: vi.fn(() => state),
    resumeBlockedSyscall: vi.fn(() => state),
    resumeVforkParent: vi.fn(() => ({ childPid: 4, kernelState: state })),
    canonicalizeChildPid: vi.fn(() => ({
      childPid: 4,
      nextPid: 5,
      pgid: 3,
      sid: 3,
      kernelState: state,
    })),
    initializeProcessLifecycle: vi.fn(() => state),
    resumeVforkExecveParent: vi.fn(() => state),
    syncProcessLifecycle: vi.fn(() => state),
    syncProcessMemfs: vi.fn(() => state),
    syncPipeState: vi.fn(() => ({ kernelState: state, pipeSlots: [] })),
    syncEpollState: vi.fn(() => state),
    syncOfdState: vi.fn(() => state),
    syncSocketState: vi.fn(() => state),
    applySocketStateSnapshot: vi.fn((_pid, kernelState) => kernelState),
    snapshotSocketState: vi.fn(() => undefined),
    syncForkSharedState: vi.fn(() => ({ kernelState: state })),
    rehydrateForkSharedState: vi.fn(),
    debugProcesses: vi.fn(() => ({ pids: [3], canonicalPids: [3] })),
    debugPageCachePath: vi.fn(() => null),
    debugEvictPageCachePath: vi.fn(() => null),
    debugPageCacheStats: vi.fn(() => ({
      byteLength: 0,
      dataUsed: 0,
      inodeCount: 0,
      dirEpoch: 0,
    })),
    releaseHostStdio: vi.fn(() => state),
  } satisfies Parameters<typeof createKernelWorkerMessageHandler>[0];
}

test("message handler routes sync-process-memfs as transitional kernel-worker bridge behavior", async () => {
  const posted: KernelWorkerOutbound[] = [];
  const deps = makeDeps(posted);
  const kernelState = makeKernelState();
  const handleMessage = createKernelWorkerMessageHandler(deps);

  await handleMessage({
    type: "sync-process-memfs",
    requestId: 17,
    pid: 7,
    kernelState,
  });

  expect(deps.syncProcessMemfs).toHaveBeenCalledWith(7, kernelState);
  expect(posted).toEqual([
    {
      type: "process-memfs-synced",
      requestId: 17,
      kernelState,
    },
  ]);
});

test("message handler forwards fd and OFD snapshots from process-fs responses", async () => {
  const posted: KernelWorkerOutbound[] = [];
  const deps = makeDeps(posted);
  const kernelState = makeKernelState();
  const fdEntries = [{ fd: 5, ofdId: 7, fdFlags: 1 }];
  const ofdSlots = [{ ofdId: 7, slot: new Uint8Array([1, 2, 3, 4]) }];
  deps.processFs.mockReturnValue({
    kernelState,
    result: 5n,
    memfsMutated: false,
    ofdStateChanged: false,
    fdEntries,
    ofdSlots,
  });
  const handleMessage = createKernelWorkerMessageHandler(deps);

  await handleMessage({
    type: "process-fs",
    requestId: 23,
    pid: 7,
    vmPtr: 0x1234,
    kernelState,
  });

  expect(posted).toEqual([
    {
      type: "fs-processed",
      requestId: 23,
      kernelState,
      result: 5n,
      memfsMutated: false,
      ofdStateChanged: false,
      fdEntries,
      ofdSlots,
      fsDebug: undefined,
    },
  ]);
});

test("message handler reports dependency errors without escaping the kernel-worker boundary", async () => {
  const posted: KernelWorkerOutbound[] = [];
  const deps = makeDeps(posted);
  deps.readFile.mockImplementation(() => {
    throw new Error("read boom");
  });
  const handleMessage = createKernelWorkerMessageHandler(deps);

  await handleMessage({
    type: "read-file",
    requestId: 19,
    path: "/tmp/a",
  });

  expect(posted).toHaveLength(1);
  expect(posted[0]?.type).toBe("error");
  expect(posted[0]?.requestId).toBe(19);
  expect(posted[0]?.type === "error" ? posted[0].message : "").toContain("read boom");
});

test("message handler registers host-file backing sources", async () => {
  const posted: KernelWorkerOutbound[] = [];
  const deps = makeDeps(posted);
  const handleMessage = createKernelWorkerMessageHandler(deps);

  await handleMessage({
    type: "register-fs-host-file-backing-source",
    requestId: 29,
    sourceIdentity: "snapshot-source:host",
    hostPath: "/tmp/host.bin",
    byteLength: 123,
  });

  expect(deps.registerHostFileBackingSource).toHaveBeenCalledWith("snapshot-source:host", "/tmp/host.bin", 123);
  expect(posted).toEqual([
    {
      type: "fs-backing-source-registered",
      requestId: 29,
    },
  ]);
});

test("message handler registers content-addressed backing sources", async () => {
  const posted: KernelWorkerOutbound[] = [];
  const deps = makeDeps(posted);
  const data = new Uint8Array([1, 2, 3]);
  const handleMessage = createKernelWorkerMessageHandler(deps);

  await handleMessage({
    type: "register-fs-content-addressed-backing-source",
    requestId: 31,
    sourceIdentity: "sha256:source",
    digest: "sha256:digest",
    data,
  });

  expect(deps.registerContentAddressedBackingSource).toHaveBeenCalledWith("sha256:source", "sha256:digest", data);
  expect(posted).toEqual([
    {
      type: "fs-backing-source-registered",
      requestId: 31,
    },
  ]);
});

test("state-owning kernel-worker modules are marked transitional-runtime-owner", () => {
  expect(KERNEL_WORKER_FS_STATE_OWNERSHIP).toBe("transitional-runtime-owner");
  expect(KERNEL_WORKER_PRIMITIVES_OWNERSHIP).toBe("transitional-runtime-owner");
  expect(KERNEL_WORKER_PROCESS_CONTROL_OWNERSHIP).toBe("transitional-runtime-owner");
  expect(KERNEL_WORKER_SHARED_STATE_SYNC_OWNERSHIP).toBe("transitional-runtime-owner");
});

test("message handler routes debug resident page eviction through kernel-worker owner", async () => {
  const posted: KernelWorkerOutbound[] = [];
  const deps = makeDeps(posted);
  const handleMessage = createKernelWorkerMessageHandler(deps);
  deps.debugEvictPageCachePath.mockReturnValue({
    path: "/lib/runtime.bin",
    ino: 2,
    pageIndex: 0,
    status: "evicted",
    residentPages: 0,
  });

  await handleMessage({
    type: "debug-evict-page-cache-path",
    requestId: 44,
    path: "/lib/runtime.bin",
    pageIndex: 0,
  });

  expect(deps.debugEvictPageCachePath).toHaveBeenCalledWith("/lib/runtime.bin", 0);
  expect(posted).toEqual([
    {
      type: "page-cache-path-evicted",
      requestId: 44,
      result: {
        path: "/lib/runtime.bin",
        ino: 2,
        pageIndex: 0,
        status: "evicted",
        residentPages: 0,
      },
    },
  ]);
});
