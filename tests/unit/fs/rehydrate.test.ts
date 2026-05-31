import { expect, test } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../src/abi.js";
import { createKernelWorkerFsUtils } from "../../../src/kernel-worker/fs-utils.js";

type MemfsEntry = {
  ino: number;
  kind: "dir" | "file" | "symlink";
  mode: number;
  target?: Uint8Array;
};

type FakeMemfsExports = KernelExports & {
  entries: Map<string, MemfsEntry>;
  ensurePath(path: string, kind: "dir" | "file" | "symlink", mode?: number, target?: Uint8Array): void;
  linkTarget(path: string): string | null;
  fileContents(path: string): string | null;
  procfsInstallTargets: string[];
};

function normalizePath(path: string): string {
  const normalized = `/${path.split("/").filter(Boolean).join("/")}`;
  return normalized || "/";
}

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
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

function makeMemfsExports(): FakeMemfsExports {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 16 });
  let runtimeState = makeKernelState();
  const entries = new Map<string, MemfsEntry>();
  const byIno = new Map<number, string>();
  const procfsInstallTargets: string[] = [];
  let nextIno = 2;

  const ensureEntry = (
    path: string,
    kind: "dir" | "file" | "symlink",
    mode: number,
    target?: Uint8Array,
  ): number => {
    const normalized = normalizePath(path);
    const current = entries.get(normalized);
    if (current) {
      current.kind = kind;
      current.mode = mode;
      current.target = target ? new Uint8Array(target) : undefined;
      return current.ino;
    }
    const ino = normalized === "/" ? 1 : nextIno++;
    entries.set(normalized, {
      ino,
      kind,
      mode,
      target: target ? new Uint8Array(target) : undefined,
    });
    byIno.set(ino, normalized);
    return ino;
  };

  const ensureDirPath = (path: string, mode = 0o555): number => {
    const normalized = normalizePath(path);
    if (normalized === "/") {
      return ensureEntry("/", "dir", 0o040000 | mode);
    }
    const parts = normalized.split("/").filter(Boolean);
    let current = "";
    let ino = 1;
    for (const part of parts) {
      current += `/${part}`;
      ino = ensureEntry(current, "dir", 0o040000 | mode);
    }
    return ino;
  };

  const readGuestBytes = (ptr: number, len: number): Uint8Array =>
    new Uint8Array(memory.buffer.slice(ptr, ptr + len));
  const decodeGuestPath = (ptr: number, len: number): string =>
    new TextDecoder().decode(readGuestBytes(ptr, len));
  const ensurePath = (
    path: string,
    kind: "dir" | "file" | "symlink",
    mode = kind === "dir" ? 0o040555 : kind === "file" ? 0o100444 : 0o120777,
    target?: Uint8Array,
  ): void => {
    const normalized = normalizePath(path);
    if (normalized !== "/") {
      ensureDirPath(normalized.split("/").slice(0, -1).join("/") || "/", 0o555);
    }
    ensureEntry(normalized, kind, mode, target);
  };

  ensureDirPath("/", 0o555);

  const exports = {
    memory,
    entries,
    kernel_heap_base: () => runtimeState.heapBase,
    kernel_heap_ptr: () => 0x1000,
    kernel_ofd_table_ptr: () => runtimeState.ofdTablePtr,
    kernel_memfs_ptr: () => runtimeState.memfsPtr,
    kernel_mem_state_ptr: () => runtimeState.memStatePtr,
    kernel_epoll_table_ptr: () => runtimeState.epollTablePtr,
    kernel_socket_table_ptr: () => runtimeState.socketTablePtr,
    kernel_socket_used: () => runtimeState.socketUsed,
    kernel_pipe_base: () => runtimeState.pipeBase,
    kernel_pipe_used: () => runtimeState.pipeUsed,
    kernel_net_buf_base: () => runtimeState.netBufBase,
    kernel_net_buf_used: () => runtimeState.netBufUsed,
    kernel_sig_slots_ptr: () => runtimeState.sigSlotsPtr,
    kernel_process_pending_ptr: () => runtimeState.processPendingPtr,
    kernel_process_runtime_state_ptr: () => runtimeState.processRuntimeStatePtr,
    kernel_time_state_ptr: () => runtimeState.timeStatePtr,
    kernel_attach_runtime_state: (
      heapBase: number,
      heapPtr: number,
      ofdTablePtr: number,
      memfsPtr: number,
      memStatePtr: number,
      epollTablePtr: number,
      socketTablePtr: number,
      socketUsed: number,
      pipeBase: number,
      pipeUsed: bigint,
      netBufBase: number,
      netBufUsed: bigint,
      sigSlotsPtr: number,
      processPendingPtr: number,
      processRuntimeStatePtr: number,
      timeStatePtr: number,
    ) => {
      runtimeState = {
        heapBase,
        heapPtr,
        ofdTablePtr,
        memfsPtr,
        memStatePtr,
        epollTablePtr,
        socketTablePtr,
        socketUsed,
        pipeBase,
        pipeUsed,
        netBufBase,
        netBufUsed,
        sigSlotsPtr,
        processPendingPtr,
        processRuntimeStatePtr,
        timeStatePtr,
      };
    },
    memfs_resolve_path: (ptr: number, len: number) => {
      const path = normalizePath(decodeGuestPath(ptr, len));
      return entries.get(path)?.ino ?? 0;
    },
    memfs_inode_mode: (ino: number) => {
      const path = byIno.get(ino);
      if (!path) {
        return 0;
      }
      return entries.get(path)?.mode ?? 0;
    },
    memfs_read_link: (ino: number, bufPtr: number, bufLen: number) => {
      const path = byIno.get(ino);
      if (!path) {
        return 0;
      }
      const target = entries.get(path)?.target;
      if (!target || bufLen <= 0) {
        return 0;
      }
      const bytes = new Uint8Array(memory.buffer);
      const len = Math.min(bufLen, target.length);
      bytes.set(target.subarray(0, len), bufPtr);
      return len;
    },
    memfs_ensure_dir: (parentIno: number, namePtr: number, nameLen: number, mode: number) => {
      const parentPath = byIno.get(parentIno);
      if (!parentPath) {
        return 0;
      }
      const name = decodeGuestPath(namePtr, nameLen);
      return ensureDirPath(`${parentPath}/${name}`, mode);
    },
    memfs_create_symlink: (
      parentIno: number,
      namePtr: number,
      nameLen: number,
      targetPtr: number,
      targetLen: number,
    ) => {
      const parentPath = byIno.get(parentIno);
      if (!parentPath) {
        return 0;
      }
      const name = decodeGuestPath(namePtr, nameLen);
      const target = readGuestBytes(targetPtr, targetLen);
      return ensureEntry(`${parentPath}/${name}`, "symlink", 0o120777, target);
    },
    process_install_procfs_snapshot: (targetPtr: number, targetLen: number) => {
      const target = readGuestBytes(targetPtr, targetLen);
      procfsInstallTargets.push(new TextDecoder().decode(target));
      ensurePath("/proc", "dir", 0o040555);
      ensurePath("/proc/self", "dir", 0o040555);
      ensurePath("/proc/self/exe", "symlink", 0o120777, target);
      ensurePath("/proc/stat", "file", 0o100444, new TextEncoder().encode("cpu  1 1 1 1 0 0 0 0 0 0\n"));
      ensurePath("/proc/cpuinfo", "file", 0o100444, new TextEncoder().encode("processor\t: 0\n"));
    },
    ensurePath,
    linkTarget: (path: string) => {
      const target = entries.get(normalizePath(path))?.target;
      return target ? new TextDecoder().decode(target) : null;
    },
    fileContents: (path: string) => {
      const entry = entries.get(normalizePath(path));
      if (!entry || entry.kind !== "file" || !entry.target) {
        return null;
      }
      return new TextDecoder().decode(entry.target);
    },
    procfsInstallTargets,
  };

  return exports as unknown as FakeMemfsExports;
}

function cloneEntries(source: FakeMemfsExports, target: FakeMemfsExports): void {
  target.entries.clear();
  for (const [path, entry] of source.entries.entries()) {
    target.ensurePath(path, entry.kind, entry.mode, entry.target);
  }
}

test("copyCanonicalMemfsToProcess rehydrates /proc/self/exe after canonical memfs replacement", () => {
  const canonicalExports = makeMemfsExports();
  const childExports = makeMemfsExports();
  childExports.ensurePath("/proc", "dir", 0o040555);
  childExports.ensurePath("/proc/self", "dir", 0o040555);
  childExports.ensurePath(
    "/proc/self/exe",
    "symlink",
    0o120777,
    new TextEncoder().encode("/bin/child-a"),
  );

  const fsUtils = createKernelWorkerFsUtils({
    getCanonicalExports: () => canonicalExports,
    getSabPageCache: () => new SharedArrayBuffer(4096),
    processKernels: new Map([[2, childExports]]),
    seedEntries: new Map(),
    encoder: new TextEncoder(),
    decoder: new TextDecoder(),
    copyMemfsState: (sourceExports, _sourceState, targetExports) => {
      cloneEntries(sourceExports as FakeMemfsExports, targetExports as FakeMemfsExports);
    },
  });

  fsUtils.copyCanonicalMemfsToProcess(makeKernelState(), childExports, makeKernelState());

  expect(childExports.linkTarget("/proc/self/exe")).toBe("/bin/child-a");
  expect(childExports.fileContents("/proc/stat")).not.toBeNull();
  expect(childExports.fileContents("/proc/cpuinfo")).not.toBeNull();
  expect(childExports.procfsInstallTargets).toEqual(["/bin/child-a"]);
});

test("fanoutCanonicalMemfs preserves each process /proc/self/exe target while replacing canonical memfs", () => {
  const canonicalExports = makeMemfsExports();
  canonicalExports.ensurePath("/workspace", "dir", 0o040755);
  const processA = makeMemfsExports();
  processA.ensurePath("/proc", "dir", 0o040555);
  processA.ensurePath("/proc/self", "dir", 0o040555);
  processA.ensurePath("/proc/self/exe", "symlink", 0o120777, new TextEncoder().encode("/bin/a"));
  const processB = makeMemfsExports();
  processB.ensurePath("/proc", "dir", 0o040555);
  processB.ensurePath("/proc/self", "dir", 0o040555);
  processB.ensurePath("/proc/self/exe", "symlink", 0o120777, new TextEncoder().encode("/bin/b"));

  const fsUtils = createKernelWorkerFsUtils({
    getCanonicalExports: () => canonicalExports,
    getSabPageCache: () => new SharedArrayBuffer(4096),
    processKernels: new Map([
      [2, processA],
      [3, processB],
    ]),
    seedEntries: new Map(),
    encoder: new TextEncoder(),
    decoder: new TextDecoder(),
    copyMemfsState: (sourceExports, _sourceState, targetExports) => {
      cloneEntries(sourceExports as FakeMemfsExports, targetExports as FakeMemfsExports);
    },
  });

  fsUtils.fanoutCanonicalMemfs();

  expect(processA.linkTarget("/proc/self/exe")).toBe("/bin/a");
  expect(processB.linkTarget("/proc/self/exe")).toBe("/bin/b");
});

test("restoreProcSelfExe does not overwrite an existing proc/self/exe symlink", () => {
  const canonicalExports = makeMemfsExports();
  const targetExports = makeMemfsExports();
  targetExports.ensurePath("/proc", "dir", 0o040555);
  targetExports.ensurePath("/proc/self", "dir", 0o040555);
  targetExports.ensurePath(
    "/proc/self/exe",
    "symlink",
    0o120777,
    new TextEncoder().encode("/bin/existing"),
  );

  const fsUtils = createKernelWorkerFsUtils({
    getCanonicalExports: () => canonicalExports,
    getSabPageCache: () => new SharedArrayBuffer(4096),
    processKernels: new Map(),
    seedEntries: new Map(),
    encoder: new TextEncoder(),
    decoder: new TextDecoder(),
    copyMemfsState: () => {},
  });

  fsUtils.restoreProcSelfExe(targetExports, new TextEncoder().encode("/bin/new-target"));

  expect(targetExports.linkTarget("/proc/self/exe")).toBe("/bin/existing");
});
