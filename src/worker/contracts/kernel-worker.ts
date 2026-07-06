import type { KernelRuntimeState } from "../../abi.js";
import type { FsSnapshotEntry, RuntimeStat } from "../../fs/types.js";
import type {
  GuestMemoryWriteSnapshot,
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPidBytesSnapshot,
  KernelWorkerPidStateSnapshot,
  KernelWorkerPipeSlotSnapshot,
  KernelWorkerSocketStateSnapshot,
} from "./snapshots.js";

export interface KernelWorkerRequestBase {
  requestId: number;
}

export type KernelWorkerFileLayerEntry =
  | { kind: "dir"; path: string; mode?: number }
  | { kind: "file"; path: string; data: Uint8Array; mode?: number }
  | { kind: "symlink"; path: string; target: Uint8Array; mode?: number };

export type KernelWorkerRequest =
  | {
      type: "init-kernel";
      wasmModule: WebAssembly.Module;
      sabPageCache: SharedArrayBuffer;
      maxMemoryPages: number;
    }
  | {
      type: "write-file";
      path: string;
      data: Uint8Array;
      mode?: number;
    }
  | {
      type: "write-files";
      entries: Array<{
        path: string;
        data: Uint8Array;
        mode?: number;
      }>;
    }
  | {
      type: "apply-file-layer";
      entries: KernelWorkerFileLayerEntry[];
    }
  | {
      type: "symlink";
      path: string;
      target: Uint8Array;
      mode?: number;
    }
  | {
      type: "read-file";
      path: string;
    }
  | {
      type: "read-file-range-by-inode";
      ino: number;
      offset: number;
      length: number;
      expectedSize: number;
    }
  | {
      type: "readlink";
      path: string;
    }
  | {
      type: "mkdir";
      path: string;
      mode?: number;
    }
  | {
      type: "stat";
      path: string;
    }
  | {
      type: "readdir";
      path: string;
    }
  | {
      type: "snapshot-fs";
      includeData?: boolean;
    }
  | {
      type: "load-fs-snapshot";
      entries: FsSnapshotEntry[];
    }
  | {
      type: "register-fs-backing-source";
      sourceIdentity: string;
      data: Uint8Array;
    }
  | {
      type: "register-fs-backing-source-range";
      sourceIdentity: string;
      byteLength: number;
      offset: number;
      data: Uint8Array;
    }
  | {
      type: "register-fs-remote-backing-source";
      sourceIdentity: string;
      byteLength: number;
      url: string;
    }
  | {
      type: "register-fs-opfs-backing-source";
      sourceIdentity: string;
      byteLength: number;
      handle: { getFile(): Promise<Blob> };
    }
  | {
      type: "register-fs-host-file-backing-source";
      sourceIdentity: string;
      hostPath: string;
      byteLength: number;
    }
  | {
      type: "register-fs-content-addressed-backing-source";
      sourceIdentity: string;
      digest: string;
      data: Uint8Array;
    }
  | {
      type: "register-process";
      pid: number;
      vmPtr: number;
      memory: WebAssembly.Memory;
      kernelState: KernelRuntimeState;
      seedMemfsMode?: "full" | "metadata" | "none";
      pipeSeedMode?: "canonical" | "process";
    }
  | {
      type: "unregister-process";
      pid: number;
    }
  | {
      type: "process-fs";
      pid: number;
      vmPtr: number;
      kernelState: KernelRuntimeState;
      debugProgress?: boolean;
    }
  | {
      type: "record-child-exit";
      pid: number;
      childPid: number;
      waitStatus: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "resume-connect";
      pid: number;
      vmPtr: number;
      result: bigint;
      connectFd?: number;
      fdEntries?: KernelWorkerFdEntrySnapshot[];
      ofdSlots?: KernelWorkerOfdSlotSnapshot[];
      kernelState: KernelRuntimeState;
    }
  | {
      type: "resume-wait4";
      pid: number;
      vmPtr: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "resume-vfork-parent";
      pid: number;
      vmPtr: number;
      waitStatus: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "canonicalize-child-pid";
      pid: number;
      vmPtr: number;
      provisionalChildPid: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "initialize-process-lifecycle";
      pid: number;
      vmPtr: number;
      ppid: number;
      nextPid: number;
      pgid: number;
      sid: number;
      didExec: boolean;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "resume-vfork-execve-parent";
      pid: number;
      vmPtr: number;
      childPid: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "rollback-vfork-execve-handoff";
      pid: number;
      vmPtr: number;
      childPid: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "sync-process-lifecycle";
      pid: number;
      vmPtr: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "sync-process-memfs";
      pid: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "sync-pipe-state";
      pid: number;
      vmPtr: number;
      fdEntries: KernelWorkerFdEntrySnapshot[];
      applyFdEntries?: boolean;
      ofdSlots?: KernelWorkerOfdSlotSnapshot[];
      pipeSlots?: KernelWorkerPipeSlotSnapshot[];
      kernelState: KernelRuntimeState;
    }
  | {
      type: "sync-epoll-state";
      pid: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "sync-ofd-state";
      pid: number;
      ofdIds: number[];
      mode?: "push" | "pull";
      kernelState: KernelRuntimeState;
    }
  | {
      type: "sync-socket-state";
      pid: number;
      kernelState: KernelRuntimeState;
      socketState?: KernelWorkerSocketStateSnapshot;
    }
  | {
      type: "sync-fork-shared-state";
      pid: number;
      vmPtr: number;
      targetPids: number[];
      targetKernelStates: KernelWorkerPidStateSnapshot[];
      ofdIds: number[];
      ofdSlots: KernelWorkerOfdSlotSnapshot[];
      fdEntries: KernelWorkerFdEntrySnapshot[];
      includeMemfs?: boolean;
      memfsOnly?: boolean;
      preserveSourceRefcounts?: boolean;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "rehydrate-fork-shared-state";
      targetPids: number[];
      targetKernelStates: KernelWorkerPidStateSnapshot[];
      ofdIds: number[];
      procSelfExeTargets?: KernelWorkerPidBytesSnapshot[];
      includeMemfs?: boolean;
    }
  | {
      type: "debug-processes";
    }
  | {
      type: "debug-page-cache-path";
      path: string;
    }
  | {
      type: "debug-evict-page-cache-path";
      path: string;
      pageIndex: number;
    }
  | {
      type: "debug-page-cache-stats";
    }
  | {
      type: "release-host-stdio";
      pid: number;
      vmPtr: number;
      kernelState: KernelRuntimeState;
    };

export type KernelWorkerInbound = KernelWorkerRequestBase & KernelWorkerRequest;

export type KernelWorkerResponse =
  | {
      type: "ready";
      requestId: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "file-written";
      requestId: number;
    }
  | {
      type: "files-written";
      requestId: number;
    }
  | {
      type: "file-layer-applied";
      requestId: number;
    }
  | {
      type: "symlink-created";
      requestId: number;
    }
  | {
      type: "file-read";
      requestId: number;
      data: Uint8Array;
    }
  | {
      type: "file-range-read";
      requestId: number;
      data: Uint8Array;
    }
  | {
      type: "readlink-result";
      requestId: number;
      target: Uint8Array;
    }
  | {
      type: "dir-created";
      requestId: number;
    }
  | {
      type: "stat-result";
      requestId: number;
      stat: RuntimeStat;
    }
  | {
      type: "readdir-result";
      requestId: number;
      entries: string[];
    }
  | {
      type: "snapshot-fs-result";
      requestId: number;
      entries: FsSnapshotEntry[];
    }
  | {
      type: "fs-loaded";
      requestId: number;
    }
  | {
      type: "fs-backing-source-registered";
      requestId: number;
    }
  | {
      type: "process-registered";
      requestId: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "process-unregistered";
      requestId: number;
    }
  | {
      type: "fs-processed";
      requestId: number;
      kernelState: KernelRuntimeState;
      result: bigint;
      memfsMutated: boolean;
      ofdStateChanged: boolean;
      fdEntries?: KernelWorkerFdEntrySnapshot[];
      ofdSlots?: KernelWorkerOfdSlotSnapshot[];
      guestMemoryWrites?: GuestMemoryWriteSnapshot[];
      fsDebug?: string;
    }
  | {
      type: "child-exit-recorded";
      requestId: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "connect-resumed";
      requestId: number;
      kernelState: KernelRuntimeState;
      socketState?: KernelWorkerSocketStateSnapshot;
    }
  | {
      type: "wait4-resumed";
      requestId: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "vfork-parent-resumed";
      requestId: number;
      childPid: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "child-pid-canonicalized";
      requestId: number;
      childPid: number;
      ppid?: number;
      nextPid: number;
      pgid: number;
      sid: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "process-lifecycle-initialized";
      requestId: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "vfork-execve-parent-resumed";
      requestId: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "vfork-execve-handoff-rolled-back";
      requestId: number;
      waitStatus: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "process-lifecycle-synced";
      requestId: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "process-memfs-synced";
      requestId: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "pipe-state-synced";
      requestId: number;
      kernelState: KernelRuntimeState;
      pipeSlots?: KernelWorkerPipeSlotSnapshot[];
    }
  | {
      type: "epoll-state-synced";
      requestId: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "ofd-state-synced";
      requestId: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "socket-state-synced";
      requestId: number;
      kernelState: KernelRuntimeState;
      socketState?: KernelWorkerSocketStateSnapshot;
    }
  | {
      type: "fork-shared-state-synced";
      requestId: number;
      kernelState: KernelRuntimeState;
      forkSharedDebug?: string;
    }
  | {
      type: "fork-shared-state-rehydrated";
      requestId: number;
    }
  | {
      type: "processes-debugged";
      requestId: number;
      pids: number[];
      canonicalPids: number[];
    }
  | {
      type: "page-cache-path-debugged";
      requestId: number;
      record: {
        path: string;
        ino: number;
        dataOff: number;
        size: number;
        mode: number;
        backingBytes?: number;
        residentPages?: number;
      } | null;
    }
  | {
      type: "page-cache-path-evicted";
      requestId: number;
      result: {
        path: string;
        ino: number;
        pageIndex: number;
        status: "evicted" | "dirty-pinned" | "not-found";
        residentPages?: number;
      } | null;
    }
  | {
      type: "page-cache-stats-debugged";
      requestId: number;
      stats: {
        byteLength: number;
        dataUsed: number;
        inodeCount: number;
        dirEpoch: number;
      };
    }
  | {
      type: "host-stdio-released";
      requestId: number;
      kernelState: KernelRuntimeState;
    }
  | {
      type: "error";
      requestId: number;
      message: string;
    };

export type KernelWorkerProcessFsProgress = {
  type: "process-fs-progress";
  requestId: number;
  pid: number;
  vmPtr: number;
  phase: string;
  elapsedMs: number;
  detail?: string;
};

export type KernelWorkerOutbound = KernelWorkerResponse | KernelWorkerProcessFsProgress;
