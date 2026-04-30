import type { KernelRuntimeState } from "../abi.js";
import type { FsSnapshotEntry } from "../fs/types.js";
import type {
  KernelWorkerInbound,
  KernelWorkerFdEntrySnapshot,
  KernelWorkerFileLayerEntry,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerOutbound,
  KernelWorkerPidStateSnapshot,
  KernelWorkerSocketStateSnapshot,
} from "../messages.js";
import { cloneSnapshotEntry } from "./common.js";

interface ProcessFsResult {
  kernelState: KernelRuntimeState;
  memfsMutated: boolean;
  ofdStateChanged: boolean;
}

interface KernelWorkerMessageHandlerDeps {
  postResponse(msg: KernelWorkerOutbound): void;
  initKernel(
    wasmModule: WebAssembly.Module,
    sabPageCache: SharedArrayBuffer,
    maxMemoryPages?: number,
  ): Promise<KernelRuntimeState>;
  writeFile(path: string, data: Uint8Array, mode?: number, fanout?: boolean): void;
  writeFiles(entries: Array<{ path: string; data: Uint8Array; mode?: number }>): void;
  applyFileLayer(entries: KernelWorkerFileLayerEntry[]): void;
  symlink(path: string, target: Uint8Array, mode?: number, fanout?: boolean): void;
  readFile(path: string): Uint8Array;
  readlink(path: string): Uint8Array;
  mkdir(path: string, mode?: number, fanout?: boolean): void;
  statPath(path: string): {
    ino: number;
    mode: number;
    nlink: number;
    uid: number;
    gid: number;
    size: number;
  };
  readdir(path: string): string[];
  snapshotCanonicalFs(includeData: boolean): FsSnapshotEntry[];
  loadFsSnapshot(entries: FsSnapshotEntry[]): void;
  registerProcess(
    pid: number,
    vmPtr: number,
    memory: WebAssembly.Memory,
    kernelState: KernelRuntimeState,
    seedMemfsMode?: "full" | "metadata" | "none",
    pipeSeedMode?: "canonical" | "process",
  ): Promise<KernelRuntimeState>;
  unregisterProcess(pid: number): void;
  processFs(pid: number, vmPtr: number, kernelState: KernelRuntimeState): ProcessFsResult;
  recordChildExit(
    pid: number,
    childPid: number,
    waitStatus: number,
    kernelState: KernelRuntimeState,
  ): KernelRuntimeState;
  resumeConnect(
    pid: number,
    vmPtr: number,
    result: bigint,
    kernelState: KernelRuntimeState,
  ): KernelRuntimeState;
  resumeWait4(pid: number, vmPtr: number, kernelState: KernelRuntimeState): KernelRuntimeState;
  resumeBlockedSyscall(pid: number, vmPtr: number, kernelState: KernelRuntimeState): KernelRuntimeState;
  resumeVforkParent(
    pid: number,
    vmPtr: number,
    waitStatus: number,
    kernelState: KernelRuntimeState,
  ): { childPid: number; kernelState: KernelRuntimeState };
  canonicalizeChildPid(
    pid: number,
    vmPtr: number,
    provisionalChildPid: number,
    kernelState: KernelRuntimeState,
  ): { childPid: number; nextPid: number; pgid: number; sid: number; kernelState: KernelRuntimeState };
  initializeProcessLifecycle(
    pid: number,
    vmPtr: number,
    ppid: number,
    nextPid: number,
    pgid: number,
    sid: number,
    didExec: boolean,
    kernelState: KernelRuntimeState,
  ): KernelRuntimeState;
  resumeVforkExecveParent(
    pid: number,
    vmPtr: number,
    childPid: number,
    kernelState: KernelRuntimeState,
  ): KernelRuntimeState;
  syncProcessLifecycle(pid: number, vmPtr: number, kernelState: KernelRuntimeState): KernelRuntimeState;
  syncProcessMemfs(pid: number, kernelState: KernelRuntimeState): KernelRuntimeState;
  syncPipeState(
    pid: number,
    kernelState: KernelRuntimeState,
    fdEntries?: KernelWorkerFdEntrySnapshot[],
    ofdSlots?: KernelWorkerOfdSlotSnapshot[],
    vmPtrOverride?: number,
  ): KernelRuntimeState;
  syncEpollState(pid: number, kernelState: KernelRuntimeState): KernelRuntimeState;
  syncOfdState(pid: number, ofdIds: number[], kernelState: KernelRuntimeState): KernelRuntimeState;
  syncSocketState(pid: number, kernelState: KernelRuntimeState): KernelRuntimeState;
  applySocketStateSnapshot(pid: number, kernelState: KernelRuntimeState, snapshot: KernelWorkerSocketStateSnapshot): KernelRuntimeState;
  snapshotSocketState(pid: number, kernelState: KernelRuntimeState): KernelWorkerSocketStateSnapshot | undefined;
  syncForkSharedState(
    pid: number,
    vmPtr: number,
    targetPids: number[],
    targetKernelStates: KernelWorkerPidStateSnapshot[],
    ofdIds: number[],
    ofdSlots: Array<{ ofdId: number; slot: Uint8Array }>,
    fdEntries: KernelWorkerFdEntrySnapshot[],
    includeMemfs: boolean,
    preserveSourceRefcounts: boolean,
    kernelState: KernelRuntimeState,
  ): KernelRuntimeState;
  rehydrateForkSharedState(
    targetPids: number[],
    targetKernelStates: KernelWorkerPidStateSnapshot[],
    ofdIds: number[],
    procSelfExeTargets?: Array<{ pid: number; bytes: Uint8Array }>,
    includeMemfs?: boolean,
  ): void;
  debugProcesses(): { pids: number[]; canonicalPids: number[] };
  debugPageCachePath(path: string): {
    path: string;
    ino: number;
    dataOff: number;
    size: number;
    mode: number;
  } | null;
  debugPageCacheStats(): {
    byteLength: number;
    dataUsed: number;
    inodeCount: number;
    dirEpoch: number;
  };
  releaseHostStdio(pid: number, vmPtr: number, kernelState: KernelRuntimeState): KernelRuntimeState;
}

export function createKernelWorkerMessageHandler(deps: KernelWorkerMessageHandlerDeps) {
  return async function handleKernelWorkerMessage(msg: KernelWorkerInbound): Promise<void> {
    try {
      switch (msg.type) {
        case "init-kernel": {
          const kernelState = await deps.initKernel(
            msg.wasmModule,
            msg.sabPageCache,
            msg.maxMemoryPages,
          );
          deps.postResponse({ type: "ready", requestId: msg.requestId, kernelState });
          break;
        }
        case "write-file":
          deps.writeFile(msg.path, msg.data, msg.mode);
          deps.postResponse({ type: "file-written", requestId: msg.requestId });
          break;
        case "write-files":
          deps.writeFiles(msg.entries);
          deps.postResponse({ type: "files-written", requestId: msg.requestId });
          break;
        case "apply-file-layer":
          deps.applyFileLayer(msg.entries);
          deps.postResponse({ type: "file-layer-applied", requestId: msg.requestId });
          break;
        case "symlink":
          deps.symlink(msg.path, msg.target, msg.mode);
          deps.postResponse({ type: "symlink-created", requestId: msg.requestId });
          break;
        case "read-file":
          deps.postResponse({
            type: "file-read",
            requestId: msg.requestId,
            data: deps.readFile(msg.path),
          });
          break;
        case "readlink":
          deps.postResponse({
            type: "readlink-result",
            requestId: msg.requestId,
            target: deps.readlink(msg.path),
          });
          break;
        case "mkdir":
          deps.mkdir(msg.path, msg.mode ?? 0o755);
          deps.postResponse({ type: "dir-created", requestId: msg.requestId });
          break;
        case "stat":
          deps.postResponse({
            type: "stat-result",
            requestId: msg.requestId,
            stat: deps.statPath(msg.path),
          });
          break;
        case "readdir":
          deps.postResponse({
            type: "readdir-result",
            requestId: msg.requestId,
            entries: deps.readdir(msg.path),
          });
          break;
        case "snapshot-fs":
          deps.postResponse({
            type: "snapshot-fs-result",
            requestId: msg.requestId,
            entries: deps.snapshotCanonicalFs(msg.includeData ?? false).map(cloneSnapshotEntry),
          });
          break;
        case "load-fs-snapshot":
          deps.loadFsSnapshot(msg.entries);
          deps.postResponse({ type: "fs-loaded", requestId: msg.requestId });
          break;
        case "register-process": {
          const memory = msg.memory;
          const kernelState = await deps.registerProcess(
            msg.pid,
            msg.vmPtr,
            memory,
            msg.kernelState,
            msg.seedMemfsMode ?? "full",
            msg.pipeSeedMode ?? "canonical",
          );
          deps.postResponse({
            type: "process-registered",
            requestId: msg.requestId,
            kernelState,
          });
          break;
        }
        case "unregister-process":
          deps.unregisterProcess(msg.pid);
          deps.postResponse({ type: "process-unregistered", requestId: msg.requestId });
          break;
        case "process-fs": {
          const result = deps.processFs(msg.pid, msg.vmPtr, msg.kernelState);
          deps.postResponse({
            type: "fs-processed",
            requestId: msg.requestId,
            kernelState: result.kernelState,
            memfsMutated: result.memfsMutated,
            ofdStateChanged: result.ofdStateChanged,
          });
          break;
        }
        case "record-child-exit":
          deps.postResponse({
            type: "child-exit-recorded",
            requestId: msg.requestId,
            kernelState: deps.recordChildExit(msg.pid, msg.childPid, msg.waitStatus, msg.kernelState),
          });
          break;
        case "resume-connect":
          deps.postResponse({
            type: "connect-resumed",
            requestId: msg.requestId,
            kernelState: deps.resumeConnect(msg.pid, msg.vmPtr, msg.result, msg.kernelState),
          });
          break;
        case "resume-wait4":
          deps.postResponse({
            type: "wait4-resumed",
            requestId: msg.requestId,
            kernelState: deps.resumeWait4(msg.pid, msg.vmPtr, msg.kernelState),
          });
          break;
        case "resume-blocked-syscall":
          deps.postResponse({
            type: "blocked-syscall-resumed",
            requestId: msg.requestId,
            kernelState: deps.resumeBlockedSyscall(msg.pid, msg.vmPtr, msg.kernelState),
          });
          break;
        case "resume-vfork-parent": {
          const { childPid, kernelState } = deps.resumeVforkParent(
            msg.pid,
            msg.vmPtr,
            msg.waitStatus,
            msg.kernelState,
          );
          deps.postResponse({
            type: "vfork-parent-resumed",
            requestId: msg.requestId,
            childPid,
            kernelState,
          });
          break;
        }
        case "canonicalize-child-pid": {
          const { childPid, nextPid, pgid, sid, kernelState } = deps.canonicalizeChildPid(
            msg.pid,
            msg.vmPtr,
            msg.provisionalChildPid,
            msg.kernelState,
          );
          deps.postResponse({
            type: "child-pid-canonicalized",
            requestId: msg.requestId,
            childPid,
            nextPid,
            pgid,
            sid,
            kernelState,
          });
          break;
        }
        case "initialize-process-lifecycle":
          deps.postResponse({
            type: "process-lifecycle-initialized",
            requestId: msg.requestId,
            kernelState: deps.initializeProcessLifecycle(
              msg.pid,
              msg.vmPtr,
              msg.ppid,
              msg.nextPid,
              msg.pgid,
              msg.sid,
              msg.didExec,
              msg.kernelState,
            ),
          });
          break;
        case "resume-vfork-execve-parent":
          deps.postResponse({
            type: "vfork-execve-parent-resumed",
            requestId: msg.requestId,
            kernelState: deps.resumeVforkExecveParent(msg.pid, msg.vmPtr, msg.childPid, msg.kernelState),
          });
          break;
        case "sync-process-lifecycle":
          deps.postResponse({
            type: "process-lifecycle-synced",
            requestId: msg.requestId,
            kernelState: deps.syncProcessLifecycle(msg.pid, msg.vmPtr, msg.kernelState),
          });
          break;
        case "sync-process-memfs":
          deps.postResponse({
            type: "process-memfs-synced",
            requestId: msg.requestId,
            kernelState: deps.syncProcessMemfs(msg.pid, msg.kernelState),
          });
          break;
        case "sync-pipe-state":
          deps.postResponse({
            type: "pipe-state-synced",
            requestId: msg.requestId,
            kernelState: deps.syncPipeState(msg.pid, msg.kernelState, msg.fdEntries, msg.ofdSlots, msg.vmPtr),
          });
          break;
        case "sync-epoll-state":
          deps.postResponse({
            type: "epoll-state-synced",
            requestId: msg.requestId,
            kernelState: deps.syncEpollState(msg.pid, msg.kernelState),
          });
          break;
        case "sync-ofd-state":
          deps.postResponse({
            type: "ofd-state-synced",
            requestId: msg.requestId,
            kernelState: deps.syncOfdState(msg.pid, msg.ofdIds, msg.kernelState),
          });
          break;
        case "sync-socket-state":
          {
            const inputState = msg.socketState
              ? deps.applySocketStateSnapshot(msg.pid, msg.kernelState, msg.socketState)
              : msg.kernelState;
            const kernelState = deps.syncSocketState(msg.pid, inputState);
            deps.postResponse({
              type: "socket-state-synced",
              requestId: msg.requestId,
              kernelState,
              socketState: deps.snapshotSocketState(msg.pid, kernelState),
            });
          }
          break;
        case "sync-fork-shared-state":
          deps.postResponse({
            type: "fork-shared-state-synced",
            requestId: msg.requestId,
            kernelState: deps.syncForkSharedState(
              msg.pid,
              msg.vmPtr,
              msg.targetPids,
              msg.targetKernelStates,
              msg.ofdIds,
              msg.ofdSlots,
              msg.fdEntries,
              msg.includeMemfs ?? true,
              msg.preserveSourceRefcounts ?? false,
              msg.kernelState,
            ),
          });
          break;
        case "rehydrate-fork-shared-state":
          deps.rehydrateForkSharedState(
            msg.targetPids,
            msg.targetKernelStates,
            msg.ofdIds,
            msg.procSelfExeTargets ?? [],
            msg.includeMemfs ?? true,
          );
          deps.postResponse({
            type: "fork-shared-state-rehydrated",
            requestId: msg.requestId,
          });
          break;
        case "debug-processes": {
          const { pids, canonicalPids } = deps.debugProcesses();
          deps.postResponse({
            type: "processes-debugged",
            requestId: msg.requestId,
            pids,
            canonicalPids,
          });
          break;
        }
        case "debug-page-cache-path":
          deps.postResponse({
            type: "page-cache-path-debugged",
            requestId: msg.requestId,
            record: deps.debugPageCachePath(msg.path),
          });
          break;
        case "debug-page-cache-stats":
          deps.postResponse({
            type: "page-cache-stats-debugged",
            requestId: msg.requestId,
            stats: deps.debugPageCacheStats(),
          });
          break;
        case "release-host-stdio":
          deps.postResponse({
            type: "host-stdio-released",
            requestId: msg.requestId,
            kernelState: deps.releaseHostStdio(msg.pid, msg.vmPtr, msg.kernelState),
          });
          break;
      }
    } catch (error) {
      deps.postResponse({
        type: "error",
        requestId: msg.requestId,
        message: error instanceof Error ? (error.stack ?? error.message) : String(error),
      });
    }
  };
}
