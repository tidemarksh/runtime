import {
  hostExecutionState,
  markRegularFileFsInFlight,
} from "../host-execution-state.js";
import { processHasKernelWorkerOwnership } from "../kernel-worker-ownership.js";
import { processHasWorker, processWorkers } from "../process-worker-state.js";
import { guestWriteWatchRegions } from "../guest-write-watch-state.js";
import { workerDebugLog } from "../worker-debug-log.js";
import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../abi.js";
import {
  collectProcessHeldOfds,
  hasPlausibleOfdState,
} from "../ownership/ofd-table.js";
import {
  applyThreadFdEntries,
  canonicalFdTableVmPtrForThread,
  supervisorProcessFdFlagsForFd,
  supervisorProcessOfdForFd,
} from "../ownership/fd-table.js";
import {
  supervisorOfdFdType,
  supervisorOfdHostFd,
  supervisorOfdIno,
  supervisorOfdOffset,
  supervisorOfdStatusFlags,
} from "../ownership/ofd-metadata.js";
import { applyThreadOfdSlots } from "../ownership/ofd-slot.js";
import {
  appendPendingGuestMemoryWrites,
  formatGuestMemoryWriteWatchOverlap,
} from "../guest-memory-writes.js";
import { ensureKernelMemoryCapacity, kernelScratchBase } from "../kernel-scratch.js";
import { adoptKernelState, adoptKernelStateForHandle } from "../kernel-state.js";
import {
  forgetMemfsDeletedPath,
  reapplyMemfsDeletedPaths,
  rememberMemfsDeletedPath,
  removeLocalMemfsPath,
  resolveLocalMemfsPath,
} from "../memfs-tombstones.js";
import {
  isProcessMemfsDirty,
  rememberProcessMemfsChangedIno,
  rememberProcessMemfsChangedPath,
  setProcessMemfsDirty,
  setProcessMemfsPublishPending,
} from "../process-memfs-state.js";
import { kernelShareHasSharedOfdId } from "../kernel-share.js";
import { processExitInProgress } from "../process-lifecycle-state.js";
import {
  type ProcessHandle,
  type ThreadWorkerState,
} from "../types.js";
import { transferDebugStats } from "../worker-debug-stats.js";
import {
  clearThreadOwnerAsyncStatus,
  getThreadOwnerAsyncStatus,
  getThreadWaitingInWorkerStatus,
  setThreadOwnerAsyncStatus,
} from "../thread-host-state.js";
import { maybeThreadActivityState, getThreadActivityState } from "../thread-activity-state.js";
import {
  appendPendingAuthoritativeFdForNextStep,
  getPendingKernelStateForNextStep,
  setPendingKernelStateForNextStep,
} from "../thread-step-payload.js";
import type {
  GuestMemoryWriteSnapshot,
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerRegularFileFsSnapshot,
  KernelWorkerPidBytesSnapshot,
} from "../../messages.js";
import type { SyncCause } from "../contracts/sync.js";
import { FS_EFFECT_SHARED_OFD_STATE } from "../../layout/fs-effects.js";
import {
  FS_OP_CLOSE,
  FS_OP_EXECVE,
  FS_OP_FSYNC,
  FS_OP_FSTAT,
  FS_OP_GETDENTS,
  FS_OP_LSEEK,
  FS_OP_MKDIR,
  FS_OP_MMAP,
  FS_OP_OPENAT,
  FS_OP_READ,
  FS_OP_RENAME,
  FS_OP_TRUNCATE,
  FS_OP_UNLINK,
  FS_OP_WRITE,
  FS_RING_COMPLETE_HEAD_OFFSET,
  FS_RING_COMPLETE_TAIL_OFFSET,
  FS_RING_REQUEST_FD_OFFSET,
  FS_RING_REQUEST_FLAGS_OFFSET,
  FS_RING_REQUEST_OPCODE_OFFSET,
  FS_RING_REQUEST_OFFSET_OFFSET,
  FS_RING_REQUEST_MODE_OFFSET,
  FS_RING_REQUEST_OFD_ID_OFFSET,
  FS_RING_REQUEST_INO_OFFSET,
  FS_RING_REQUEST_BUF_PTR_OFFSET,
  FS_RING_REQUEST_PATH_PTR_OFFSET,
  FS_RING_REQUEST_LEN_OFFSET,
  FS_RING_REQUEST_VM_PTR_OFFSET,
  FS_RING_RESPONSE_RESULT_OFFSET,
  FS_RING_RESPONSE_VM_PTR_OFFSET,
  FS_RING_SUBMIT_HEAD_OFFSET,
  FS_RING_SUBMIT_TAIL_OFFSET,
  FS_RING_TOTAL_BYTES,
  fsRingCompletionEntryOffset,
  fsRingRequestEntryOffset,
} from "../../layout/fs-ring.js";
import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_OFFSET_OFFSET,
  OFD_STATUS_FLAGS_OFFSET,
  OFD_TYPE_OFFSET,
} from "../../layout/ofd.js";
import {
  completedKernelRpcOutcome,
  type KernelRpcResumeOutcome,
} from "../kernel-rpc/outcomes.js";
import { submitSupervisorShadowStatusEvent } from "../supervisor-shadow.js";
import { supervisorProcessStatusIsExecveSpawn } from "../supervisor-status.js";
import { requireKernelWorkerOwnership } from "../kernel-worker-ownership.js";
import { expectKernelWorkerResponse } from "../kernel-rpc/transport.js";

const FS_DEBUG_PATH_BUF_LEN = 4096;
const FS_LIFECYCLE_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FS_LIFECYCLE_DEBUG ?? "") === "1";
const FS_PROCESS_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FS_PROCESS_DEBUG ?? "") === "1";
const FS_FD_STATE_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FS_FD_STATE_DEBUG ?? "") === "1";
const FS_CREATED_FD_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FS_CREATED_FD_DEBUG ?? "") === "1";
const FS_TRACE_INO = Number((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FS_TRACE_INO ?? "0");
const FS_TRACE_STALE_READ = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FS_TRACE_STALE_READ ?? "") === "1";
const FS_TRACE_FAILED_WRITE = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FS_TRACE_FAILED_WRITE ?? "") === "1";
const FS_TRACE_WRITE_RESUME = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FS_TRACE_WRITE_RESUME ?? "") === "1";
const FS_FD_STATE_DETAIL_ENABLED = FS_LIFECYCLE_DEBUG_ENABLED || FS_FD_STATE_DEBUG_ENABLED;
const MAX_CHAINED_FS_RESUME_ATTEMPTS = 64;

function supervisorNumber(value: bigint | number | undefined, name: string): number {
  const numeric = typeof value === "bigint" ? Number(value) : value;
  if (typeof numeric !== "number" || !Number.isSafeInteger(numeric)) {
    throw new RangeError(`invalid ${name} supervisor export value: ${String(value)}`);
  }
  return numeric;
}

function supervisorNumericExport(handle: ProcessHandle, exportName: string): number {
  const read = (handle.kernel as unknown as Record<string, unknown>)[exportName];
  if (typeof read !== "function") {
    throw new Error(`missing ${exportName} export`);
  }
  return supervisorNumber((read as () => bigint | number).call(handle.kernel), exportName);
}

interface PendingFsContext {
  opcode: number;
  effects: number;
  targetIno: number;
  fd: number;
  flags: number;
  path: string | null;
  secondaryPath: string | null;
}

interface FsFdStateSnapshot {
  vmPtr: number;
  fdEntries: Array<{ fd: number; ofdId: number; fdFlags: number; generation?: number }>;
  ofdTablePtr: number;
  ofdTable: Uint8Array | null;
}

type FsFdStateSnapshotScope =
  | { kind: "none" }
  | { kind: "fds"; fds: readonly number[] }
  | { kind: "all" };

interface FilteredFsFdState {
  fdEntries?: KernelWorkerFdEntrySnapshot[];
  ofdSlots?: KernelWorkerOfdSlotSnapshot[];
  skippedFdEntries: number;
  skippedOfdSlots: number;
}

interface FsRingProgressSnapshot {
  vmPtr: number;
  ringBase: number;
  submitHead: number;
  submitTail: number;
  completeHead: number;
  completeTail: number;
}

interface RegularFileIdentity {
  fileType: number;
  ino: number;
  hostFd: number;
  statusFlags: number;
  offset: bigint;
}

interface PendingRegularFileFsRequest {
  fd: number;
  ofdId: number;
  ringOfdId: number;
  ino: number;
  requestOffset: bigint | null;
  generation?: number;
  identity: RegularFileIdentity | null;
}

interface FsGuestMemoryWriteOptions {
  queueThreadWorkerWrites?: boolean;
}

interface SupervisorFsCompletionDecision {
  memfsMutated: boolean;
  memfsPublishReady: boolean;
  ofdStateChanged: boolean;
  regularFileRequest: boolean;
}

interface SupervisorFsResponseDisposition {
  adoptKernelState: boolean;
  applyGuestMemoryWrites: boolean;
  queueThreadWorkerWrites: boolean;
  applyLocalMetadata: boolean;
  clearTombstones: boolean;
  fdResponseMayClearLiveFds: boolean;
  storePendingKernelState: boolean;
  resolvedToExecveSpawn: boolean;
  processOwnerMatches: boolean;
}

interface SupervisorFsLocalMetadataActions {
  tombstonePrimary: boolean;
  forgetPrimary: boolean;
  forgetSecondary: boolean;
}

export interface WorkerKernelRpcFsDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  assertProcessStandardFdIntegrity(handle: ProcessHandle, context: string): void;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  handleExecveSpawn?(handle: ProcessHandle, worker: Worker, thread: ThreadWorkerState): Promise<void>;
  markRuntimeFsDirty(handle: ProcessHandle): void;
  notifyHostEffectProgress(handle: ProcessHandle): void;
  kernelShareOtherMembers(handle: ProcessHandle): ProcessHandle[];
  syncDirtyMemfsPeerViaKernelWorker(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options: {
      cause: SyncCause;
      includeMemfs?: boolean;
      trackHeldOfds?: boolean;
      deltaOfdIds?: boolean;
      memfsOnly?: boolean;
      preserveSourceRefcounts?: boolean;
      sourceVmPtr?: number;
      ofdIds?: number[];
    },
  ): Promise<void>;
  syncSharedOfdStateAfterFs(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options: { sourceVmPtr: number },
  ): Promise<void>;
  rehydrateForkSharedStateViaKernelWorker(
    targets: ProcessHandle[],
    ofdIds: number[],
    options: { cause: SyncCause; procSelfExeTargets?: KernelWorkerPidBytesSnapshot[]; includeMemfs?: boolean },
  ): Promise<void>;
  publishRuntimeKernelState(handle: ProcessHandle): void;
}

export function createWorkerKernelRpcFsHelpers(deps: WorkerKernelRpcFsDeps) {
  const fsResumeInFlight = new WeakMap<ProcessHandle, Map<string, Promise<KernelRpcResumeOutcome>>>();
  const fsResumeProcessTails = new WeakMap<ProcessHandle, Promise<void>>();
  const decoder = new TextDecoder();

function decodeKernelBytes(memory: WebAssembly.Memory, ptr: number, length: number): string {
  const bytes = new Uint8Array(length);
  bytes.set(new Uint8Array(memory.buffer, ptr, length));
  return decoder.decode(bytes);
}

function readPendingFsPath(handle: ProcessHandle, vmPtr: number, secondary = false): string | null {
  const absolutePath = secondary
    ? handle.kernel.thread_pending_fs_secondary_absolute_path
    : handle.kernel.thread_pending_fs_absolute_path;
  if (!absolutePath) {
    return null;
  }
  const bufPtr = kernelScratchBase(handle.kernel) + 0x8000 + (secondary ? FS_DEBUG_PATH_BUF_LEN : 0);
  ensureKernelMemoryCapacity(handle.kernel, bufPtr + FS_DEBUG_PATH_BUF_LEN);
  const length = Number(absolutePath(vmPtr, bufPtr, FS_DEBUG_PATH_BUF_LEN) ?? 0n);
  if (length <= 0 || bufPtr + length > handle.kernel.memory.buffer.byteLength) {
    return null;
  }
  return decodeKernelBytes(handle.kernel.memory, bufPtr, length);
}

function readPendingFsContext(handle: ProcessHandle, vmPtr: number): PendingFsContext {
  const request = readPendingFsRingRequest(handle, vmPtr);
  return {
    opcode: handle.kernel.thread_pending_fs_opcode(vmPtr),
    effects: handle.kernel.thread_pending_fs_effects(vmPtr),
    targetIno: handle.kernel.thread_pending_fs_target_ino(vmPtr),
    fd: request?.fd ?? -1,
    flags: request?.flags ?? 0,
    path: readPendingFsPath(handle, vmPtr),
    secondaryPath: readPendingFsPath(handle, vmPtr, true),
  };
}

function submitSupervisorFsReadyEvent(
  handle: ProcessHandle,
  vmPtr: number,
  status: number | undefined,
): void {
  const okStatus = handle.kernel.abi_status_ok?.();
  if (okStatus === undefined || status !== okStatus) {
    return;
  }
  const result = submitSupervisorShadowStatusEvent(handle, {
    eventKind: supervisorNumericExport(handle, "supervisor_thread_event_kind_backing_store_ready"),
    vmPtr,
    pid: handle.pid,
    tid: handle.kernel.thread_tid?.(vmPtr) ?? 0,
    status,
    syncEffects: 0,
    blockedReason: 0,
    pc: handle.kernel.thread_pc?.(vmPtr) ?? 0n,
    sp: handle.kernel.thread_sp?.(vmPtr) ?? 0n,
  });
  if (result !== undefined && result < 0) {
    deps.appendWorkerDebugLog(handle, `supervisor-fs-ready-event:submit-error:${result}`);
  }
}

function formatPendingFsDebugSuffix(context: PendingFsContext): string {
  const pathSuffix = context.path ? `:path=${JSON.stringify(context.path)}` : "";
  const secondaryPathSuffix = context.secondaryPath
    ? `:path2=${JSON.stringify(context.secondaryPath)}`
    : "";
  return `:op=${context.opcode}:fx=${context.effects}:ino=${context.targetIno}:fd=${context.fd}:flags=${context.flags}${pathSuffix}${secondaryPathSuffix}`;
}

function formatRegularFilePendingFsDebugSuffix(
  handle: ProcessHandle,
  request: PendingRegularFileFsRequest | null,
): string {
  if (request === null) {
    return "";
  }
  const maxFds = handle.kernel.abi_max_fds?.() ?? 0;
  if (request.ofdId <= 0 || request.ofdId >= maxFds) {
    return `:regular=fd=${request.fd}/ofd=${request.ofdId}/invalid`;
  }
  if (request.identity !== null) {
    return [
      `:regular=fd=${request.fd}`,
      `ofd=${request.ofdId}`,
      `type=${request.identity.fileType}`,
      `ino=${request.identity.ino}`,
      `host=${request.identity.hostFd}`,
      `status=${request.identity.statusFlags}`,
      `offset=${request.identity.offset.toString()}`,
      `generation=${request.generation ?? "n/a"}`,
    ].join("/");
  }
  return [
    `:regular=fd=${request.fd}`,
    `ofd=${request.ofdId}`,
    `type=${supervisorOfdFdType(handle, request.ofdId)}`,
    `ino=${supervisorOfdIno(handle, request.ofdId)}`,
    `host=${supervisorOfdHostFd(handle, request.ofdId)}`,
    `status=${supervisorOfdStatusFlags(handle, request.ofdId)}`,
    `offset=${supervisorOfdOffset(handle, request.ofdId).toString()}`,
    `generation=${request.generation ?? "n/a"}`,
  ].join("/");
}

function readRegularFileIdentity(handle: ProcessHandle, ofdId: number): RegularFileIdentity | null {
  const maxFds = handle.kernel.abi_max_fds?.() ?? 0;
  if (ofdId <= 0 || ofdId >= maxFds) {
    return null;
  }
  const fileType = supervisorOfdFdType(handle, ofdId);
  if (fileType <= 0) {
    return null;
  }
  return {
    fileType,
    ino: supervisorOfdIno(handle, ofdId),
    hostFd: supervisorOfdHostFd(handle, ofdId),
    statusFlags: supervisorOfdStatusFlags(handle, ofdId),
    offset: supervisorOfdOffset(handle, ofdId),
  };
}

function regularFileIdentityMatches(
  left: RegularFileIdentity | null,
  right: RegularFileIdentity | null,
  options: { compareOffset?: boolean } = {},
): boolean {
  if (left === null || right === null) {
    return true;
  }
  return left.fileType === right.fileType
    && left.ino === right.ino
    && left.hostFd === right.hostFd
    && left.statusFlags === right.statusFlags
    && (options.compareOffset === false || left.offset === right.offset);
}

function applyLocalMetadataMutationAfterFs(
  handle: ProcessHandle,
  vmPtr: number,
  pendingFs: PendingFsContext,
  actions: SupervisorFsLocalMetadataActions,
): void {
  if (!actions.tombstonePrimary || !pendingFs.path) {
    return;
  }
  rememberMemfsDeletedPath(handle, pendingFs.path);
  const removed = removeLocalMemfsPath(handle, pendingFs.path);
  appendFsLifecycleEvent(
    handle,
    "local-metadata",
    vmPtr,
    `:op=UNLINK:path=${JSON.stringify(pendingFs.path)}:removed=${removed ? 1 : 0}:post_ino=${resolveLocalMemfsPath(handle, pendingFs.path)}`,
  );
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
  for (const member of deps.kernelShareOtherMembers(handle)) {
    rememberMemfsDeletedPath(member, pendingFs.path);
    const memberRemoved = removeLocalMemfsPath(member, pendingFs.path);
    appendFsLifecycleEvent(
      member,
      "local-metadata-peer",
      member.vmPtr,
      `:source=${handle.pid}:op=UNLINK:path=${JSON.stringify(pendingFs.path)}:removed=${memberRemoved ? 1 : 0}:post_ino=${resolveLocalMemfsPath(member, pendingFs.path)}`,
    );
    member.kernelState = readKernelRuntimeState(member.kernel);
    attachKernelRuntimeState(member.kernel, member.kernelState);
  }
}

function clearSupersededMemfsDeletedPath(
  handle: ProcessHandle,
  pendingFs: PendingFsContext,
  actions: SupervisorFsLocalMetadataActions,
): void {
  if (actions.forgetPrimary && pendingFs.path) {
    forgetMemfsDeletedPath(handle, pendingFs.path);
  }
  if (actions.forgetSecondary && pendingFs.secondaryPath) {
    forgetMemfsDeletedPath(handle, pendingFs.secondaryPath);
  }
}

function reapplyLocalMemfsDeletedPathTombstones(
  handle: ProcessHandle,
  vmPtr: number,
  phase: string,
): void {
  const results = reapplyMemfsDeletedPaths(handle);
  if (results.length === 0) {
    return;
  }
  for (const result of results) {
    appendFsLifecycleEvent(
      handle,
      "local-metadata-tombstone",
      vmPtr,
      `:${phase}:path=${JSON.stringify(result.path)}:removed=${result.removed ? 1 : 0}:post_ino=${result.postIno}`,
    );
  }
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  attachKernelRuntimeState(handle.kernel, handle.kernelState);
}

function appendFsLifecycleEvent(
  handle: ProcessHandle,
  phase: string,
  vmPtr: number,
  detail = "",
): void {
  const timestamp = FS_LIFECYCLE_DEBUG_ENABLED ? `:t=${Date.now()}` : "";
  const event = `fs-${phase}:${handle.pid}:0x${vmPtr.toString(16)}${timestamp}${detail}`;
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    event,
  );
  if (FS_LIFECYCLE_DEBUG_ENABLED) {
    console.error(`[tidemark-fs] ${event}`);
  }
}

function fsTimingStart(): number {
  return FS_LIFECYCLE_DEBUG_ENABLED ? Date.now() : 0;
}

function formatFsTimingTotal(startMs: number, detail = ""): string {
  if (!FS_LIFECYCLE_DEBUG_ENABLED || startMs === 0) {
    return detail;
  }
  return `:total=${Date.now() - startMs}${detail}`;
}

function appendFsTimingSince(
  handle: ProcessHandle,
  phase: string,
  vmPtr: number,
  startMs: number,
  detail = "",
): void {
  if (!FS_LIFECYCLE_DEBUG_ENABLED || startMs === 0) {
    return;
  }
  appendFsLifecycleEvent(handle, `timing-${phase}`, vmPtr, `:dt=${Date.now() - startMs}${detail}`);
}

function formatFdEntrySummary(
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
): string {
  const nonzeroEntries = (fdEntries ?? [])
    .filter((entry) => entry.ofdId !== 0)
    .slice(0, 16)
    .map((entry) => `${entry.fd}->${entry.ofdId}/${entry.fdFlags}/g${entry.generation ?? "n/a"}`);
  return nonzeroEntries.length > 0 ? nonzeroEntries.join(",") : "none";
}

function formatOfdSlotSummary(
  ofdSlots: readonly KernelWorkerOfdSlotSnapshot[] | undefined,
): string {
  const entries = (ofdSlots ?? [])
    .filter((slot) => slot.ofdId !== 0)
    .slice(0, 16)
    .map((slot) => {
      if (slot.slot.byteLength < OFD_INO_OFFSET + 4) {
        return `${slot.ofdId}/short=${slot.slot.byteLength}`;
      }
      const view = new DataView(slot.slot.buffer, slot.slot.byteOffset, slot.slot.byteLength);
      const offset = slot.slot.byteLength >= OFD_OFFSET_OFFSET + 8
        ? view.getBigInt64(OFD_OFFSET_OFFSET, true).toString()
        : "short";
      return [
        slot.ofdId,
        `t${view.getInt32(OFD_TYPE_OFFSET, true)}`,
        `h${view.getInt32(OFD_HOST_FD_OFFSET, true)}`,
        `sf${view.getInt32(OFD_STATUS_FLAGS_OFFSET, true)}`,
        `i${view.getUint32(OFD_INO_OFFSET, true)}`,
        `o${offset}`,
      ].join("/");
    });
  return entries.length > 0 ? entries.join(",") : "none";
}

function formatCreatedFdResponseSummary(
  fd: number | null,
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
  ofdSlots: readonly KernelWorkerOfdSlotSnapshot[] | undefined,
): string {
  if (fd === null) {
    return "";
  }
  const entry = fdEntries?.find((candidate) => candidate.fd === fd);
  if (!entry) {
    return `:created=${fd}->missing`;
  }
  const slot = ofdSlots?.find((candidate) => candidate.ofdId === entry.ofdId);
  return `:created=${fd}->${entry.ofdId}/${entry.fdFlags}`
    + `:createdOfd=${slot ? formatOfdSlotSummary([slot]) : "missing"}`;
}

function formatRuntimeFdForFd(handle: ProcessHandle, vmPtr: number, fd: number | null): string {
  if (fd === null) {
    return "";
  }
  const ofdId = supervisorProcessOfdForFd(handle, vmPtr, fd);
  return [
    `:created=${fd}->${ofdId}`,
    `ff${supervisorProcessFdFlagsForFd(handle, vmPtr, fd)}`,
    `t${supervisorOfdFdType(handle, ofdId)}`,
    `h${supervisorOfdHostFd(handle, ofdId)}`,
    `i${supervisorOfdIno(handle, ofdId)}`,
    `sf${supervisorOfdStatusFlags(handle, ofdId)}`,
    `o${supervisorOfdOffset(handle, ofdId).toString()}`,
  ].join("/");
}

function formatRuntimeFdSummary(handle: ProcessHandle, vmPtr: number): string {
  const maxFds = handle.kernel.abi_max_fds?.() ?? 0;
  if (vmPtr === 0 || maxFds <= 0) {
    return "none";
  }
  const entries: string[] = [];
  for (let fd = 0; fd < maxFds && entries.length < 16; fd += 1) {
    const ofdId = supervisorProcessOfdForFd(handle, vmPtr, fd);
    if (ofdId === 0) {
      continue;
    }
    entries.push([
      `${fd}->${ofdId}`,
      `ff${supervisorProcessFdFlagsForFd(handle, vmPtr, fd)}`,
      `t${supervisorOfdFdType(handle, ofdId)}`,
      `h${supervisorOfdHostFd(handle, ofdId)}`,
      `i${supervisorOfdIno(handle, ofdId)}`,
      `sf${supervisorOfdStatusFlags(handle, ofdId)}`,
      `o${supervisorOfdOffset(handle, ofdId).toString()}`,
    ].join("/"));
  }
  return entries.length > 0 ? entries.join(",") : "none";
}

function fsOpName(opcode: number): string {
  switch (opcode) {
    case FS_OP_READ:
      return "READ";
    case FS_OP_WRITE:
      return "WRITE";
    case FS_OP_OPENAT:
      return "OPENAT";
    case FS_OP_CLOSE:
      return "CLOSE";
    case FS_OP_FSTAT:
      return "FSTAT";
    case FS_OP_MKDIR:
      return "MKDIR";
    case FS_OP_UNLINK:
      return "UNLINK";
    case FS_OP_RENAME:
      return "RENAME";
    case FS_OP_GETDENTS:
      return "GETDENTS";
    case FS_OP_TRUNCATE:
      return "TRUNCATE";
    case FS_OP_LSEEK:
      return "LSEEK";
    case FS_OP_MMAP:
      return "MMAP";
    case FS_OP_EXECVE:
      return "EXECVE";
    case FS_OP_FSYNC:
      return "FSYNC";
    default:
      return `OP${opcode}`;
  }
}

function readGuestCStringForFsDebug(
  handle: ProcessHandle,
  guestPtr: bigint | number,
): string | null {
  const ptr = typeof guestPtr === "bigint" ? guestPtr : BigInt(guestPtr);
  if (ptr === 0n || ptr < 0n || ptr > BigInt(Number.MAX_SAFE_INTEGER)) {
    return null;
  }
  const absolute = BigInt(handle.guestBase) + ptr;
  if (absolute < 0n || absolute > BigInt(Number.MAX_SAFE_INTEGER)) {
    return null;
  }
  const bytes = new Uint8Array(handle.memory.buffer);
  let index = Number(absolute);
  if (index < 0 || index >= bytes.length) {
    return null;
  }
  const start = index;
  while (index < bytes.length && bytes[index] !== 0) {
    index += 1;
  }
  if (index >= bytes.length) {
    return null;
  }
  return new TextDecoder().decode(bytes.subarray(start, index));
}

function formatGuestStrvecForFsDebug(
  handle: ProcessHandle,
  vecPtr: bigint,
  maxEntries: number,
): string {
  if (vecPtr === 0n || vecPtr < 0n || vecPtr > BigInt(Number.MAX_SAFE_INTEGER)) {
    return "null";
  }
  const absolute = BigInt(handle.guestBase) + vecPtr;
  if (absolute < 0n || absolute > BigInt(Number.MAX_SAFE_INTEGER)) {
    return "bad";
  }
  const view = new DataView(handle.memory.buffer);
  const entries: string[] = [];
  const base = Number(absolute);
  for (let index = 0; index < maxEntries; index += 1) {
    const entryOffset = base + index * 8;
    if (entryOffset < 0 || entryOffset + 8 > view.byteLength) {
      entries.push(`@${index}=bad`);
      break;
    }
    const ptr = view.getBigUint64(entryOffset, true);
    if (ptr === 0n) {
      entries.push(`@${index}=null`);
      break;
    }
    entries.push(`@${index}=0x${ptr.toString(16)}:${JSON.stringify(readGuestCStringForFsDebug(handle, ptr) ?? "")}`);
  }
  return entries.join(",");
}

function formatFsRingDebug(handle: ProcessHandle, vmPtr: number): string {
  const progress = snapshotFsRingProgress(handle, vmPtr);
  if (progress === null) {
    return "";
  }
  const {
    ringBase,
    submitHead,
    submitTail,
    completeHead,
    completeTail,
  } = progress;
  const view = new DataView(handle.memory.buffer);
  if (!FS_LIFECYCLE_DEBUG_ENABLED) {
    if (submitTail < submitHead) {
      const entry = fsRingRequestEntryOffset(ringBase, submitTail);
      const opcode = view.getUint32(entry + FS_RING_REQUEST_OPCODE_OFFSET, true);
      const fd = view.getInt32(entry + FS_RING_REQUEST_FD_OFFSET, true);
      const offset = view.getBigUint64(entry + FS_RING_REQUEST_OFFSET_OFFSET, true);
      const len = view.getBigUint64(entry + FS_RING_REQUEST_LEN_OFFSET, true);
      const flags = view.getUint32(entry + FS_RING_REQUEST_FLAGS_OFFSET, true);
      const mode = view.getUint32(entry + FS_RING_REQUEST_MODE_OFFSET, true);
      const ofdId = view.getUint32(entry + FS_RING_REQUEST_OFD_ID_OFFSET, true);
      const ino = view.getUint32(entry + FS_RING_REQUEST_INO_OFFSET, true);
      return `:req=${submitTail}:op=${opcode}/${fsOpName(opcode)}/fd=${fd}/off=${offset.toString()}/len=${len.toString()}/flags=${flags}/mode=${mode}/ofd=${ofdId}/ino=${ino}`;
    }
    if (completeTail < completeHead) {
      const entry = fsRingCompletionEntryOffset(ringBase, completeTail);
      const result = view.getBigInt64(entry + FS_RING_RESPONSE_RESULT_OFFSET, true);
      return `:cq=${completeTail}:rv=${result.toString()}`;
    }
    return "";
  }
  const requests: string[] = [];
  for (let pos = submitTail; pos < submitHead && requests.length < 4; pos += 1) {
    const entry = fsRingRequestEntryOffset(ringBase, pos);
    const opcode = view.getUint32(entry + FS_RING_REQUEST_OPCODE_OFFSET, true);
    const fd = view.getInt32(entry + FS_RING_REQUEST_FD_OFFSET, true);
    const offset = view.getBigUint64(entry + FS_RING_REQUEST_OFFSET_OFFSET, true);
    const len = view.getBigUint64(entry + FS_RING_REQUEST_LEN_OFFSET, true);
    const bufPtr = view.getBigUint64(entry + FS_RING_REQUEST_BUF_PTR_OFFSET, true);
    const pathPtr = view.getBigUint64(entry + FS_RING_REQUEST_PATH_PTR_OFFSET, true);
    const flags = view.getUint32(entry + FS_RING_REQUEST_FLAGS_OFFSET, true);
    const requestVmPtr = view.getUint32(entry + FS_RING_REQUEST_VM_PTR_OFFSET, true);
    requests.push([
      `${pos}:op=${opcode}/${fsOpName(opcode)}`,
      `fd=${fd}`,
      `off=${offset.toString()}`,
      `len=${len.toString()}`,
      `buf=0x${bufPtr.toString(16)}`,
      opcode === FS_OP_EXECVE
        ? `path=0x${pathPtr.toString(16)}:${JSON.stringify(readGuestCStringForFsDebug(handle, pathPtr) ?? "")}`
        : "",
      opcode === FS_OP_EXECVE
        ? `argv=0x${bufPtr.toString(16)}:${formatGuestStrvecForFsDebug(handle, bufPtr, 8)}`
        : "",
      `flags=${flags}`,
      `vm=0x${requestVmPtr.toString(16)}`,
    ].filter(Boolean).join("/"));
  }
  const completions: string[] = [];
  for (let pos = completeTail; pos < completeHead && completions.length < 4; pos += 1) {
    const entry = fsRingCompletionEntryOffset(ringBase, pos);
    const result = view.getBigInt64(entry + FS_RING_RESPONSE_RESULT_OFFSET, true);
    const completionVmPtr = view.getUint32(entry + FS_RING_RESPONSE_VM_PTR_OFFSET, true);
    completions.push(`${pos}:rv=${result.toString()}/vm=0x${completionVmPtr.toString(16)}`);
  }
  return [
    `:ring=0x${ringBase.toString(16)}`,
    `:sq=${submitTail}->${submitHead}[${requests.join("|")}]`,
    `:cq=${completeTail}->${completeHead}[${completions.join("|")}]`,
  ].join("");
}

function fsRingHasPendingRequestForVm(handle: ProcessHandle, vmPtr: number): boolean {
  const progress = snapshotFsRingProgress(handle, vmPtr);
  if (progress === null) {
    return false;
  }
  const view = new DataView(handle.memory.buffer);
  const { ringBase, submitHead, submitTail } = progress;
  for (let pos = submitTail; pos < submitHead; pos += 1) {
    const entry = fsRingRequestEntryOffset(ringBase, pos);
    const requestVmPtr = view.getUint32(entry + FS_RING_REQUEST_VM_PTR_OFFSET, true);
    if (requestVmPtr === vmPtr) {
      return true;
    }
  }
  return false;
}

interface PendingFsRingRequest {
  opcode: number;
  fd: number;
  offset: bigint;
  length: bigint;
  bufPtr: bigint;
  flags: number;
  ofdId: number;
  ino: number;
}

interface RegularFileReadBufferSnapshot {
  guestPtr: number;
  bytes: Uint8Array;
}

function fsRingU64ToSafeNumber(value: bigint): number | null {
  return value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER)
    ? Number(value)
    : null;
}

function readPendingFsRingRequest(handle: ProcessHandle, vmPtr: number): PendingFsRingRequest | null {
  const progress = snapshotFsRingProgress(handle, vmPtr);
  if (progress === null) {
    return null;
  }
  const view = new DataView(handle.memory.buffer);
  const { ringBase, submitHead, submitTail } = progress;
  for (let pos = submitTail; pos < submitHead; pos += 1) {
    const entry = fsRingRequestEntryOffset(ringBase, pos);
    const requestVmPtr = view.getUint32(entry + FS_RING_REQUEST_VM_PTR_OFFSET, true);
    if (requestVmPtr === vmPtr) {
      return {
        opcode: view.getUint32(entry + FS_RING_REQUEST_OPCODE_OFFSET, true),
        fd: view.getInt32(entry + FS_RING_REQUEST_FD_OFFSET, true),
        offset: view.getBigUint64(entry + FS_RING_REQUEST_OFFSET_OFFSET, true),
        length: view.getBigUint64(entry + FS_RING_REQUEST_LEN_OFFSET, true),
        bufPtr: view.getBigUint64(entry + FS_RING_REQUEST_BUF_PTR_OFFSET, true),
        flags: view.getUint32(entry + FS_RING_REQUEST_FLAGS_OFFSET, true),
        ofdId: view.getUint32(entry + FS_RING_REQUEST_OFD_ID_OFFSET, true),
        ino: view.getUint32(entry + FS_RING_REQUEST_INO_OFFSET, true),
      };
    }
  }
  return null;
}

function snapshotPendingRegularFileReadBuffer(
  handle: ProcessHandle,
  request: PendingFsRingRequest | null,
): RegularFileReadBufferSnapshot | null {
  if (request === null || request.opcode !== FS_OP_READ) {
    return null;
  }
  const guestPtr = fsRingU64ToSafeNumber(request.bufPtr);
  const length = fsRingU64ToSafeNumber(request.length);
  if (guestPtr === null || length === null || guestPtr <= 0 || length <= 0) {
    return null;
  }
  const start = handle.guestBase + guestPtr;
  const end = start + length;
  if (
    start < handle.guestBase
    || end < start
    || end > handle.memory.buffer.byteLength
  ) {
    return null;
  }
  return {
    guestPtr,
    bytes: new Uint8Array(handle.memory.buffer.slice(start, end)),
  };
}

function restoreRegularFileReadBufferSnapshot(
  handle: ProcessHandle,
  snapshot: RegularFileReadBufferSnapshot | null,
  bytesWritten: bigint,
): boolean {
  if (snapshot === null || bytesWritten <= 0n) {
    return false;
  }
  const length = bytesWritten > BigInt(Number.MAX_SAFE_INTEGER)
    ? snapshot.bytes.byteLength
    : Math.min(Number(bytesWritten), snapshot.bytes.byteLength);
  if (length <= 0) {
    return false;
  }
  const start = handle.guestBase + snapshot.guestPtr;
  const end = start + length;
  if (
    start < handle.guestBase
    || end < start
    || end > handle.memory.buffer.byteLength
  ) {
    return false;
  }
  new Uint8Array(handle.memory.buffer).set(snapshot.bytes.subarray(0, length), start);
  return true;
}

function regularFileReadUsesSubmittedIdentity(
  request: PendingRegularFileFsRequest | null,
  ringRequest: PendingFsRingRequest | null,
): boolean {
  return request !== null
    && ringRequest !== null
    && ringRequest.opcode === FS_OP_READ
    && ringRequest.fd === request.fd
    && ringRequest.ofdId === request.ofdId
    && ringRequest.ino > 0
    && ringRequest.ino === request.ino;
}

function traceRegularFileRead(
  handle: ProcessHandle,
  phase: "submit" | "response",
  vmPtr: number,
  request: PendingRegularFileFsRequest | null,
  ringRequest: PendingFsRingRequest | null,
  result?: bigint,
): void {
  const requestIsStale = phase === "response"
    && request !== null
    && !regularFileRequestStillCurrent(handle, vmPtr, request);
  if (
    ringRequest?.opcode !== FS_OP_READ
    || (
      !(
        FS_TRACE_INO === -1
        || (
          Number.isSafeInteger(FS_TRACE_INO)
          && FS_TRACE_INO > 0
          && (ringRequest.ino === FS_TRACE_INO || request?.identity?.ino === FS_TRACE_INO)
        )
      )
      && !(FS_TRACE_STALE_READ && requestIsStale)
    )
  ) {
    return;
  }
  const guestPtr = fsRingU64ToSafeNumber(ringRequest.bufPtr);
  const requestedLength = fsRingU64ToSafeNumber(ringRequest.length);
  const visibleLength = result === undefined
    ? requestedLength
    : Math.min(
      requestedLength ?? 0,
      result > 0n && result <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(result) : 0,
    );
  let buffer = "unavailable";
  if (guestPtr !== null && visibleLength !== null && guestPtr > 0 && visibleLength > 0) {
    const start = handle.guestBase + guestPtr;
    const end = start + visibleLength;
    if (start >= handle.guestBase && end >= start && end <= handle.memory.buffer.byteLength) {
      const bytes = new Uint8Array(handle.memory.buffer, start, visibleLength);
      const hex = (chunk: Uint8Array) => [...chunk]
        .map((byte) => byte.toString(16).padStart(2, "0"))
        .join("");
      let hash = 0x811c9dc5;
      for (const byte of bytes) {
        hash = Math.imul(hash ^ byte, 0x01000193);
      }
      buffer = `len=${visibleLength}:hash=${(hash >>> 0).toString(16)}`
        + `:head=${hex(bytes.subarray(0, Math.min(32, bytes.length)))}`
        + `:tail=${hex(bytes.subarray(Math.max(0, bytes.length - 32)))}`;
    }
  }
  const currentIdentity = request === null ? null : readRegularFileIdentity(handle, request.ofdId);
  const currentGeneration = request === null
    ? undefined
    : handle.kernel.process_fd_generation?.(vmPtr, request.fd);
  console.error(
    `[tidemark-fs-trace] ${phase}:pid=${handle.pid}:vm=0x${vmPtr.toString(16)}`
      + `:fd=${ringRequest.fd}:ofd=${ringRequest.ofdId}:ino=${ringRequest.ino}`
      + `:off=${ringRequest.offset.toString()}:req=${ringRequest.length.toString()}`
      + `:buf=0x${ringRequest.bufPtr.toString(16)}`
      + `:pendingOfd=${request?.ofdId ?? 0}:pendingIno=${request?.ino ?? 0}`
      + `:pendingGen=${request?.generation ?? "n/a"}:currentGen=${currentGeneration ?? "n/a"}`
      + `:identityIno=${request?.identity?.ino ?? 0}:currentIno=${currentIdentity?.ino ?? 0}`
      + (result === undefined ? "" : `:result=${result.toString()}`)
      + `:${buffer}`,
  );
}

function traceFailedRegularFileWrite(
  handle: ProcessHandle,
  vmPtr: number,
  request: PendingRegularFileFsRequest | null,
  ringRequest: PendingFsRingRequest | null,
  result: bigint,
): void {
  if (
    !FS_TRACE_FAILED_WRITE
    || ringRequest?.opcode !== FS_OP_WRITE
    || BigInt.asIntN(64, result) >= 0n
  ) {
    return;
  }
  const currentIdentity = request === null ? null : readRegularFileIdentity(handle, request.ofdId);
  console.error(
    `[tidemark-fs-failed-write] pid=${handle.pid}:vm=0x${vmPtr.toString(16)}`
      + `:result=${BigInt.asIntN(64, result).toString()}`
      + `:ring-fd=${ringRequest.fd}:ring-ofd=${ringRequest.ofdId}:ring-ino=${ringRequest.ino}`
      + `:request-fd=${request?.fd ?? -1}:request-ofd=${request?.ofdId ?? 0}`
      + `:request-ring-ofd=${request?.ringOfdId ?? 0}:request-ino=${request?.ino ?? 0}`
      + `:live-type=${currentIdentity?.fileType ?? 0}:live-ino=${currentIdentity?.ino ?? 0}`
      + `:live-flags=${currentIdentity?.statusFlags ?? 0}:live-offset=${currentIdentity?.offset.toString() ?? "0"}`,
  );
}

function traceRegularFileWriteResume(
  phase: "response" | "ready",
  handle: ProcessHandle,
  vmPtr: number,
  request: PendingRegularFileFsRequest | null,
  ringRequest: PendingFsRingRequest | null,
  result: bigint,
): void {
  if (!FS_TRACE_WRITE_RESUME || ringRequest?.opcode !== FS_OP_WRITE) {
    return;
  }
  console.error(
    `[tidemark-fs-write-resume] ${phase}:pid=${handle.pid}:vm=0x${vmPtr.toString(16)}`
      + `:result=${BigInt.asIntN(64, result).toString()}`
      + `:a0=${BigInt.asIntN(64, handle.kernel.thread_a0(vmPtr)).toString()}`
      + `:status=${handle.kernel.thread_status(vmPtr)}`
      + `:ring-fd=${ringRequest.fd}:ring-ofd=${ringRequest.ofdId}:ring-ino=${ringRequest.ino}`
      + `:request-fd=${request?.fd ?? -1}:request-ofd=${request?.ofdId ?? 0}`
      + `:request-ino=${request?.ino ?? 0}`,
  );
}

function pendingFsRequestFd(handle: ProcessHandle, vmPtr: number): number | null {
  return readPendingFsRingRequest(handle, vmPtr)?.fd ?? null;
}

function pendingFsSharedOfdIds(handle: ProcessHandle, vmPtr: number): number[] {
  const pendingRegularFile = supervisorPendingRegularFileFsRequest(handle, vmPtr);
  if (pendingRegularFile !== null) {
    const { ofdId } = pendingRegularFile;
    if (
      ofdId > 0
      && ofdId < handle.kernel.abi_max_fds()
      && kernelShareHasSharedOfdId(handle, ofdId)
    ) {
      return [ofdId];
    }
    return [];
  }
  const fd = pendingFsRequestFd(handle, vmPtr);
  if (fd === null || fd < 0 || fd >= handle.kernel.abi_max_fds()) {
    return [];
  }
  const ofdId = supervisorProcessOfdForFd(handle, vmPtr, fd);
  if (
    ofdId > 0
    && ofdId < handle.kernel.abi_max_fds()
    && kernelShareHasSharedOfdId(handle, ofdId)
  ) {
    return [ofdId];
  }
  return [];
}

function hasLiveWorkerState(
  handle: ProcessHandle,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
): boolean {
  if (
    (hostExecutionState(handle).runningWorker !== null && hostExecutionState(handle).runningWorker !== undefined)
    || (hostExecutionState(handle).vforkReadyWorkers?.length ?? 0) > 0
    || (hostExecutionState(handle).hostBlockedWorkers?.size ?? 0) > 0
  ) {
    return true;
  }
  for (const worker of processWorkers(handle)) {
    const thread = threadWorkerStates.get(worker);
    if (getThreadWaitingInWorkerStatus(thread) !== null) {
      return true;
    }
  }
  return false;
}

function supervisorPendingRingRequest(
  handle: ProcessHandle,
  vmPtr: number,
): boolean {
  const pendingRingRequest = handle.kernel.supervisor_fs_pending_ring_request;
  if (typeof pendingRingRequest !== "function") {
    throw new Error("missing supervisor_fs_pending_ring_request export");
  }
  return pendingRingRequest.call(handle.kernel, vmPtr) !== 0;
}

function snapshotPendingRegularFileRequestOfd(
  handle: ProcessHandle,
  vmPtr: number,
  request: PendingRegularFileFsRequest | null,
): KernelWorkerRegularFileFsSnapshot | undefined {
  if (request === null || !regularFileRequestStillCurrent(handle, vmPtr, request)) {
    return undefined;
  }
  const maxFds = handle.kernel.abi_max_fds?.() ?? 0;
  const tableBytes = handle.kernel.kernel_ofd_table_bytes?.() ?? 0;
  const slotBytes = maxFds > 0 ? tableBytes / maxFds : 0;
  const slotOffset = handle.kernelState.ofdTablePtr + request.ofdId * slotBytes;
  if (
    request.ofdId <= 0
    || request.ofdId >= maxFds
    || !Number.isInteger(slotBytes)
    || slotBytes <= 0
    || slotOffset < 0
    || slotOffset + slotBytes > handle.memory.buffer.byteLength
  ) {
    return undefined;
  }
  const ofdSlot = new Uint8Array(
    handle.memory.buffer.slice(slotOffset, slotOffset + slotBytes),
  );
  if (request.requestOffset !== null) {
    new DataView(ofdSlot.buffer, ofdSlot.byteOffset, ofdSlot.byteLength)
      .setBigInt64(OFD_OFFSET_OFFSET, request.requestOffset, true);
  }
  return {
    fd: request.fd,
    ofdId: request.ofdId,
    ofdSlot,
  };
}

function supervisorPendingRegularFileFsRequest(
  handle: ProcessHandle,
  vmPtr: number,
): PendingRegularFileFsRequest | null {
  const pendingFd = handle.kernel.supervisor_fs_pending_regular_file_fd;
  const pendingOfd = handle.kernel.supervisor_fs_pending_regular_file_ofd;
  if (typeof pendingFd !== "function" || typeof pendingOfd !== "function") {
    throw new Error("missing supervisor pending regular-file FS request exports");
  }
  const fd = pendingFd.call(handle.kernel, vmPtr);
  const ofdId = pendingOfd.call(handle.kernel, vmPtr);
  const ringRequest = fd >= 0 ? readPendingFsRingRequest(handle, vmPtr) : null;
  return fd >= 0 && ofdId > 0
    ? {
      fd,
      ofdId,
      ringOfdId: ringRequest?.fd === fd ? ringRequest.ofdId : 0,
      ino: ringRequest?.fd === fd ? ringRequest.ino : 0,
      requestOffset: ringRequest?.fd === fd ? ringRequest.offset : null,
      generation: handle.kernel.process_fd_generation?.(vmPtr, fd),
      identity: readRegularFileIdentity(handle, ofdId),
    }
    : null;
}

function supervisorFsCompletionDecision(
  handle: ProcessHandle,
  vmPtr: number,
  pendingFs: PendingFsContext,
  fsResult: bigint | number,
  options: {
    hostMemfsMutated?: boolean;
    hostOfdStateChanged?: boolean;
    pendingRingRequest: boolean;
    regularFileRequest: boolean;
  },
): SupervisorFsCompletionDecision {
  const decide = handle.kernel.supervisor_fs_completion_decision;
  if (typeof decide !== "function") {
    throw new Error("missing supervisor_fs_completion_decision export");
  }
  const flags = decide.call(
    handle.kernel,
    vmPtr,
    pendingFs.opcode,
    pendingFs.effects,
    fsResult,
    options.hostMemfsMutated === true ? 1 : 0,
    options.hostOfdStateChanged === true ? 1 : 0,
    options.pendingRingRequest ? 1 : 0,
    options.regularFileRequest ? 1 : 0,
  );
  const memfsDirtyFlag = supervisorNumericExport(handle, "supervisor_fs_completion_flag_memfs_dirty");
  const memfsPublishReadyFlag = supervisorNumericExport(handle, "supervisor_fs_completion_flag_memfs_publish_ready");
  const ofdStateChangedFlag = supervisorNumericExport(handle, "supervisor_fs_completion_flag_ofd_state_changed");
  const regularFileRequestFlag = supervisorNumericExport(handle, "supervisor_fs_completion_flag_regular_file_request");
  return {
    memfsMutated: (flags & memfsDirtyFlag) !== 0,
    memfsPublishReady: (flags & memfsPublishReadyFlag) !== 0,
    ofdStateChanged: (flags & ofdStateChangedFlag) !== 0,
    regularFileRequest: (flags & regularFileRequestFlag) !== 0,
  };
}

function supervisorFsResponseFlag(handle: ProcessHandle, name: keyof typeof responseFlagAccessors): number {
  return supervisorNumericExport(handle, responseFlagAccessors[name]);
}

const responseFlagAccessors = {
  adoptKernelState: "supervisor_fs_response_flag_adopt_kernel_state",
  applyGuestMemoryWrites: "supervisor_fs_response_flag_apply_guest_memory_writes",
  queueThreadWorkerWrites: "supervisor_fs_response_flag_queue_thread_worker_writes",
  applyLocalMetadata: "supervisor_fs_response_flag_apply_local_metadata",
  clearTombstones: "supervisor_fs_response_flag_clear_tombstones",
  fdResponseMayClearLiveFds: "supervisor_fs_response_flag_fd_response_may_clear_live_fds",
  storePendingKernelState: "supervisor_fs_response_flag_store_pending_kernel_state",
  resolvedToExecveSpawn: "supervisor_fs_response_flag_resolved_to_execve_spawn",
  processOwnerMatches: "supervisor_fs_response_flag_process_owner_matches",
} as const;

function supervisorFsResponseDisposition(
  handle: ProcessHandle,
  vmPtr: number,
  pendingFs: PendingFsContext,
  fsResult: bigint | number,
  threadState: ThreadWorkerState | undefined,
): SupervisorFsResponseDisposition {
  const decide = handle.kernel.supervisor_fs_response_disposition;
  if (typeof decide !== "function") {
    throw new Error("missing supervisor_fs_response_disposition export");
  }
  const flags = decide.call(
    handle.kernel,
    vmPtr,
    handle.pid,
    pendingFs.opcode,
    fsResult,
    getThreadOwnerAsyncStatus(threadState) ?? 0,
    maybeThreadActivityState(threadState)?.lastStatus ?? 0,
  );
  return {
    adoptKernelState: (flags & supervisorFsResponseFlag(handle, "adoptKernelState")) !== 0,
    applyGuestMemoryWrites: (flags & supervisorFsResponseFlag(handle, "applyGuestMemoryWrites")) !== 0,
    queueThreadWorkerWrites: (flags & supervisorFsResponseFlag(handle, "queueThreadWorkerWrites")) !== 0,
    applyLocalMetadata: (flags & supervisorFsResponseFlag(handle, "applyLocalMetadata")) !== 0,
    clearTombstones: (flags & supervisorFsResponseFlag(handle, "clearTombstones")) !== 0,
    fdResponseMayClearLiveFds: (flags & supervisorFsResponseFlag(handle, "fdResponseMayClearLiveFds")) !== 0,
    storePendingKernelState: (flags & supervisorFsResponseFlag(handle, "storePendingKernelState")) !== 0,
    resolvedToExecveSpawn: (flags & supervisorFsResponseFlag(handle, "resolvedToExecveSpawn")) !== 0,
    processOwnerMatches: (flags & supervisorFsResponseFlag(handle, "processOwnerMatches")) !== 0,
  };
}

function findSupervisorExecveSpawnStatus(
  handle: ProcessHandle,
  candidates: ReadonlyArray<number | null | undefined>,
): number | undefined {
  for (const status of candidates) {
    if (
      status !== null
      && status !== undefined
      && supervisorProcessStatusIsExecveSpawn(handle.kernel, status)
    ) {
      return status;
    }
  }
  return undefined;
}

function supervisorFsLocalMetadataActions(
  handle: ProcessHandle,
  pendingFs: PendingFsContext,
  fsResult: bigint | number,
): SupervisorFsLocalMetadataActions {
  const decide = handle.kernel.supervisor_fs_local_metadata_action;
  if (typeof decide !== "function") {
    throw new Error("missing supervisor_fs_local_metadata_action export");
  }
  const action = decide.call(
    handle.kernel,
    pendingFs.opcode,
    pendingFs.effects,
    fsResult,
    pendingFs.path ? 1 : 0,
    pendingFs.secondaryPath ? 1 : 0,
  );
  const none = supervisorNumericExport(handle, "supervisor_fs_local_metadata_action_none");
  const tombstonePrimary = supervisorNumericExport(handle, "supervisor_fs_local_metadata_action_tombstone_primary");
  const forgetPrimary = supervisorNumericExport(handle, "supervisor_fs_local_metadata_action_forget_primary");
  const forgetSecondary = supervisorNumericExport(handle, "supervisor_fs_local_metadata_action_forget_secondary");
  const known = none | tombstonePrimary | forgetPrimary | forgetSecondary;
  if ((action & ~known) !== 0) {
    throw new Error(`unknown supervisor fs local metadata action: ${action}`);
  }
  return {
    tombstonePrimary: (action & tombstonePrimary) !== 0,
    forgetPrimary: (action & forgetPrimary) !== 0,
    forgetSecondary: (action & forgetSecondary) !== 0,
  };
}

function regularFileSnapshotIdentity(
  snapshot: KernelWorkerOfdSlotSnapshot,
): RegularFileIdentity | null {
  if (snapshot.slot.byteLength < OFD_INO_OFFSET + 4) {
    return null;
  }
  const view = new DataView(snapshot.slot.buffer, snapshot.slot.byteOffset, snapshot.slot.byteLength);
  return {
    fileType: view.getInt32(OFD_TYPE_OFFSET, true),
    ino: view.getUint32(OFD_INO_OFFSET, true),
    hostFd: view.getInt32(OFD_HOST_FD_OFFSET, true),
    statusFlags: view.getInt32(OFD_STATUS_FLAGS_OFFSET, true),
    offset: view.getBigInt64(OFD_OFFSET_OFFSET, true),
  };
}

function regularFileRequestStillCurrent(
  handle: ProcessHandle,
  vmPtr: number,
  request: PendingRegularFileFsRequest,
): boolean {
  if (supervisorProcessOfdForFd(handle, vmPtr, request.fd) !== request.ofdId) {
    return false;
  }
  if (request.ringOfdId > 0 && request.ringOfdId !== request.ofdId) {
    return false;
  }
  if (request.ino > 0 && supervisorOfdIno(handle, request.ofdId) !== request.ino) {
    return false;
  }
  const currentGeneration = handle.kernel.process_fd_generation?.(vmPtr, request.fd);
  if (request.generation !== undefined && currentGeneration !== undefined && currentGeneration !== request.generation) {
    return false;
  }
  return regularFileIdentityMatches(
    request.identity,
    readRegularFileIdentity(handle, request.ofdId),
    { compareOffset: false },
  );
}

function regularFileSnapshotMatchesRequest(
  request: PendingRegularFileFsRequest,
  snapshot: KernelWorkerOfdSlotSnapshot,
): boolean {
  const identity = regularFileSnapshotIdentity(snapshot);
  if (request.ino > 0) {
    return identity !== null
      && identity.ino === request.ino
      && regularFileIdentityMatches(
        request.identity,
        identity,
        { compareOffset: false },
      );
  }
  return regularFileIdentityMatches(
    request.identity,
    identity,
    { compareOffset: false },
  );
}

function currentRegularFileOfdMatchesRequest(
  handle: ProcessHandle,
  request: PendingRegularFileFsRequest,
): boolean {
  const currentIdentity = readRegularFileIdentity(handle, request.ofdId);
  if (currentIdentity === null) {
    return false;
  }
  if (request.ino > 0 && currentIdentity.ino !== request.ino) {
    return false;
  }
  return regularFileIdentityMatches(
    request.identity,
    currentIdentity,
    { compareOffset: false },
  );
}

function regularFileResponseMatchesCurrentRequest(
  handle: ProcessHandle,
  vmPtr: number,
  request: PendingRegularFileFsRequest,
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
  ofdSlots: readonly KernelWorkerOfdSlotSnapshot[] | undefined,
): boolean {
  const responseFd = fdEntries?.find((entry) => (
    entry.fd === request.fd
    && entry.ofdId === request.ofdId
  ));
  const currentGeneration = handle.kernel.process_fd_generation?.(vmPtr, request.fd);
  if (
    responseFd?.generation === undefined
    || currentGeneration === undefined
    || responseFd.generation !== currentGeneration
    || supervisorProcessOfdForFd(handle, vmPtr, request.fd) !== responseFd.ofdId
    || supervisorProcessFdFlagsForFd(handle, vmPtr, request.fd) !== responseFd.fdFlags
    || !currentRegularFileOfdMatchesRequest(handle, request)
  ) {
    return false;
  }
  return ofdSlots?.some((snapshot) => (
    snapshot.ofdId === request.ofdId
    && regularFileSnapshotMatchesRequest(request, snapshot)
  )) ?? false;
}

function filterFsResponseToRegularFileRequest(
  request: PendingRegularFileFsRequest,
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
  ofdSlots: readonly KernelWorkerOfdSlotSnapshot[] | undefined,
): {
  fdEntries?: KernelWorkerFdEntrySnapshot[];
  ofdSlots?: KernelWorkerOfdSlotSnapshot[];
  skippedIdentityMismatch: boolean;
} {
  const requestFdEntries = (fdEntries ?? [])
    .filter((entry) => entry.fd === request.fd && entry.ofdId === request.ofdId);
  const requestOfdIds = new Set<number>([request.ofdId]);
  for (const entry of requestFdEntries) {
    requestOfdIds.add(entry.ofdId);
  }
  const candidateOfdSlots = (ofdSlots ?? [])
    .filter((snapshot) => requestOfdIds.has(snapshot.ofdId));
  const requestOfdSlots = candidateOfdSlots
    .filter((snapshot) => regularFileSnapshotMatchesRequest(request, snapshot));
  const skippedIdentityMismatch = candidateOfdSlots.length > 0 && requestOfdSlots.length === 0;
  return {
    fdEntries: skippedIdentityMismatch || requestFdEntries.length === 0 ? undefined : requestFdEntries,
    ofdSlots: requestOfdSlots.length > 0 ? requestOfdSlots : undefined,
    skippedIdentityMismatch,
  };
}

function fdEntryMatches(entry: { ofdId: number; fdFlags: number }, ofdId: number, fdFlags: number): boolean {
  return entry.ofdId === ofdId && entry.fdFlags === fdFlags;
}

function fdGenerationMatches(
  baselineGeneration: number | undefined,
  currentGeneration: number | undefined,
  responseGeneration: number | undefined,
): boolean {
  if (baselineGeneration === undefined || currentGeneration === undefined) {
    return true;
  }
  return currentGeneration === baselineGeneration
    || (responseGeneration !== undefined && currentGeneration === responseGeneration);
}

function byteRangesEqual(
  left: Uint8Array,
  right: Uint8Array,
): boolean {
  if (left.byteLength !== right.byteLength) {
    return false;
  }
  for (let offset = 0; offset < left.byteLength; offset += 1) {
    if (left[offset] !== right[offset]) {
      return false;
    }
  }
  return true;
}

function baselineOfdSlot(
  baseline: FsFdStateSnapshot,
  ofdId: number,
  slotBytes: number,
): Uint8Array | null {
  if (baseline.ofdTable === null || ofdId <= 0 || slotBytes <= 0) {
    return null;
  }
  const sourceOffset = ofdId * slotBytes;
  if (sourceOffset < 0 || sourceOffset + slotBytes > baseline.ofdTable.byteLength) {
    return null;
  }
  return baseline.ofdTable.subarray(sourceOffset, sourceOffset + slotBytes);
}

function currentOfdSlot(
  handle: ProcessHandle,
  ofdId: number,
  slotBytes: number,
): Uint8Array | null {
  const targetOffset = handle.kernelState.ofdTablePtr + ofdId * slotBytes;
  if (
    ofdId <= 0
    || slotBytes <= 0
    || targetOffset < 0
    || targetOffset + slotBytes > handle.memory.buffer.byteLength
  ) {
    return null;
  }
  return new Uint8Array(handle.memory.buffer, targetOffset, slotBytes);
}

function filterStaleFsResponseFdState(
  handle: ProcessHandle,
  vmPtr: number,
  baseline: FsFdStateSnapshot | null,
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
  ofdSlots: readonly KernelWorkerOfdSlotSnapshot[] | undefined,
): FilteredFsFdState {
  if (baseline === null || baseline.vmPtr !== vmPtr) {
    return {
      fdEntries: fdEntries === undefined ? undefined : [...fdEntries],
      ofdSlots: ofdSlots === undefined ? undefined : [...ofdSlots],
      skippedFdEntries: 0,
      skippedOfdSlots: 0,
    };
  }

  const baselineFdEntries = new Map<number, {
    ofdId: number;
    fdFlags: number;
    generation?: number;
  }>();
  for (const entry of baseline.fdEntries) {
    baselineFdEntries.set(entry.fd, entry);
  }

  const keptFdEntries: KernelWorkerFdEntrySnapshot[] = [];
  const staleOfdIds = new Set<number>();
  let skippedFdEntries = 0;
  for (const entry of fdEntries ?? []) {
    const baselineEntry = baselineFdEntries.get(entry.fd);
    if (baselineEntry === undefined) {
      keptFdEntries.push(entry);
      continue;
    }
    const currentOfdId = supervisorProcessOfdForFd(handle, vmPtr, entry.fd);
    const currentFdFlags = supervisorProcessFdFlagsForFd(handle, vmPtr, entry.fd);
    const currentGeneration = handle.kernel.process_fd_generation?.(vmPtr, entry.fd);
    if (!fdGenerationMatches(baselineEntry.generation, currentGeneration, entry.generation)) {
      if (entry.ofdId > 0) {
        staleOfdIds.add(entry.ofdId);
      }
      skippedFdEntries += 1;
      continue;
    }
    if (
      fdEntryMatches(baselineEntry, currentOfdId, currentFdFlags)
      || fdEntryMatches(entry, currentOfdId, currentFdFlags)
    ) {
      keptFdEntries.push(entry);
      continue;
    }
    skippedFdEntries += 1;
    if (entry.ofdId > 0) {
      staleOfdIds.add(entry.ofdId);
    }
  }

  const maxFds = handle.kernel.abi_max_fds?.() ?? baseline.fdEntries.length;
  const tableBytes = handle.kernel.kernel_ofd_table_bytes?.() ?? 0;
  const slotBytes = maxFds > 0 ? tableBytes / maxFds : 0;
  const canFilterOfdSlots = baseline.ofdTable !== null
    && Number.isInteger(slotBytes)
    && slotBytes > 0;
  const keptOfdSlots: KernelWorkerOfdSlotSnapshot[] = [];
  let skippedOfdSlots = 0;
  for (const snapshot of ofdSlots ?? []) {
    if (staleOfdIds.has(snapshot.ofdId)) {
      skippedOfdSlots += 1;
      continue;
    }
    if (!canFilterOfdSlots || snapshot.slot.byteLength !== slotBytes) {
      keptOfdSlots.push(snapshot);
      continue;
    }
    const baselineSlot = baselineOfdSlot(baseline, snapshot.ofdId, slotBytes);
    const currentSlot = currentOfdSlot(handle, snapshot.ofdId, slotBytes);
    if (
      baselineSlot === null
      || currentSlot === null
      || byteRangesEqual(currentSlot, baselineSlot)
      || byteRangesEqual(currentSlot, snapshot.slot)
    ) {
      keptOfdSlots.push(snapshot);
      continue;
    }
    skippedOfdSlots += 1;
  }

  return {
    fdEntries: keptFdEntries.length > 0 ? keptFdEntries : undefined,
    ofdSlots: keptOfdSlots.length > 0 ? keptOfdSlots : undefined,
    skippedFdEntries,
    skippedOfdSlots,
  };
}

function fsRingHasPendingCompletionForVm(handle: ProcessHandle, vmPtr: number): boolean {
  const progress = snapshotFsRingProgress(handle, vmPtr);
  if (progress === null) {
    return false;
  }
  const view = new DataView(handle.memory.buffer);
  const { ringBase, completeHead, completeTail } = progress;
  for (let pos = completeTail; pos < completeHead; pos += 1) {
    const entry = fsRingCompletionEntryOffset(ringBase, pos);
    const completionVmPtr = view.getUint32(entry + FS_RING_RESPONSE_VM_PTR_OFFSET, true);
    if (completionVmPtr === vmPtr) {
      return true;
    }
  }
  return false;
}

function snapshotFsRingProgress(handle: ProcessHandle, vmPtr: number): FsRingProgressSnapshot | null {
  let ringBase = 0;
  try {
    ringBase = handle.kernel.thread_fs_ring_base?.(vmPtr) ?? 0;
  } catch {
    return null;
  }
  if (ringBase <= 0 || ringBase + FS_RING_TOTAL_BYTES > handle.memory.buffer.byteLength) {
    return null;
  }
  const view = new DataView(handle.memory.buffer);
  return {
    vmPtr,
    ringBase,
    submitHead: view.getUint32(ringBase + FS_RING_SUBMIT_HEAD_OFFSET, true),
    submitTail: view.getUint32(ringBase + FS_RING_SUBMIT_TAIL_OFFSET, true),
    completeHead: view.getUint32(ringBase + FS_RING_COMPLETE_HEAD_OFFSET, true),
    completeTail: view.getUint32(ringBase + FS_RING_COMPLETE_TAIL_OFFSET, true),
  };
}

function counterBehind(current: number, target: number): boolean {
  const distance = (target - current) >>> 0;
  return distance !== 0 && distance < 0x80000000;
}

function restoreFsRingProgress(handle: ProcessHandle, snapshot: FsRingProgressSnapshot | null): void {
  if (!snapshot) {
    return;
  }
  const ringBase = handle.kernel.thread_fs_ring_base?.(snapshot.vmPtr) ?? 0;
  if (
    ringBase !== snapshot.ringBase
    || ringBase <= 0
    || ringBase + FS_RING_COMPLETE_TAIL_OFFSET + 4 > handle.memory.buffer.byteLength
  ) {
    return;
  }
  const view = new DataView(handle.memory.buffer);
  const advance = (offset: number, target: number): void => {
    const current = view.getUint32(ringBase + offset, true);
    if (counterBehind(current, target)) {
      view.setUint32(ringBase + offset, target, true);
    }
  };
  advance(FS_RING_SUBMIT_HEAD_OFFSET, snapshot.submitHead);
  advance(FS_RING_SUBMIT_TAIL_OFFSET, snapshot.submitTail);
  advance(FS_RING_COMPLETE_HEAD_OFFSET, snapshot.completeHead);
  advance(FS_RING_COMPLETE_TAIL_OFFSET, snapshot.completeTail);
}

function fsResumeKey(handle: ProcessHandle, vmPtr: number): string {
  const pendingFs = readPendingFsContext(handle, vmPtr);
  const submitHead = handle.kernel.thread_fs_ring_submit_head?.(vmPtr) ?? 0;
  const submitTail = handle.kernel.thread_fs_ring_submit_tail?.(vmPtr) ?? 0;
  return [
    vmPtr,
    submitHead,
    submitTail,
    pendingFs.opcode,
    pendingFs.effects,
    pendingFs.targetIno,
    pendingFs.path ?? "",
    pendingFs.secondaryPath ?? "",
  ].join(":");
}

function fsRequestRequiresProcessWidePumpGate(
  handle: ProcessHandle,
  pendingFs: PendingFsContext,
): boolean {
  switch (pendingFs.opcode) {
    case FS_OP_CLOSE:
    case FS_OP_MKDIR:
    case FS_OP_UNLINK:
    case FS_OP_RENAME:
    case FS_OP_TRUNCATE:
    case FS_OP_LSEEK:
    case FS_OP_FSYNC:
      return true;
    case FS_OP_OPENAT: {
      const mutatingOpenFlags = (handle.kernel.abi_o_creat?.() ?? 0x40)
        | (handle.kernel.abi_o_trunc?.() ?? 0x200);
      return (pendingFs.flags & mutatingOpenFlags) !== 0;
    }
    default:
      return false;
  }
}

function beginRegularFileFsInFlight(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  pendingFs: PendingFsContext,
  request: PendingRegularFileFsRequest | null,
): () => void {
  return markRegularFileFsInFlight(
    handle,
    request === null
      ? (
        fsRequestRequiresProcessWidePumpGate(handle, pendingFs)
          ? {
            worker,
            vmPtr,
            fd: pendingFs.fd,
            ofdId: 0,
          }
          : null
      )
      : {
        worker,
        vmPtr,
        fd: request.fd,
        ofdId: request.ofdId,
      },
  );
}

function normalizeFsResult(result: bigint | number): bigint {
  return typeof result === "bigint" ? result : BigInt(result);
}

function formatFsErrorDetail(error: unknown): string {
  const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
  return `:err=${JSON.stringify(detail.slice(0, 1000))}`;
}

function recordFsTransferStats(
  handle: ProcessHandle,
  pendingFs: PendingFsContext,
  fsResult: bigint | number,
  fsSucceeded: boolean,
  viaKernelWorker: boolean,
  fsCompletion: SupervisorFsCompletionDecision,
): void {
  const stats = transferDebugStats(handle);
  stats.fsResumeCalls += 1;
  if (viaKernelWorker) {
    stats.fsKernelWorkerCalls += 1;
  } else {
    stats.fsLocalCalls += 1;
  }
  if (!fsSucceeded) {
    return;
  }

  if (fsCompletion.memfsMutated) {
    stats.memfsMutationOps += 1;
  }
  if (fsCompletion.memfsPublishReady) {
    stats.pageCachePublishCandidates += 1;
  }

  const byteCount = Number(BigInt.asIntN(64, normalizeFsResult(fsResult)));
  if (!Number.isFinite(byteCount) || byteCount <= 0) {
    return;
  }
  if (pendingFs.opcode === FS_OP_READ) {
    stats.fsReadCalls += 1;
    stats.fsReadBytes += byteCount;
  } else if (pendingFs.opcode === FS_OP_WRITE) {
    stats.fsWriteCalls += 1;
    stats.fsWriteBytes += byteCount;
  }
}

function rememberPendingFsMemfsChange(handle: ProcessHandle, pendingFs: PendingFsContext): void {
  if (pendingFs.path) {
    rememberProcessMemfsChangedPath(handle, pendingFs.path);
  }
  if (pendingFs.secondaryPath) {
    rememberProcessMemfsChangedPath(handle, pendingFs.secondaryPath);
  }
  if (pendingFs.targetIno > 0) {
    rememberProcessMemfsChangedIno(handle, pendingFs.targetIno);
  }
}

function fsResumeStillPending(handle: ProcessHandle, vmPtr: number, fsResult: bigint | number): boolean {
  const result = Number(BigInt.asIntN(64, normalizeFsResult(fsResult)));
  const eagain = handle.kernel.abi_eagain?.() ?? 11;
  const fsStatus = handle.kernel.abi_status_fs?.() ?? -1;
  if (handle.kernel.thread_status?.(vmPtr) !== fsStatus) {
    return false;
  }
  return result === eagain || supervisorPendingRingRequest(handle, vmPtr);
}

function clearStaleFsStatusAfterResume(handle: ProcessHandle, vmPtr: number, fsResult: bigint | number): void {
  const result = Number(BigInt.asIntN(64, normalizeFsResult(fsResult)));
  const eagain = handle.kernel.abi_eagain?.() ?? 11;
  const fsStatus = handle.kernel.abi_status_fs?.() ?? -1;
  if (
    result === eagain
    || handle.kernel.thread_status?.(vmPtr) !== fsStatus
    || fsRingHasPendingRequestForVm(handle, vmPtr)
    || fsRingHasPendingCompletionForVm(handle, vmPtr)
  ) {
    return;
  }
  const before = formatFsRingDebug(handle, vmPtr);
  const clearedResult = handle.kernel.thread_resume_fs_result(vmPtr, BigInt(result));
  appendFsLifecycleEvent(
    handle,
    "clear-stale-status",
    vmPtr,
    `:rv=${clearedResult.toString()}:status=${handle.kernel.thread_status(vmPtr)}${before}`,
  );
}

function fsFdStateSnapshotScope(
  handle: ProcessHandle,
  pendingFs: PendingFsContext,
  regularFileRequest: PendingRegularFileFsRequest | null,
): FsFdStateSnapshotScope {
  if (regularFileRequest !== null) {
    return { kind: "fds", fds: [regularFileRequest.fd] };
  }
  if ((pendingFs.effects & FS_EFFECT_SHARED_OFD_STATE) !== 0) {
    return { kind: "all" };
  }
  return fsRequestRequiresProcessWidePumpGate(handle, pendingFs)
    ? { kind: "none" }
    : { kind: "all" };
}

function snapshotFsFdState(
  handle: ProcessHandle,
  vmPtr: number,
  scope: FsFdStateSnapshotScope,
): FsFdStateSnapshot | null {
  const maxFds = handle.kernel.abi_max_fds?.() ?? 0;
  if (vmPtr === 0 || maxFds <= 0 || scope.kind === "none") {
    return null;
  }
  const fds = scope.kind === "all"
    ? Array.from({ length: maxFds }, (_value, fd) => fd)
    : scope.fds.filter((fd) => fd >= 0 && fd < maxFds);
  if (fds.length === 0) {
    return null;
  }
  const fdEntries: FsFdStateSnapshot["fdEntries"] = [];
  for (const fd of fds) {
    fdEntries.push({
      fd,
      ofdId: supervisorProcessOfdForFd(handle, vmPtr, fd),
      fdFlags: supervisorProcessFdFlagsForFd(handle, vmPtr, fd),
      generation: handle.kernel.process_fd_generation?.(vmPtr, fd),
    });
  }
  const tableBytes = handle.kernel.kernel_ofd_table_bytes?.() ?? 0;
  const ofdTablePtr = handle.kernelState.ofdTablePtr;
  let ofdTable: Uint8Array | null = null;
  if (tableBytes > 0 && ofdTablePtr > 0) {
    const memory = new Uint8Array(handle.memory.buffer);
    if (ofdTablePtr + tableBytes <= memory.byteLength) {
      ofdTable = memory.slice(ofdTablePtr, ofdTablePtr + tableBytes);
    }
  }
  return { vmPtr, fdEntries, ofdTablePtr, ofdTable };
}

function restoreFsFdState(handle: ProcessHandle, snapshot: FsFdStateSnapshot | null): void {
  if (!snapshot || snapshot.vmPtr === 0 || !handle.kernel.process_set_fd_entry) {
    return;
  }
  const maxFds = handle.kernel.abi_max_fds?.() ?? snapshot.fdEntries.length;
  const tableBytes = handle.kernel.kernel_ofd_table_bytes?.() ?? 0;
  const ofdSlotBytes = maxFds > 0 ? tableBytes / maxFds : 0;
  const canRestoreOfdSlot = snapshot.ofdTable !== null
    && snapshot.ofdTablePtr > 0
    && Number.isInteger(ofdSlotBytes)
    && ofdSlotBytes > 0;
  const memory = canRestoreOfdSlot ? new Uint8Array(handle.memory.buffer) : null;
  for (const entry of snapshot.fdEntries) {
    if (entry.fd < 0 || entry.fd >= maxFds) {
      continue;
    }
    const currentOfdId = supervisorProcessOfdForFd(handle, snapshot.vmPtr, entry.fd);
    const currentFdFlags = supervisorProcessFdFlagsForFd(handle, snapshot.vmPtr, entry.fd);
    if (currentOfdId === entry.ofdId && currentFdFlags === entry.fdFlags) {
      continue;
    }
    handle.kernel.process_set_fd_entry(snapshot.vmPtr, entry.fd, entry.ofdId, entry.fdFlags);
    const currentOfdType = entry.ofdId > 0
      ? supervisorOfdFdType(handle, entry.ofdId)
      : 0;
    if (
      canRestoreOfdSlot
      && memory
      && snapshot.ofdTable
      && entry.ofdId > 0
      && entry.ofdId < maxFds
      && currentOfdType === 0
    ) {
      const sourceOffset = entry.ofdId * ofdSlotBytes;
      const targetOffset = snapshot.ofdTablePtr + sourceOffset;
      if (
        sourceOffset + ofdSlotBytes <= snapshot.ofdTable.length
        && targetOffset + ofdSlotBytes <= memory.byteLength
      ) {
        memory.set(
          snapshot.ofdTable.subarray(sourceOffset, sourceOffset + ofdSlotBytes),
          targetOffset,
        );
      }
    }
  }
}

function applyFsResponseFdEntries(
  handle: ProcessHandle,
  vmPtr: number,
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
  options?: Parameters<typeof applyThreadFdEntries>[3],
): void {
  applyThreadFdEntries(handle, vmPtr, fdEntries, options);
  const canonicalVmPtr = canonicalFdTableVmPtrForThread(handle, vmPtr);
  if (canonicalVmPtr !== vmPtr) {
    applyThreadFdEntries(handle, canonicalVmPtr, fdEntries, options);
  }
}

function normalizeOptionalSyscallNr(value: bigint | number | undefined): bigint | undefined {
  return value === undefined ? undefined : BigInt(value);
}

function fsRestoreSyscallNr(
  handle: ProcessHandle,
  pendingFs: PendingFsContext,
): bigint | undefined {
  switch (pendingFs.opcode) {
    case FS_OP_OPENAT:
      return normalizeOptionalSyscallNr(handle.kernel.abi_sys_openat?.());
    case FS_OP_CLOSE:
      return normalizeOptionalSyscallNr(handle.kernel.abi_sys_close?.());
    case FS_OP_EXECVE:
      return normalizeOptionalSyscallNr(handle.kernel.abi_sys_execve?.());
    default:
      return undefined;
  }
}

function fsCreatedFdFromResult(
  handle: ProcessHandle,
  pendingFs: PendingFsContext,
  fsResult: bigint,
  fsSucceeded: boolean,
): number | null {
  if (!fsSucceeded || pendingFs.opcode !== FS_OP_OPENAT) {
    return null;
  }
  const createdFd = Number(BigInt.asIntN(64, fsResult));
  const maxFds = handle.kernel.abi_max_fds?.() ?? 0;
  return Number.isInteger(createdFd) && createdFd >= 0 && createdFd < maxFds
    ? createdFd
    : null;
}

function fdEntriesExcludingFd(
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
  fd: number | null,
): readonly KernelWorkerFdEntrySnapshot[] | undefined {
  if (!fdEntries || fd === null) {
    return fdEntries;
  }
  const filtered = fdEntries.filter((entry) => entry.fd !== fd);
  return filtered.length > 0 ? filtered : undefined;
}

function fdEntriesForFd(
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
  fd: number | null,
): readonly KernelWorkerFdEntrySnapshot[] | undefined {
  if (!fdEntries || fd === null) {
    return undefined;
  }
  const filtered = fdEntries.filter((entry) => entry.fd === fd);
  return filtered.length > 0 ? filtered : undefined;
}

function applySubmittedIdentityRegularFileOfdSlots(
  handle: ProcessHandle,
  vmPtr: number,
  request: PendingRegularFileFsRequest,
  ofdSlots: readonly KernelWorkerOfdSlotSnapshot[] | undefined,
): boolean {
  if (!currentRegularFileOfdMatchesRequest(handle, request)) {
    return false;
  }
  const requestFsState = filterFsResponseToRegularFileRequest(
    request,
    undefined,
    ofdSlots,
  );
  if (!requestFsState.ofdSlots) {
    return false;
  }
  applyThreadOfdSlots(handle, {
    vmPtr,
    ofdSlots: requestFsState.ofdSlots,
    recordRegularFileOfdOffsets: true,
  });
  return true;
}

function applyFsGuestMemoryWrites(
  handle: ProcessHandle,
  worker: Worker,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
  writes: GuestMemoryWriteSnapshot[] | undefined,
  options?: FsGuestMemoryWriteOptions,
): void {
  if (!writes || writes.length === 0) {
    return;
  }
  const memory = new Uint8Array(handle.memory.buffer);
  const pendingWrites: GuestMemoryWriteSnapshot[] = [];
  const queueThreadWorkerWrites = options?.queueThreadWorkerWrites ?? true;
  for (const write of writes) {
    const start = handle.guestBase + write.guestPtr;
    if (
      !Number.isFinite(start)
      || start < handle.guestBase
      || write.bytes.length === 0
      || start + write.bytes.length > memory.byteLength
    ) {
      continue;
    }
    const overlap = formatGuestMemoryWriteWatchOverlap(
      "fs-guest-write",
      guestWriteWatchRegions(handle),
      write.guestPtr,
      write.bytes,
    );
    if (overlap !== null) {
      workerDebugLog(handle).push(overlap);
      if (workerDebugLog(handle).length > 4096) {
        workerDebugLog(handle).splice(0, workerDebugLog(handle).length - 4096);
      }
    }
    memory.set(write.bytes, start);
    pendingWrites.push({
      guestPtr: write.guestPtr,
      bytes: memory.slice(start, start + write.bytes.length),
    });
  }
  if (!queueThreadWorkerWrites || pendingWrites.length === 0) {
    return;
  }
  const thread = threadWorkerStates.get(worker);
  if (thread) {
    appendPendingGuestMemoryWrites(thread, pendingWrites);
  }
}

async function resumeFsViaKernelWorker(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
): Promise<KernelRpcResumeOutcome> {
  let byVmPtr = fsResumeInFlight.get(handle);
  const key = fsResumeKey(handle, vmPtr);
  const existing = byVmPtr?.get(key);
  if (existing) {
    deps.appendWorkerDebugLog(handle, `fs-join:0x${vmPtr.toString(16)}`);
    appendFsLifecycleEvent(handle, "join", vmPtr, "");
    return existing;
  }
  if (byVmPtr === undefined) {
    byVmPtr = new Map();
    fsResumeInFlight.set(handle, byVmPtr);
  }
  const previousTail = fsResumeProcessTails.get(handle);
  const resume = (async () => {
    await previousTail;
    return resumeFsViaKernelWorkerInner(handle, worker, vmPtr);
  })();
  const processTail = resume.then(
    () => undefined,
    () => undefined,
  );
  fsResumeProcessTails.set(handle, processTail);
  byVmPtr.set(key, resume);
  try {
    return await resume;
  } finally {
    if (byVmPtr.get(key) === resume) {
      byVmPtr.delete(key);
    }
    if (byVmPtr.size === 0) {
      fsResumeInFlight.delete(handle);
    }
    if (fsResumeProcessTails.get(handle) === processTail) {
      fsResumeProcessTails.delete(handle);
    }
  }
}

async function completeFsResumeFromSupervisorDecision(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  fsResult: bigint,
  options: {
    source: "local" | "kernel-worker";
    applyPostFsWake(): void | Promise<void>;
    finalStatus?: number;
    forceExecveSpawn?: boolean;
  },
): Promise<KernelRpcResumeOutcome> {
  const completionStartMs = fsTimingStart();
  const sourcePrefix = options.source === "local" ? ":local" : "";
  const finalStatus = options.finalStatus ?? handle.kernel.thread_status?.(vmPtr);
  appendFsTimingSince(handle, "complete-start", vmPtr, completionStartMs, sourcePrefix);
  deps.appendWorkerDebugLog(
    handle,
    `fs-done:0x${vmPtr.toString(16)}${sourcePrefix}:rv=${fsResult.toString()}`,
  );
  appendFsLifecycleEvent(
    handle,
    "done",
    vmPtr,
    `${sourcePrefix}:rv=${fsResult.toString()}:status=${finalStatus ?? "n/a"}${formatFsRingDebug(handle, vmPtr)}`,
  );
  if (fsResumeStillPending(handle, vmPtr, fsResult)) {
    appendFsLifecycleEvent(
      handle,
      "pending",
      vmPtr,
      `${sourcePrefix}:rv=${fsResult.toString()}${formatFsRingDebug(handle, vmPtr)}`,
    );
    await options.applyPostFsWake();
    appendFsTimingSince(handle, "post-wake-done", vmPtr, completionStartMs, sourcePrefix);
    const status = handle.kernel.abi_status_fs?.() ?? -1;
    return { kind: "blocked", status };
  }
  const resolvedToExecveSpawn = options.forceExecveSpawn === true
    || (finalStatus !== undefined && supervisorProcessStatusIsExecveSpawn(handle.kernel, finalStatus));
  if (resolvedToExecveSpawn && finalStatus !== undefined) {
    await options.applyPostFsWake();
    appendFsTimingSince(handle, "post-wake-done", vmPtr, completionStartMs, sourcePrefix);
    await handleFsExecveSpawn(handle, worker, vmPtr, finalStatus);
    appendFsTimingSince(handle, "execve-spawn-done", vmPtr, completionStartMs, sourcePrefix);
    return completedKernelRpcOutcome();
  }
  await options.applyPostFsWake();
  appendFsTimingSince(handle, "post-wake-done", vmPtr, completionStartMs, sourcePrefix);
  submitSupervisorFsReadyEvent(handle, vmPtr, finalStatus);
  const thread = deps.threadWorkerStates.get(worker);
  if (thread?.vmPtr === vmPtr) {
    clearThreadOwnerAsyncStatus(thread);
  }
  deps.enqueueWorker(handle, worker);
  appendFsTimingSince(handle, "enqueue-done", vmPtr, completionStartMs, sourcePrefix);
  return completedKernelRpcOutcome(finalStatus, { workerQueued: true });
}

async function handleFsExecveSpawn(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  status: number,
): Promise<void> {
  const thread = deps.threadWorkerStates.get(worker);
  if (!thread || thread.vmPtr !== vmPtr) {
    return;
  }
  if (!deps.handleExecveSpawn) {
    throw new Error("kernel-rpc fs helper missing execve spawn host executor");
  }
  setThreadOwnerAsyncStatus(thread, status);
  try {
    await deps.handleExecveSpawn(handle, worker, thread);
  } finally {
    if (processHasWorker(handle, worker) && deps.threadWorkerStates.get(worker) === thread) {
      clearThreadOwnerAsyncStatus(thread);
    }
  }
}

async function resumeFsViaKernelWorkerInner(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
): Promise<KernelRpcResumeOutcome> {
  for (let attempt = 0; attempt < MAX_CHAINED_FS_RESUME_ATTEMPTS; attempt += 1) {
    const outcome = await resumeFsViaKernelWorkerOnce(handle, worker, vmPtr);
    const fsStatus = handle.kernel.abi_status_fs?.() ?? -1;
    if (handle.kernel.thread_status?.(vmPtr) !== fsStatus || !supervisorPendingRingRequest(handle, vmPtr)) {
      return outcome;
    }
    appendFsLifecycleEvent(
      handle,
      "chain",
      vmPtr,
      `:attempt=${attempt + 1}${formatFsRingDebug(handle, vmPtr)}`,
    );
  }
  throw new Error(
    `fs resume chain exceeded ${MAX_CHAINED_FS_RESUME_ATTEMPTS} attempts: `
      + `vm=0x${vmPtr.toString(16)}${formatFsRingDebug(handle, vmPtr)}`,
  );
}

function isActiveVforkChildWorker(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
): boolean {
  const executionState = hostExecutionState(handle);
  return executionState.vforkWorker === worker
    && executionState.vforkSnapshot?.vmPtr === vmPtr;
}

async function resumeFsViaKernelWorkerOnce(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
): Promise<KernelRpcResumeOutcome> {
  const resumeStartMs = fsTimingStart();
  const pendingFs = readPendingFsContext(handle, vmPtr);
  const pendingRingRequest = supervisorPendingRingRequest(handle, vmPtr);
  const regularFilePendingFs = supervisorPendingRegularFileFsRequest(handle, vmPtr);
  const fsDebugSuffix = formatPendingFsDebugSuffix(pendingFs)
    + (
      FS_LIFECYCLE_DEBUG_ENABLED
        ? formatRegularFilePendingFsDebugSuffix(handle, regularFilePendingFs)
        : ""
    );
  const regularFileOnlyPendingFs = regularFilePendingFs !== null;
  const kernelWorkerRequest = requireKernelWorkerOwnership(handle, "resumeFsViaKernelWorker");
  const clearRegularFileFsInFlight = beginRegularFileFsInFlight(
    handle,
    worker,
    vmPtr,
    pendingFs,
    regularFilePendingFs,
  );

  try {
    deps.appendWorkerDebugLog(handle, `fs-start:0x${vmPtr.toString(16)}${fsDebugSuffix}`);
    appendFsLifecycleEvent(handle, "start", vmPtr, `${fsDebugSuffix}${formatFsRingDebug(handle, vmPtr)}`);
    appendFsTimingSince(handle, "inner-entry", vmPtr, resumeStartMs, fsDebugSuffix);
    const forkShareMembers = deps.kernelShareOtherMembers(handle);
    const heldOfdIds = forkShareMembers.length > 0
      ? collectProcessHeldOfds(handle)
      : null;
    const missingSharedOfdIds = forkShareMembers.length === 0
      ? []
      : pendingFsSharedOfdIds(handle, vmPtr)
        .filter((ofdId) => !heldOfdIds?.has(ofdId) || !hasPlausibleOfdState(handle, ofdId));
    if (missingSharedOfdIds.length > 0) {
      await deps.rehydrateForkSharedStateViaKernelWorker([handle], missingSharedOfdIds, {
        cause: { kind: "syscall", nr: -1n, name: "fs-rehydrate-shared-ofd" },
        includeMemfs: false,
      });
    }
    const dirtyMemfsPeers = deps.kernelShareOtherMembers(handle)
      .filter((member) => (
        isProcessMemfsDirty(member)
        && hasLiveWorkerState(member, deps.threadWorkerStates)
        && hostExecutionState(member).runningWorker === null
        && !processExitInProgress(member)
        && processHasKernelWorkerOwnership(member)
      ));
    for (const peer of dirtyMemfsPeers) {
      const targets = [handle, ...deps.kernelShareOtherMembers(peer)]
        .filter((member, index, members) => (
          member !== peer
          && !processExitInProgress(member)
          && hostExecutionState(member).runningWorker === null
          && members.indexOf(member) === index
        ));
      transferDebugStats(peer).forkSharedMemfsSyncCalls += 1;
      await deps.syncDirtyMemfsPeerViaKernelWorker(peer, targets, {
        cause: { kind: "syscall", nr: -1n, name: "fs-dirty-memfs-peer" },
        includeMemfs: true,
        memfsOnly: true,
        sourceVmPtr: peer.vmPtr,
      });
      setProcessMemfsDirty(peer, false);
    }
    if (!isActiveVforkChildWorker(handle, worker, vmPtr)) {
      deps.assertProcessStandardFdIntegrity(handle, "resume-fs-before");
    }
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    const fdStateSnapshotScope = fsFdStateSnapshotScope(handle, pendingFs, regularFilePendingFs);
    const requestFsFdState = snapshotFsFdState(handle, vmPtr, fdStateSnapshotScope);
    const requestRingState = readPendingFsRingRequest(handle, vmPtr);
    const regularFileRequest = snapshotPendingRegularFileRequestOfd(handle, vmPtr, regularFilePendingFs);
    const regularFileReadBuffer = regularFilePendingFs === null
      ? null
      : snapshotPendingRegularFileReadBuffer(handle, requestRingState);
    traceRegularFileRead(handle, "submit", vmPtr, regularFilePendingFs, requestRingState);
    appendFsTimingSince(handle, "pre-kernel-worker", vmPtr, resumeStartMs, fsDebugSuffix);
    const kernelWorkerStartMs = fsTimingStart();
    const processFsResponse = await kernelWorkerRequest({
      type: "process-fs",
      pid: handle.pid,
      vmPtr,
      kernelState: handle.kernelState,
      ...(regularFileRequest ? { regularFileRequest } : {}),
      debugProgress: FS_PROCESS_DEBUG_ENABLED,
    });
    appendFsTimingSince(
      handle,
      "kernel-worker-done",
      vmPtr,
      kernelWorkerStartMs,
      formatFsTimingTotal(resumeStartMs, fsDebugSuffix),
    );
    const responseApplyStartMs = fsTimingStart();
    const fsProcessedResponse = expectKernelWorkerResponse(
      processFsResponse,
      "fs-processed",
      "process-fs",
    );
    if (fsProcessedResponse.fsDebug) {
      appendFsLifecycleEvent(handle, "processed", vmPtr, `:${fsProcessedResponse.fsDebug}`);
    }
    const fsResult = normalizeFsResult(fsProcessedResponse.result);
    traceRegularFileRead(handle, "response", vmPtr, regularFilePendingFs, requestRingState, fsResult);
    traceFailedRegularFileWrite(handle, vmPtr, regularFilePendingFs, requestRingState, fsResult);
    traceRegularFileWriteResume(
      "response",
      handle,
      vmPtr,
      regularFilePendingFs,
      requestRingState,
      fsResult,
    );
    const fsSucceeded = Number(BigInt.asIntN(64, fsResult)) >= 0;
    const responseCreatedFd = fsCreatedFdFromResult(handle, pendingFs, fsResult, fsSucceeded);
    if (FS_CREATED_FD_DEBUG_ENABLED && responseCreatedFd !== null) {
      console.error(
        `[tidemark-fs-created] response:pid=${handle.pid}:vm=0x${vmPtr.toString(16)}`
          + formatCreatedFdResponseSummary(
            responseCreatedFd,
            fsProcessedResponse.fdEntries,
            fsProcessedResponse.ofdSlots,
          ),
      );
    }
    if (fsProcessedResponse.fdEntries) {
      const fdDetail = FS_FD_STATE_DETAIL_ENABLED
        ? `:fds=${formatFdEntrySummary(fsProcessedResponse.fdEntries)}:ofds=${formatOfdSlotSummary(fsProcessedResponse.ofdSlots)}`
        : `:fdEntries=${fsProcessedResponse.fdEntries.length}:ofdSlots=${fsProcessedResponse.ofdSlots?.length ?? 0}`
          + formatCreatedFdResponseSummary(
            responseCreatedFd,
            fsProcessedResponse.fdEntries,
            fsProcessedResponse.ofdSlots,
          );
      appendFsLifecycleEvent(
        handle,
        "fd-response",
        vmPtr,
        fdDetail,
      );
    }
    const threadState = deps.threadWorkerStates.get(worker);
    const fsResponseDisposition = supervisorFsResponseDisposition(
      handle,
      vmPtr,
      pendingFs,
      fsResult,
      threadState,
    );
    const localMetadataActions = supervisorFsLocalMetadataActions(handle, pendingFs, fsResult);
    const responseCurrentPid = handle.kernel.process_current_pid?.(vmPtr) ?? handle.pid;
    if (fsResponseDisposition.adoptKernelState) {
      handle.kernelState = adoptKernelStateForHandle(handle, fsProcessedResponse.kernelState);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
    }
    const regularFileRequestCurrent = regularFilePendingFs === null
      || regularFileRequestStillCurrent(handle, vmPtr, regularFilePendingFs);
    const regularFileReadHasSubmittedIdentity = regularFileReadUsesSubmittedIdentity(
      regularFilePendingFs,
      requestRingState,
    );
    if (
      regularFilePendingFs !== null
      && !regularFileRequestCurrent
      && !regularFileReadHasSubmittedIdentity
      && restoreRegularFileReadBufferSnapshot(handle, regularFileReadBuffer, fsResult)
    ) {
      appendFsLifecycleEvent(
        handle,
        "read-buffer-restored",
        vmPtr,
        `:fd=${regularFilePendingFs.fd}:ofd=${regularFilePendingFs.ofdId}:reason=request-stale`,
      );
    } else if (
      regularFilePendingFs !== null
      && !regularFileRequestCurrent
      && regularFileReadHasSubmittedIdentity
      && regularFileReadBuffer !== null
    ) {
      appendFsLifecycleEvent(
        handle,
        "read-buffer-kept",
        vmPtr,
        `:fd=${regularFilePendingFs.fd}:ofd=${regularFilePendingFs.ofdId}:reason=submitted-identity`,
      );
    }
    if (
      fsResponseDisposition.applyGuestMemoryWrites
      && (regularFileRequestCurrent || regularFileReadHasSubmittedIdentity)
    ) {
      applyFsGuestMemoryWrites(
        handle,
        worker,
        deps.threadWorkerStates,
        fsProcessedResponse.guestMemoryWrites,
        {
          queueThreadWorkerWrites: fsResponseDisposition.queueThreadWorkerWrites,
        },
      );
    } else if (
      fsResponseDisposition.applyGuestMemoryWrites
      && regularFilePendingFs
      && (fsProcessedResponse.guestMemoryWrites?.length ?? 0) > 0
    ) {
      appendFsLifecycleEvent(
        handle,
        "guest-write-skipped",
        vmPtr,
        `:fd=${regularFilePendingFs.fd}:ofd=${regularFilePendingFs.ofdId}:reason=request-stale`,
      );
    }
    if (fsResponseDisposition.storePendingKernelState) {
      appendFsLifecycleEvent(
        handle,
        "child-state-skipped",
        vmPtr,
        `:rv=${fsResult.toString()}:pid=${responseCurrentPid}${fsDebugSuffix}`,
      );
    }
    if (fsResponseDisposition.applyLocalMetadata) {
      applyLocalMetadataMutationAfterFs(handle, vmPtr, pendingFs, localMetadataActions);
    }
    if (fsResponseDisposition.clearTombstones) {
      clearSupersededMemfsDeletedPath(handle, pendingFs, localMetadataActions);
      reapplyLocalMemfsDeletedPathTombstones(handle, vmPtr, "worker-after-adopt");
    }
    const responseFdState = filterStaleFsResponseFdState(
      handle,
      vmPtr,
      requestFsFdState,
      fsProcessedResponse.fdEntries,
      fsProcessedResponse.ofdSlots,
    );
    if (responseFdState.skippedFdEntries > 0 || responseFdState.skippedOfdSlots > 0) {
      appendFsLifecycleEvent(
        handle,
        "fd-stale-skipped",
        vmPtr,
        `:fds=${responseFdState.skippedFdEntries}:ofds=${responseFdState.skippedOfdSlots}`,
      );
    }
    if (
      fsResponseDisposition.adoptKernelState
      && !regularFileOnlyPendingFs
      && (responseFdState.ofdSlots || responseFdState.fdEntries)
    ) {
      const restoreSyscallNr = fsRestoreSyscallNr(handle, pendingFs);
      const createdFd = fsCreatedFdFromResult(handle, pendingFs, fsResult, fsSucceeded);
      const protectedFdEntries = fdEntriesExcludingFd(responseFdState.fdEntries, createdFd);
      const createdFdEntries = fdEntriesForFd(responseFdState.fdEntries, createdFd);
      applyThreadOfdSlots(handle, {
        vmPtr,
        fdEntries: responseFdState.fdEntries,
        ofdSlots: responseFdState.ofdSlots,
        allowRegularFileIdentityReplacement: true,
      });
      applyFsResponseFdEntries(handle, vmPtr, protectedFdEntries, {
        preserveLiveEpollDependentDescriptors: !fsResponseDisposition.fdResponseMayClearLiveFds,
        preserveLiveRegularFileDescriptors: !fsResponseDisposition.fdResponseMayClearLiveFds,
        syscallNr: restoreSyscallNr,
      });
      applyFsResponseFdEntries(handle, vmPtr, createdFdEntries, {
        syscallNr: restoreSyscallNr,
      });
      if (createdFd !== null && createdFdEntries !== undefined) {
        for (const processWorker of processWorkers(handle)) {
          const processThread = deps.threadWorkerStates.get(processWorker);
          if (processThread) {
            appendPendingAuthoritativeFdForNextStep(processThread, createdFd);
          }
        }
      }
      appendFsLifecycleEvent(
        handle,
        "fd-applied",
        vmPtr,
        FS_FD_STATE_DETAIL_ENABLED
          ? `:fds=${formatRuntimeFdSummary(handle, vmPtr)}`
          : formatRuntimeFdForFd(handle, vmPtr, createdFd),
      );
      if (FS_CREATED_FD_DEBUG_ENABLED && createdFd !== null) {
        console.error(
          `[tidemark-fs-created] applied:pid=${handle.pid}:vm=0x${vmPtr.toString(16)}`
            + formatRuntimeFdForFd(handle, vmPtr, createdFd),
        );
      }
    } else if (fsResponseDisposition.adoptKernelState && regularFilePendingFs) {
      const requestFsState = filterFsResponseToRegularFileRequest(
        regularFilePendingFs,
        responseFdState.fdEntries,
        responseFdState.ofdSlots,
      );
      const requestStillCurrent = regularFileRequestCurrent
        || regularFileResponseMatchesCurrentRequest(
          handle,
          vmPtr,
          regularFilePendingFs,
          responseFdState.fdEntries,
          responseFdState.ofdSlots,
        );
      const submittedIdentityOfdApplied = !requestStillCurrent && regularFileReadHasSubmittedIdentity
        ? applySubmittedIdentityRegularFileOfdSlots(
          handle,
          vmPtr,
          regularFilePendingFs,
          fsProcessedResponse.ofdSlots,
        )
        : false;
      if ((requestFsState.fdEntries || requestFsState.ofdSlots) && requestStillCurrent) {
        applyThreadOfdSlots(handle, {
          vmPtr,
          fdEntries: requestFsState.fdEntries,
          ofdSlots: requestFsState.ofdSlots,
          recordRegularFileOfdOffsets: true,
          allowRegularFileIdentityReplacement: true,
        });
        applyFsResponseFdEntries(handle, vmPtr, requestFsState.fdEntries);
        appendFsLifecycleEvent(
          handle,
          "fd-apply-partial",
          vmPtr,
          `:fd=${regularFilePendingFs.fd}:ofd=${regularFilePendingFs.ofdId}`
            + (FS_FD_STATE_DETAIL_ENABLED ? `:fds=${formatRuntimeFdSummary(handle, vmPtr)}` : ""),
        );
      } else if (submittedIdentityOfdApplied) {
        appendFsLifecycleEvent(
          handle,
          "fd-apply-submitted-identity",
          vmPtr,
          `:fd=${regularFilePendingFs.fd}:ofd=${regularFilePendingFs.ofdId}`
            + (FS_FD_STATE_DETAIL_ENABLED ? `:fds=${formatRuntimeFdSummary(handle, vmPtr)}` : ""),
        );
      } else {
        appendFsLifecycleEvent(
          handle,
          "fd-apply-skipped",
          vmPtr,
          `:fd=${regularFilePendingFs.fd}:ofd=${regularFilePendingFs.ofdId}`
            + (requestStillCurrent ? "" : ":reason=request-stale")
            + (requestFsState.skippedIdentityMismatch ? ":reason=identity-mismatch" : "")
            + (FS_FD_STATE_DETAIL_ENABLED ? `:fds=${formatFdEntrySummary(fsProcessedResponse.fdEntries)}` : ""),
        );
      }
    } else if (regularFileOnlyPendingFs) {
      appendFsLifecycleEvent(
        handle,
        "fd-apply-skipped",
        vmPtr,
        FS_FD_STATE_DETAIL_ENABLED ? `:fds=${formatFdEntrySummary(fsProcessedResponse.fdEntries)}` : "",
      );
    }
    appendFsTimingSince(
      handle,
      "response-apply-done",
      vmPtr,
      responseApplyStartMs,
      formatFsTimingTotal(resumeStartMs, fsDebugSuffix),
    );
    const postStateStartMs = fsTimingStart();
    const fsCompletion = supervisorFsCompletionDecision(handle, vmPtr, pendingFs, fsResult, {
      hostMemfsMutated: fsProcessedResponse.memfsMutated,
      hostOfdStateChanged: fsProcessedResponse.ofdStateChanged,
      pendingRingRequest,
      regularFileRequest: regularFileOnlyPendingFs,
    });
    const memfsMutated = fsCompletion.memfsMutated;
    const memfsPublishReady = fsCompletion.memfsPublishReady;
    const ofdStateChanged = fsCompletion.ofdStateChanged;
    recordFsTransferStats(handle, pendingFs, fsResult, fsSucceeded, true, fsCompletion);
    const postFsRingProgress = snapshotFsRingProgress(handle, vmPtr);
    const postFsFdState = snapshotFsFdState(handle, vmPtr, fdStateSnapshotScope);
    if (fsResponseDisposition.adoptKernelState) {
      handle.kernelState = readKernelRuntimeState(handle.kernel);
    }
    if (!processHasWorker(handle, worker) || !threadState) {
      return { kind: "process-gone" };
    }
    if (fsResponseDisposition.storePendingKernelState) {
      const localWorkerKernelState = getPendingKernelStateForNextStep(threadState);
      setPendingKernelStateForNextStep(
        threadState,
        localWorkerKernelState === undefined
          ? fsProcessedResponse.kernelState
          : adoptKernelState(localWorkerKernelState, fsProcessedResponse.kernelState),
      );
    }
    const statusAfterFs = findSupervisorExecveSpawnStatus(handle, [
      handle.kernel.thread_status?.(vmPtr),
      getThreadOwnerAsyncStatus(threadState),
      getThreadActivityState(threadState).lastStatus,
    ]);
    if (
      fsResponseDisposition.resolvedToExecveSpawn
      && statusAfterFs !== undefined
    ) {
      restoreFsFdState(handle, postFsFdState);
      restoreFsRingProgress(handle, postFsRingProgress);
      clearStaleFsStatusAfterResume(handle, vmPtr, fsResult);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      return completeFsResumeFromSupervisorDecision(handle, worker, vmPtr, fsResult, {
        source: "kernel-worker",
        finalStatus: statusAfterFs,
        forceExecveSpawn: true,
        applyPostFsWake: () => {
          deps.publishRuntimeKernelState(handle);
          deps.notifyHostEffectProgress(handle);
        },
      });
    }
    if (fsResponseDisposition.processOwnerMatches) {
      deps.assertProcessStandardFdIntegrity(handle, "resume-fs");
    }
    const syncTargets = forkShareMembers
      .filter((member) => hostExecutionState(member).runningWorker === null);
    const syncSharedOfdAfterFs = ofdStateChanged
      && !regularFileOnlyPendingFs
      && fsResponseDisposition.processOwnerMatches
      && forkShareMembers.length > 0;
    restoreFsFdState(handle, postFsFdState);
    if (fsResponseDisposition.adoptKernelState) {
      handle.kernelState = readKernelRuntimeState(handle.kernel);
    }
    const dirtyMemfsHandles: ProcessHandle[] = [];
    if (memfsMutated) {
      rememberPendingFsMemfsChange(handle, pendingFs);
      setProcessMemfsDirty(handle, false);
      setProcessMemfsPublishPending(handle, true);
      if (fsResponseDisposition.processOwnerMatches) {
        if (memfsPublishReady && forkShareMembers.length > 0) {
          for (const member of forkShareMembers) {
            rememberPendingFsMemfsChange(member, pendingFs);
            dirtyMemfsHandles.push(member);
          }
        }
      }
      dirtyMemfsHandles.push(handle);
    }
    restoreFsFdState(handle, postFsFdState);
    restoreFsRingProgress(handle, postFsRingProgress);
    if (fsResponseDisposition.adoptKernelState) {
      clearStaleFsStatusAfterResume(handle, vmPtr, fsResult);
    }
    if (fsResponseDisposition.adoptKernelState) {
      handle.kernelState = readKernelRuntimeState(handle.kernel);
    }
    appendFsTimingSince(
      handle,
      "post-state-done",
      vmPtr,
      postStateStartMs,
      formatFsTimingTotal(resumeStartMs, fsDebugSuffix),
    );
    let postFsWakeApplied = false;
    const applyPostFsWake = async () => {
      const postWakeStartMs = fsTimingStart();
      if (postFsWakeApplied) {
        return;
      }
      postFsWakeApplied = true;
      if (syncSharedOfdAfterFs) {
        await deps.syncSharedOfdStateAfterFs(handle, syncTargets, { sourceVmPtr: vmPtr });
      }
      for (const dirtyHandle of dirtyMemfsHandles) {
        deps.markRuntimeFsDirty(dirtyHandle);
      }
      deps.publishRuntimeKernelState(handle);
      deps.notifyHostEffectProgress(handle);
      appendFsTimingSince(
        handle,
        "apply-post-wake-inner-done",
        vmPtr,
        postWakeStartMs,
        formatFsTimingTotal(resumeStartMs, fsDebugSuffix),
      );
    };
    const completeStartMs = fsTimingStart();
    traceRegularFileWriteResume(
      "ready",
      handle,
      vmPtr,
      regularFilePendingFs,
      requestRingState,
      fsResult,
    );
    const completeResult = await completeFsResumeFromSupervisorDecision(handle, worker, vmPtr, fsResult, {
      source: "kernel-worker",
      applyPostFsWake,
    });
    appendFsTimingSince(
      handle,
      "inner-complete-done",
      vmPtr,
      completeStartMs,
      formatFsTimingTotal(resumeStartMs, fsDebugSuffix),
    );
    return completeResult;
  } catch (error) {
    const errorDetail = formatFsErrorDetail(error);
    deps.appendWorkerDebugLog(handle, `fs-err:0x${vmPtr.toString(16)}${errorDetail}`);
    appendFsLifecycleEvent(handle, "err", vmPtr, `${formatFsRingDebug(handle, vmPtr)}${errorDetail}`);
    if (!processHasWorker(handle, worker) || !deps.threadWorkerStates.has(worker)) {
      return { kind: "process-gone" };
    }
    deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
    return { kind: "process-gone" };
  } finally {
    clearRegularFileFsInFlight();
  }
}

  async function resumeFsViaKernelWorkerOutcome(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
  ): Promise<KernelRpcResumeOutcome> {
    return resumeFsViaKernelWorker(handle, worker, vmPtr);
  }

  return {
    resumeFsViaKernelWorker,
    resumeFsViaKernelWorkerOutcome,
  };
}
