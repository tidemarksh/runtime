import { attachKernelRuntimeState, readKernelRuntimeState, type KernelExports, type KernelRuntimeState } from "../abi.js";
import type { FsSnapshotEntry } from "../fs/types.js";
import type {
  GuestMemoryWriteSnapshot,
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPipeSlotSnapshot,
} from "../messages.js";
import {
  DEFAULT_RESIDENT_PAGE_SIZE,
  publishResidentPage,
  readResidentFileData,
  resetPageCache,
} from "../fs/sab-page-cache.js";
import { FS_OP_EXECVE } from "../layout/fs-ring.js";
import {
  clearSupervisorPendingBackingFill,
  supervisorBackingFillSpan,
  readSupervisorPendingBackingFill,
  supervisorBackingRetryLimit,
  type SupervisorBackingFillSpan,
  type SupervisorPendingBackingFillRequest,
} from "../fs/supervisor-backing-fill.js";
import { supervisorResidentPageTableAuthority } from "../fs/supervisor-page-table.js";
import {
  restoreThreadWorkerPrepareMemory,
  snapshotThreadWorkerPrepareMemory,
} from "../wasm/prepare-memory.js";
import { ensureMemoryCapacityFor, scratchBaseFor } from "./common.js";
import { createKernelEnv } from "../wasm/env.js";
import { instantiateKernelWithSharedMemory } from "../wasm/kernel-memory.js";
import {
  configureKernelInstanceStack,
  kernelHeapBase,
  KERNEL_WORKER_STACK_SLOT,
} from "../wasm/stack.js";

const FS_PUBLICATION_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FS_PUBLICATION_DEBUG ?? "") === "1";
export const KERNEL_WORKER_PROCESS_CONTROL_OWNERSHIP = "transitional-runtime-owner" as const;

interface CanonicalProcessEntry {
  pid: number;
  ppid: number;
  pgid: number;
  sid: number;
  didExec: boolean;
  vmPtr: number;
}

interface ProcessFsResult {
  kernelState: KernelRuntimeState;
  result: bigint;
  memfsMutated: boolean;
  ofdStateChanged: boolean;
  fdEntries?: KernelWorkerFdEntrySnapshot[];
  ofdSlots?: KernelWorkerOfdSlotSnapshot[];
  guestMemoryWrites?: GuestMemoryWriteSnapshot[];
  fsDebug?: string;
}

interface PendingMmapGuestWriteRequest {
  guestPtrHint: number;
  length: number;
}

type PendingBackingFillRequest = SupervisorPendingBackingFillRequest;

interface ExecveBackingRetryStats {
  attempts: number;
  misses: number;
  published: number;
  exhausted: boolean;
  publishFailed: boolean;
  lastMiss: PendingBackingFillRequest | null;
}

interface ProcessFsProgress {
  phase: string;
  elapsedMs: number;
  detail?: string;
}

type ProcessFsDebug = (phase: string, detail?: string) => void;

interface PendingFsMutationContext {
  effects: number;
  pendingPath: string | null;
  pendingSecondaryPath: string | null;
  fileDataTargetIno: number;
}

interface AppliedFsMutationResult {
  memfsMutated: boolean;
  ofdStateChanged: boolean;
  needsFullSync: boolean;
  treeMutated: boolean;
  fileDataMutated: boolean;
  handledMetadataMutation: boolean;
  handledFileDataMutation: boolean;
}

interface KernelWorkerProcessControlDeps {
  getCanonicalExports(): KernelExports;
  setCanonicalExports(nextExports: KernelExports): void;
  getWasmModule(): WebAssembly.Module | null;
  setWasmModule(module: WebAssembly.Module | null): void;
  getSabPageCache(): SharedArrayBuffer;
  setSabPageCache(pageCache: SharedArrayBuffer): void;
  processKernels: Map<number, KernelExports>;
  lastProcessPipeSlotModes: Map<number, Map<number, number>>;
  canonicalProcesses: Map<number, CanonicalProcessEntry>;
  seedEntries: Map<string, FsSnapshotEntry>;
  encoder: TextEncoder;
  getCanonicalNextPid(): number;
  setCanonicalNextPid(nextPid: number): void;
  publishRootInode(mode?: number): void;
  copyOfdState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
  ): void;
  copyMemfsState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    options?: { includeData?: boolean },
  ): void;
  rebuildPageCacheFromExports(sourceExports: KernelExports): void;
  rebuildPageCacheFromCanonical(): void;
  restorePublishedRegularFileDataFromPageCache?(targetExports?: KernelExports): void;
  restorePublishedRegularFileDataForInode?(ino: number, targetExports?: KernelExports): boolean;
  readBackingFileRangeByInode(ino: number, offset: number, length: number): Uint8Array | null;
  flushDirtyResidentFileByInode(ino: number): boolean;
  truncateDirtyResidentFileByInode(ino: number, size: number): number;
  writeDirtyResidentFileByInode(
    ino: number,
    offset: number,
    data: Uint8Array,
    sourceExports?: KernelExports,
  ): number;
  ensureBackingSourceRangeForInode?(
    ino: number,
    offset: number,
    length: number,
    expectedSize: number,
  ): Promise<boolean>;
  syncPipeState(
    pid: number,
    kernelState: KernelRuntimeState,
    fdEntries?: KernelWorkerFdEntrySnapshot[],
    ofdSlots?: KernelWorkerOfdSlotSnapshot[],
    pipeSlots?: KernelWorkerPipeSlotSnapshot[],
    vmPtrOverride?: number,
    applyFdEntries?: boolean,
  ): { kernelState: KernelRuntimeState; pipeSlots: KernelWorkerPipeSlotSnapshot[] };
  syncEpollState(pid: number, kernelState: KernelRuntimeState): KernelRuntimeState;
  syncSocketState(pid: number, kernelState: KernelRuntimeState): KernelRuntimeState;
  copyCanonicalMemfsToProcess(
    canonicalState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    options?: { includeData?: boolean; targetVmPtr?: number },
  ): void;
  snapshotProcSelfExe(targetExports: KernelExports): Uint8Array | null;
  restoreProcSelfExe(targetExports: KernelExports, target: Uint8Array | null): void;
  composeProcessPipeUsed(processExports: KernelExports, vmPtr: number, sharedPipeUsed: bigint): bigint;
  collectProcessHeldEpollOfdIds(processExports: KernelExports, vmPtr: number): number[];
  collectProcessHeldPipeSlots(processExports: KernelExports, vmPtr: number): Set<number>;
  mergePipeSlotsFromCanonical(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    targetVmPtr: number,
    pipeSlots: Iterable<number>,
  ): void;
  recomputeCanonicalPipeControlState(): {
    kernelState: KernelRuntimeState;
    readOpen: Int32Array;
    writeOpen: Int32Array;
    managedPipeSlots?: number[];
  };
  applyPipeControlState(
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    readOpen: Int32Array,
    writeOpen: Int32Array,
    pipeUsed: bigint,
    managedPipeSlots: Iterable<number>,
  ): void;
  copyEpollState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
  ): void;
  copySelectedEpollState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    epollOfdIds: Iterable<number>,
  ): void;
  copySocketState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
  ): void;
  syncFileDataMutation(sourceExports: KernelExports, ino: number): boolean;
  syncFileDataMutationByPath(sourceExports: KernelExports, path: string): boolean;
  syncMetadataMutation(sourceExports: KernelExports, path: string): boolean;
  fanoutCanonicalMemfs(includeData?: boolean): void;
  fsEffectMemfsMutation: number;
  fsEffectSharedOfdState: number;
  fsEffectMemfsFileData: number;
  fsOpOpenAt: number;
  fsOpMkdir: number;
  fsOpUnlink: number;
}

export function createKernelWorkerProcessControl(deps: KernelWorkerProcessControlDeps) {
  const decoder = new TextDecoder();

  function decodeKernelBytes(memory: WebAssembly.Memory, ptr: number, length: number): string {
    const bytes = new Uint8Array(length);
    bytes.set(new Uint8Array(memory.buffer, ptr, length));
    return decoder.decode(bytes);
  }

  function readPendingFsPath(processExports: KernelExports, vmPtr: number, secondary = false): string | null {
    const absPathPtr = scratchBaseFor(processExports) + 0x8000 + (secondary ? 4096 : 0);
    ensureMemoryCapacityFor(processExports.memory, absPathPtr + 4096);
    const absPathLen = Number(
      (secondary
        ? processExports.thread_pending_fs_secondary_absolute_path?.(vmPtr, absPathPtr, 4096)
        : processExports.thread_pending_fs_absolute_path?.(vmPtr, absPathPtr, 4096)) ?? 0n,
    );
    if (absPathLen > 0 && absPathPtr + absPathLen <= processExports.memory.buffer.byteLength) {
      return decodeKernelBytes(processExports.memory, absPathPtr, absPathLen);
    }
    if (secondary) {
      return null;
    }
    const pathPtr = processExports.thread_pending_fs_path_ptr?.(vmPtr) ?? 0n;
    const pathLen = processExports.thread_pending_fs_path_len?.(vmPtr) ?? 0;
    const guestBase = processExports.thread_guest_base?.(vmPtr) ?? 0;
    const pathPtrBig = BigInt(pathPtr);
    if (pathPtrBig === 0n || pathLen === 0 || guestBase === 0) {
      return null;
    }
    const linearPtrBig = BigInt(guestBase) + pathPtrBig;
    if (linearPtrBig > BigInt(Number.MAX_SAFE_INTEGER)) {
      return null;
    }
    const linearPtr = Number(linearPtrBig);
    if (linearPtr + pathLen > processExports.memory.buffer.byteLength) {
      return null;
    }
    return decodeKernelBytes(processExports.memory, linearPtr, pathLen);
  }

  function fallbackToMetadataMemfsSync(
    processExports: KernelExports,
    canonicalExports: KernelExports,
  ): void {
    const sourceState = readKernelRuntimeState(processExports);
    const canonicalState = readKernelRuntimeState(canonicalExports);
    const canonicalProcSelfExe = deps.snapshotProcSelfExe(canonicalExports);
    deps.copyMemfsState(processExports, sourceState, canonicalExports, canonicalState, {
      includeData: false,
    });
    deps.restoreProcSelfExe(canonicalExports, canonicalProcSelfExe);
    deps.restorePublishedRegularFileDataFromPageCache?.();
    deps.rebuildPageCacheFromCanonical();
    deps.fanoutCanonicalMemfs(false);
  }

  function writeResidentFileRange(
    ino: number,
    offset: number,
    data: Uint8Array,
    sourceExports?: KernelExports,
  ): number {
    return deps.writeDirtyResidentFileByInode(ino, offset, data, sourceExports);
  }

  function truncateResidentFile(ino: number, size: number): number {
    return deps.truncateDirtyResidentFileByInode(ino, size);
  }

  function readPendingFsMutationContext(
    processExports: KernelExports,
    vmPtr: number,
  ): PendingFsMutationContext {
    const effects = processExports.thread_pending_fs_effects(vmPtr);
    return {
      effects,
      pendingPath: (effects & (deps.fsEffectMemfsMutation | deps.fsEffectMemfsFileData)) !== 0
        ? readPendingFsPath(processExports, vmPtr)
        : null,
      pendingSecondaryPath: (effects & deps.fsEffectMemfsMutation) !== 0
        ? readPendingFsPath(processExports, vmPtr, true)
        : null,
      fileDataTargetIno: (effects & deps.fsEffectMemfsFileData) !== 0
        ? processExports.thread_pending_fs_target_ino(vmPtr)
        : 0,
    };
  }

  function supervisorU64ToSafeNumber(value: bigint | number | undefined): number | null {
    if (value === undefined) {
      return null;
    }
    const numeric = typeof value === "bigint" ? Number(value) : value;
    if (!Number.isSafeInteger(numeric) || numeric < 0) {
      return null;
    }
    return numeric;
  }

  function supervisorNumber(value: bigint | number | undefined, name: string): number {
    const numeric = typeof value === "bigint" ? Number(value) : value;
    if (typeof numeric !== "number" || !Number.isSafeInteger(numeric)) {
      throw new RangeError(`invalid ${name}: ${String(value)}`);
    }
    return numeric;
  }

  function supervisorNumericExport(
    processExports: KernelExports,
    name:
      | "supervisor_fs_metadata_full_sync_action_none"
      | "supervisor_fs_metadata_full_sync_action_required"
      | "supervisor_fs_backing_fill_mode_direct"
      | "supervisor_fs_backing_fill_mode_resident",
    label: string,
  ): number {
    const read = processExports[name];
    if (typeof read !== "function") {
      throw new Error(`missing ${name} export`);
    }
    return supervisorNumber(read(), label);
  }

  function supervisorMetadataFullSyncRequired(
    processExports: KernelExports,
    treeMutated: boolean,
    handledMetadataMutation: boolean,
  ): boolean {
    const decide = processExports.supervisor_fs_metadata_full_sync_decision;
    if (typeof decide !== "function") {
      throw new Error("missing supervisor_fs_metadata_full_sync_decision export");
    }
    const decision = supervisorNumber(
      decide(treeMutated ? 1 : 0, handledMetadataMutation ? 1 : 0),
      "supervisor metadata full-sync decision",
    );
    const none = supervisorNumericExport(
      processExports,
      "supervisor_fs_metadata_full_sync_action_none",
      "supervisor metadata full-sync none action",
    );
    const required = supervisorNumericExport(
      processExports,
      "supervisor_fs_metadata_full_sync_action_required",
      "supervisor metadata full-sync required action",
    );
    if (decision === none) {
      return false;
    }
    if (decision === required) {
      return true;
    }
    throw new Error(`unknown supervisor metadata full-sync decision: ${decision}`);
  }

  function readPendingMmapGuestWriteRequest(
    processExports: KernelExports,
    vmPtr: number,
  ): PendingMmapGuestWriteRequest | null {
    if (
      typeof processExports.supervisor_fs_pending_mmap_guest_write_ptr !== "function"
      || typeof processExports.supervisor_fs_pending_mmap_guest_write_len !== "function"
    ) {
      throw new Error("missing supervisor pending mmap guest-write exports");
    }
    const guestPtrHint = supervisorU64ToSafeNumber(
      processExports.supervisor_fs_pending_mmap_guest_write_ptr(vmPtr),
    );
    const length = supervisorU64ToSafeNumber(
      processExports.supervisor_fs_pending_mmap_guest_write_len(vmPtr),
    );
    if (guestPtrHint === null || length === null || length <= 0) {
      return null;
    }
    return { guestPtrHint, length };
  }

  function readPendingBackingFillRequest(
    processExports: KernelExports,
    vmPtr: number,
  ): PendingBackingFillRequest | null {
    return readSupervisorPendingBackingFill(processExports, vmPtr);
  }

  function backingProbeIsReadable(ino: number, offset: number, length: number): boolean {
    const range = deps.readBackingFileRangeByInode(ino, offset, length);
    return range !== null && range.length === length;
  }

  function supervisorBackingFillShouldProbeDirect(
    processExports: KernelExports,
    request: PendingBackingFillRequest,
    spanLength: number,
  ): boolean {
    const decide = processExports.supervisor_fs_backing_fill_probe_direct;
    if (typeof decide !== "function") {
      throw new Error("missing supervisor_fs_backing_fill_probe_direct export");
    }
    const result = supervisorNumber(
      decide(request.opcode ?? 0, BigInt(spanLength)),
      "supervisor backing-fill probe decision",
    );
    if (result < 0) {
      throw new Error(`supervisor backing-fill probe decision failed: ${result}`);
    }
    return result !== 0;
  }

  function supervisorBackingFillUsesDirect(
    processExports: KernelExports,
    request: PendingBackingFillRequest,
    spanLength: number,
    headProbeReadable: boolean,
    tailProbeReadable: boolean,
  ): boolean {
    const decide = processExports.supervisor_fs_backing_fill_mode_decision;
    if (typeof decide !== "function") {
      throw new Error("missing supervisor_fs_backing_fill_mode_decision export");
    }
    const mode = supervisorNumber(
      decide(
        request.opcode ?? 0,
        BigInt(spanLength),
        headProbeReadable ? 1 : 0,
        tailProbeReadable ? 1 : 0,
      ),
      "supervisor backing-fill mode decision",
    );
    if (mode < 0) {
      throw new Error(`supervisor backing-fill mode decision failed: ${mode}`);
    }
    const direct = supervisorNumericExport(
      processExports,
      "supervisor_fs_backing_fill_mode_direct",
      "supervisor backing-fill direct mode",
    );
    const resident = supervisorNumericExport(
      processExports,
      "supervisor_fs_backing_fill_mode_resident",
      "supervisor backing-fill resident mode",
    );
    if (mode === direct) {
      return true;
    }
    if (mode === resident) {
      return false;
    }
    throw new Error(`unknown supervisor backing-fill mode decision: ${mode}`);
  }

  function hasDirectBackingForFill(
    processExports: KernelExports,
    request: PendingBackingFillRequest,
    span: SupervisorBackingFillSpan,
  ): boolean {
    if (!supervisorBackingFillShouldProbeDirect(processExports, request, span.length)) {
      return false;
    }
    const probeLength = Math.min(DEFAULT_RESIDENT_PAGE_SIZE, span.length);
    const headProbeReadable = backingProbeIsReadable(request.ino, span.offset, probeLength);
    const tailOffset = span.end - probeLength;
    const tailProbeReadable = tailOffset === span.offset
      ? headProbeReadable
      : backingProbeIsReadable(request.ino, tailOffset, probeLength);
    return supervisorBackingFillUsesDirect(
      processExports,
      request,
      span.length,
      headProbeReadable,
      tailProbeReadable,
    );
  }

  async function publishBackingRange(
    processExports: KernelExports,
    request: PendingBackingFillRequest,
  ): Promise<boolean> {
    const span = supervisorBackingFillSpan(processExports, request);
    if (!span) {
      return false;
    }
    if (hasDirectBackingForFill(processExports, request, span)) {
      return true;
    }
    let alignedRange = deps.readBackingFileRangeByInode(request.ino, span.offset, span.length);
    if (alignedRange === null) {
      const ensured = await deps.ensureBackingSourceRangeForInode?.(
        request.ino,
        span.offset,
        span.length,
        request.expectedSize,
      );
      if (ensured === false) {
        return false;
      }
      alignedRange = deps.readBackingFileRangeByInode(request.ino, span.offset, span.length);
    }
    if (alignedRange === null) {
      return false;
    }
    const sab = deps.getSabPageCache();
    for (let pageStart = span.offset; pageStart < span.end; pageStart += DEFAULT_RESIDENT_PAGE_SIZE) {
      const pageLen = Math.min(DEFAULT_RESIDENT_PAGE_SIZE, request.expectedSize - pageStart);
      if (pageLen <= 0 || readResidentFileData(sab, request.ino, pageStart, pageLen)) {
        continue;
      }
      const backingOffset = pageStart - span.offset;
      const page = alignedRange.subarray(backingOffset, backingOffset + pageLen);
      if (page.length !== pageLen) {
        return false;
      }
      if (!publishResidentPage(sab, request.ino, pageStart / DEFAULT_RESIDENT_PAGE_SIZE, page, {
        authority: supervisorResidentPageTableAuthority(processExports),
      })) {
        return false;
      }
    }
    if (readResidentFileData(sab, request.ino, request.offset, request.length) === null) {
      return false;
    }
    return true;
  }

  async function ensurePendingReadBackingRange(
    processExports: KernelExports,
    vmPtr: number,
  ): Promise<void> {
    const request = readPendingBackingFillRequest(processExports, vmPtr);
    if (request) {
      const published = await publishBackingRange(processExports, request);
      if (!published) {
        throw new Error(`failed to publish backing range: ${formatBackingFillDebug(request)}`);
      }
    }
  }

  async function processFsWithExecveBackingRetry(
    processExports: KernelExports,
    vmPtr: number,
    debug?: ProcessFsDebug,
  ): Promise<ExecveBackingRetryStats> {
    const stats: ExecveBackingRetryStats = {
      attempts: 0,
      misses: 0,
      published: 0,
      exhausted: false,
      publishFailed: false,
      lastMiss: null,
    };
    let maxAttempts = 1;
    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      stats.attempts += 1;
      debug?.("thread_process_fs-start", `attempt=${attempt}`);
      processExports.thread_process_fs(vmPtr);
      debug?.("thread_process_fs-done", `attempt=${attempt}:status=${processExports.thread_status?.(vmPtr) ?? "n/a"}`);
      const miss = readPendingBackingFillRequest(processExports, vmPtr);
      if (!miss || miss.opcode !== FS_OP_EXECVE) {
        return stats;
      }
      stats.misses += 1;
      stats.lastMiss = miss;
      maxAttempts = Math.max(maxAttempts, supervisorBackingRetryLimit(processExports, miss));
      debug?.("execve-backing-miss", formatBackingFillDebug(miss));
      debug?.("execve-backing-publish-start", formatBackingFillDebug(miss));
      const published = await publishBackingRange(processExports, miss);
      debug?.("execve-backing-publish-done", `published=${published ? 1 : 0}`);
      clearSupervisorPendingBackingFill(processExports, vmPtr);
      if (!published) {
        stats.publishFailed = true;
        return stats;
      }
      stats.published += 1;
    }
    stats.exhausted = true;
    return stats;
  }

  function formatBackingFillDebug(request: PendingBackingFillRequest | null): string {
    if (request === null) {
      return "backing=none";
    }
    return [
      `ino=${request.ino}`,
      `off=${request.offset}`,
      `len=${request.length}`,
      `expected=${request.expectedSize}`,
    ].join(":");
  }

  function formatMmapGuestWriteDebug(request: PendingMmapGuestWriteRequest | null): string {
    if (request === null) {
      return "mmap=none";
    }
    return `mmap=hint=0x${request.guestPtrHint.toString(16)}:len=${request.length}`;
  }

  function pendingPageCacheRestoreInodes(
    pendingContext: PendingFsMutationContext,
    pendingMmapGuestWrite: PendingMmapGuestWriteRequest | null,
    pendingBackingFill: PendingBackingFillRequest | null,
  ): number[] | null {
    const inodes = new Set<number>();
    if (pendingMmapGuestWrite !== null) {
      const mmapIno = pendingBackingFill?.ino ?? 0;
      if (mmapIno <= 0) {
        return null;
      }
      inodes.add(mmapIno);
    }
    if ((pendingContext.effects & deps.fsEffectMemfsFileData) !== 0) {
      if (pendingContext.fileDataTargetIno <= 0) {
        return null;
      }
      inodes.add(pendingContext.fileDataTargetIno);
    }
    return [...inodes];
  }

  function restorePublishedRegularFileDataForPendingFs(
    processExports: KernelExports,
    pendingContext: PendingFsMutationContext,
    pendingMmapGuestWrite: PendingMmapGuestWriteRequest | null,
    pendingBackingFill: PendingBackingFillRequest | null,
  ): string {
    const targetInodes = pendingPageCacheRestoreInodes(
      pendingContext,
      pendingMmapGuestWrite,
      pendingBackingFill,
    );
    if (targetInodes !== null && deps.restorePublishedRegularFileDataForInode) {
      for (const ino of targetInodes) {
        deps.restorePublishedRegularFileDataForInode(ino, processExports);
      }
      return targetInodes.length === 0 ? "targeted:none" : `targeted:${targetInodes.join(",")}`;
    }
    deps.restorePublishedRegularFileDataFromPageCache?.(processExports);
    return "full";
  }

  function snapshotMmapGuestMemoryWrite(
    processExports: KernelExports,
    vmPtr: number,
    request: PendingMmapGuestWriteRequest | null,
    result: bigint,
  ): GuestMemoryWriteSnapshot[] | undefined {
    if (request === null || result < 0n) {
      return undefined;
    }
    const returnedGuestPtr = Number(result);
    const guestPtr = returnedGuestPtr > 0 ? returnedGuestPtr : request.guestPtrHint;
    const snapshotLen = processExports.supervisor_fs_pending_mmap_guest_write_snapshot_len;
    if (typeof snapshotLen !== "function") {
      throw new Error("missing supervisor_fs_pending_mmap_guest_write_snapshot_len export");
    }
    const length = supervisorU64ToSafeNumber(
      snapshotLen(vmPtr),
    );
    const guestBase = processExports.thread_guest_base?.(vmPtr) ?? 0;
    const start = guestBase + guestPtr;
    const end = length === null ? -1 : start + length;
    if (
      !Number.isFinite(guestPtr)
      || length === null
      || guestPtr <= 0
      || length <= 0
      || start < 0
      || end > processExports.memory.buffer.byteLength
    ) {
      return undefined;
    }
    return [{
      guestPtr,
      bytes: new Uint8Array(processExports.memory.buffer.slice(start, end)),
    }];
  }

  function applyPendingFsMutation(
    processExports: KernelExports,
    context: PendingFsMutationContext,
    fsSucceeded: boolean,
  ): AppliedFsMutationResult {
    const treeMutated = fsSucceeded && (context.effects & deps.fsEffectMemfsMutation) !== 0;
    const fileDataMutated = fsSucceeded && (context.effects & deps.fsEffectMemfsFileData) !== 0;
    let handledFileDataMutation = false;
    let handledMetadataMutation = false;

    if (fileDataMutated && context.fileDataTargetIno !== 0) {
      handledFileDataMutation = deps.syncFileDataMutation(processExports, context.fileDataTargetIno);
    } else if (fileDataMutated && context.pendingPath) {
      handledFileDataMutation = deps.syncFileDataMutationByPath(processExports, context.pendingPath);
    }

    if (treeMutated && context.pendingPath) {
      handledMetadataMutation = deps.syncMetadataMutation(processExports, context.pendingPath);
      if (
        handledMetadataMutation
        && context.pendingSecondaryPath
        && context.pendingSecondaryPath !== context.pendingPath
      ) {
        handledMetadataMutation = deps.syncMetadataMutation(processExports, context.pendingSecondaryPath);
      }
    }

    const needsMetadataFullSync = supervisorMetadataFullSyncRequired(
      processExports,
      treeMutated,
      handledMetadataMutation,
    );

    return {
      memfsMutated: treeMutated || fileDataMutated,
      ofdStateChanged: fsSucceeded && (context.effects & deps.fsEffectSharedOfdState) !== 0,
      needsFullSync: needsMetadataFullSync,
      treeMutated,
      fileDataMutated,
      handledMetadataMutation,
      handledFileDataMutation,
    };
  }

  function formatProcessFsDebug(
    pid: number,
    vmPtr: number,
    context: PendingFsMutationContext,
    fsSucceeded: boolean,
    mutation: AppliedFsMutationResult,
    fullSyncApplied: boolean,
    execveRetryStats: ExecveBackingRetryStats,
  ): string {
    const path = context.pendingPath ? `:path=${JSON.stringify(context.pendingPath)}` : "";
    const path2 = context.pendingSecondaryPath ? `:path2=${JSON.stringify(context.pendingSecondaryPath)}` : "";
    const lastExecveMiss = execveRetryStats.lastMiss
      ? `:execveLast=ino=${execveRetryStats.lastMiss.ino}/off=${execveRetryStats.lastMiss.offset}/len=${execveRetryStats.lastMiss.length}/size=${execveRetryStats.lastMiss.expectedSize}`
      : "";
    return [
      `pid=${pid}`,
      `vm=0x${vmPtr.toString(16)}`,
      `ok=${fsSucceeded ? 1 : 0}`,
      `fx=${context.effects}`,
      `ino=${context.fileDataTargetIno}`,
      `tree=${mutation.treeMutated ? 1 : 0}`,
      `file=${mutation.fileDataMutated ? 1 : 0}`,
      `metaFast=${mutation.handledMetadataMutation ? 1 : 0}`,
      `dataFast=${mutation.handledFileDataMutation ? 1 : 0}`,
      `full=${fullSyncApplied ? 1 : 0}`,
      `execveAttempts=${execveRetryStats.attempts}`,
      `execveMisses=${execveRetryStats.misses}`,
      `execvePublished=${execveRetryStats.published}`,
      `execveExhausted=${execveRetryStats.exhausted ? 1 : 0}`,
      `execvePublishFailed=${execveRetryStats.publishFailed ? 1 : 0}`,
      `${path}${path2}`,
      `${lastExecveMiss}`,
    ].join(":");
  }

  function shouldEmitProcessFsDebug(
    _context: PendingFsMutationContext,
    mutation: AppliedFsMutationResult,
    fullSyncApplied: boolean,
    execveRetryStats: ExecveBackingRetryStats,
  ): boolean {
    if (
      execveRetryStats.exhausted
      || execveRetryStats.publishFailed
    ) {
      return true;
    }
    if (!FS_PUBLICATION_DEBUG_ENABLED) {
      return false;
    }
    if (
      fullSyncApplied
      || (mutation.fileDataMutated && !mutation.handledFileDataMutation)
      || execveRetryStats.misses > 0
    ) {
      return true;
    }
    return false;
  }

  async function initKernel(
    module: WebAssembly.Module,
    pageCache: SharedArrayBuffer,
    maxMemoryPages = 32768,
  ): Promise<KernelRuntimeState> {
    deps.setWasmModule(module);
    deps.setSabPageCache(pageCache);

    const boundedMaxMemoryPages = Math.max(maxMemoryPages, 1024);
    const { instance } = await instantiateKernelWithSharedMemory(module, {
      initialPages: 1024,
      maximumPages: boundedMaxMemoryPages,
    }, {
      sabPageCache: pageCache,
      flushResidentFileDataByInode: deps.flushDirtyResidentFileByInode,
      truncateResidentFileDataByInode: truncateResidentFile,
      writeResidentFileDataByInode: writeResidentFileRange,
    });
    const nextExports = instance.exports as unknown as KernelExports;
    configureKernelInstanceStack(nextExports, KERNEL_WORKER_STACK_SLOT);
    deps.setCanonicalExports(nextExports);

    const wasmHeapBase = kernelHeapBase(nextExports, 0x200000);
    nextExports.kernel_init(wasmHeapBase);

    const now = Date.now();
    nextExports.set_base_time(
      BigInt(Math.floor(now / 1000)),
      BigInt((now % 1000) * 1_000_000),
    );

    deps.seedEntries.clear();
    deps.seedEntries.set("/", { kind: "dir", path: "/", mode: 0o755 });
    resetPageCache(pageCache);
    deps.publishRootInode();
    deps.setCanonicalNextPid(3);
    deps.canonicalProcesses.clear();
    deps.canonicalProcesses.set(1, {
      pid: 1,
      ppid: 0,
      pgid: 1,
      sid: 1,
      didExec: false,
      vmPtr: 0,
    });

    return readKernelRuntimeState(nextExports);
  }

  async function registerProcess(
    pid: number,
    vmPtr: number,
    memory: WebAssembly.Memory,
    kernelState: KernelRuntimeState,
    seedMemfsMode: "full" | "metadata" | "none" = "full",
    pipeSeedMode: "canonical" | "process" = "canonical",
  ): Promise<KernelRuntimeState> {
    const wasmModule = deps.getWasmModule();
    if (wasmModule === null) {
      throw new Error("kernel worker is not initialized");
    }
    const canonicalExports = deps.getCanonicalExports();
    const memorySnapshot = snapshotThreadWorkerPrepareMemory(
      memory,
      null,
      kernelState,
      kernelState.heapBase,
      { includeMemfsData: seedMemfsMode === "none" },
    );
    let processExports: KernelExports | null = null;
    const instance = await WebAssembly.instantiate(wasmModule, {
      env: createKernelEnv(memory, {
        sabPageCache: deps.getSabPageCache(),
        readResidentFileDataByInode: deps.readBackingFileRangeByInode,
        flushResidentFileDataByInode: deps.flushDirtyResidentFileByInode,
        truncateResidentFileDataByInode: truncateResidentFile,
        writeResidentFileDataByInode: (ino, offset, data) => (
          writeResidentFileRange(ino, offset, data, processExports ?? undefined)
        ),
      }),
    });
    // Registering a process must not mutate the caller-owned shared memory.
    // Browsers can replay data segments on instantiation. Restore the live
    // thread/process regions that sit outside the static kernel image instead
    // of snapshotting the entire shared linear memory.
    restoreThreadWorkerPrepareMemory(memory, memorySnapshot);
    processExports = instance.exports as unknown as KernelExports;
    configureKernelInstanceStack(processExports, KERNEL_WORKER_STACK_SLOT);
    attachKernelRuntimeState(processExports, kernelState);
    deps.processKernels.set(pid, processExports);
    const existingProcess = deps.canonicalProcesses.get(pid);
    deps.canonicalProcesses.set(pid, {
      pid,
      ppid: existingProcess?.ppid ?? 0,
      pgid: existingProcess?.pgid ?? pid,
      sid: existingProcess?.sid ?? pid,
      didExec: existingProcess?.didExec ?? false,
      vmPtr,
    });
    deps.setCanonicalNextPid(Math.max(deps.getCanonicalNextPid(), pid + 1));
    if (deps.processKernels.size === 1) {
      const sourceState = readKernelRuntimeState(processExports);
      const canonicalState = readKernelRuntimeState(canonicalExports);
      const canonicalHasSeedSnapshot = deps.seedEntries.size > 1;
      deps.copyOfdState(processExports, sourceState, canonicalExports, canonicalState);
      if (seedMemfsMode === "full" && !canonicalHasSeedSnapshot) {
        deps.copyMemfsState(processExports, sourceState, canonicalExports, canonicalState, {
          includeData: false,
        });
        deps.rebuildPageCacheFromExports(processExports);
      }
      const pipeState = deps.syncPipeState(pid, readKernelRuntimeState(processExports)).kernelState;
      const epollState = deps.syncEpollState(pid, pipeState);
      const socketState = deps.syncSocketState(pid, epollState);
      if (seedMemfsMode !== "none") {
        const memberState = readKernelRuntimeState(processExports);
        const updatedCanonicalState = readKernelRuntimeState(canonicalExports);
        deps.copyCanonicalMemfsToProcess(updatedCanonicalState, processExports, memberState, {
          includeData: seedMemfsMode === "full",
          targetVmPtr: vmPtr,
        });
      }
      return readKernelRuntimeState(processExports) ?? socketState;
    }

    const canonicalState = readKernelRuntimeState(canonicalExports);
    const processState = readKernelRuntimeState(processExports);
    if (pipeSeedMode === "process") {
      deps.syncPipeState(pid, processState);
    } else {
      attachKernelRuntimeState(processExports, {
        ...processState,
        pipeUsed: deps.composeProcessPipeUsed(processExports, vmPtr, canonicalState.pipeUsed),
        socketUsed: canonicalState.socketUsed,
        netBufUsed: canonicalState.netBufUsed,
      });
      const registeredProcessState = readKernelRuntimeState(processExports);
      const sharedPipeSlots = deps.collectProcessHeldPipeSlots(processExports, vmPtr);
      deps.mergePipeSlotsFromCanonical(
        canonicalExports,
        canonicalState,
        processExports,
        registeredProcessState,
        vmPtr,
        sharedPipeSlots,
      );
      const { readOpen, writeOpen } = deps.recomputeCanonicalPipeControlState();
      deps.applyPipeControlState(
        processExports,
        registeredProcessState,
        readOpen,
        writeOpen,
        deps.composeProcessPipeUsed(processExports, vmPtr, readKernelRuntimeState(canonicalExports).pipeUsed),
        sharedPipeSlots,
      );
    }
    const registeredCanonicalState = readKernelRuntimeState(canonicalExports);
    const registeredProcessState = readKernelRuntimeState(processExports);
    if (seedMemfsMode !== "none") {
      deps.copyCanonicalMemfsToProcess(registeredCanonicalState, processExports, registeredProcessState, {
        includeData: seedMemfsMode === "full",
        targetVmPtr: vmPtr,
      });
    }
    deps.copySelectedEpollState(
      canonicalExports,
      registeredCanonicalState,
      processExports,
      registeredProcessState,
      deps.collectProcessHeldEpollOfdIds(processExports, vmPtr),
    );
    deps.copySocketState(canonicalExports, registeredCanonicalState, processExports, registeredProcessState);
    return readKernelRuntimeState(processExports);
  }

  function unregisterProcess(pid: number): void {
    deps.processKernels.delete(pid);
    deps.lastProcessPipeSlotModes.delete(pid);
    deps.canonicalProcesses.delete(pid);
    for (const processExports of deps.processKernels.values()) {
      processExports.process_remove_process_entry(pid);
    }
    const {
      kernelState: canonicalState,
      readOpen,
      writeOpen,
      managedPipeSlots = [],
    } = deps.recomputeCanonicalPipeControlState();
    for (const [memberPid, memberExports] of deps.processKernels) {
      const memberProcess = deps.canonicalProcesses.get(memberPid);
      const memberState = readKernelRuntimeState(memberExports);
      const memberPipeUsed = memberProcess?.vmPtr
        ? deps.composeProcessPipeUsed(memberExports, memberProcess.vmPtr, canonicalState.pipeUsed)
        : canonicalState.pipeUsed;
      deps.applyPipeControlState(
        memberExports,
        memberState,
        readOpen,
        writeOpen,
        memberPipeUsed,
        managedPipeSlots,
      );
    }
  }

  function debugProcesses(): { pids: number[]; canonicalPids: number[] } {
    return {
      pids: [...deps.processKernels.keys()].sort((left, right) => left - right),
      canonicalPids: [...deps.canonicalProcesses.keys()].sort((left, right) => left - right),
    };
  }

  function requireProcessKernel(pid: number): KernelExports {
    const processExports = deps.processKernels.get(pid);
    if (!processExports) {
      throw new Error(`process ${pid} is not registered`);
    }
    return processExports;
  }

  function snapshotProcessFdEntries(
    processExports: KernelExports,
    vmPtr: number,
  ): KernelWorkerFdEntrySnapshot[] {
    const maxFds = processExports.abi_max_fds();
    const fdEntries: KernelWorkerFdEntrySnapshot[] = [];
    for (let fd = 0; fd < maxFds; fd += 1) {
      fdEntries.push({
        fd,
        ofdId: processExports.process_fd_ofd_id(vmPtr, fd),
        fdFlags: processExports.process_fd_flags(vmPtr, fd),
        generation: processExports.process_fd_generation?.(vmPtr, fd),
      });
    }
    return fdEntries;
  }

  function snapshotOfdSlotsForFdEntries(
    processExports: KernelExports,
    kernelState: KernelRuntimeState,
    fdEntries: readonly KernelWorkerFdEntrySnapshot[],
  ): KernelWorkerOfdSlotSnapshot[] {
    const maxFds = processExports.abi_max_fds();
    const tableBytes = processExports.kernel_ofd_table_bytes();
    const slotBytes = tableBytes / maxFds;
    if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
      return [];
    }
    const memory = new Uint8Array(processExports.memory.buffer);
    const ofdSlots: KernelWorkerOfdSlotSnapshot[] = [];
    for (const ofdId of [...new Set(fdEntries.map((entry) => entry.ofdId))]) {
      if (ofdId <= 0 || ofdId >= maxFds) {
        continue;
      }
      const offset = kernelState.ofdTablePtr + ofdId * slotBytes;
      ofdSlots.push({
        ofdId,
        slot: memory.slice(offset, offset + slotBytes),
      });
    }
    return ofdSlots;
  }

  async function processFs(
    pid: number,
    vmPtr: number,
    kernelState: KernelRuntimeState,
    postProgress?: (progress: ProcessFsProgress) => void,
  ): Promise<ProcessFsResult> {
    const startedMs = Date.now();
    const debug: ProcessFsDebug | undefined = postProgress
      ? (phase, detail) => {
        postProgress({
          phase,
          elapsedMs: Date.now() - startedMs,
          ...(detail ? { detail } : {}),
        });
      }
      : undefined;
    const canonicalExports = deps.getCanonicalExports();
    const processExports = requireProcessKernel(pid);
    debug?.("start");
    attachKernelRuntimeState(processExports, kernelState);
    debug?.("attached-kernel-state");
    const pendingContext = readPendingFsMutationContext(processExports, vmPtr);
    const pendingMmapGuestWrite = readPendingMmapGuestWriteRequest(processExports, vmPtr);
    const pendingBackingFill = readPendingBackingFillRequest(processExports, vmPtr);
    debug?.(
      "pending-read",
      [
        `effects=${pendingContext.effects}`,
        formatMmapGuestWriteDebug(pendingMmapGuestWrite),
        formatBackingFillDebug(pendingBackingFill),
      ].join(":"),
    );
    debug?.("ensure-backing-start", formatBackingFillDebug(pendingBackingFill));
    await ensurePendingReadBackingRange(processExports, vmPtr);
    debug?.("ensure-backing-done");
    const shouldRestorePageCacheForFs =
      pendingMmapGuestWrite !== null
      || (pendingContext.effects & deps.fsEffectMemfsFileData) !== 0;
    if (shouldRestorePageCacheForFs) {
      debug?.(
        "restore-page-cache-start",
        pendingMmapGuestWrite !== null
          ? formatMmapGuestWriteDebug(pendingMmapGuestWrite)
          : `file-data:ino=${pendingContext.fileDataTargetIno}`,
      );
      const restored = restorePublishedRegularFileDataForPendingFs(
        processExports,
        pendingContext,
        pendingMmapGuestWrite,
        pendingBackingFill,
      );
      debug?.("restore-page-cache-done", restored);
    }
    debug?.("process-fs-kernel-start");
    const execveRetryStats = await processFsWithExecveBackingRetry(processExports, vmPtr, debug);
    debug?.("process-fs-kernel-done", `attempts=${execveRetryStats.attempts}:misses=${execveRetryStats.misses}`);
    debug?.("resume-fs-start");
    const result = BigInt(processExports.thread_resume_fs(vmPtr));
    debug?.("resume-fs-done", `result=${result.toString()}:status=${processExports.thread_status?.(vmPtr) ?? "n/a"}`);
    const fsSucceeded = Number(BigInt.asIntN(64, result)) >= 0;
    debug?.("snapshot-mmap-start", formatMmapGuestWriteDebug(pendingMmapGuestWrite));
    const guestMemoryWrites = snapshotMmapGuestMemoryWrite(
      processExports,
      vmPtr,
      pendingMmapGuestWrite,
      result,
    );
    debug?.("snapshot-mmap-done", `writes=${guestMemoryWrites?.length ?? 0}`);
    debug?.("read-state-start");
    const completedKernelState = readKernelRuntimeState(processExports);
    debug?.("read-state-done");
    debug?.("snapshot-fd-start");
    const fdEntries = fsSucceeded
      ? snapshotProcessFdEntries(processExports, vmPtr)
      : undefined;
    debug?.("snapshot-fd-done", `entries=${fdEntries?.length ?? 0}`);
    debug?.("snapshot-ofd-start");
    const ofdSlots = fdEntries
      ? snapshotOfdSlotsForFdEntries(processExports, completedKernelState, fdEntries)
      : undefined;
    debug?.("snapshot-ofd-done", `slots=${ofdSlots?.length ?? 0}`);
    debug?.("apply-mutation-start");
    const mutation = applyPendingFsMutation(processExports, pendingContext, fsSucceeded);
    debug?.("apply-mutation-done", `memfs=${mutation.memfsMutated ? 1 : 0}:ofd=${mutation.ofdStateChanged ? 1 : 0}:full=${mutation.needsFullSync ? 1 : 0}`);
    let fullSyncApplied = false;
    if (mutation.needsFullSync) {
      debug?.("full-sync-start");
      fallbackToMetadataMemfsSync(processExports, canonicalExports);
      fullSyncApplied = true;
      debug?.("full-sync-done");
    }
    debug?.("read-next-state-start");
    const nextKernelState = readKernelRuntimeState(processExports);
    debug?.("read-next-state-done");
    const fsDebug = shouldEmitProcessFsDebug(pendingContext, mutation, fullSyncApplied, execveRetryStats)
        ? formatProcessFsDebug(
            pid,
            vmPtr,
            pendingContext,
            fsSucceeded,
            mutation,
            fullSyncApplied,
            execveRetryStats,
          )
        : undefined;
    debug?.("done", `result=${result.toString()}`);
    return {
      kernelState: nextKernelState,
      result,
      memfsMutated: mutation.memfsMutated,
      ofdStateChanged: mutation.ofdStateChanged,
      fdEntries,
      ofdSlots,
      guestMemoryWrites,
      fsDebug,
    };
  }

  return {
    debugProcesses,
    initKernel,
    processFs,
    registerProcess,
    requireProcessKernel,
    unregisterProcess,
  };
}
