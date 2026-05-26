import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../abi.js";
import { publishKernelRuntimeState } from "../bridge/runtime-state.js";
import { activeKernelShareOfdIds, collectProcessHeldOfds, hasPlausibleOfdState } from "./fork-shared.js";
import {
  kernelShareOtherMembers,
  reconcileKernelSharePipeControl,
} from "./kernel-share.js";
import { createWorkerKernelRpcLifecycleHelpers } from "./kernel-rpc/lifecycle.js";
import { ensureKernelMemoryCapacity, kernelScratchBase } from "./kernel-scratch.js";
import { forceNetworkClosedFds } from "./network-socket-close.js";
import { drainProcessOutput } from "./stdio.js";
import { adoptKernelStateForHandle } from "./sync.js";
import {
  createTransferDebugStats,
  type KernelWorkerRequestFn,
  type ProcessHandle,
  type ThreadWorkerState,
} from "./types.js";
import type {
  KernelWorkerPidBytesSnapshot,
  KernelWorkerPipeSlotSnapshot,
} from "../messages.js";

const FS_DEBUG_PATH_BUF_LEN = 4096;
const GUEST_TICKS_PER_SEC = 100_000_000n;
const FS_OP_READ = 1;
const FS_OP_WRITE = 2;
const FS_RING_SUBMIT_HEAD_OFFSET = 0;
const FS_RING_SUBMIT_TAIL_OFFSET = 4;
const FS_RING_COMPLETE_HEAD_OFFSET = 8;
const FS_RING_COMPLETE_TAIL_OFFSET = 12;
const FS_RING_HEADER_BYTES = 32;
const FS_RING_SUBMIT_QUEUE_SIZE = 64;
const FS_RING_COMPLETE_QUEUE_SIZE = 64;
const FS_RING_REQUEST_BYTES = 64;
const FS_RING_RESPONSE_BYTES = 16;
const FS_RING_REQUEST_OPCODE_OFFSET = 0;
const FS_RING_REQUEST_VM_PTR_OFFSET = 48;
const FS_RING_RESPONSE_RESULT_OFFSET = 0;
const FS_RING_RESPONSE_VM_PTR_OFFSET = 8;
const FS_RING_COMPLETE_QUEUE_OFFSET = FS_RING_HEADER_BYTES + FS_RING_SUBMIT_QUEUE_SIZE * FS_RING_REQUEST_BYTES;
const FS_RING_TOTAL_BYTES = FS_RING_COMPLETE_QUEUE_OFFSET + FS_RING_COMPLETE_QUEUE_SIZE * FS_RING_RESPONSE_BYTES;
const POLLFD_BYTES = 8;
const PIPE_HEAD_OFFSET = 0;
const PIPE_TAIL_OFFSET = 4;
const PIPE_WRITE_OPEN_OFFSET = 12;
const PIPE_SLOT_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_PIPE_SLOT_DEBUG ?? "") === "1";
const PIPE_RESUME_DEBUG_ENABLED = PIPE_SLOT_DEBUG_ENABLED
  || ((globalThis as {
    process?: { env?: Record<string, string | undefined> };
  }).process?.env?.TIDEMARK_PIPE_RESUME_DEBUG ?? "") === "1";
const PIPE_RESUME_IN_WORKER_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_PIPE_RESUME_IN_WORKER ?? "1") !== "0";
const FS_LIFECYCLE_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_FS_LIFECYCLE_DEBUG ?? "") === "1";

interface PendingFsContext {
  opcode: number;
  effects: number;
  targetIno: number;
  path: string | null;
  secondaryPath: string | null;
}

interface FsFdStateSnapshot {
  vmPtr: number;
  fdEntries: Array<{ fd: number; ofdId: number; fdFlags: number }>;
  ofdTablePtr: number;
  ofdTable: Uint8Array | null;
}

interface FsRingProgressSnapshot {
  vmPtr: number;
  ringBase: number;
  submitHead: number;
  submitTail: number;
  completeHead: number;
  completeTail: number;
}

interface WorkerKernelRpcDeps {
  processHandles: Map<number, ProcessHandle>;
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  assertProcessStandardFdIntegrity(handle: ProcessHandle, context: string): void;
  blockWorker(handle: ProcessHandle, worker: Worker, status: number, vmPtr: number): void;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  handleBlockedRetryResult(handle: ProcessHandle, worker: Worker, vmPtr: number, status: number): void;
  kickBlockedWorkers(handle: ProcessHandle): void;
  markRuntimeFsDirty(handle: ProcessHandle): void;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  syncBlockedForkSharedStateViaKernelWorker(handle: ProcessHandle, sourceVmPtr?: number): Promise<void>;
  syncForkSharedStateLocally(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options?: { includeMemfs?: boolean; trackHeldOfds?: boolean; ofdIds?: number[] },
  ): void;
  syncForkSharedStateViaKernelWorker(
    handle: ProcessHandle,
    targets: ProcessHandle[],
    options?: {
      includeMemfs?: boolean;
      trackHeldOfds?: boolean;
      deltaOfdIds?: boolean;
      memfsOnly?: boolean;
      preserveSourceRefcounts?: boolean;
      preservePublishedRegularFileData?: boolean;
      sourceVmPtr?: number;
      ofdIds?: number[];
    },
  ): Promise<void>;
  rehydrateForkSharedStateViaKernelWorker(
    targets: ProcessHandle[],
    ofdIds: number[],
    options?: { procSelfExeTargets?: KernelWorkerPidBytesSnapshot[]; includeMemfs?: boolean },
  ): Promise<void>;
  syncPipeStateViaKernelWorker(handle: ProcessHandle): Promise<bigint>;
  syncSocketStateViaKernelWorker(handle: ProcessHandle): Promise<{ socketUsed: number; netBufUsed: bigint }>;
  collectTrackableForkSharedOfds(handle: ProcessHandle): Set<number>;
}

function activeChildMayHoldBlockedPipeWriter(
  deps: WorkerKernelRpcDeps,
  handle: ProcessHandle,
  vmPtr: number,
): boolean {
  const blockedOfdId = blockedPipeReaderOfdId(handle, vmPtr);
  if (
    blockedOfdId <= 0
    || handle.kernel.kernel_ofd_fd_type(blockedOfdId) !== handle.kernel.abi_fd_type_pipe()
    || handle.kernel.kernel_ofd_host_fd(blockedOfdId) < 0
  ) {
    return false;
  }
  const pipeSlot = handle.kernel.kernel_ofd_ino(blockedOfdId);
  if (pipeSlot <= 0 || pipeSlot >= handle.kernel.abi_max_pipes()) {
    return false;
  }
  const processFdTableMayHoldPipeWriter = (candidate: ProcessHandle): boolean => {
    if (candidate.vmPtr === 0) {
      return false;
    }
    const maxFds = candidate.kernel.abi_max_fds();
    for (let fd = 0; fd < maxFds; fd += 1) {
      const ofdId = candidate.kernel.process_fd_ofd_id(candidate.vmPtr, fd);
      if (
        ofdId > 0
        && ofdId < maxFds
        && candidate.kernel.kernel_ofd_fd_type(ofdId) === candidate.kernel.abi_fd_type_pipe()
        && candidate.kernel.kernel_ofd_ino(ofdId) === pipeSlot
        && candidate.kernel.kernel_ofd_host_fd(ofdId) < 0
      ) {
        return true;
      }
    }
    return false;
  };
  for (const candidate of deps.processHandles.values()) {
    const ownsVisibleOutputForHandle = candidate.stdioOwner === handle
      || candidate.stdioOwner?.pid === handle.pid;
    if (
      candidate.exitInProgress
      || deps.processHandles.get(candidate.pid) !== candidate
      || candidate.workers.size === 0
      || (candidate.parentPid !== handle.pid && !ownsVisibleOutputForHandle)
    ) {
      continue;
    }
    if (processFdTableMayHoldPipeWriter(candidate)) {
      return true;
    }
    for (const ofdId of candidate.kernelShare.sharedOfdIds) {
      if (
        ofdId > 0
        && ofdId < candidate.kernel.abi_max_fds()
        && candidate.kernel.kernel_ofd_fd_type(ofdId) === candidate.kernel.abi_fd_type_pipe()
        && candidate.kernel.kernel_ofd_ino(ofdId) === pipeSlot
        && candidate.kernel.kernel_ofd_host_fd(ofdId) < 0
      ) {
        return true;
      }
    }
  }
  return false;
}

function blockedPipeReaderOfdId(handle: ProcessHandle, vmPtr: number): number {
  const pinnedOfdId = handle.kernel.thread_blocked_ofd_id?.(vmPtr) ?? 0;
  if (pinnedOfdId > 0) {
    return pinnedOfdId;
  }

  const candidateFds: number[] = [];
  const packedPollPipe = handle.kernel.thread_poll_wait_first_pipe_revents?.(vmPtr);
  if (packedPollPipe !== undefined && packedPollPipe !== 0) {
    candidateFds.push(packedPollPipe & 0xff);
  }
  const blockedFd = handle.kernel.thread_blocked_fd(vmPtr);
  if (blockedFd >= 0) {
    candidateFds.push(blockedFd);
  }
  const syscallFd = Number(handle.kernel.thread_a0(vmPtr));
  if (syscallFd >= 0) {
    candidateFds.push(syscallFd);
  }

  const seen = new Set<number>();
  for (const fd of candidateFds) {
    if (seen.has(fd)) {
      continue;
    }
    seen.add(fd);
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (
      ofdId > 0
      && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_pipe()
      && handle.kernel.kernel_ofd_host_fd(ofdId) >= 0
    ) {
      return ofdId;
    }
  }
  return 0;
}

export function clearVforkSnapshot(handle: ProcessHandle): void {
  handle.vforkSnapshot = null;
  handle.vforkWorker = null;
}

export async function resolveProcessCwdInoViaKernelWorker(
  kernelWorkerRequest: KernelWorkerRequestFn,
  cwd: string | undefined,
): Promise<number> {
  if (!cwd) {
    return 1;
  }
  const response = await kernelWorkerRequest({
    type: "stat",
    path: cwd,
  });
  if (response.type !== "stat-result") {
    throw new Error(`unexpected kernel-worker response for stat: ${response.type}`);
  }
  if ((response.stat.mode & 0o170000) !== 0o040000) {
    throw new Error(`cwd is not a directory: ${cwd}`);
  }
  return response.stat.ino;
}

export function createWorkerKernelRpcHelpers(deps: WorkerKernelRpcDeps) {
  const fsResumeInFlight = new WeakMap<ProcessHandle, Map<string, Promise<void>>>();

  function monotonicTicksCurrent(handle: ProcessHandle): bigint {
    const ticks = handle.kernel.monotonic_ticks_current?.();
    return ticks === undefined ? 0n : BigInt(ticks);
  }

  function advanceGuestTimeByMs(handle: ProcessHandle, timeoutMs: number): void {
    if (timeoutMs <= 0 || !Number.isFinite(timeoutMs)) {
      return;
    }
    const deltaTicks = BigInt(Math.ceil(timeoutMs)) * GUEST_TICKS_PER_SEC / 1000n;
    handle.kernel.sync_monotonic_ticks_floor?.(monotonicTicksCurrent(handle) + deltaTicks);
  }

  const decoder = new TextDecoder();

  function formatGuestBufferPreview(handle: ProcessHandle, ptr: bigint | number, length: bigint | number): string {
    const start = typeof ptr === "bigint" ? Number(ptr) : ptr;
    const requested = typeof length === "bigint" ? Number(length) : length;
    if (!Number.isFinite(start) || !Number.isFinite(requested) || start <= 0 || requested <= 0) {
      return "";
    }
    const bytes = new Uint8Array(handle.memory.buffer);
    const absolute = handle.guestBase + start;
    const previewLen = Math.min(requested, 48);
    if (absolute < 0 || absolute + previewLen > bytes.byteLength) {
      return "";
    }
    const preview: string[] = [];
    for (let offset = 0; offset < previewLen; offset += 1) {
      const value = bytes[absolute + offset];
      preview.push(value >= 0x20 && value <= 0x7e ? String.fromCharCode(value) : ".");
    }
    return preview.join("");
  }

  function queueGuestBufferWrite(
    handle: ProcessHandle,
    worker: Worker,
    ptr: bigint | number,
    length: bigint | number,
  ): void {
    const start = typeof ptr === "bigint" ? Number(ptr) : ptr;
    const requested = typeof length === "bigint" ? Number(length) : length;
    if (!Number.isFinite(start) || !Number.isFinite(requested) || start <= 0 || requested <= 0) {
      return;
    }
    const absolute = handle.guestBase + start;
    const writeLen = Math.min(requested, 65_536);
    const memory = new Uint8Array(handle.memory.buffer);
    if (absolute < 0 || absolute + writeLen > memory.byteLength) {
      return;
    }
    const thread = deps.threadWorkerStates.get(worker);
    if (!thread) {
      return;
    }
    thread.pendingGuestMemoryWrites.push({
      guestPtr: start,
      bytes: memory.slice(absolute, absolute + writeLen),
    });
    if (thread.pendingGuestMemoryWrites.length > 32) {
      thread.pendingGuestMemoryWrites.splice(0, thread.pendingGuestMemoryWrites.length - 32);
    }
  }

  function syscallMatches(
    syscallNr: bigint,
    abiSyscall: (() => bigint | number) | undefined,
  ): boolean {
    const value = abiSyscall?.();
    return value !== undefined && syscallNr === BigInt(value);
  }

  function queueBlockedPollWaitGuestMemoryWrites(
    handle: ProcessHandle,
    worker: Worker,
    syscallNr: bigint,
    arg0: bigint,
    arg1: bigint,
    arg2: bigint,
    arg3: bigint,
  ): boolean {
    const maxFds = Math.max(0, handle.kernel.abi_max_fds?.() ?? 1024);
    if (syscallMatches(syscallNr, handle.kernel.abi_sys_ppoll)) {
      const nfds = Math.min(Number(arg1), maxFds);
      if (Number.isFinite(nfds) && nfds > 0) {
        queueGuestBufferWrite(handle, worker, arg0, nfds * POLLFD_BYTES);
      }
      return true;
    }
    if (syscallMatches(syscallNr, handle.kernel.abi_sys_pselect6)) {
      const nfds = Math.min(Number(arg0), maxFds);
      if (!Number.isFinite(nfds) || nfds <= 0) {
        return true;
      }
      const setBytes = Math.floor((nfds + 63) / 64) * 8;
      queueGuestBufferWrite(handle, worker, arg1, setBytes);
      queueGuestBufferWrite(handle, worker, arg2, setBytes);
      queueGuestBufferWrite(handle, worker, arg3, setBytes);
      return true;
    }
    return false;
  }

  function blockedPipeResumeWritesGuestBuffer(handle: ProcessHandle, syscallNr: bigint): boolean {
    return syscallMatches(syscallNr, handle.kernel.abi_sys_read);
  }

  function blockedSocketResumeWritesGuestBuffer(handle: ProcessHandle, syscallNr: bigint): boolean {
    return syscallMatches(syscallNr, handle.kernel.abi_sys_read)
      || syscallMatches(syscallNr, handle.kernel.abi_sys_recvfrom);
  }

  function snapshotPipeSlotForThreadStepFd(
    handle: ProcessHandle,
    vmPtr: number,
    fd: number,
    options: { includeEmpty?: boolean; useBlockedOfd?: boolean } = {},
  ): KernelWorkerPipeSlotSnapshot[] {
    const blockedOfdId = options.useBlockedOfd === false
      ? 0
      : handle.kernel.thread_blocked_ofd_id?.(vmPtr) ?? 0;
    const ofdId = blockedOfdId > 0
      ? blockedOfdId
      : handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (
      ofdId <= 0
      || ofdId >= handle.kernel.abi_max_fds()
      || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
    ) {
      return [];
    }
    const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
    const maxPipes = handle.kernel.abi_max_pipes();
    const slotBytes = handle.kernel.kernel_pipe_region_bytes() / maxPipes;
    if (
      pipeSlot <= 0
      || pipeSlot >= maxPipes
      || !Number.isInteger(slotBytes)
      || slotBytes <= 0
    ) {
      return [];
    }
    const offset = handle.kernel.kernel_pipe_base() + pipeSlot * slotBytes;
    const memory = new Uint8Array(handle.memory.buffer);
    if (offset < 0 || offset + slotBytes > memory.byteLength) {
      return [];
    }
    const view = new DataView(handle.memory.buffer);
    const head = view.getUint32(offset, true);
    const tail = view.getUint32(offset + 4, true);
    if (!options.includeEmpty && head === tail) {
      return [];
    }
    return [{
      pipeSlot,
      slot: memory.slice(offset, offset + slotBytes),
    }];
  }

  function snapshotPipeSlotForThreadStepSlot(
    handle: ProcessHandle,
    pipeSlot: number,
    options: { includeEmpty?: boolean } = {},
  ): KernelWorkerPipeSlotSnapshot[] {
    const maxPipes = handle.kernel.abi_max_pipes();
    const slotBytes = handle.kernel.kernel_pipe_region_bytes() / maxPipes;
    if (
      pipeSlot <= 0
      || pipeSlot >= maxPipes
      || !Number.isInteger(slotBytes)
      || slotBytes <= 0
    ) {
      return [];
    }
    const offset = handle.kernel.kernel_pipe_base() + pipeSlot * slotBytes;
    const memory = new Uint8Array(handle.memory.buffer);
    if (offset < 0 || offset + slotBytes > memory.byteLength) {
      return [];
    }
    const view = new DataView(handle.memory.buffer);
    const head = view.getUint32(offset, true);
    const tail = view.getUint32(offset + 4, true);
    if (!options.includeEmpty && head === tail) {
      return [];
    }
    return [{
      pipeSlot,
      slot: memory.slice(offset, offset + slotBytes),
    }];
  }

  function pipeSlotForThreadStepFd(
    handle: ProcessHandle,
    vmPtr: number,
    fd: number,
    options: { useBlockedOfd?: boolean } = {},
  ): number | null {
    const blockedOfdId = options.useBlockedOfd === false
      ? 0
      : handle.kernel.thread_blocked_ofd_id?.(vmPtr) ?? 0;
    const ofdId = blockedOfdId > 0
      ? blockedOfdId
      : handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (
      ofdId <= 0
      || ofdId >= handle.kernel.abi_max_fds()
      || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
    ) {
      return null;
    }
    const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
    return pipeSlot > 0 && pipeSlot < handle.kernel.abi_max_pipes()
      ? pipeSlot
      : null;
  }

  function pipeSlotWriteOpenForThreadStepFd(
    handle: ProcessHandle,
    vmPtr: number,
    fd: number,
    options: { useBlockedOfd?: boolean } = {},
  ): number {
    const blockedOfdId = options.useBlockedOfd === false
      ? 0
      : handle.kernel.thread_blocked_ofd_id?.(vmPtr) ?? 0;
    const ofdId = blockedOfdId > 0
      ? blockedOfdId
      : handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (
      ofdId <= 0
      || ofdId >= handle.kernel.abi_max_fds()
      || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
    ) {
      return 0;
    }
    return pipeSlotWriteOpen(handle, handle.kernel.kernel_ofd_ino(ofdId));
  }

  function pipeSlotWriteOpen(handle: ProcessHandle, pipeSlot: number): number {
    const maxPipes = handle.kernel.abi_max_pipes();
    const slotBytes = handle.kernel.kernel_pipe_region_bytes() / maxPipes;
    if (
      pipeSlot <= 0
      || pipeSlot >= maxPipes
      || !Number.isInteger(slotBytes)
      || slotBytes <= PIPE_WRITE_OPEN_OFFSET + 4
    ) {
      return 0;
    }
    const offset = handle.kernel.kernel_pipe_base() + pipeSlot * slotBytes;
    const view = new DataView(handle.memory.buffer);
    if (offset < 0 || offset + PIPE_WRITE_OPEN_OFFSET + 4 > view.byteLength) {
      return 0;
    }
    return view.getInt32(offset + PIPE_WRITE_OPEN_OFFSET, true);
  }

  function pipeSlotBufferedLenForThreadStepFd(
    handle: ProcessHandle,
    vmPtr: number,
    fd: number,
    options: { useBlockedOfd?: boolean } = {},
  ): number {
    const blockedOfdId = options.useBlockedOfd === false
      ? 0
      : handle.kernel.thread_blocked_ofd_id?.(vmPtr) ?? 0;
    const ofdId = blockedOfdId > 0
      ? blockedOfdId
      : handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (
      ofdId <= 0
      || ofdId >= handle.kernel.abi_max_fds()
      || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
    ) {
      return 0;
    }
    return pipeSlotBufferedLen(handle, handle.kernel.kernel_ofd_ino(ofdId));
  }

  function pipeSlotBufferedLen(handle: ProcessHandle, pipeSlot: number): number {
    const maxPipes = handle.kernel.abi_max_pipes();
    const slotBytes = handle.kernel.kernel_pipe_region_bytes() / maxPipes;
    if (
      pipeSlot <= 0
      || pipeSlot >= maxPipes
      || !Number.isInteger(slotBytes)
      || slotBytes <= PIPE_WRITE_OPEN_OFFSET + 4
    ) {
      return 0;
    }
    const offset = handle.kernel.kernel_pipe_base() + pipeSlot * slotBytes;
    const view = new DataView(handle.memory.buffer);
    if (offset < 0 || offset + PIPE_WRITE_OPEN_OFFSET + 4 > view.byteLength) {
      return 0;
    }
    const head = view.getUint32(offset + PIPE_HEAD_OFFSET, true);
    const tail = view.getUint32(offset + PIPE_TAIL_OFFSET, true);
    return tail >= head ? tail - head : 1;
  }

  function pollPipeSlotsBufferedLenForThreadStep(handle: ProcessHandle, vmPtr: number): number {
    let bufferedLen = 0;
    for (const pipeSlot of pollPipeSlotsForThreadStep(handle, vmPtr)) {
      bufferedLen = Math.max(bufferedLen, pipeSlotBufferedLen(handle, pipeSlot));
    }
    return bufferedLen;
  }

  function pollPipeSlotsWriteOpenForThreadStep(handle: ProcessHandle, vmPtr: number): number {
    let writeOpen = 0;
    for (const pipeSlot of pollPipeSlotsForThreadStep(handle, vmPtr)) {
      writeOpen = Math.max(writeOpen, pipeSlotWriteOpen(handle, pipeSlot));
    }
    return writeOpen;
  }

  function preserveHostStdinWriterForBlockedRead(handle: ProcessHandle, vmPtr: number): void {
    if (handle.stdioOwner !== null) {
      return;
    }
    const syscallNr = blockedPipeWaitSyscallNr(handle, vmPtr);
    const readSyscall = handle.kernel.abi_sys_read?.();
    const blockedFd = handle.kernel.thread_blocked_fd?.(vmPtr) ?? 0;
    const syscallFd = Number(handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 0) ?? handle.kernel.thread_a0(vmPtr));
    if (
      readSyscall === undefined
      || syscallNr !== BigInt(readSyscall)
      || (blockedFd > 0 ? blockedFd : syscallFd) !== 0
    ) {
      return;
    }
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, 0);
    if (
      ofdId <= 0
      || ofdId >= handle.kernel.abi_max_fds()
      || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
    ) {
      return;
    }
    const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
    if (![handle.hostStdio.stdinPipe, handle.stdinPipe].includes(pipeSlot)) {
      return;
    }
    const maxPipes = handle.kernel.abi_max_pipes();
    const slotBytes = handle.kernel.kernel_pipe_region_bytes() / maxPipes;
    if (
      pipeSlot <= 0
      || pipeSlot >= maxPipes
      || !Number.isInteger(slotBytes)
      || slotBytes <= PIPE_WRITE_OPEN_OFFSET + 4
    ) {
      return;
    }
    const offset = handle.kernel.kernel_pipe_base() + pipeSlot * slotBytes;
    const view = new DataView(handle.memory.buffer);
    if (offset < 0 || offset + PIPE_WRITE_OPEN_OFFSET + 4 > view.byteLength) {
      return;
    }
    if (view.getInt32(offset + PIPE_WRITE_OPEN_OFFSET, true) <= 0) {
      view.setInt32(offset + PIPE_WRITE_OPEN_OFFSET, 1, true);
    }
  }

  function snapshotBlockedPipeSlotForThreadStep(
    handle: ProcessHandle,
    vmPtr: number,
    options: { includeEmpty?: boolean } = {},
  ): KernelWorkerPipeSlotSnapshot[] {
    return snapshotPipeSlotForThreadStepFd(handle, vmPtr, blockedPipeWaitFd(handle, vmPtr), options);
  }

  function blockedPipeWaitSyscallNr(handle: ProcessHandle, vmPtr: number): bigint | undefined {
    const blockedSyscallNr = handle.kernel.thread_blocked_syscall_nr?.(vmPtr);
    if (blockedSyscallNr !== undefined && blockedSyscallNr !== 0n) {
      return blockedSyscallNr;
    }
    return handle.kernel.thread_a7?.(vmPtr);
  }

  function blockedPipeWaitFd(handle: ProcessHandle, vmPtr: number): number {
    const blockedFd = handle.kernel.thread_blocked_fd?.(vmPtr) ?? -1;
    if (blockedFd >= 0) {
      return blockedFd;
    }
    const blockedArg0 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 0);
    if (blockedArg0 !== undefined) {
      return Number(blockedArg0);
    }
    return Number(handle.kernel.thread_a0(vmPtr));
  }

  function isPollLikePipeWait(handle: ProcessHandle, vmPtr: number): boolean {
    const syscallNr = blockedPipeWaitSyscallNr(handle, vmPtr);
    if (syscallNr === undefined) {
      return false;
    }
    const ppoll = handle.kernel.abi_sys_ppoll?.();
    const pselect6 = handle.kernel.abi_sys_pselect6?.();
    return (ppoll !== undefined && syscallNr === BigInt(ppoll))
      || (pselect6 !== undefined && syscallNr === BigInt(pselect6));
  }

  function snapshotPollPipeSlotForThreadStep(
    handle: ProcessHandle,
    vmPtr: number,
    options: { includeEmpty?: boolean } = {},
  ): KernelWorkerPipeSlotSnapshot[] {
    const pipeSlots = pollPipeSlotsForThreadStep(handle, vmPtr);
    if (pipeSlots.size > 0) {
      return [...pipeSlots].flatMap((pipeSlot) =>
        snapshotPipeSlotForThreadStepSlot(handle, pipeSlot, options)
      );
    }
    const packed = handle.kernel.thread_poll_wait_first_pipe_revents?.(vmPtr);
    if (packed === undefined || packed === 0) {
      return [];
    }
    return snapshotPipeSlotForThreadStepFd(handle, vmPtr, packed & 0xff, {
      ...options,
      useBlockedOfd: false,
    });
  }

  function pollPipeSlotsForThreadStep(handle: ProcessHandle, vmPtr: number): Set<number> {
    const rawMask = handle.kernel.thread_poll_wait_pipe_slots_mask?.(vmPtr);
    const mask = rawMask === undefined
      ? 0n
      : typeof rawMask === "bigint" ? rawMask : BigInt(rawMask);
    const pipeSlots = new Set<number>();
    const maxPipes = Math.min(handle.kernel.abi_max_pipes?.() ?? 64, 64);
    for (let pipeSlot = 1; pipeSlot < maxPipes; pipeSlot += 1) {
      if ((mask & (1n << BigInt(pipeSlot))) !== 0n) {
        pipeSlots.add(pipeSlot);
      }
    }
    if (pipeSlots.size > 0) {
      return pipeSlots;
    }
    const packed = handle.kernel.thread_poll_wait_first_pipe_revents?.(vmPtr);
    if (packed === undefined || packed === 0) {
      return pipeSlots;
    }
    const pipeSlot = pipeSlotForThreadStepFd(handle, vmPtr, packed & 0xff, { useBlockedOfd: false });
    if (pipeSlot !== null) {
      pipeSlots.add(pipeSlot);
    }
    return pipeSlots;
  }

  function pipeSlotsForBlockedPipeWait(handle: ProcessHandle, vmPtr: number): Set<number> | null {
    if (isPollLikePipeWait(handle, vmPtr)) {
      const pipeSlots = pollPipeSlotsForThreadStep(handle, vmPtr);
      return pipeSlots.size === 0 ? null : pipeSlots;
    }
    const pipeSlot = pipeSlotForThreadStepFd(handle, vmPtr, blockedPipeWaitFd(handle, vmPtr), {
      useBlockedOfd: true,
    });
    return pipeSlot === null ? null : new Set([pipeSlot]);
  }

  function processReferencesAnyPipeSlot(
    handle: ProcessHandle,
    vmPtr: number,
    pipeSlots: ReadonlySet<number>,
  ): boolean {
    const maxFds = handle.kernel.abi_max_fds();
    for (let fd = 0; fd < maxFds; fd += 1) {
      const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
      if (
        ofdId <= 0
        || ofdId >= maxFds
        || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
      ) {
        continue;
      }
      if (pipeSlots.has(handle.kernel.kernel_ofd_ino(ofdId))) {
        return true;
      }
    }
    const blockedOfdId = handle.kernel.thread_blocked_ofd_id?.(vmPtr) ?? 0;
    return blockedOfdId > 0
      && blockedOfdId < maxFds
      && handle.kernel.kernel_ofd_fd_type(blockedOfdId) === handle.kernel.abi_fd_type_pipe()
      && pipeSlots.has(handle.kernel.kernel_ofd_ino(blockedOfdId));
  }

  function formatBlockedPipeSlotHolders(handle: ProcessHandle, vmPtr: number): string {
    const blockedOfdId = handle.kernel.thread_blocked_ofd_id?.(vmPtr) ?? 0;
    if (
      blockedOfdId <= 0
      || blockedOfdId >= handle.kernel.abi_max_fds()
      || handle.kernel.kernel_ofd_fd_type(blockedOfdId) !== handle.kernel.abi_fd_type_pipe()
    ) {
      return "n/a";
    }
    const pipeSlot = handle.kernel.kernel_ofd_ino(blockedOfdId);
    const holders: string[] = [];
    for (const member of handle.kernelShare.members) {
      if (member.vmPtr === 0 || member.exitInProgress) {
        continue;
      }
      const maxFds = member.kernel.abi_max_fds();
      for (let fd = 0; fd < maxFds; fd += 1) {
        const ofdId = member.kernel.process_fd_ofd_id(member.vmPtr, fd);
        if (
          ofdId <= 0
          || ofdId >= maxFds
          || member.kernel.kernel_ofd_fd_type(ofdId) !== member.kernel.abi_fd_type_pipe()
          || member.kernel.kernel_ofd_ino(ofdId) !== pipeSlot
        ) {
          continue;
        }
        const mode = member.kernel.kernel_ofd_host_fd(ofdId) >= 0 ? "r" : "w";
        holders.push(`${member.pid}/${fd}/${ofdId}/${mode}`);
      }
    }
    return holders.join(".") || "none";
  }

  function pollPipeWriteOpenForThreadStep(handle: ProcessHandle, vmPtr: number): number {
    const slotsWriteOpen = pollPipeSlotsWriteOpenForThreadStep(handle, vmPtr);
    if (slotsWriteOpen > 0) {
      return slotsWriteOpen;
    }
    const packed = handle.kernel.thread_poll_wait_first_pipe_revents?.(vmPtr);
    if (packed === undefined || packed === 0) {
      return 0;
    }
    return pipeSlotWriteOpenForThreadStepFd(handle, vmPtr, packed & 0xff, { useBlockedOfd: false });
  }

  function pollPipeBufferedLenForThreadStep(handle: ProcessHandle, vmPtr: number): number {
    const slotsBufferedLen = pollPipeSlotsBufferedLenForThreadStep(handle, vmPtr);
    if (slotsBufferedLen > 0) {
      return slotsBufferedLen;
    }
    const packed = handle.kernel.thread_poll_wait_first_pipe_revents?.(vmPtr);
    if (packed === undefined || packed === 0) {
      return 0;
    }
    return pipeSlotBufferedLenForThreadStepFd(handle, vmPtr, packed & 0xff, { useBlockedOfd: false });
  }

  function blockedPollPipeHasBufferedData(handle: ProcessHandle, vmPtr: number): boolean {
    if (pollPipeSlotsBufferedLenForThreadStep(handle, vmPtr) > 0) {
      return true;
    }
    const packed = handle.kernel.thread_poll_wait_first_pipe_revents?.(vmPtr);
    if (packed === undefined || packed === 0) {
      return false;
    }
    const fd = packed & 0xff;
    const interest = (packed >>> 8) & 0xffff;
    const revents = (packed >>> 24) & 0xff;
    if (revents !== 0) {
      return true;
    }
    if ((interest & 0x001) === 0) {
      return false;
    }
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (
      ofdId <= 0
      || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
    ) {
      return false;
    }
    const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
    const maxPipes = handle.kernel.abi_max_pipes();
    const slotBytes = handle.kernel.kernel_pipe_region_bytes() / maxPipes;
    if (
      pipeSlot <= 0
      || pipeSlot >= maxPipes
      || !Number.isInteger(slotBytes)
      || slotBytes <= 0
    ) {
      return false;
    }
    const offset = handle.kernel.kernel_pipe_base() + pipeSlot * slotBytes;
    const view = new DataView(handle.memory.buffer);
    return view.getUint32(offset, true) !== view.getUint32(offset + 4, true);
  }

  async function syncSocketStatePreservingNetworkBridge(handle: ProcessHandle): Promise<{
    socketUsed: number;
    netBufUsed: bigint;
  }> {
    if (handle.networkConnections.size > 0) {
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      return {
        socketUsed: handle.kernelState.socketUsed,
        netBufUsed: handle.kernelState.netBufUsed,
      };
    }
    return deps.syncSocketStateViaKernelWorker(handle);
  }

  const kernelRpcLifecycleHelpers = createWorkerKernelRpcLifecycleHelpers({
    appendWorkerDebugLog: deps.appendWorkerDebugLog,
    processHandles: deps.processHandles,
    threadWorkerStates: deps.threadWorkerStates,
    drainProcessOutput,
    enqueueWorker: deps.enqueueWorker,
    failProcess: deps.failProcess,
    handleBlockedRetryResult: deps.handleBlockedRetryResult,
    kickBlockedWorkers: deps.kickBlockedWorkers,
    requestPipeStateSync: deps.requestPipeStateSync,
    syncSocketStateViaKernelWorker: syncSocketStatePreservingNetworkBridge,
    clearVforkSnapshot,
  });

  const {
    canonicalizeActiveVforkChildViaKernelWorker,
    canonicalizeChildLifecycleViaKernelWorker,
    initializeProcessLifecycleViaKernelWorker,
    recordChildExitViaKernelWorker,
    registerProcessWithKernelWorker,
    releaseHostStdioViaKernelWorker,
    resumeConnectViaKernelWorker,
    resumeVforkExecveParentViaKernelWorker,
    resumeVforkParentViaKernelWorker,
    resumeWait4ViaKernelWorker,
    syncProcessMemfsViaKernelWorker,
    syncProcessLifecycleViaKernelWorker,
    unregisterProcessWithKernelWorker,
  } = kernelRpcLifecycleHelpers;

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
    return {
      opcode: handle.kernel.thread_pending_fs_opcode(vmPtr),
      effects: handle.kernel.thread_pending_fs_effects(vmPtr),
      targetIno: handle.kernel.thread_pending_fs_target_ino(vmPtr),
      path: readPendingFsPath(handle, vmPtr),
      secondaryPath: readPendingFsPath(handle, vmPtr, true),
    };
  }

  function formatPendingFsDebugSuffix(context: PendingFsContext): string {
    const pathSuffix = context.path ? `:path=${JSON.stringify(context.path)}` : "";
    const secondaryPathSuffix = context.secondaryPath
      ? `:path2=${JSON.stringify(context.secondaryPath)}`
      : "";
    return `:op=${context.opcode}:fx=${context.effects}:ino=${context.targetIno}${pathSuffix}${secondaryPathSuffix}`;
  }

  function appendFsLifecycleEvent(
    handle: ProcessHandle,
    phase: string,
    vmPtr: number,
    detail = "",
  ): void {
    const event = `fs-${phase}:${handle.pid}:0x${vmPtr.toString(16)}${detail}`;
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      event,
    );
    if (FS_LIFECYCLE_DEBUG_ENABLED) {
      console.error(`[tidemark-fs] ${event}`);
    }
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
    const requests: string[] = [];
    for (let pos = submitTail; pos < submitHead && requests.length < 4; pos += 1) {
      const entry = fsRingRequestEntryOffset(ringBase, pos);
      const opcode = view.getUint32(entry + FS_RING_REQUEST_OPCODE_OFFSET, true);
      const requestVmPtr = view.getUint32(entry + FS_RING_REQUEST_VM_PTR_OFFSET, true);
      requests.push(`${pos}:op=${opcode}/vm=0x${requestVmPtr.toString(16)}`);
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

  function fsRingHasPendingRequests(handle: ProcessHandle, vmPtr: number): boolean {
    const progress = snapshotFsRingProgress(handle, vmPtr);
    return progress !== null && progress.submitHead !== progress.submitTail;
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

  function fsRingRequestEntryOffset(ringBase: number, pos: number): number {
    return ringBase + FS_RING_HEADER_BYTES + (pos % FS_RING_SUBMIT_QUEUE_SIZE) * FS_RING_REQUEST_BYTES;
  }

  function fsRingCompletionEntryOffset(ringBase: number, pos: number): number {
    return ringBase + FS_RING_COMPLETE_QUEUE_OFFSET + (pos % FS_RING_COMPLETE_QUEUE_SIZE) * FS_RING_RESPONSE_BYTES;
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

  function normalizeFsResult(result: bigint | number): bigint {
    return typeof result === "bigint" ? result : BigInt(result);
  }

  function isPackageIndexTransferPath(path: string | null): boolean {
    return path?.includes("_Packages.xz") === true || path?.endsWith("/Packages") === true;
  }

  function recordFsTransferStats(
    handle: ProcessHandle,
    pendingFs: PendingFsContext,
    fsResult: bigint | number,
    fsSucceeded: boolean,
    fsEffects: { memfsMutation: number; memfsFileData: number; sharedOfdState: number },
    viaKernelWorker: boolean,
    memfsPublishReady: boolean,
  ): void {
    handle.transferDebugStats ??= createTransferDebugStats();
    const stats = handle.transferDebugStats;
    stats.fsResumeCalls += 1;
    if (viaKernelWorker) {
      stats.fsKernelWorkerCalls += 1;
    } else {
      stats.fsLocalCalls += 1;
    }
    if (!fsSucceeded) {
      return;
    }

    if ((pendingFs.effects & fsEffects.memfsMutation) !== 0) {
      stats.memfsMutationOps += 1;
    }
    if ((pendingFs.effects & fsEffects.memfsFileData) !== 0) {
      stats.memfsFileDataOps += 1;
    }
    if (memfsPublishReady) {
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
      if (isPackageIndexTransferPath(pendingFs.path)) {
        stats.packagePathWriteCalls += 1;
        stats.packagePathWriteBytes += byteCount;
      }
    }
  }

  function fsResultIsPending(handle: ProcessHandle, vmPtr: number, fsResult: bigint | number): boolean {
    const result = Number(BigInt.asIntN(64, normalizeFsResult(fsResult)));
    const eagain = handle.kernel.abi_eagain?.() ?? 11;
    const fsStatus = handle.kernel.abi_status_fs?.() ?? -1;
    return result === eagain
      && handle.kernel.thread_status?.(vmPtr) === fsStatus;
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
    const clearedResult = handle.kernel.thread_resume_fs(vmPtr);
    appendFsLifecycleEvent(
      handle,
      "clear-stale-status",
      vmPtr,
      `:rv=${clearedResult.toString()}:status=${handle.kernel.thread_status(vmPtr)}${before}`,
    );
  }

  function snapshotFsFdState(handle: ProcessHandle, vmPtr: number): FsFdStateSnapshot | null {
    const maxFds = handle.kernel.abi_max_fds?.() ?? 0;
    if (vmPtr === 0 || maxFds <= 0 || !handle.kernel.process_fd_ofd_id || !handle.kernel.process_fd_flags) {
      return null;
    }
    const fdEntries: FsFdStateSnapshot["fdEntries"] = [];
    for (let fd = 0; fd < maxFds; fd += 1) {
      fdEntries.push({
        fd,
        ofdId: handle.kernel.process_fd_ofd_id(vmPtr, fd),
        fdFlags: handle.kernel.process_fd_flags(vmPtr, fd),
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
      const currentOfdId = handle.kernel.process_fd_ofd_id?.(snapshot.vmPtr, entry.fd) ?? entry.ofdId;
      const currentFdFlags = handle.kernel.process_fd_flags?.(snapshot.vmPtr, entry.fd) ?? entry.fdFlags;
      if (currentOfdId === entry.ofdId && currentFdFlags === entry.fdFlags) {
        continue;
      }
      handle.kernel.process_set_fd_entry(snapshot.vmPtr, entry.fd, entry.ofdId, entry.fdFlags);
      const currentOfdType = entry.ofdId > 0
        ? handle.kernel.kernel_ofd_fd_type?.(entry.ofdId) ?? 0
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

  async function resumeFsViaKernelWorker(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    fsEffects: { memfsMutation: number; memfsFileData: number; sharedOfdState: number },
  ): Promise<void> {
    let byVmPtr = fsResumeInFlight.get(handle);
    const key = fsResumeKey(handle, vmPtr);
    const existing = byVmPtr?.get(key);
    if (existing) {
      deps.appendWorkerDebugLog(handle, `fs-join:0x${vmPtr.toString(16)}`);
      appendFsLifecycleEvent(handle, "join", vmPtr, "");
      await existing;
      return;
    }
    if (byVmPtr === undefined) {
      byVmPtr = new Map();
      fsResumeInFlight.set(handle, byVmPtr);
    }
    const resume = resumeFsViaKernelWorkerInner(handle, worker, vmPtr, fsEffects);
    byVmPtr.set(key, resume);
    try {
      await resume;
    } finally {
      if (byVmPtr.get(key) === resume) {
        byVmPtr.delete(key);
      }
      if (byVmPtr.size === 0) {
        fsResumeInFlight.delete(handle);
      }
    }
  }

  async function resumeFsViaKernelWorkerInner(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    fsEffects: { memfsMutation: number; memfsFileData: number; sharedOfdState: number },
  ): Promise<void> {
    const pendingFs = readPendingFsContext(handle, vmPtr);
    const fsDebugSuffix = formatPendingFsDebugSuffix(pendingFs);
    const pendingRingRequest = pendingFs.opcode === 0 && fsRingHasPendingRequests(handle, vmPtr);
    const memfsStateMayChange = (pendingFs.effects & (fsEffects.memfsMutation | fsEffects.memfsFileData)) !== 0;
    const memfsPublishMayBeReady = (pendingFs.effects & fsEffects.memfsMutation) !== 0
      || (
        (pendingFs.effects & fsEffects.memfsFileData) !== 0
        && pendingFs.opcode === FS_OP_WRITE
      );
    const ofdStateMayChange = (pendingFs.effects & fsEffects.sharedOfdState) !== 0;
    const preservePublishedRegularFileData = (pendingFs.effects & fsEffects.memfsMutation) !== 0
      && (pendingFs.effects & fsEffects.memfsFileData) === 0;
    if (!handle.kernelWorkerRequest || handle.pid <= 0) {
      deps.appendWorkerDebugLog(handle, `fs-start:0x${vmPtr.toString(16)}:local${fsDebugSuffix}`);
      appendFsLifecycleEvent(handle, "start", vmPtr, `:local${fsDebugSuffix}${formatFsRingDebug(handle, vmPtr)}`);
      handle.kernel.thread_process_fs(vmPtr);
      const fsResult = normalizeFsResult(handle.kernel.thread_resume_fs(vmPtr));
      const fsSucceeded = Number(BigInt.asIntN(64, fsResult)) >= 0;
      const memfsMutated = fsSucceeded && (memfsStateMayChange || pendingRingRequest);
      const memfsPublishReady = fsSucceeded && memfsPublishMayBeReady;
      const ofdStateChanged = fsSucceeded && (ofdStateMayChange || pendingRingRequest);
      recordFsTransferStats(handle, pendingFs, fsResult, fsSucceeded, fsEffects, false, memfsPublishReady);
      const postFsRingProgress = snapshotFsRingProgress(handle, vmPtr);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      if (memfsMutated) {
        handle.processMemfsDirty = true;
        if (memfsPublishReady) {
          handle.transferDebugStats.pageCachePublishCalls += 1;
          handle.transferDebugStats.forkSharedMemfsSyncCalls += 1;
          deps.syncForkSharedStateLocally(handle, kernelShareOtherMembers(handle));
        }
        deps.markRuntimeFsDirty(handle);
      } else if (ofdStateChanged) {
        deps.syncForkSharedStateLocally(
          handle,
          kernelShareOtherMembers(handle),
          { includeMemfs: false },
        );
      }
      restoreFsRingProgress(handle, postFsRingProgress);
      clearStaleFsStatusAfterResume(handle, vmPtr, fsResult);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      deps.kickBlockedWorkers(handle);
      deps.appendWorkerDebugLog(handle, `fs-done:0x${vmPtr.toString(16)}:local:rv=${fsResult.toString()}`);
      appendFsLifecycleEvent(
        handle,
        "done",
        vmPtr,
        `:local:rv=${fsResult.toString()}:status=${handle.kernel.thread_status(vmPtr)}${formatFsRingDebug(handle, vmPtr)}`,
      );
      if (fsResultIsPending(handle, vmPtr, fsResult)) {
        appendFsLifecycleEvent(
          handle,
          "pending",
          vmPtr,
          `:local:rv=${fsResult.toString()}${formatFsRingDebug(handle, vmPtr)}`,
        );
        deps.blockWorker(handle, worker, handle.kernel.abi_status_fs?.() ?? -1, vmPtr);
        return;
      }
      deps.enqueueWorker(handle, worker);
      return;
    }

    try {
      deps.appendWorkerDebugLog(handle, `fs-start:0x${vmPtr.toString(16)}${fsDebugSuffix}`);
      appendFsLifecycleEvent(handle, "start", vmPtr, `${fsDebugSuffix}${formatFsRingDebug(handle, vmPtr)}`);
      const heldOfdIds = collectProcessHeldOfds(handle);
      const missingSharedOfdIds = activeKernelShareOfdIds(handle)
        .filter((ofdId) => !heldOfdIds.has(ofdId) || !hasPlausibleOfdState(handle, ofdId));
      if (missingSharedOfdIds.length > 0) {
        await deps.rehydrateForkSharedStateViaKernelWorker([handle], missingSharedOfdIds, {
          includeMemfs: false,
        });
      }
      const dirtyMemfsPeers = kernelShareOtherMembers(handle)
        .filter((member) => (
          member.processMemfsDirty
          && member.runningWorker === null
          && !member.exitInProgress
          && member.kernelWorkerRequest
          && member.pid > 0
        ));
      for (const peer of dirtyMemfsPeers) {
        const targets = [handle, ...kernelShareOtherMembers(peer)]
          .filter((member, index, members) => (
            member !== peer
            && !member.exitInProgress
            && member.runningWorker === null
            && members.indexOf(member) === index
          ));
        peer.transferDebugStats.forkSharedMemfsSyncCalls += 1;
        await deps.syncForkSharedStateViaKernelWorker(peer, targets, {
          includeMemfs: true,
          memfsOnly: true,
          preservePublishedRegularFileData: true,
          sourceVmPtr: peer.vmPtr,
        });
        peer.processMemfsDirty = false;
      }
      deps.assertProcessStandardFdIntegrity(handle, "resume-fs-before");
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      const processFsResponse = await handle.kernelWorkerRequest({
        type: "process-fs",
        pid: handle.pid,
        vmPtr,
        kernelState: handle.kernelState,
      });
      if (processFsResponse.type !== "fs-processed") {
        throw new Error(`unexpected kernel-worker response for process-fs: ${processFsResponse.type}`);
      }
      if (processFsResponse.fsDebug) {
        appendFsLifecycleEvent(handle, "processed", vmPtr, `:${processFsResponse.fsDebug}`);
      }
      handle.kernelState = adoptKernelStateForHandle(handle, processFsResponse.kernelState);
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      const fsResult = normalizeFsResult(processFsResponse.result);
      const fsSucceeded = Number(BigInt.asIntN(64, fsResult)) >= 0;
      const memfsMutated = processFsResponse.memfsMutated || (fsSucceeded && (memfsStateMayChange || pendingRingRequest));
      const memfsPublishReady = fsSucceeded && memfsPublishMayBeReady;
      const ofdStateChanged = processFsResponse.ofdStateChanged || (fsSucceeded && (ofdStateMayChange || pendingRingRequest));
      recordFsTransferStats(handle, pendingFs, fsResult, fsSucceeded, fsEffects, true, memfsPublishReady);
      const postFsRingProgress = snapshotFsRingProgress(handle, vmPtr);
      const postFsFdState = snapshotFsFdState(handle, vmPtr);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      const currentPid = handle.kernel.process_current_pid?.(vmPtr) ?? handle.pid;
      const transientProcessOwnerMismatch = currentPid > 0 && currentPid !== handle.pid;
      if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
        return;
      }
      deps.assertProcessStandardFdIntegrity(handle, "resume-fs");
      const forkShareMembers = kernelShareOtherMembers(handle);
      const syncTargets = forkShareMembers
        .filter((member) => member.runningWorker === null);
      if (ofdStateChanged && !transientProcessOwnerMismatch && forkShareMembers.length > 0) {
        await deps.syncForkSharedStateViaKernelWorker(
          handle,
          syncTargets,
          {
            includeMemfs: false,
            trackHeldOfds: false,
            deltaOfdIds: true,
            sourceVmPtr: vmPtr,
          },
        );
      }
      restoreFsFdState(handle, postFsFdState);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      if (memfsMutated) {
        handle.processMemfsDirty = true;
        if (!transientProcessOwnerMismatch) {
          if (memfsPublishReady) {
            appendFsLifecycleEvent(
              handle,
              "publish-memfs",
              vmPtr,
              `${fsDebugSuffix}:targets=${syncTargets.length}:members=${forkShareMembers.length}`,
            );
            // MemFS publication must not fan out OFD slots: during fork/exec
            // a child may not hold the parent's pipe OFDs, and copying those
            // slots from the child clears the parent's standard streams.
            handle.transferDebugStats.pageCachePublishCalls += 1;
            handle.transferDebugStats.forkSharedMemfsSyncCalls += 1;
            await deps.syncForkSharedStateViaKernelWorker(
              handle,
              syncTargets,
              {
                includeMemfs: true,
                trackHeldOfds: false,
                memfsOnly: true,
                preservePublishedRegularFileData,
                sourceVmPtr: vmPtr,
              },
            );
            if (forkShareMembers.length > 0) {
              await deps.syncPipeStateViaKernelWorker(handle);
              for (const member of forkShareMembers) {
                deps.markRuntimeFsDirty(member);
              }
            }
          }
        }
        deps.markRuntimeFsDirty(handle);
      }
      restoreFsFdState(handle, postFsFdState);
      restoreFsRingProgress(handle, postFsRingProgress);
      clearStaleFsStatusAfterResume(handle, vmPtr, fsResult);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
      deps.kickBlockedWorkers(handle);
      deps.appendWorkerDebugLog(handle, `fs-done:0x${vmPtr.toString(16)}:rv=${fsResult.toString()}`);
      appendFsLifecycleEvent(
        handle,
        "done",
        vmPtr,
        `:rv=${fsResult.toString()}:status=${handle.kernel.thread_status?.(vmPtr) ?? "n/a"}${formatFsRingDebug(handle, vmPtr)}`,
      );
      if (fsResultIsPending(handle, vmPtr, fsResult)) {
        appendFsLifecycleEvent(
          handle,
          "pending",
          vmPtr,
          `:rv=${fsResult.toString()}${formatFsRingDebug(handle, vmPtr)}`,
        );
        deps.blockWorker(handle, worker, handle.kernel.abi_status_fs(), vmPtr);
        return;
      }
      deps.enqueueWorker(handle, worker);
    } catch (error) {
      deps.appendWorkerDebugLog(handle, `fs-err:0x${vmPtr.toString(16)}`);
      appendFsLifecycleEvent(handle, "err", vmPtr, formatFsRingDebug(handle, vmPtr));
      if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
        return;
      }
      deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
    }
  }

  async function resumeBlockedSyscallLocally(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    blockedStatus: number,
    timeoutExpired = false,
  ): Promise<void> {
    let rpcGeneration: number | null = null;
    const finishBlockedRpc = (): void => {
      const blocked = handle.blockedWorkers.get(worker);
      if (
        blocked
        && blocked.vmPtr === vmPtr
        && (rpcGeneration === null || blocked.rpcGeneration === rpcGeneration)
      ) {
        blocked.rpcPending = false;
      }
    };
    try {
      deps.appendWorkerDebugLog(handle, `blocked-rpc-start:${blockedStatus}:0x${vmPtr.toString(16)}`);
      deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:start:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-start:${handle.pid}:${blockedStatus}`);
      const blockedAtStart = handle.blockedWorkers.get(worker);
      if (blockedAtStart && blockedAtStart.vmPtr === vmPtr) {
        blockedAtStart.rpcPending = true;
        blockedAtStart.rpcGeneration = (blockedAtStart.rpcGeneration ?? 0) + 1;
        rpcGeneration = blockedAtStart.rpcGeneration;
      } else {
        return;
      }
      const isCurrentBlockedRpc = (): boolean => {
        const blocked = handle.blockedWorkers.get(worker);
        return blocked !== undefined
          && blocked.vmPtr === vmPtr
          && rpcGeneration !== null
          && blocked.rpcGeneration === rpcGeneration;
      };
      const abortIfStaleBlockedRpc = (): boolean => {
        if (isCurrentBlockedRpc()) {
          return false;
        }
        deps.appendWorkerDebugLog(handle, `blocked-rpc-stale:${blockedStatus}:0x${vmPtr.toString(16)}`);
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-stale:${handle.pid}:${blockedStatus}`);
        return true;
      };
      forceNetworkClosedFds(handle, vmPtr);
      let forkSharedSyncedBeforeResume = false;
      const resumePollTimeoutFn = timeoutExpired
        && blockedStatus === handle.kernel.abi_status_pipe_wait()
        ? handle.kernel.thread_resume_poll_wait_timeout
        : undefined;
      const resumePollTimeout = resumePollTimeoutFn !== undefined;
      if (blockedStatus === handle.kernel.abi_status_pipe_wait() && !resumePollTimeout) {
        const blockedPipeSlots = pipeSlotsForBlockedPipeWait(handle, vmPtr);
        if (handle.kernelShare.forkSharedSyncPromise !== null) {
          deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:await-fork-sync:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
          await handle.kernelShare.forkSharedSyncPromise;
          if (abortIfStaleBlockedRpc()) {
            return;
          }
        }
        for (const member of [...kernelShareOtherMembers(handle), handle]) {
          if (
            member !== handle
            && (
              deps.processHandles.get(member.pid) !== member
              || member.exitInProgress
              || member.workers.size === 0
            )
          ) {
            deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-pipe-skip-stale:${member.pid}:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
            continue;
          }
          if (member !== handle && member.runningWorker !== null) {
            deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-pipe-skip-running:${member.pid}:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
            continue;
          }
          if (
            member !== handle
            && blockedPipeSlots !== null
            && !processReferencesAnyPipeSlot(member, member.vmPtr, blockedPipeSlots)
          ) {
            deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-pipe-skip-unrelated:${member.pid}:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-skip-unrelated:${handle.pid}->${member.pid}`);
            continue;
          }
          if (
            member !== handle
            && [...member.blockedWorkers.values()].some((blocked) => blocked.rpcPending)
          ) {
            deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-pipe-skip-rpc-pending:${member.pid}:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-skip-rpc-pending:${handle.pid}->${member.pid}`);
            continue;
          }
          if (member.kernelWorkerRequest && member.pid > 0) {
            deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-pipe:${member.pid}:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-start:${handle.pid}->${member.pid}`);
            await deps.requestPipeStateSync(member, false);
            if (abortIfStaleBlockedRpc()) {
              return;
            }
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-done:${handle.pid}->${member.pid}`);
          }
        }
        if (handle.kernelWorkerRequest && handle.pid > 0) {
          deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-blocked:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-blocked-start:${handle.pid}`);
          await deps.syncBlockedForkSharedStateViaKernelWorker(handle, vmPtr);
          if (abortIfStaleBlockedRpc()) {
            return;
          }
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-blocked-done:${handle.pid}`);
          forkSharedSyncedBeforeResume = true;
          if (blockedStatus === handle.kernel.abi_status_pipe_wait()) {
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-before-resume-start:${handle.pid}`);
            await deps.requestPipeStateSync(handle, true);
            if (abortIfStaleBlockedRpc()) {
              return;
            }
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-before-resume-done:${handle.pid}`);
          }
        }
        deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:reconcile:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
        reconcileKernelSharePipeControl(handle);
        if (handle.kernel.thread_poll_wait_has_socket?.(vmPtr) === 1) {
          deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-socket:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
          await syncSocketStatePreservingNetworkBridge(handle);
          if (abortIfStaleBlockedRpc()) {
            return;
          }
        }
      }
      if (abortIfStaleBlockedRpc()) {
        return;
      }
      if (blockedStatus === handle.kernel.abi_status_pipe_wait()) {
        preserveHostStdinWriterForBlockedRead(handle, vmPtr);
      }
      if (
        blockedStatus === handle.kernel.abi_status_pipe_wait()
        && !resumePollTimeout
        && handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) === 1
        && (handle.kernel.thread_blocked_pipe_buffered_len?.(vmPtr) ?? 0) === 0
      ) {
        const blockedPipeWriteOpen = handle.kernel.thread_blocked_pipe_write_open?.(vmPtr) ?? 0;
        if (
          blockedPipeWriteOpen > 0
          && handle.kernel.thread_resume_pipe_read_open_empty?.(vmPtr) === 1
        ) {
          handle.kernelState = readKernelRuntimeState(handle.kernel);
          finishBlockedRpc();
          deps.appendWorkerDebugLog(handle, `blocked-rpc-pipe-open-empty:0x${vmPtr.toString(16)}`);
          deps.handleBlockedRetryResult(handle, worker, vmPtr, handle.kernel.thread_status(vmPtr));
          return;
        }
        if (activeChildMayHoldBlockedPipeWriter(deps, handle, vmPtr)) {
          finishBlockedRpc();
          deps.appendWorkerDebugLog(handle, `blocked-rpc-defer-pipe-eof:0x${vmPtr.toString(16)}`);
          deps.handleBlockedRetryResult(handle, worker, vmPtr, blockedStatus);
          return;
        }
        if (handle.pendingChildExitRecords > 0) {
          finishBlockedRpc();
          deps.appendWorkerDebugLog(handle, `blocked-rpc-defer-pipe-eof-child-exit:0x${vmPtr.toString(16)}`);
          deps.handleBlockedRetryResult(handle, worker, vmPtr, blockedStatus);
          return;
        }
      }
      const blockedPipeReadReady = handle.kernel.thread_blocked_pipe_read_ready?.(vmPtr);
      const blockedPipeWriteReady = handle.kernel.thread_blocked_pipe_write_ready?.(vmPtr);
      const blockedPipePollReadyCount = handle.kernel.thread_poll_wait_ready_count?.(vmPtr);
      const blockedPollPipeBuffered = blockedStatus === handle.kernel.abi_status_pipe_wait()
        ? blockedPollPipeHasBufferedData(handle, vmPtr)
        : false;
      const pollLikePipeWaitNoWriter = blockedStatus === handle.kernel.abi_status_pipe_wait()
        && !resumePollTimeout
        && isPollLikePipeWait(handle, vmPtr)
        && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) !== 1
        && pollPipeBufferedLenForThreadStep(handle, vmPtr) === 0
        && pollPipeWriteOpenForThreadStep(handle, vmPtr) <= 0;
      if (
        blockedStatus === handle.kernel.abi_status_pipe_wait()
        && !resumePollTimeout
        && isPollLikePipeWait(handle, vmPtr)
        && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) !== 1
        && pollPipeBufferedLenForThreadStep(handle, vmPtr) === 0
        && handle.kernel.thread_poll_hup_sync_pending?.(vmPtr) !== 1
        && activeChildMayHoldBlockedPipeWriter(deps, handle, vmPtr)
      ) {
        handle.kernelState = readKernelRuntimeState(handle.kernel);
        finishBlockedRpc();
        deps.appendWorkerDebugLog(handle, `blocked-rpc-defer-poll-hup-active-child:0x${vmPtr.toString(16)}`);
        deps.handleBlockedRetryResult(handle, worker, vmPtr, blockedStatus);
        return;
      }
      if (
        blockedStatus === handle.kernel.abi_status_pipe_wait()
        && !resumePollTimeout
        && blockedPipeReadReady === 0
        && (blockedPipeWriteReady === undefined || blockedPipeWriteReady <= 0)
        && (blockedPipePollReadyCount === undefined || blockedPipePollReadyCount <= 0)
        && !blockedPollPipeBuffered
        && !pollLikePipeWaitNoWriter
        && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) !== 1
        && handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) !== 1
        && handle.kernel.thread_poll_hup_sync_pending?.(vmPtr) !== 1
      ) {
        handle.kernelState = readKernelRuntimeState(handle.kernel);
        finishBlockedRpc();
        deps.appendWorkerDebugLog(
          handle,
          `blocked-rpc-defer-pipe-not-ready:0x${vmPtr.toString(16)}:read=${blockedPipeReadReady}:write=${blockedPipeWriteReady ?? "n/a"}:poll=${blockedPipePollReadyCount ?? "n/a"}:buffered=${blockedPollPipeBuffered ? 1 : 0}`,
        );
        deps.handleBlockedRetryResult(handle, worker, vmPtr, blockedStatus);
        return;
      }
      const pollLikePipeWaitWithReadySocket = blockedStatus === handle.kernel.abi_status_pipe_wait()
        && !resumePollTimeout
        && isPollLikePipeWait(handle, vmPtr)
        && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) === 1
        && (handle.kernel.thread_poll_wait_ready_count?.(vmPtr) ?? 0) > 0;
      if (pollLikePipeWaitWithReadySocket) {
        const blocked = handle.blockedWorkers.get(worker);
        if (!blocked || blocked.vmPtr !== vmPtr) {
          return;
        }
        if (blocked.retryTimer !== null) {
          clearTimeout(blocked.retryTimer);
          blocked.retryTimer = null;
        }
        const thread = deps.threadWorkerStates.get(worker);
        if (!thread) {
          return;
        }
        thread.resumeBlockedOnNextStep = true;
        handle.kernelState = readKernelRuntimeState(handle.kernel);
        finishBlockedRpc();
        handle.blockedWorkers.delete(worker);
        deps.appendWorkerDebugLog(handle, `blocked-rpc-resume-socket-poll-in-worker:0x${vmPtr.toString(16)}`);
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-resume-socket-poll-in-worker:${handle.pid}`);
        deps.enqueueWorker(handle, worker);
        return;
      }
      const blockedPipeFd = blockedPipeWaitFd(handle, vmPtr);
      const readPipeReachedEofAfterWait = blockedStatus === handle.kernel.abi_status_pipe_wait()
        && !resumePollTimeout
        && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) !== 1
        && !isPollLikePipeWait(handle, vmPtr)
        && blockedPipeReadReady === 1
        && blockedPipeWriteReady !== 1
        && handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) !== 1
        && pipeSlotBufferedLenForThreadStepFd(
          handle,
          vmPtr,
          blockedPipeFd,
        ) === 0
        && pipeSlotWriteOpenForThreadStepFd(
          handle,
          vmPtr,
          blockedPipeFd,
        ) <= 0;
      const readPipeEofPendingAfterSync = blockedStatus === handle.kernel.abi_status_pipe_wait()
        && !resumePollTimeout
        && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) !== 1
        && !isPollLikePipeWait(handle, vmPtr)
        && handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) === 1
        && (handle.kernel.thread_blocked_pipe_buffered_len?.(vmPtr) ?? 0) === 0
        && (handle.kernel.thread_blocked_pipe_write_open?.(vmPtr) ?? 0) <= 0;
      if (readPipeEofPendingAfterSync) {
        attachKernelRuntimeState(handle.kernel, handle.kernelState);
        const resumeFd = BigInt(blockedPipeFd);
        const resumeResult = handle.kernel.thread_resume_blocked_syscall(vmPtr);
        handle.kernelState = readKernelRuntimeState(handle.kernel);
        finishBlockedRpc();
        deps.appendWorkerDebugLog(
          handle,
          `blocked-rpc-resume-pipe-eof:0x${vmPtr.toString(16)}:fd=${resumeFd.toString()}:ret=${String(resumeResult)}:status=${handle.kernel.thread_status(vmPtr)}`,
        );
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `blocked-rpc-resume-pipe-eof:${handle.pid}:fd=${resumeFd.toString()}:ret=${String(resumeResult)}:status=${handle.kernel.thread_status(vmPtr)}`,
        );
        deps.handleBlockedRetryResult(handle, worker, vmPtr, handle.kernel.thread_status(vmPtr));
        return;
      }
      if (
        blockedStatus === handle.kernel.abi_status_pipe_wait()
        && PIPE_RESUME_IN_WORKER_ENABLED
        && !resumePollTimeout
        && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) !== 1
        && !isPollLikePipeWait(handle, vmPtr)
        && blockedPipeWriteReady !== 1
        && !readPipeReachedEofAfterWait
      ) {
        const pollLikePipeWait = false;
        const bufferedLen = handle.kernel.thread_blocked_pipe_buffered_len?.(vmPtr) ?? 0;
        const writeOpen = handle.kernel.thread_blocked_pipe_write_open?.(vmPtr) ?? 0;
        const syncedWriteOpen = pollLikePipeWait
          ? pollPipeWriteOpenForThreadStep(handle, vmPtr)
          : pipeSlotWriteOpenForThreadStepFd(handle, vmPtr, blockedPipeFd);
        const syncedBufferedLen = pollLikePipeWait
          ? pollPipeBufferedLenForThreadStep(handle, vmPtr)
          : pipeSlotBufferedLenForThreadStepFd(handle, vmPtr, blockedPipeFd);
        const eofReady = (
          handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) === 1
          && bufferedLen === 0
          && writeOpen <= 0
          && syncedWriteOpen <= 0
        ) || (
            bufferedLen === 0
            && syncedBufferedLen === 0
            && syncedWriteOpen <= 0
            && blockedPipeWriteReady !== 1
        );
        const pipeReadReady = handle.kernel.thread_blocked_pipe_read_ready?.(vmPtr) === 1
          && !(syncedBufferedLen === 0 && syncedWriteOpen > 0);
        const pollReady = (blockedPipePollReadyCount ?? 0) > 0 || blockedPollPipeBuffered;
        const currentFdOpenEmpty = !pollLikePipeWait && syncedBufferedLen === 0 && syncedWriteOpen > 0;
        if (PIPE_RESUME_DEBUG_ENABLED) {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            [
              `pipe-resume-check:${handle.pid}`,
              `a7=${handle.kernel.thread_a7?.(vmPtr)?.toString() ?? "n/a"}`,
              `sys=${blockedPipeWaitSyscallNr(handle, vmPtr)?.toString() ?? "n/a"}`,
              `fd=${blockedPipeFd}`,
              `blockedFd=${handle.kernel.thread_blocked_fd?.(vmPtr) ?? "n/a"}`,
              `blockedOfd=${handle.kernel.thread_blocked_ofd_id?.(vmPtr) ?? "n/a"}`,
              `poll=${pollLikePipeWait ? 1 : 0}`,
              `ready=${blockedPipeReadReady ?? "n/a"}`,
              `writeReady=${blockedPipeWriteReady ?? "n/a"}`,
              `pollReady=${blockedPipePollReadyCount ?? "n/a"}`,
              `buf=${bufferedLen}`,
              `wo=${writeOpen}`,
              `syncedBuf=${syncedBufferedLen}`,
              `syncedWo=${syncedWriteOpen}`,
              `eof=${eofReady ? 1 : 0}`,
              `openEmpty=${currentFdOpenEmpty ? 1 : 0}`,
              `eofPending=${handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) ?? "n/a"}`,
              `hupPending=${handle.kernel.thread_poll_hup_sync_pending?.(vmPtr) ?? "n/a"}`,
              `holders=${formatBlockedPipeSlotHolders(handle, vmPtr)}`,
            ].join(":"),
          );
        }
        if (currentFdOpenEmpty) {
          handle.kernelState = readKernelRuntimeState(handle.kernel);
          finishBlockedRpc();
          deps.appendWorkerDebugLog(
            handle,
            `blocked-rpc-defer-pipe-open-empty-synced:0x${vmPtr.toString(16)}:buf=${bufferedLen}:wo=${writeOpen}:syncedBuf=${syncedBufferedLen}:syncedWo=${syncedWriteOpen}`,
          );
          deps.handleBlockedRetryResult(handle, worker, vmPtr, blockedStatus);
          return;
        }
        const pipeSlotsForResume = pollLikePipeWait
          ? snapshotPollPipeSlotForThreadStep(handle, vmPtr, { includeEmpty: eofReady || pollReady })
          : snapshotBlockedPipeSlotForThreadStep(handle, vmPtr, { includeEmpty: eofReady });
        if (!pipeReadReady && !eofReady && pipeSlotsForResume.length === 0) {
          handle.kernelState = readKernelRuntimeState(handle.kernel);
          finishBlockedRpc();
          deps.appendWorkerDebugLog(
            handle,
            `blocked-rpc-defer-pipe-thread-resume-not-ready:0x${vmPtr.toString(16)}:ready=${pipeReadReady ? 1 : 0}:eof=${eofReady ? 1 : 0}:buf=${bufferedLen}:wo=${writeOpen}:slots=0`,
          );
          deps.handleBlockedRetryResult(handle, worker, vmPtr, blockedStatus);
          return;
        }
        const blocked = handle.blockedWorkers.get(worker);
        if (!blocked || blocked.vmPtr !== vmPtr) {
          return;
        }
        if (blocked.retryTimer !== null) {
          clearTimeout(blocked.retryTimer);
          blocked.retryTimer = null;
        }
        const thread = deps.threadWorkerStates.get(worker);
        if (!thread) {
          return;
        }
        if (pipeSlotsForResume.length === 0) {
          handle.kernelState = readKernelRuntimeState(handle.kernel);
          finishBlockedRpc();
          deps.appendWorkerDebugLog(handle, `blocked-rpc-defer-pipe-empty-resume:0x${vmPtr.toString(16)}`);
          deps.handleBlockedRetryResult(handle, worker, vmPtr, blockedStatus);
          return;
        }
        thread.pendingPipeSlotsForNextStep.push(...pipeSlotsForResume);
        thread.resumeBlockedOnNextStep = true;
        handle.kernelState = readKernelRuntimeState(handle.kernel);
        finishBlockedRpc();
        handle.blockedWorkers.delete(worker);
        deps.appendWorkerDebugLog(handle, `blocked-rpc-resume-in-worker:0x${vmPtr.toString(16)}`);
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-resume-in-worker:${handle.pid}`);
        deps.enqueueWorker(handle, worker);
        return;
      }
      if (readPipeReachedEofAfterWait) {
        handle.kernelState = readKernelRuntimeState(handle.kernel);
      }
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      const blockedSyscallNr = handle.kernel.thread_blocked_syscall_nr?.(vmPtr) ?? 0n;
      const blockedArg0 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 0) ?? 0n;
      const blockedArg1 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 1) ?? 0n;
      const blockedArg2 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 2) ?? 0n;
      const blockedArg3 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 3) ?? 0n;
      if (resumePollTimeout) {
        advanceGuestTimeByMs(handle, Number(handle.kernel.thread_poll_wait_timeout?.(vmPtr) ?? 0));
        resumePollTimeoutFn(vmPtr);
        queueBlockedPollWaitGuestMemoryWrites(
          handle,
          worker,
          blockedSyscallNr,
          blockedArg0,
          blockedArg1,
          blockedArg2,
          blockedArg3,
        );
      } else if (
        timeoutExpired
        && (
          blockedStatus === handle.kernel.abi_status_connect()
          || blockedStatus === handle.kernel.abi_status_socket_recv()
          || blockedStatus === handle.kernel.abi_status_socket_send()
        )
        && handle.kernel.thread_resume_socket_wait_timeout
      ) {
        handle.kernel.thread_resume_socket_wait_timeout(vmPtr);
      } else {
        forceNetworkClosedFds(handle, vmPtr);
        const resumeFd = blockedArg0;
        const resumeBuf = blockedArg1;
        const resumeCount = blockedArg2;
        const resumeResult = handle.kernel.thread_resume_blocked_syscall(vmPtr);
        if (blockedStatus === handle.kernel.abi_status_pipe_wait() && resumeResult > 0n) {
          const queuedPollWrites = queueBlockedPollWaitGuestMemoryWrites(
            handle,
            worker,
            blockedSyscallNr,
            blockedArg0,
            blockedArg1,
            blockedArg2,
            blockedArg3,
          );
          if (!queuedPollWrites && blockedPipeResumeWritesGuestBuffer(handle, blockedSyscallNr)) {
            queueGuestBufferWrite(handle, worker, resumeBuf, resumeResult);
          }
        }
        if (
          blockedStatus === handle.kernel.abi_status_socket_recv()
          && resumeResult > 0n
          && blockedSocketResumeWritesGuestBuffer(handle, blockedSyscallNr)
        ) {
          queueGuestBufferWrite(handle, worker, resumeBuf, resumeResult);
        }
        if (PIPE_RESUME_DEBUG_ENABLED && blockedStatus === handle.kernel.abi_status_pipe_wait()) {
          const event = [
            `blocked-rpc-resume-pipe:${handle.pid}`,
            `fd=${resumeFd.toString()}`,
            `buf=0x${resumeBuf.toString(16)}`,
            `count=${resumeCount.toString()}`,
            `saved=${blockedSyscallNr.toString()}/${blockedArg0.toString()}/0x${blockedArg1.toString(16)}/${blockedArg2.toString()}`,
            `ret=${resumeResult.toString()}`,
            `status=${handle.kernel.thread_status(vmPtr)}`,
            `data="${formatGuestBufferPreview(handle, resumeBuf, resumeResult > 0n ? resumeResult : resumeCount)}"`,
          ].join(":");
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, event);
          deps.appendWorkerDebugLog(
            handle,
            [
              `blocked-rpc-resume-pipe:0x${vmPtr.toString(16)}`,
              `fd=${resumeFd.toString()}`,
              `buf=0x${resumeBuf.toString(16)}`,
              `count=${resumeCount.toString()}`,
              `saved=${blockedSyscallNr.toString()}/${blockedArg0.toString()}/0x${blockedArg1.toString(16)}/${blockedArg2.toString()}`,
              `ret=${resumeResult.toString()}`,
              `status=${handle.kernel.thread_status(vmPtr)}`,
              `data="${formatGuestBufferPreview(handle, resumeBuf, resumeResult > 0n ? resumeResult : resumeCount)}"`,
            ].join(":"),
          );
        }
      }
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      const currentPid = handle.kernel.process_current_pid?.(vmPtr) ?? handle.pid;
      const transientProcessOwnerMismatch = currentPid > 0 && currentPid !== handle.pid;
      if (!transientProcessOwnerMismatch && !forkSharedSyncedBeforeResume && !resumePollTimeout) {
        deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-after-resume:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
        await deps.syncBlockedForkSharedStateViaKernelWorker(handle, vmPtr);
        if (abortIfStaleBlockedRpc()) {
          return;
        }
      }

      if (
        !transientProcessOwnerMismatch
        && blockedStatus === handle.kernel.abi_status_pipe_wait()
        && !resumePollTimeout
        && handle.kernel.thread_status(vmPtr) === handle.kernel.abi_status_pipe_wait()
      ) {
        deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-pipe-after:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-after-start:${handle.pid}`);
        await deps.requestPipeStateSync(handle, true);
        if (abortIfStaleBlockedRpc()) {
          return;
        }
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-after-done:${handle.pid}`);
        for (let attempt = 0; attempt < 2; attempt += 1) {
          const readReadyAfterSync = handle.kernel.thread_blocked_pipe_read_ready?.(vmPtr) === 1;
          const writeReadyAfterSync = handle.kernel.thread_blocked_pipe_write_ready?.(vmPtr) === 1;
          if (
            handle.kernel.thread_status(vmPtr) !== handle.kernel.abi_status_pipe_wait()
            || (!readReadyAfterSync && !writeReadyAfterSync)
          ) {
            break;
          }
          if (abortIfStaleBlockedRpc()) {
            return;
          }
          deps.appendWorkerDebugLog(handle, `blocked-rpc-pipe-ready-after-sync:${attempt}:0x${vmPtr.toString(16)}`);
          const blockedSyscallNr = handle.kernel.thread_blocked_syscall_nr?.(vmPtr) ?? 0n;
          const blockedArg0 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 0) ?? 0n;
          const blockedArg1 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 1) ?? 0n;
          const blockedArg2 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 2) ?? 0n;
          const blockedArg3 = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 3) ?? 0n;
          const resumeFd = blockedArg0;
          const resumeBuf = blockedArg1;
          const resumeCount = blockedArg2;
          const resumeResult = handle.kernel.thread_resume_blocked_syscall(vmPtr);
          if (resumeResult > 0n) {
            const queuedPollWrites = queueBlockedPollWaitGuestMemoryWrites(
              handle,
              worker,
              blockedSyscallNr,
              blockedArg0,
              blockedArg1,
              blockedArg2,
              blockedArg3,
            );
            if (!queuedPollWrites && blockedPipeResumeWritesGuestBuffer(handle, blockedSyscallNr)) {
              queueGuestBufferWrite(handle, worker, resumeBuf, resumeResult);
            }
          }
          if (PIPE_RESUME_DEBUG_ENABLED) {
            const event = [
              `blocked-rpc-resume-pipe-after:${handle.pid}:${attempt}`,
              `fd=${resumeFd.toString()}`,
              `buf=0x${resumeBuf.toString(16)}`,
              `count=${resumeCount.toString()}`,
              `saved=${blockedSyscallNr.toString()}/${blockedArg0.toString()}/0x${blockedArg1.toString(16)}/${blockedArg2.toString()}`,
              `ret=${resumeResult.toString()}`,
              `status=${handle.kernel.thread_status(vmPtr)}`,
              `data="${formatGuestBufferPreview(handle, resumeBuf, resumeResult > 0n ? resumeResult : resumeCount)}"`,
            ].join(":");
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, event);
            deps.appendWorkerDebugLog(
              handle,
              [
                `blocked-rpc-resume-pipe-after:${attempt}:0x${vmPtr.toString(16)}`,
                `fd=${resumeFd.toString()}`,
                `buf=0x${resumeBuf.toString(16)}`,
              `count=${resumeCount.toString()}`,
              `saved=${blockedSyscallNr.toString()}/${blockedArg0.toString()}/0x${blockedArg1.toString(16)}/${blockedArg2.toString()}`,
              `ret=${resumeResult.toString()}`,
              `status=${handle.kernel.thread_status(vmPtr)}`,
              `data="${formatGuestBufferPreview(handle, resumeBuf, resumeResult > 0n ? resumeResult : resumeCount)}"`,
            ].join(":"),
            );
          }
          handle.kernelState = readKernelRuntimeState(handle.kernel);
          if (handle.kernel.thread_status(vmPtr) !== handle.kernel.abi_status_pipe_wait()) {
            break;
          }
          await deps.requestPipeStateSync(handle, true);
          if (abortIfStaleBlockedRpc()) {
            return;
          }
        }
      } else if (
        pollLikePipeWaitNoWriter
        && handle.kernel.thread_status(vmPtr) === handle.kernel.abi_status_pipe_wait()
      ) {
        const resumeFd = blockedArg0;
        const resumeBuf = blockedArg1;
        const resumeCount = blockedArg2;
        const resumeResult = handle.kernel.thread_resume_blocked_syscall(vmPtr);
        if (resumeResult > 0n) {
          const queuedPollWrites = queueBlockedPollWaitGuestMemoryWrites(
            handle,
            worker,
            blockedSyscallNr,
            blockedArg0,
            blockedArg1,
            blockedArg2,
            blockedArg3,
          );
          if (!queuedPollWrites && blockedPipeResumeWritesGuestBuffer(handle, blockedSyscallNr)) {
            queueGuestBufferWrite(handle, worker, resumeBuf, resumeResult);
          }
        }
        handle.kernelState = readKernelRuntimeState(handle.kernel);
        finishBlockedRpc();
        deps.appendWorkerDebugLog(
          handle,
          [
            `blocked-rpc-resume-poll-hup:0x${vmPtr.toString(16)}`,
            `fd=${resumeFd.toString()}`,
            `buf=0x${resumeBuf.toString(16)}`,
            `count=${resumeCount.toString()}`,
            `ret=${resumeResult.toString()}`,
            `status=${handle.kernel.thread_status(vmPtr)}`,
          ].join(":"),
        );
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `blocked-rpc-resume-poll-hup:${handle.pid}:ret=${resumeResult.toString()}:status=${handle.kernel.thread_status(vmPtr)}`,
        );
        deps.handleBlockedRetryResult(handle, worker, vmPtr, handle.kernel.thread_status(vmPtr));
        return;
      } else if (
        !transientProcessOwnerMismatch
        && (
          blockedStatus === handle.kernel.abi_status_connect()
          || blockedStatus === handle.kernel.abi_status_socket_recv()
          || blockedStatus === handle.kernel.abi_status_socket_send()
        )
      ) {
        await syncSocketStatePreservingNetworkBridge(handle);
        if (abortIfStaleBlockedRpc()) {
          return;
        }
      }

      const blocked = handle.blockedWorkers.get(worker);
      if (!blocked || blocked.vmPtr !== vmPtr) {
        return;
      }
      finishBlockedRpc();
      deps.appendWorkerDebugLog(handle, `blocked-rpc-done:${handle.kernel.thread_status(vmPtr)}:0x${vmPtr.toString(16)}`);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-done:${handle.pid}:${handle.kernel.thread_status(vmPtr)}`);

      if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
        return;
      }

      deps.handleBlockedRetryResult(handle, worker, vmPtr, handle.kernel.thread_status(vmPtr));
    } catch (error) {
      finishBlockedRpc();
      if (handle.workers.has(worker) && deps.threadWorkerStates.has(worker)) {
        const fd = blockedPipeWaitFd(handle, vmPtr);
        const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
        const buf = handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 1) ?? handle.kernel.thread_a1(vmPtr);
        const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
        const debug = [
          `vm=0x${vmPtr.toString(16)}`,
          `fd=${fd}`,
          `buf=${buf.toString()}`,
          `process_ptr=0x${handle.kernel.thread_process_ptr(vmPtr).toString(16)}`,
          `ofd_table_ptr=0x${handle.kernel.kernel_ofd_table_ptr().toString(16)}`,
          `ofd_id=${ofdId}`,
          `ofd_type=${handle.kernel.kernel_ofd_fd_type(ofdId)}`,
          `ofd_host_fd=${handle.kernel.kernel_ofd_host_fd(ofdId)}`,
          `ofd_flags=${handle.kernel.kernel_ofd_status_flags(ofdId)}`,
          `ofd_ino=${handle.kernel.kernel_ofd_ino(ofdId)}`,
          `ofd_offset=${handle.kernel.kernel_ofd_offset(ofdId).toString()}`,
          `blocked_fd=${handle.kernel.thread_blocked_fd(vmPtr)}`,
          `blocked_ofd_id=${handle.kernel.thread_blocked_ofd_id(vmPtr)}`,
        ].join(" ");
        deps.failProcess(handle, new Error(`${detail}\n${debug}`));
      }
    } finally {
      finishBlockedRpc();
    }
  }

  return {
    canonicalizeActiveVforkChildViaKernelWorker,
    canonicalizeChildLifecycleViaKernelWorker,
    initializeProcessLifecycleViaKernelWorker,
    recordChildExitViaKernelWorker,
    registerProcessWithKernelWorker,
    releaseHostStdioViaKernelWorker,
    resumeBlockedSyscallLocally,
    resumeConnectViaKernelWorker,
    resumeFsViaKernelWorker,
    resumeVforkExecveParentViaKernelWorker,
    resumeVforkParentViaKernelWorker,
    resumeWait4ViaKernelWorker,
    syncProcessMemfsViaKernelWorker,
    syncProcessLifecycleViaKernelWorker,
    unregisterProcessWithKernelWorker,
  };
}
