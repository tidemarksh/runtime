import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../abi.js";
import { resumeBlockedSyscallBySupervisor } from "../../supervisor-blocked-resume.js";
import { kernelShareOtherMembers, reconcileKernelSharePipeControl } from "../kernel-share.js";
import { forceNetworkClosedFds } from "../network-socket-close.js";
import { syncPipeOfdGenerationsForSlot } from "../ownership/ofd-table.js";
import { pipeSlotHasLiveWriter } from "../ownership/pipe-control.js";
import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import type { KernelWorkerPipeSlotSnapshot } from "../../messages.js";
import { PIPE_HEAD_OFFSET, PIPE_READ_OPEN_OFFSET, PIPE_TAIL_OFFSET, PIPE_WRITE_OPEN_OFFSET } from "../../layout/pipe.js";
import { POLLFD_BYTES } from "../../layout/poll.js";
import { activeChildMayHoldBlockedPipeWriter } from "./pipe.js";
import {
  completedKernelRpcOutcome,
  kernelRpcResumeEffectsNeedContinuation,
  type KernelRpcResumeEffect,
  type KernelRpcResumeOutcome,
} from "./outcomes.js";
import type { SyncCause } from "../contracts/sync.js";
import { appendPendingGuestMemoryWrites } from "../guest-memory-writes.js";

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
const O_NONBLOCK_FALLBACK = 0x800;

export interface WorkerKernelRpcBlockedRpcDeps {
  processHandles: Map<number, ProcessHandle>;
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  clearHostBlockedRetryTimer(handle: ProcessHandle, worker: Worker): void;
  requestThreadWorkerBlockedResume(handle: ProcessHandle, worker: Worker): void;
  handleHostBlockedRetryResult(handle: ProcessHandle, worker: Worker, vmPtr: number, status: number): void;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  syncBlockedForkSharedStateViaKernelWorker(handle: ProcessHandle, sourceVmPtr: number | undefined, cause: SyncCause): Promise<void>;
  syncSocketStateViaKernelWorker(handle: ProcessHandle, cause: SyncCause): Promise<{ socketUsed: number; netBufUsed: bigint }>;
}

function createWorkerKernelRpcBlockedRpcHelperInternals(deps: WorkerKernelRpcBlockedRpcDeps) {
  const pendingOutcomeEffects = new WeakMap<Worker, KernelRpcResumeEffect[]>();

  function pushOutcomeEffect(worker: Worker, effect: KernelRpcResumeEffect): void {
    const effects = pendingOutcomeEffects.get(worker);
    if (effects) {
      effects.push(effect);
      return;
    }
    pendingOutcomeEffects.set(worker, [effect]);
  }

  function takeOutcomeEffects(worker: Worker): KernelRpcResumeEffect[] | undefined {
    const effects = pendingOutcomeEffects.get(worker);
    pendingOutcomeEffects.delete(worker);
    return effects;
  }

  function outcomeEffectsNeedResumeContinuation(worker: Worker): boolean {
    return kernelRpcResumeEffectsNeedContinuation(pendingOutcomeEffects.get(worker));
  }

  function emitHostBlockedRetryResult(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    status: number,
  ): void {
    deps.handleHostBlockedRetryResult(handle, worker, vmPtr, status);
  }

function blockedRpcSyncCause(status: number): SyncCause {
  return { kind: "blocked-rpc", status };
}

function blockedStatusIsEpollWait(handle: ProcessHandle, status: number): boolean {
  const epollWaitStatus = handle.kernel.abi_status_epoll_wait?.();
  return epollWaitStatus !== undefined && status === epollWaitStatus;
}

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
  const writeLen = requested;
  const memory = new Uint8Array(handle.memory.buffer);
  if (absolute < 0 || absolute + writeLen > memory.byteLength) {
    return;
  }
  const thread = deps.threadWorkerStates.get(worker);
  if (!thread) {
    return;
  }
  appendPendingGuestMemoryWrites(thread, [{
    guestPtr: start,
    bytes: memory.slice(absolute, absolute + writeLen),
  }]);
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

function blockedPipeWaitWritesPipe(handle: ProcessHandle, syscallNr: bigint): boolean {
  return syscallMatches(syscallNr, handle.kernel.abi_sys_write)
    || syscallMatches(syscallNr, handle.kernel.abi_sys_writev);
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

function blockedPipeReadOpenEmptyAfterSync(
  handle: ProcessHandle,
  vmPtr: number,
  blockedStatus: number,
): boolean {
  if (
    blockedStatus !== handle.kernel.abi_status_pipe_wait()
    || handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) !== 1
    || (handle.kernel.thread_blocked_pipe_buffered_len?.(vmPtr) ?? 0) !== 0
    || (handle.kernel.thread_blocked_pipe_write_open?.(vmPtr) ?? 0) <= 0
  ) {
    return false;
  }
  const fd = blockedPipeWaitFd(handle, vmPtr);
  if (fd < 0 || fd >= handle.kernel.abi_max_fds()) {
    return false;
  }
  const blockedOfdId = handle.kernel.thread_blocked_ofd_id?.(vmPtr) ?? 0;
  const ofdId = blockedOfdId > 0 ? blockedOfdId : handle.kernel.process_fd_ofd_id(vmPtr, fd);
  if (
    ofdId <= 0
    || ofdId >= handle.kernel.abi_max_fds()
    || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
  ) {
    return false;
  }
  const nonblock = handle.kernel.abi_o_nonblock?.() ?? O_NONBLOCK_FALLBACK;
  return (handle.kernel.kernel_ofd_status_flags(ofdId) & nonblock) !== 0;
}

function blockedPipeWaitAlreadyReadyForSelfResume(
  handle: ProcessHandle,
  vmPtr: number,
  blockedStatus: number,
): boolean {
  if (
    blockedStatus !== handle.kernel.abi_status_pipe_wait()
    || handle.kernel.thread_poll_wait_has_socket?.(vmPtr) === 1
    || isPollLikePipeWait(handle, vmPtr)
  ) {
    return false;
  }
  const syscallNr = blockedPipeWaitSyscallNr(handle, vmPtr) ?? 0n;
  if (blockedPipeResumeWritesGuestBuffer(handle, syscallNr)) {
    return handle.kernel.thread_blocked_pipe_read_ready?.(vmPtr) === 1
      || handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) === 1;
  }
  if (blockedPipeWaitWritesPipe(handle, syscallNr)) {
    return handle.kernel.thread_blocked_pipe_write_ready?.(vmPtr) === 1;
  }
  return false;
}

function processHasOwnerAsyncWorker(handle: ProcessHandle): boolean {
  for (const worker of handle.workers) {
    const thread = deps.threadWorkerStates.get(worker);
    if ((thread?.ownerAsyncStatus ?? null) !== null) {
      return true;
    }
  }
  return false;
}

function recoverOwnerAsyncOkPipePeer(
  blockedHandle: ProcessHandle,
  peerHandle: ProcessHandle,
  pipeSlots: ReadonlySet<number> | null,
): boolean {
  if (pipeSlots === null) {
    return false;
  }
  const okStatus = peerHandle.kernel.abi_status_ok?.();
  if (okStatus === undefined) {
    return false;
  }
  for (const worker of peerHandle.workers) {
    const thread = deps.threadWorkerStates.get(worker);
    if (
      thread === undefined
      || thread.ownerAsyncStatus !== okStatus
      || thread.initialized !== true
      || thread.waitingInWorkerStatus !== null
      || (thread.pendingGuestMemoryWrites?.length ?? 0) > 0
      || peerHandle.kernel.thread_status(thread.vmPtr) !== okStatus
      || peerHandle.kernel.thread_process_ptr(thread.vmPtr) === 0
      || !processReferencesAnyPipeSlot(peerHandle, thread.vmPtr, pipeSlots)
    ) {
      continue;
    }
    thread.ownerAsyncStatus = null;
    thread.runningGeneration = null;
    deps.appendWorkerDebugLog(
      blockedHandle,
      `blocked-rpc-recover-owner-async-ok-peer:${blockedHandle.pid}->${peerHandle.pid}:0x${thread.vmPtr.toString(16)}`,
    );
    deps.appendRuntimeLifecycleEvent(
      blockedHandle.runtimeId,
      `blocked-rpc-recover-owner-async-ok-peer:${blockedHandle.pid}->${peerHandle.pid}`,
    );
    deps.enqueueWorker(peerHandle, worker);
    return true;
  }
  return false;
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

function pipeSlotReadOpen(handle: ProcessHandle, pipeSlot: number): number {
  const maxPipes = handle.kernel.abi_max_pipes();
  const slotBytes = handle.kernel.kernel_pipe_region_bytes() / maxPipes;
  if (
    pipeSlot <= 0
    || pipeSlot >= maxPipes
    || !Number.isInteger(slotBytes)
    || slotBytes <= PIPE_READ_OPEN_OFFSET + 4
  ) {
    return 0;
  }
  const offset = handle.kernel.kernel_pipe_base() + pipeSlot * slotBytes;
  const view = new DataView(handle.memory.buffer);
  if (offset < 0 || offset + PIPE_READ_OPEN_OFFSET + 4 > view.byteLength) {
    return 0;
  }
  return view.getInt32(offset + PIPE_READ_OPEN_OFFSET, true);
}

function localPipeWriteOpenForSlot(handle: ProcessHandle, vmPtr: number, pipeSlot: number): number {
  return localPipeOpenCountsForSlot(handle, vmPtr, pipeSlot).writeOpen;
}

function pipeSlotHasHostStdinWriter(handle: ProcessHandle, vmPtr: number, pipeSlot: number): boolean {
  if (handle.stdioOwner !== null || handle.stdinClosed === true || handle.hostStdio.refCount <= 0) {
    return false;
  }
  if (pipeSlot <= 0 || ![handle.hostStdio.stdinPipe, handle.stdinPipe].includes(pipeSlot)) {
    return false;
  }
  const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, 0);
  return (
    ofdId > 0
    && ofdId < handle.kernel.abi_max_fds()
    && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_pipe()
    && handle.kernel.kernel_ofd_ino(ofdId) === pipeSlot
  );
}

function pipeSlotHasVisibleWriter(handle: ProcessHandle, vmPtr: number, pipeSlot: number): boolean {
  return pipeSlotHasLiveWriter(handle, vmPtr, pipeSlot)
    || pipeSlotHasHostStdinWriter(handle, vmPtr, pipeSlot);
}

function localPipeOpenCountsForSlot(handle: ProcessHandle, vmPtr: number, pipeSlot: number): {
  readOpen: number;
  writeOpen: number;
} {
  if (pipeSlot <= 0 || pipeSlot >= handle.kernel.abi_max_pipes()) {
    return { readOpen: 0, writeOpen: 0 };
  }
  const maxFds = handle.kernel.abi_max_fds();
  let readOpen = 0;
  let writeOpen = 0;
  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (
      ofdId <= 0
      || ofdId >= maxFds
      || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
      || handle.kernel.kernel_ofd_ino(ofdId) !== pipeSlot
    ) {
      continue;
    }
    if (handle.kernel.kernel_ofd_host_fd(ofdId) >= 0) {
      readOpen += 1;
    } else {
      writeOpen += 1;
    }
  }
  return { readOpen, writeOpen };
}

function setPipeSlotWriteOpen(handle: ProcessHandle, pipeSlot: number, writeOpen: number): boolean {
  const maxPipes = handle.kernel.abi_max_pipes();
  const slotBytes = handle.kernel.kernel_pipe_region_bytes() / maxPipes;
  if (
    pipeSlot <= 0
    || pipeSlot >= maxPipes
    || !Number.isInteger(slotBytes)
    || slotBytes <= PIPE_WRITE_OPEN_OFFSET + 4
  ) {
    return false;
  }
  const offset = handle.kernel.kernel_pipe_base() + pipeSlot * slotBytes;
  const view = new DataView(handle.memory.buffer);
  if (offset < 0 || offset + PIPE_WRITE_OPEN_OFFSET + 4 > view.byteLength) {
    return false;
  }
  view.setInt32(offset + PIPE_WRITE_OPEN_OFFSET, writeOpen, true);
  return true;
}

function preservePipeSlotOpenCounts(
  handle: ProcessHandle,
  pipeSlot: number,
  counts: { readOpen?: number; writeOpen?: number },
): boolean {
  const maxPipes = handle.kernel.abi_max_pipes();
  const slotBytes = handle.kernel.kernel_pipe_region_bytes() / maxPipes;
  if (
    pipeSlot <= 0
    || pipeSlot >= maxPipes
    || !Number.isInteger(slotBytes)
    || slotBytes <= PIPE_WRITE_OPEN_OFFSET + 4
  ) {
    return false;
  }
  const offset = handle.kernel.kernel_pipe_base() + pipeSlot * slotBytes;
  const view = new DataView(handle.memory.buffer);
  if (offset < 0 || offset + PIPE_WRITE_OPEN_OFFSET + 4 > view.byteLength) {
    return false;
  }
  if (counts.readOpen !== undefined) {
    const currentReadOpen = view.getInt32(offset + PIPE_READ_OPEN_OFFSET, true);
    if (currentReadOpen < counts.readOpen) {
      view.setInt32(offset + PIPE_READ_OPEN_OFFSET, counts.readOpen, true);
    }
  }
  if (counts.writeOpen !== undefined) {
    const currentWriteOpen = view.getInt32(offset + PIPE_WRITE_OPEN_OFFSET, true);
    if (currentWriteOpen < counts.writeOpen) {
      view.setInt32(offset + PIPE_WRITE_OPEN_OFFSET, counts.writeOpen, true);
    }
  }
  return true;
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
  if (handle.stdinClosed === true) {
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
  if (blockedSyscallNr !== undefined) {
    const normalizedBlockedSyscallNr = BigInt(blockedSyscallNr);
    if (normalizedBlockedSyscallNr !== 0n) {
      return normalizedBlockedSyscallNr;
    }
  }
  const currentSyscallNr = handle.kernel.thread_a7?.(vmPtr);
  return currentSyscallNr === undefined ? undefined : BigInt(currentSyscallNr);
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

function guestAbsoluteRange(handle: ProcessHandle, ptr: bigint, byteLength: number): number | null {
  const guestPtr = Number(ptr);
  if (!Number.isFinite(guestPtr) || guestPtr <= 0 || byteLength <= 0) {
    return null;
  }
  const absolute = (handle.guestBase ?? 0) + guestPtr;
  if (absolute < 0 || absolute + byteLength > handle.memory.buffer.byteLength) {
    return null;
  }
  return absolute;
}

function pollLikeWaitHasNoWatchedFds(handle: ProcessHandle, vmPtr: number): boolean {
  const syscallNr = blockedPipeWaitSyscallNr(handle, vmPtr);
  if (syscallNr === undefined) {
    return false;
  }
  const ppoll = handle.kernel.abi_sys_ppoll?.();
  const pselect6 = handle.kernel.abi_sys_pselect6?.();
  const maxFds = handle.kernel.abi_max_fds?.() ?? 1024;
  const view = new DataView(handle.memory.buffer);

  if (ppoll !== undefined && syscallNr === BigInt(ppoll)) {
    const fdsPtr = BigInt(handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 0) ?? 0n);
    const nfds = Number(handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 1) ?? 0n);
    if (!Number.isFinite(nfds) || nfds <= 0) {
      return true;
    }
    const count = Math.min(nfds, maxFds);
    const absolute = guestAbsoluteRange(handle, fdsPtr, count * POLLFD_BYTES);
    if (absolute === null) {
      return false;
    }
    for (let index = 0; index < count; index += 1) {
      const offset = absolute + index * POLLFD_BYTES;
      const fd = view.getInt32(offset, true);
      const events = view.getUint16(offset + 4, true);
      if (fd >= 0 && events !== 0) {
        return false;
      }
    }
    return true;
  }

  if (pselect6 !== undefined && syscallNr === BigInt(pselect6)) {
    const nfds = Number(handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 0) ?? 0n);
    if (!Number.isFinite(nfds) || nfds <= 0) {
      return true;
    }
    const count = Math.min(nfds, maxFds);
    const setBytes = Math.floor((count + 63) / 64) * 8;
    const readPtr = BigInt(handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 1) ?? 0n);
    const writePtr = BigInt(handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 2) ?? 0n);
    const exceptPtr = BigInt(handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 3) ?? 0n);
    const readBase = readPtr === 0n ? null : guestAbsoluteRange(handle, readPtr, setBytes);
    const writeBase = writePtr === 0n ? null : guestAbsoluteRange(handle, writePtr, setBytes);
    const exceptBase = exceptPtr === 0n ? null : guestAbsoluteRange(handle, exceptPtr, setBytes);
    if (
      (readPtr !== 0n && readBase === null)
      || (writePtr !== 0n && writeBase === null)
      || (exceptPtr !== 0n && exceptBase === null)
    ) {
      return false;
    }
    const hasBit = (base: number | null, fd: number): boolean => {
      if (base === null) {
        return false;
      }
      return (view.getUint8(base + Math.floor(fd / 8)) & (1 << (fd % 8))) !== 0;
    };
    for (let fd = 0; fd < count; fd += 1) {
      if (hasBit(readBase, fd) || hasBit(writeBase, fd) || hasBit(exceptBase, fd)) {
        return false;
      }
    }
    return true;
  }

  return false;
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

function processHoldsPipeWriterForAnyPipeSlot(
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
      || !pipeSlots.has(handle.kernel.kernel_ofd_ino(ofdId))
    ) {
      continue;
    }
    if (handle.kernel.kernel_ofd_host_fd(ofdId) < 0) {
      return true;
    }
  }
  const blockedOfdId = handle.kernel.thread_blocked_ofd_id?.(vmPtr) ?? 0;
  return blockedOfdId > 0
    && blockedOfdId < maxFds
    && handle.kernel.kernel_ofd_fd_type(blockedOfdId) === handle.kernel.abi_fd_type_pipe()
    && pipeSlots.has(handle.kernel.kernel_ofd_ino(blockedOfdId))
    && handle.kernel.kernel_ofd_host_fd(blockedOfdId) < 0;
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

async function syncSocketStatePreservingNetworkBridge(handle: ProcessHandle, status: number): Promise<{
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
  return deps.syncSocketStateViaKernelWorker(handle, blockedRpcSyncCause(status));
}



// Executes host-side synchronization and guest-memory writeback around the
// supervisor-owned blocked-syscall resume decision.
async function executeHostBlockedSyscallResume(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  blockedStatus: number,
  collectEffects = false,
  collectedRetryResult?: { status?: number },
): Promise<void> {
  let rpcGeneration: number | null = null;
  let canonicalEofReadyAtStart = false;
  let preserveCanonicalEofReadyOnFinish = false;
  const preserveCanonicalEofReadyForDeferredRetry = (): void => {
    if (canonicalEofReadyAtStart && blockedStatus === handle.kernel.abi_status_pipe_wait()) {
      preserveCanonicalEofReadyOnFinish = true;
    }
  };
  const emitBlockedRpcRetryResult = (status: number): void => {
    if (collectEffects) {
      if (collectedRetryResult !== undefined) {
        collectedRetryResult.status = status;
      }
      return;
    }
    emitHostBlockedRetryResult(handle, worker, vmPtr, status);
  };
  const finishBlockedRpc = (): void => {
    const blocked = handle.hostBlockedWorkers.get(worker);
    if (
      blocked
      && blocked.vmPtr === vmPtr
      && (rpcGeneration === null || blocked.rpcGeneration === rpcGeneration)
    ) {
      blocked.rpcPending = false;
      if (!preserveCanonicalEofReadyOnFinish) {
        blocked.pipeCanonicalEofReady = false;
      }
    }
  };
  try {
    deps.appendWorkerDebugLog(handle, `blocked-rpc-start:${blockedStatus}:0x${vmPtr.toString(16)}`);
    deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:start:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-start:${handle.pid}:${blockedStatus}`);
    const blockedAtStart = handle.hostBlockedWorkers.get(worker);
    if (blockedAtStart && blockedAtStart.vmPtr === vmPtr) {
      blockedAtStart.rpcPending = true;
      blockedAtStart.rpcGeneration = (blockedAtStart.rpcGeneration ?? 0) + 1;
      rpcGeneration = blockedAtStart.rpcGeneration;
    } else {
      return;
    }
    canonicalEofReadyAtStart = blockedAtStart.pipeCanonicalEofReady === true;
    const isCurrentBlockedRpc = (): boolean => {
      const blocked = handle.hostBlockedWorkers.get(worker);
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
    const requestPipeStateSyncForBlockedRpc = async (
      target: ProcessHandle,
      wakeWaiters: boolean,
      inflightEvent: string,
    ): Promise<boolean> => {
      const inFlightPipeSync = target === handle ? target.pipeSyncPromise : null;
      const requestedPipeSync = deps.requestPipeStateSync(target, wakeWaiters);
      if (inFlightPipeSync !== null && requestedPipeSync === inFlightPipeSync) {
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, inflightEvent);
        return true;
      }
      await requestedPipeSync;
      return false;
    };
    forceNetworkClosedFds(handle, vmPtr);
    let forkSharedSyncedBeforeResume = false;
    const canonicalWriteOpenBeforeReconcileByPipeSlot = new Map<number, number>();
    if (blockedStatus === handle.kernel.abi_status_pipe_wait()) {
      const blockedPipeSlots = pipeSlotsForBlockedPipeWait(handle, vmPtr);
      if (handle.kernelShare.forkSharedSyncPromise !== null) {
        if (canonicalEofReadyAtStart) {
          deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:skip-fork-sync-canonical-eof:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-skip-fork-sync-canonical-eof:${handle.pid}`);
        } else {
          deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:await-fork-sync:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
          await handle.kernelShare.forkSharedSyncPromise;
          if (abortIfStaleBlockedRpc()) {
            return;
          }
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
          canonicalEofReadyAtStart
          && member !== handle
          && blockedPipeSlots !== null
          && !processHoldsPipeWriterForAnyPipeSlot(member, member.vmPtr, blockedPipeSlots)
        ) {
          deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-pipe-skip-canonical-eof-peer:${member.pid}:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-skip-canonical-eof-peer:${handle.pid}->${member.pid}`);
          continue;
        }
        if (
          member !== handle
          && [...member.hostBlockedWorkers.values()].some((blocked) => blocked.rpcPending)
        ) {
          deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-pipe-skip-rpc-pending:${member.pid}:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-skip-rpc-pending:${handle.pid}->${member.pid}`);
          continue;
        }
        if (
          member !== handle
          && !collectEffects
          && recoverOwnerAsyncOkPipePeer(handle, member, blockedPipeSlots)
        ) {
          preserveCanonicalEofReadyForDeferredRetry();
          finishBlockedRpc();
          emitBlockedRpcRetryResult(blockedStatus);
          return;
        }
        if (member !== handle && processHasOwnerAsyncWorker(member)) {
          deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-pipe-skip-owner-async-peer:${member.pid}:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-skip-owner-async-peer:${handle.pid}->${member.pid}`);
          continue;
        }
        if (member === handle && canonicalEofReadyAtStart) {
          deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-pipe-skip-canonical-eof-self:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-skip-canonical-eof-self:${handle.pid}`);
          continue;
        }
        if (
          member === handle
          && blockedPipeWaitAlreadyReadyForSelfResume(handle, vmPtr, blockedStatus)
        ) {
          deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-pipe-skip-ready-self:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-skip-ready-self:${handle.pid}`);
          continue;
        }
        if (member.kernelWorkerRequest && member.pid > 0) {
          deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-pipe:${member.pid}:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-start:${handle.pid}->${member.pid}`);
          if (collectEffects) {
            pushOutcomeEffect(worker, {
              kind: "request-pipe-state-sync",
              handle: member,
              wakeWaiters: false,
            });
            return;
          } else {
            const skippedSelfAwait = await requestPipeStateSyncForBlockedRpc(
              member,
              false,
              `blocked-rpc-sync-pipe-inflight:${handle.pid}->${member.pid}`,
            );
            if (!skippedSelfAwait && abortIfStaleBlockedRpc()) {
              return;
            }
          }
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-done:${handle.pid}->${member.pid}`);
        }
      }
      if (handle.kernelWorkerRequest && handle.pid > 0) {
        deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-blocked:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-blocked-start:${handle.pid}`);
        if (collectEffects) {
          pushOutcomeEffect(worker, {
            kind: "sync-blocked-fork-shared-state",
            sourceVmPtr: vmPtr,
            status: blockedStatus,
          });
          return;
        } else {
          await deps.syncBlockedForkSharedStateViaKernelWorker(handle, vmPtr, blockedRpcSyncCause(blockedStatus));
          if (abortIfStaleBlockedRpc()) {
            return;
          }
        }
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-blocked-done:${handle.pid}`);
        forkSharedSyncedBeforeResume = true;
        if (blockedStatus === handle.kernel.abi_status_pipe_wait()) {
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-before-resume-start:${handle.pid}`);
          if (canonicalEofReadyAtStart) {
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-before-resume-skip-canonical-eof:${handle.pid}`);
          } else if (collectEffects) {
            pushOutcomeEffect(worker, { kind: "request-pipe-state-sync", wakeWaiters: true });
            return;
          } else {
            const existingSelfPipeSync = handle.pipeSyncPromise;
            const requestedPipeSync = deps.requestPipeStateSync(handle, true);
            const selfPipeSyncWasAlreadyInFlight = existingSelfPipeSync !== null
              && requestedPipeSync === existingSelfPipeSync;
            const pendingPipeReadSyscallNr = blockedPipeWaitSyscallNr(handle, vmPtr) ?? 0n;
            const pendingPipeReadWait = blockedStatus === handle.kernel.abi_status_pipe_wait()
              && !isPollLikePipeWait(handle, vmPtr)
              && blockedPipeResumeWritesGuestBuffer(handle, pendingPipeReadSyscallNr);
            const pendingPipeReadFd = blockedPipeWaitFd(handle, vmPtr);
            const pendingPipeDataIsReady = pendingPipeReadWait
              && (
                (handle.kernel.thread_blocked_pipe_buffered_len?.(vmPtr) ?? 0) > 0
                || pipeSlotBufferedLenForThreadStepFd(handle, vmPtr, pendingPipeReadFd) > 0
              );
            const pendingPipeEofIsReady = pendingPipeReadWait
              && handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) === 1
              && (handle.kernel.thread_blocked_pipe_buffered_len?.(vmPtr) ?? 0) === 0
              && (handle.kernel.thread_blocked_pipe_write_open?.(vmPtr) ?? 0) <= 0
              && !activeChildMayHoldBlockedPipeWriter(deps, handle, vmPtr)
              && handle.pendingChildExitRecords <= 0;
            if (selfPipeSyncWasAlreadyInFlight && !pendingPipeDataIsReady && !pendingPipeEofIsReady) {
              deps.appendRuntimeLifecycleEvent(
                handle.runtimeId,
                `blocked-rpc-sync-pipe-before-resume-inflight:${handle.pid}`,
              );
              preserveCanonicalEofReadyForDeferredRetry();
              finishBlockedRpc();
              deps.appendWorkerDebugLog(handle, `blocked-rpc-defer-pipe-sync-inflight:0x${vmPtr.toString(16)}`);
              emitBlockedRpcRetryResult(blockedStatus);
              return;
            } else if (selfPipeSyncWasAlreadyInFlight) {
              deps.appendRuntimeLifecycleEvent(
                handle.runtimeId,
                `blocked-rpc-sync-pipe-before-resume-inflight-read-ready:${handle.pid}:buf=${pendingPipeDataIsReady ? 1 : 0}:eof=${pendingPipeEofIsReady ? 1 : 0}`,
              );
            } else {
              await requestedPipeSync;
              if (abortIfStaleBlockedRpc()) {
                return;
              }
              deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-before-resume-done:${handle.pid}`);
            }
          }
        }
      }
      deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:reconcile:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
      const closedBlockedPipeSlotsBeforeReconcile = new Set<number>();
      const snapshotClosedBlockedPipeSlotsBeforeReconcile = new Set<number>();
      const writePipeReadOpenPreserveSlots = new Set<number>();
      const localBlockedPipeOpenCountsBeforeReconcile = new Map<number, {
        canonicalReadOpen: number;
        canonicalWriteOpen: number;
        readOpen: number;
        writeOpen: number;
      }>();
      if (blockedPipeSlots !== null) {
        const blockedPipeSyscallNr = blockedPipeWaitSyscallNr(handle, vmPtr) ?? 0n;
        const pollLikePipeWait = isPollLikePipeWait(handle, vmPtr);
        const readPipeWait = !pollLikePipeWait
          && blockedPipeResumeWritesGuestBuffer(handle, blockedPipeSyscallNr);
        const writePipeWait = !pollLikePipeWait
          && blockedPipeWaitWritesPipe(handle, blockedPipeSyscallNr);
        const blockedSnapshotClosedSinglePipeSlot = blockedPipeSlots.size === 1
          && readPipeWait
          && blockedStatus === handle.kernel.abi_status_pipe_wait()
          && (handle.kernel.thread_blocked_pipe_buffered_len?.(vmPtr) ?? 0) === 0
          && (handle.kernel.thread_blocked_pipe_write_open?.(vmPtr) ?? 0) <= 0;
        for (const pipeSlot of blockedPipeSlots) {
          const pipeBufferedLen = pipeSlotBufferedLen(handle, pipeSlot);
          const canonicalReadOpen = pipeSlotReadOpen(handle, pipeSlot);
          const canonicalWriteOpen = pipeSlotWriteOpen(handle, pipeSlot);
          const localCounts = localPipeOpenCountsForSlot(handle, vmPtr, pipeSlot);
          const readPipeOpenEmptyVisibleWriter = readPipeWait
            && pipeBufferedLen === 0
            && (
              canonicalWriteOpen > 0
              ? pipeSlotHasVisibleWriter(handle, vmPtr, pipeSlot)
              : localCounts.writeOpen > 0
            );
          const snapshotClosedBlockedPipeSlot = blockedSnapshotClosedSinglePipeSlot
            && !readPipeOpenEmptyVisibleWriter;
          canonicalWriteOpenBeforeReconcileByPipeSlot.set(pipeSlot, canonicalWriteOpen);
          localBlockedPipeOpenCountsBeforeReconcile.set(pipeSlot, {
            canonicalReadOpen,
            canonicalWriteOpen,
            ...localCounts,
          });
          if (writePipeWait && localCounts.readOpen > 0) {
            writePipeReadOpenPreserveSlots.add(pipeSlot);
          }
          if (
            (
              pipeBufferedLen === 0
              && canonicalWriteOpen <= 0
            )
            || snapshotClosedBlockedPipeSlot
          ) {
            closedBlockedPipeSlotsBeforeReconcile.add(pipeSlot);
            if (snapshotClosedBlockedPipeSlot) {
              snapshotClosedBlockedPipeSlotsBeforeReconcile.add(pipeSlot);
            }
          }
        }
      }
      reconcileKernelSharePipeControl(handle);
      for (const pipeSlot of writePipeReadOpenPreserveSlots) {
        const localCounts = localBlockedPipeOpenCountsBeforeReconcile.get(pipeSlot);
        if (localCounts === undefined || localCounts.readOpen <= 0) {
          continue;
        }
        preservePipeSlotOpenCounts(handle, pipeSlot, {
          readOpen: localCounts.readOpen,
          writeOpen: localCounts.writeOpen,
        });
      }
      for (const pipeSlot of closedBlockedPipeSlotsBeforeReconcile) {
        const localCounts = localBlockedPipeOpenCountsBeforeReconcile.get(pipeSlot);
        if (
          localCounts !== undefined
          && localCounts.canonicalWriteOpen <= 0
          && localCounts.writeOpen > 0
        ) {
          preservePipeSlotOpenCounts(handle, pipeSlot, {
            readOpen: localCounts.readOpen,
            writeOpen: localCounts.writeOpen,
          });
          continue;
        }
        if (
          localCounts !== undefined
          && localCounts.canonicalWriteOpen <= 0
          && !snapshotClosedBlockedPipeSlotsBeforeReconcile.has(pipeSlot)
          && (localCounts.readOpen > 0 || localCounts.writeOpen > 0)
        ) {
          preservePipeSlotOpenCounts(handle, pipeSlot, {
            readOpen: localCounts.readOpen,
            writeOpen: localCounts.writeOpen,
          });
          continue;
        }
        if (
          !snapshotClosedBlockedPipeSlotsBeforeReconcile.has(pipeSlot)
          && localPipeWriteOpenForSlot(handle, vmPtr, pipeSlot) > 0
        ) {
          continue;
        }
        if (setPipeSlotWriteOpen(handle, pipeSlot, 0) && PIPE_RESUME_DEBUG_ENABLED) {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `blocked-rpc-preserve-closed-pipe-slot:${handle.pid}:slot=${pipeSlot}`,
          );
        }
      }
      if (handle.kernel.thread_poll_wait_has_socket?.(vmPtr) === 1) {
        deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-socket:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
        if (collectEffects) {
          pushOutcomeEffect(worker, { kind: "sync-blocked-socket-state", status: blockedStatus });
          return;
        } else {
          await syncSocketStatePreservingNetworkBridge(handle, blockedStatus);
          if (abortIfStaleBlockedRpc()) {
            return;
          }
        }
      }
    }
    if (abortIfStaleBlockedRpc()) {
      return;
    }
    if (blockedStatus === handle.kernel.abi_status_pipe_wait()) {
      preserveHostStdinWriterForBlockedRead(handle, vmPtr);
    }
    const pipeReadOpenEmpty = blockedPipeReadOpenEmptyAfterSync(
      handle,
      vmPtr,
      blockedStatus,
    );
    if (
      blockedStatus === handle.kernel.abi_status_pipe_wait()
      && handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) === 1
      && (handle.kernel.thread_blocked_pipe_buffered_len?.(vmPtr) ?? 0) === 0
    ) {
      const blockedPipeFd = blockedPipeWaitFd(handle, vmPtr);
      const blockedPipeSlot = pipeSlotForThreadStepFd(handle, vmPtr, blockedPipeFd);
      const blockedPipeWasCanonicalClosedBeforeReconcile = blockedPipeSlot !== null
        && (canonicalWriteOpenBeforeReconcileByPipeSlot.get(blockedPipeSlot) ?? 1) <= 0;
      const blockedPipeOpenEmptyAfterSync = !isPollLikePipeWait(handle, vmPtr)
        && !blockedPipeWasCanonicalClosedBeforeReconcile
        && pipeSlotBufferedLenForThreadStepFd(handle, vmPtr, blockedPipeFd) === 0
        && pipeSlotWriteOpenForThreadStepFd(handle, vmPtr, blockedPipeFd) > 0;
      if (!pipeReadOpenEmpty && !blockedPipeOpenEmptyAfterSync) {
        if (activeChildMayHoldBlockedPipeWriter(deps, handle, vmPtr)) {
          preserveCanonicalEofReadyForDeferredRetry();
          finishBlockedRpc();
          deps.appendWorkerDebugLog(handle, `blocked-rpc-defer-pipe-eof:0x${vmPtr.toString(16)}`);
          emitBlockedRpcRetryResult(blockedStatus);
          return;
        }
        if (handle.pendingChildExitRecords > 0) {
          preserveCanonicalEofReadyForDeferredRetry();
          finishBlockedRpc();
          deps.appendWorkerDebugLog(handle, `blocked-rpc-defer-pipe-eof-child-exit:0x${vmPtr.toString(16)}`);
          emitBlockedRpcRetryResult(blockedStatus);
          return;
        }
      }
    }
    const blockedPipeReadReady = handle.kernel.thread_blocked_pipe_read_ready?.(vmPtr);
    const blockedPipeWriteReady = handle.kernel.thread_blocked_pipe_write_ready?.(vmPtr);
    const blockedPipePollReadyCount = handle.kernel.thread_poll_wait_ready_count?.(vmPtr);
    const blockedPollPipeBuffered = blockedStatus === handle.kernel.abi_status_pipe_wait()
      ? blockedPollPipeHasBufferedData(handle, vmPtr)
      : false;
    const blockedPipeFd = blockedPipeWaitFd(handle, vmPtr);
    const blockedPipeSlot = pipeSlotForThreadStepFd(handle, vmPtr, blockedPipeFd);
    const pollLikePipeWait = isPollLikePipeWait(handle, vmPtr);
    const pollPipeSlots = pollLikePipeWait
      ? pollPipeSlotsForThreadStep(handle, vmPtr)
      : null;
    const pollWaitHasNoWatchedFds = pollLikePipeWait
      && pollLikeWaitHasNoWatchedFds(handle, vmPtr);
    const pollLikePipeTargetPresent = (pollPipeSlots?.size ?? 0) > 0
      || (!pollWaitHasNoWatchedFds && blockedPipeSlot !== null);
    const pollLikePipeKernelReportsNoWriter = blockedStatus === handle.kernel.abi_status_pipe_wait()
      && pollLikePipeWait
      && pollLikePipeTargetPresent
      && (handle.kernel.thread_blocked_pipe_buffered_len?.(vmPtr) ?? 0) === 0
      && handle.kernel.thread_blocked_pipe_write_open?.(vmPtr) !== undefined
      && (handle.kernel.thread_blocked_pipe_write_open?.(vmPtr) ?? 1) <= 0;
    const pollLikePipeWaitNoWriter = blockedStatus === handle.kernel.abi_status_pipe_wait()
      && pollLikePipeWait
      && pollLikePipeTargetPresent
      && pollPipeBufferedLenForThreadStep(handle, vmPtr) === 0
      && (
        pollPipeWriteOpenForThreadStep(handle, vmPtr) <= 0
        || pollLikePipeKernelReportsNoWriter
      );
    if (
      blockedStatus === handle.kernel.abi_status_pipe_wait()
      && isPollLikePipeWait(handle, vmPtr)
      && pollPipeBufferedLenForThreadStep(handle, vmPtr) === 0
      && handle.kernel.thread_poll_hup_sync_pending?.(vmPtr) !== 1
      && activeChildMayHoldBlockedPipeWriter(deps, handle, vmPtr, {
        ignoreExitingWritersWithoutBufferedData: true,
      })
    ) {
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      preserveCanonicalEofReadyForDeferredRetry();
      finishBlockedRpc();
      deps.appendWorkerDebugLog(handle, `blocked-rpc-defer-poll-hup-active-child:0x${vmPtr.toString(16)}`);
      emitBlockedRpcRetryResult(blockedStatus);
      return;
    }
    const blockedPipeSyscallNr = blockedPipeWaitSyscallNr(handle, vmPtr) ?? 0n;
    const blockedPipeBufferedLen = handle.kernel.thread_blocked_pipe_buffered_len?.(vmPtr) ?? 0;
    const blockedPipeWriteOpen = handle.kernel.thread_blocked_pipe_write_open?.(vmPtr) ?? 0;
    const syncedPipeBufferedLen = pipeSlotBufferedLenForThreadStepFd(
      handle,
      vmPtr,
      blockedPipeFd,
    );
    const syncedPipeWriteOpen = pipeSlotWriteOpenForThreadStepFd(
      handle,
      vmPtr,
      blockedPipeFd,
    );
    const blockedPipeCanonicalWriteOpenBeforeReconcile = blockedPipeSlot === null
      ? undefined
      : canonicalWriteOpenBeforeReconcileByPipeSlot.get(blockedPipeSlot);
    const blockedPipeWasCanonicalClosedBeforeReconcile =
      blockedPipeCanonicalWriteOpenBeforeReconcile !== undefined
      && blockedPipeCanonicalWriteOpenBeforeReconcile <= 0;
    const localPipeWriteOpen = blockedPipeSlot === null
      ? null
      : localPipeWriteOpenForSlot(handle, vmPtr, blockedPipeSlot);
    const blockedPipeHasVisibleWriter = blockedPipeSlot !== null
      && pipeSlotHasVisibleWriter(handle, vmPtr, blockedPipeSlot);
    const activeChildMayHoldCanonicalPipeWriter = activeChildMayHoldBlockedPipeWriter(deps, handle, vmPtr);
    const pipeEofMayStillReceiveWriter = activeChildMayHoldCanonicalPipeWriter
      || handle.pendingChildExitRecords > 0;
    const readPipeCanonicalEofCandidate = blockedStatus === handle.kernel.abi_status_pipe_wait()
      && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) !== 1
      && !isPollLikePipeWait(handle, vmPtr)
      && blockedPipeResumeWritesGuestBuffer(handle, blockedPipeSyscallNr)
      && blockedPipeReadReady !== 1
      && blockedPipeWriteReady !== 1
      && handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) !== 1
      && blockedPipeBufferedLen === 0
      && syncedPipeBufferedLen === 0
      && (syncedPipeWriteOpen <= 0 || !blockedPipeHasVisibleWriter)
      && !pipeEofMayStillReceiveWriter;
    const readPipeCanonicalEofAfterSync = readPipeCanonicalEofCandidate;
    const readPipeOpenEmptyAfterSync = blockedStatus === handle.kernel.abi_status_pipe_wait()
      && !pollLikePipeWait
      && blockedPipeResumeWritesGuestBuffer(handle, blockedPipeSyscallNr)
      && blockedPipeReadReady !== 1
      && blockedPipeWriteReady !== 1
      && blockedPipeBufferedLen === 0
      && syncedPipeBufferedLen === 0
      && syncedPipeWriteOpen > 0
      && !blockedPipeWasCanonicalClosedBeforeReconcile;
    if (PIPE_RESUME_DEBUG_ENABLED && blockedStatus === handle.kernel.abi_status_pipe_wait()) {
      const event = [
        `blocked-rpc-pipe-candidate:${handle.pid}`,
        `vm=0x${vmPtr.toString(16)}`,
        `fd=${blockedPipeFd}`,
        `sys=${blockedPipeSyscallNr.toString()}`,
        `poll=${pollLikePipeWait ? 1 : 0}`,
        `pollNoWriter=${pollLikePipeWaitNoWriter ? 1 : 0}`,
        `readReady=${blockedPipeReadReady ?? "n/a"}`,
        `writeReady=${blockedPipeWriteReady ?? "n/a"}`,
        `pollReady=${blockedPipePollReadyCount ?? "n/a"}`,
        `buf=${blockedPipeBufferedLen}`,
        `wo=${blockedPipeWriteOpen}`,
        `syncedBuf=${syncedPipeBufferedLen}`,
        `syncedWo=${syncedPipeWriteOpen}`,
        `visibleWriter=${blockedPipeHasVisibleWriter ? 1 : 0}`,
        `slot=${blockedPipeSlot ?? "n/a"}`,
        `candidate=${readPipeCanonicalEofAfterSync ? 1 : 0}`,
        `eofPending=${handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) ?? "n/a"}`,
        `hupPending=${handle.kernel.thread_poll_hup_sync_pending?.(vmPtr) ?? "n/a"}`,
        `activeChild=${activeChildMayHoldCanonicalPipeWriter ? 1 : 0}`,
        `pendingChildExit=${handle.pendingChildExitRecords}`,
      ].join(":");
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, event);
      deps.appendWorkerDebugLog(handle, event);
    }
    if (
      blockedStatus === handle.kernel.abi_status_pipe_wait()
      && blockedPipeReadReady === 0
      && (blockedPipeWriteReady === undefined || blockedPipeWriteReady <= 0)
      && (blockedPipePollReadyCount === undefined || blockedPipePollReadyCount <= 0)
      && !blockedPollPipeBuffered
      && !pollLikePipeWaitNoWriter
      && !readPipeCanonicalEofAfterSync
      && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) !== 1
      && handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) !== 1
      && handle.kernel.thread_poll_hup_sync_pending?.(vmPtr) !== 1
      && !readPipeOpenEmptyAfterSync
    ) {
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      preserveCanonicalEofReadyForDeferredRetry();
      finishBlockedRpc();
      if (PIPE_RESUME_DEBUG_ENABLED) {
        const event = [
          `blocked-rpc-defer-pipe-not-ready:${handle.pid}`,
          `vm=0x${vmPtr.toString(16)}`,
          `fd=${blockedPipeFd}`,
          `sys=${blockedPipeSyscallNr.toString()}`,
          `read=${blockedPipeReadReady}`,
          `write=${blockedPipeWriteReady ?? "n/a"}`,
          `poll=${blockedPipePollReadyCount ?? "n/a"}`,
          `pollBuf=${blockedPollPipeBuffered ? 1 : 0}`,
          `pollNoWriter=${pollLikePipeWaitNoWriter ? 1 : 0}`,
          `canonical=${readPipeCanonicalEofAfterSync ? 1 : 0}`,
          `slot=${blockedPipeSlot ?? "n/a"}`,
          `buf=${blockedPipeBufferedLen}`,
          `wo=${blockedPipeWriteOpen}`,
          `syncedBuf=${syncedPipeBufferedLen}`,
          `syncedWo=${syncedPipeWriteOpen}`,
          `slotWo=${blockedPipeSlot === null ? "n/a" : pipeSlotWriteOpen(handle, blockedPipeSlot)}`,
          `localWo=${localPipeWriteOpen ?? "n/a"}`,
          `visibleWriter=${blockedPipeHasVisibleWriter ? 1 : 0}`,
          `ofd=${handle.kernel.thread_blocked_ofd_id?.(vmPtr) ?? "n/a"}`,
          `eofPending=${handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) ?? "n/a"}`,
          `activeChild=${activeChildMayHoldCanonicalPipeWriter ? 1 : 0}`,
          `pendingChildExit=${handle.pendingChildExitRecords}`,
        ].join(":");
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, event);
        deps.appendWorkerDebugLog(handle, event);
      }
      deps.appendWorkerDebugLog(
        handle,
        `blocked-rpc-defer-pipe-not-ready:0x${vmPtr.toString(16)}:read=${blockedPipeReadReady}:write=${blockedPipeWriteReady ?? "n/a"}:poll=${blockedPipePollReadyCount ?? "n/a"}:buffered=${blockedPollPipeBuffered ? 1 : 0}`,
      );
      emitBlockedRpcRetryResult(blockedStatus);
      return;
    }
    const pollLikePipeWaitWithReadySocket = blockedStatus === handle.kernel.abi_status_pipe_wait()
      && isPollLikePipeWait(handle, vmPtr)
      && !pollLikePipeWaitNoWriter
      && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) === 1
      && (handle.kernel.thread_poll_wait_ready_count?.(vmPtr) ?? 0) > 0;
    if (pollLikePipeWaitWithReadySocket) {
      const blocked = handle.hostBlockedWorkers.get(worker);
      if (!blocked || blocked.vmPtr !== vmPtr) {
        return;
      }
      deps.clearHostBlockedRetryTimer(handle, worker);
      const thread = deps.threadWorkerStates.get(worker);
      if (!thread) {
        return;
      }
      deps.requestThreadWorkerBlockedResume(handle, worker);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      finishBlockedRpc();
      handle.hostBlockedWorkers.delete(worker);
      deps.appendWorkerDebugLog(handle, `blocked-rpc-resume-socket-poll-in-worker:0x${vmPtr.toString(16)}`);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-resume-socket-poll-in-worker:${handle.pid}`);
      pushOutcomeEffect(worker, { kind: "enqueue-worker" });
      return;
    }
    const readPipeReachedEofAfterWait = blockedStatus === handle.kernel.abi_status_pipe_wait()
      && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) !== 1
      && !isPollLikePipeWait(handle, vmPtr)
      && blockedPipeReadReady === 1
      && blockedPipeWriteReady !== 1
      && handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) !== 1
      && syncedPipeBufferedLen === 0
      && syncedPipeWriteOpen <= 0
      && !pipeEofMayStillReceiveWriter;
    const readPipeEofPendingAfterSync = blockedStatus === handle.kernel.abi_status_pipe_wait()
      && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) !== 1
      && !isPollLikePipeWait(handle, vmPtr)
      && handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) === 1
      && blockedPipeBufferedLen === 0
      && syncedPipeBufferedLen === 0
      && (
        syncedPipeWriteOpen <= 0
        || pipeReadOpenEmpty
        || (!blockedPipeWasCanonicalClosedBeforeReconcile && !blockedPipeHasVisibleWriter)
      )
      && !pipeEofMayStillReceiveWriter;
    if (readPipeEofPendingAfterSync || readPipeCanonicalEofAfterSync) {
      if (readPipeCanonicalEofAfterSync) {
        handle.kernelState = readKernelRuntimeState(handle.kernel);
      }
      attachKernelRuntimeState(handle.kernel, handle.kernelState);
      if (readPipeCanonicalEofAfterSync || readPipeEofPendingAfterSync) {
        const pipeSlot = pipeSlotForThreadStepFd(handle, vmPtr, blockedPipeFd);
        if (pipeSlot !== null) {
          syncPipeOfdGenerationsForSlot(handle, pipeSlot);
          const staleWriteOpen = Math.max(blockedPipeWriteOpen, syncedPipeWriteOpen);
          if (staleWriteOpen > 0 && setPipeSlotWriteOpen(handle, pipeSlot, 0)) {
            deps.appendRuntimeLifecycleEvent(
              handle.runtimeId,
              `blocked-rpc-clear-stale-pipe-writer:${handle.pid}:slot=${pipeSlot}:was=${staleWriteOpen}`,
            );
          }
        }
      }
      const pipeSlotsForResume = snapshotBlockedPipeSlotForThreadStep(handle, vmPtr, { includeEmpty: true });
      const blocked = handle.hostBlockedWorkers.get(worker);
      if (!blocked || blocked.vmPtr !== vmPtr) {
        return;
      }
      deps.clearHostBlockedRetryTimer(handle, worker);
      const thread = deps.threadWorkerStates.get(worker);
      if (!thread) {
        return;
      }
      if (pipeSlotsForResume.length === 0) {
        handle.kernelState = readKernelRuntimeState(handle.kernel);
        preserveCanonicalEofReadyForDeferredRetry();
        finishBlockedRpc();
        deps.appendWorkerDebugLog(handle, `blocked-rpc-defer-pipe-eof-empty-resume:0x${vmPtr.toString(16)}`);
        emitBlockedRpcRetryResult(blockedStatus);
        return;
      }
      thread.pendingPipeSlotsForNextStep.push(...pipeSlotsForResume);
      deps.requestThreadWorkerBlockedResume(handle, worker);
      if (canonicalEofReadyAtStart) {
        thread.pipeCanonicalEofReadyOnNextBlock = true;
      }
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      finishBlockedRpc();
      handle.hostBlockedWorkers.delete(worker);
      deps.appendWorkerDebugLog(
        handle,
        `blocked-rpc-resume-pipe-eof-in-worker:0x${vmPtr.toString(16)}:fd=${blockedPipeFd}:slots=${pipeSlotsForResume.length}`,
      );
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `blocked-rpc-resume-pipe-eof-in-worker:${handle.pid}:vm=0x${vmPtr.toString(16)}:fd=${blockedPipeFd}:slots=${pipeSlotsForResume.length}`,
      );
      if (collectEffects) {
        pushOutcomeEffect(worker, { kind: "enqueue-worker" });
      } else {
        deps.enqueueWorker(handle, worker);
      }
      return;
    }
    if (
      blockedStatus === handle.kernel.abi_status_pipe_wait()
      && PIPE_RESUME_IN_WORKER_ENABLED
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
      const eofReady = !pipeEofMayStillReceiveWriter && (
        (
          handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) === 1
          && bufferedLen === 0
          && writeOpen <= 0
          && syncedWriteOpen <= 0
        ) || (
          bufferedLen === 0
          && syncedBufferedLen === 0
          && syncedWriteOpen <= 0
          && blockedPipeWriteReady !== 1
        )
      );
      const pipeReadReady = handle.kernel.thread_blocked_pipe_read_ready?.(vmPtr) === 1
        && !(syncedBufferedLen === 0 && syncedWriteOpen > 0);
      const bufferedReadWillReachCanonicalEof = !pollLikePipeWait
        && blockedPipeResumeWritesGuestBuffer(handle, blockedPipeSyscallNr)
        && syncedBufferedLen > 0
        && syncedWriteOpen <= 0
        && !pipeEofMayStillReceiveWriter;
      const pollReady = (blockedPipePollReadyCount ?? 0) > 0 || blockedPollPipeBuffered;
      const currentFdOpenEmpty = !pollLikePipeWait
        && !blockedPipeWasCanonicalClosedBeforeReconcile
        && syncedBufferedLen === 0
        && syncedWriteOpen > 0;
      if (PIPE_RESUME_DEBUG_ENABLED) {
        const event = [
          `pipe-resume-check:${handle.pid}`,
          `vm=0x${vmPtr.toString(16)}`,
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
          `activeChild=${activeChildMayHoldCanonicalPipeWriter ? 1 : 0}`,
          `pendingChildExit=${handle.pendingChildExitRecords}`,
          `holders=${formatBlockedPipeSlotHolders(handle, vmPtr)}`,
        ].join(":");
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, event);
        deps.appendWorkerDebugLog(handle, event);
      }
      if (
        currentFdOpenEmpty
        && !pipeReadReady
        && !pollReady
        && pipeEofMayStillReceiveWriter
      ) {
        handle.kernelState = readKernelRuntimeState(handle.kernel);
        preserveCanonicalEofReadyForDeferredRetry();
        finishBlockedRpc();
        if (PIPE_RESUME_DEBUG_ENABLED) {
          const event = [
            `blocked-rpc-defer-pipe-open-empty-active-child:${handle.pid}`,
            `vm=0x${vmPtr.toString(16)}`,
            `fd=${blockedPipeFd}`,
            `sys=${blockedPipeWaitSyscallNr(handle, vmPtr)?.toString() ?? "n/a"}`,
            `buf=${bufferedLen}`,
            `wo=${writeOpen}`,
            `syncedBuf=${syncedBufferedLen}`,
            `syncedWo=${syncedWriteOpen}`,
            `activeChild=${activeChildMayHoldCanonicalPipeWriter ? 1 : 0}`,
            `pendingChildExit=${handle.pendingChildExitRecords}`,
          ].join(":");
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, event);
          deps.appendWorkerDebugLog(handle, event);
        }
        emitBlockedRpcRetryResult(blockedStatus);
        return;
      }
      const pipeSlotsForResume = pollLikePipeWait
        ? snapshotPollPipeSlotForThreadStep(handle, vmPtr, { includeEmpty: eofReady || pollReady })
        : snapshotBlockedPipeSlotForThreadStep(handle, vmPtr, { includeEmpty: eofReady || pipeReadOpenEmpty || currentFdOpenEmpty });
      if (!pipeReadReady && !eofReady && !pipeReadOpenEmpty && !currentFdOpenEmpty && pipeSlotsForResume.length === 0) {
        handle.kernelState = readKernelRuntimeState(handle.kernel);
        preserveCanonicalEofReadyForDeferredRetry();
        finishBlockedRpc();
        deps.appendWorkerDebugLog(
          handle,
          `blocked-rpc-defer-pipe-thread-resume-not-ready:0x${vmPtr.toString(16)}:ready=${pipeReadReady ? 1 : 0}:eof=${eofReady ? 1 : 0}:buf=${bufferedLen}:wo=${writeOpen}:slots=0`,
        );
        emitBlockedRpcRetryResult(blockedStatus);
        return;
      }
      const blocked = handle.hostBlockedWorkers.get(worker);
      if (!blocked || blocked.vmPtr !== vmPtr) {
        return;
      }
      deps.clearHostBlockedRetryTimer(handle, worker);
      const thread = deps.threadWorkerStates.get(worker);
      if (!thread) {
        return;
      }
      if (pipeSlotsForResume.length === 0) {
        handle.kernelState = readKernelRuntimeState(handle.kernel);
        preserveCanonicalEofReadyForDeferredRetry();
        finishBlockedRpc();
        deps.appendWorkerDebugLog(handle, `blocked-rpc-defer-pipe-empty-resume:0x${vmPtr.toString(16)}`);
        emitBlockedRpcRetryResult(blockedStatus);
        return;
      }
      thread.pendingPipeSlotsForNextStep.push(...pipeSlotsForResume);
      deps.requestThreadWorkerBlockedResume(handle, worker);
      if (canonicalEofReadyAtStart || bufferedReadWillReachCanonicalEof) {
        thread.pipeCanonicalEofReadyOnNextBlock = true;
      }
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      finishBlockedRpc();
      handle.hostBlockedWorkers.delete(worker);
      deps.appendWorkerDebugLog(handle, `blocked-rpc-resume-in-worker:0x${vmPtr.toString(16)}`);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-resume-in-worker:${handle.pid}`);
      if (collectEffects) {
        pushOutcomeEffect(worker, { kind: "enqueue-worker" });
      } else {
        deps.enqueueWorker(handle, worker);
      }
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
    forceNetworkClosedFds(handle, vmPtr);
    const resumeFd = blockedArg0;
    const resumeBuf = blockedArg1;
    const resumeCount = blockedArg2;
    const resumeResult = resumeBlockedSyscallBySupervisor(handle.kernel, vmPtr);
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
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    const currentPid = handle.kernel.process_current_pid?.(vmPtr) ?? handle.pid;
    const transientProcessOwnerMismatch = currentPid > 0 && currentPid !== handle.pid;
    if (
      !transientProcessOwnerMismatch
      && !forkSharedSyncedBeforeResume
      && !blockedStatusIsEpollWait(handle, blockedStatus)
    ) {
      deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-after-resume:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
      if (collectEffects) {
        pushOutcomeEffect(worker, {
          kind: "sync-blocked-fork-shared-state",
          sourceVmPtr: vmPtr,
          status: blockedStatus,
        });
        return;
      } else {
        await deps.syncBlockedForkSharedStateViaKernelWorker(handle, vmPtr, blockedRpcSyncCause(blockedStatus));
        if (abortIfStaleBlockedRpc()) {
          return;
        }
      }
    }

    if (
      !transientProcessOwnerMismatch
      && blockedStatus === handle.kernel.abi_status_pipe_wait()
      && !pollLikePipeWaitNoWriter
      && handle.kernel.thread_status(vmPtr) === handle.kernel.abi_status_pipe_wait()
    ) {
      deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:sync-pipe-after:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-after-start:${handle.pid}`);
      if (collectEffects) {
        pushOutcomeEffect(worker, { kind: "request-pipe-state-sync", wakeWaiters: true });
        return;
      } else {
        await deps.requestPipeStateSync(handle, true);
        if (abortIfStaleBlockedRpc()) {
          return;
        }
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
        const resumeResult = resumeBlockedSyscallBySupervisor(handle.kernel, vmPtr);
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
        if (collectEffects) {
          pushOutcomeEffect(worker, { kind: "request-pipe-state-sync", wakeWaiters: true });
          return;
        } else {
          await deps.requestPipeStateSync(handle, true);
          if (abortIfStaleBlockedRpc()) {
            return;
          }
        }
      }
    } else if (
      pollLikePipeWaitNoWriter
      && handle.kernel.thread_status(vmPtr) === handle.kernel.abi_status_pipe_wait()
    ) {
      if (pollLikePipeKernelReportsNoWriter && pollPipeSlots !== null) {
        for (const pipeSlot of pollPipeSlots) {
          if (pipeSlotBufferedLen(handle, pipeSlot) !== 0) {
            continue;
          }
          syncPipeOfdGenerationsForSlot(handle, pipeSlot);
          if (setPipeSlotWriteOpen(handle, pipeSlot, 0) && PIPE_RESUME_DEBUG_ENABLED) {
            deps.appendRuntimeLifecycleEvent(
              handle.runtimeId,
              `blocked-rpc-clear-stale-poll-writer:${handle.pid}:slot=${pipeSlot}`,
            );
          }
        }
      }
      const resumeFd = blockedArg0;
      const resumeBuf = blockedArg1;
      const resumeCount = blockedArg2;
      const resumeResult = resumeBlockedSyscallBySupervisor(handle.kernel, vmPtr);
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
      emitBlockedRpcRetryResult(handle.kernel.thread_status(vmPtr));
      return;
    } else if (
      !transientProcessOwnerMismatch
      && (
        blockedStatus === handle.kernel.abi_status_connect()
        || blockedStatus === handle.kernel.abi_status_socket_recv()
        || blockedStatus === handle.kernel.abi_status_socket_send()
      )
    ) {
      if (collectEffects) {
        pushOutcomeEffect(worker, { kind: "sync-blocked-socket-state", status: blockedStatus });
        return;
      } else {
        await syncSocketStatePreservingNetworkBridge(handle, blockedStatus);
        if (abortIfStaleBlockedRpc()) {
          return;
        }
      }
    }

    const blocked = handle.hostBlockedWorkers.get(worker);
    if (!blocked || blocked.vmPtr !== vmPtr) {
      return;
    }
    finishBlockedRpc();
    deps.appendWorkerDebugLog(handle, `blocked-rpc-done:${handle.kernel.thread_status(vmPtr)}:0x${vmPtr.toString(16)}`);
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-done:${handle.pid}:${handle.kernel.thread_status(vmPtr)}`);

    if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
      return;
    }

    emitBlockedRpcRetryResult(handle.kernel.thread_status(vmPtr));
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
    if (!collectEffects || !outcomeEffectsNeedResumeContinuation(worker)) {
      finishBlockedRpc();
    }
  }
}



  async function executeHostBlockedSyscallResumeOutcome(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    blockedStatus: number,
  ): Promise<KernelRpcResumeOutcome> {
    pendingOutcomeEffects.delete(worker);
    const collectedRetryResult: { status?: number } = {};
    await executeHostBlockedSyscallResume(
      handle,
      worker,
      vmPtr,
      blockedStatus,
      true,
      collectedRetryResult,
    );
    const needsResumeContinuation = outcomeEffectsNeedResumeContinuation(worker);
    const effects = takeOutcomeEffects(worker);
    if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
      return { kind: "process-gone", effects };
    }
    if (!handle.hostBlockedWorkers.has(worker)) {
      return { kind: "queued-in-worker", effects };
    }
    if (
      collectedRetryResult.status !== undefined
      && !needsResumeContinuation
    ) {
      return completedKernelRpcOutcome(collectedRetryResult.status, effects);
    }
    const status = handle.kernel.thread_status?.(vmPtr);
    if (status === undefined) {
      return { kind: "stale", effects };
    }
    return { kind: "blocked", status, effects };
  }

  return {
    executeHostBlockedSyscallResume,
    executeHostBlockedSyscallResumeOutcome,
  };
}

export function createWorkerKernelRpcBlockedRpcHelpers(deps: WorkerKernelRpcBlockedRpcDeps) {
  const { executeHostBlockedSyscallResumeOutcome } = createWorkerKernelRpcBlockedRpcHelperInternals(deps);
  return { executeHostBlockedSyscallResumeOutcome };
}

export function createWorkerKernelRpcBlockedRpcTestHelpers(
  deps: Omit<WorkerKernelRpcBlockedRpcDeps, "clearHostBlockedRetryTimer" | "requestThreadWorkerBlockedResume">
    & Partial<Pick<WorkerKernelRpcBlockedRpcDeps, "clearHostBlockedRetryTimer" | "requestThreadWorkerBlockedResume">>,
) {
  return createWorkerKernelRpcBlockedRpcHelperInternals({
    ...deps,
    clearHostBlockedRetryTimer: deps.clearHostBlockedRetryTimer ?? (() => {}),
    requestThreadWorkerBlockedResume: deps.requestThreadWorkerBlockedResume ?? (() => {}),
  });
}
