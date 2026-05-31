import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../abi.js";
import { kernelShareOtherMembers, reconcileKernelSharePipeControl } from "../kernel-share.js";
import { forceNetworkClosedFds } from "../network-socket-close.js";
import type {
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import type { KernelWorkerPipeSlotSnapshot } from "../../messages.js";
import { PIPE_HEAD_OFFSET, PIPE_TAIL_OFFSET, PIPE_WRITE_OPEN_OFFSET } from "../../layout/pipe.js";
import { POLLFD_BYTES } from "../../layout/poll.js";
import { activeChildMayHoldBlockedPipeWriter } from "./pipe.js";
import type { KernelRpcResumeEffect, KernelRpcResumeOutcome } from "./outcomes.js";
import type { SyncCause } from "../contracts/sync.js";

const GUEST_TICKS_PER_SEC = 100_000_000n;
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

export interface WorkerKernelRpcBlockedRpcDeps {
  processHandles: Map<number, ProcessHandle>;
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  enqueueWorker(handle: ProcessHandle, worker: Worker): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  handleBlockedRetryResult(handle: ProcessHandle, worker: Worker, vmPtr: number, status: number): void;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  syncBlockedForkSharedStateViaKernelWorker(handle: ProcessHandle, sourceVmPtr: number | undefined, cause: SyncCause): Promise<void>;
  syncSocketStateViaKernelWorker(handle: ProcessHandle, cause: SyncCause): Promise<{ socketUsed: number; netBufUsed: bigint }>;
}

export function createWorkerKernelRpcBlockedRpcHelpers(deps: WorkerKernelRpcBlockedRpcDeps) {
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

  function emitBlockedRetryResult(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    status: number,
    collectEffects: boolean,
  ): void {
    if (collectEffects) {
      pushOutcomeEffect(worker, { kind: "handle-blocked-retry-result", status });
      return;
    }
    deps.handleBlockedRetryResult(handle, worker, vmPtr, status);
  }

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

function blockedRpcSyncCause(status: number): SyncCause {
  return { kind: "blocked-rpc", status };
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



async function resumeBlockedSyscallLocally(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  blockedStatus: number,
  timeoutExpired = false,
  collectEffects = false,
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
          if (collectEffects) {
            pushOutcomeEffect(worker, {
              kind: "request-pipe-state-sync",
              handle: member,
              wakeWaiters: false,
            });
            return;
          } else {
            await deps.requestPipeStateSync(member, false);
            if (abortIfStaleBlockedRpc()) {
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
          if (collectEffects) {
            pushOutcomeEffect(worker, { kind: "request-pipe-state-sync", wakeWaiters: true });
            return;
          } else {
            await deps.requestPipeStateSync(handle, true);
            if (abortIfStaleBlockedRpc()) {
              return;
            }
          }
          deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-pipe-before-resume-done:${handle.pid}`);
        }
      }
      deps.appendWorkerDebugLog(handle, `blocked-rpc-stage:${blockedStatus}:reconcile:pid=${handle.pid}:0x${vmPtr.toString(16)}`);
      reconcileKernelSharePipeControl(handle);
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
        emitBlockedRetryResult(handle, worker, vmPtr, handle.kernel.thread_status(vmPtr), collectEffects);
        return;
      }
      if (activeChildMayHoldBlockedPipeWriter(deps, handle, vmPtr)) {
        finishBlockedRpc();
        deps.appendWorkerDebugLog(handle, `blocked-rpc-defer-pipe-eof:0x${vmPtr.toString(16)}`);
        emitBlockedRetryResult(handle, worker, vmPtr, blockedStatus, collectEffects);
        return;
      }
      if (handle.pendingChildExitRecords > 0) {
        finishBlockedRpc();
        deps.appendWorkerDebugLog(handle, `blocked-rpc-defer-pipe-eof-child-exit:0x${vmPtr.toString(16)}`);
        emitBlockedRetryResult(handle, worker, vmPtr, blockedStatus, collectEffects);
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
      emitBlockedRetryResult(handle, worker, vmPtr, blockedStatus, collectEffects);
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
      emitBlockedRetryResult(handle, worker, vmPtr, blockedStatus, collectEffects);
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
      pushOutcomeEffect(worker, { kind: "enqueue-worker" });
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
      emitBlockedRetryResult(handle, worker, vmPtr, handle.kernel.thread_status(vmPtr), collectEffects);
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
        emitBlockedRetryResult(handle, worker, vmPtr, blockedStatus, collectEffects);
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
        emitBlockedRetryResult(handle, worker, vmPtr, blockedStatus, collectEffects);
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
        emitBlockedRetryResult(handle, worker, vmPtr, blockedStatus, collectEffects);
        return;
      }
      thread.pendingPipeSlotsForNextStep.push(...pipeSlotsForResume);
      thread.resumeBlockedOnNextStep = true;
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      finishBlockedRpc();
      handle.blockedWorkers.delete(worker);
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
      && !resumePollTimeout
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
      emitBlockedRetryResult(handle, worker, vmPtr, handle.kernel.thread_status(vmPtr), collectEffects);
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

    emitBlockedRetryResult(handle, worker, vmPtr, handle.kernel.thread_status(vmPtr), collectEffects);
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



  async function resumeBlockedSyscallOutcome(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    blockedStatus: number,
    timeoutExpired = false,
  ): Promise<KernelRpcResumeOutcome> {
    pendingOutcomeEffects.delete(worker);
    await resumeBlockedSyscallLocally(handle, worker, vmPtr, blockedStatus, timeoutExpired, true);
    const effects = takeOutcomeEffects(worker);
    if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
      return { kind: "process-gone", effects };
    }
    if (!handle.blockedWorkers.has(worker)) {
      return { kind: "queued-in-worker", effects };
    }
    const status = handle.kernel.thread_status?.(vmPtr);
    if (status === undefined) {
      return { kind: "stale", effects };
    }
    return { kind: "blocked", status, effects };
  }

  return {
    resumeBlockedSyscallLocally,
    resumeBlockedSyscallOutcome,
  };
}
