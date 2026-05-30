import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../abi.js";
import {
  PIPE_BUFFER_BYTES,
  PIPE_DATA_OFFSET,
  PIPE_HEAD_OFFSET,
  PIPE_READ_OPEN_OFFSET,
  PIPE_SLOT_BYTES,
  PIPE_TAIL_OFFSET,
  PIPE_WRITE_OPEN_OFFSET,
} from "../../layout/pipe.js";
import { POLLFD_BYTES } from "../../layout/poll.js";
import type {
  BlockedWorkerState,
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";

const PIPE_OFF_HEAD = PIPE_HEAD_OFFSET;
const PIPE_OFF_TAIL = PIPE_TAIL_OFFSET;
const PIPE_OFF_READ_OPEN = PIPE_READ_OPEN_OFFSET;
const PIPE_OFF_WRITE_OPEN = PIPE_WRITE_OPEN_OFFSET;
const PIPE_DATA_START = PIPE_DATA_OFFSET;
const PIPE_BUF_SIZE = PIPE_BUFFER_BYTES;

export interface PipePollSchedulerDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  advanceGuestTimeByMs(handle: ProcessHandle, timeoutMs: number): void;
  handleBlockedRetryResult(handle: ProcessHandle, worker: Worker, vmPtr: number, status: number): void;
  scheduleBlockedRetry(handle: ProcessHandle, worker: Worker, delayMs?: number, timeoutExpired?: boolean): void;
  pipeChildExitRecordsAlreadyResumedForWait(
    handle: ProcessHandle,
    vmPtr: number,
    pipeChildExitResumeKey?: string | null,
  ): boolean;
  hasActiveChildProcess(parentPid: number): boolean;
}

export interface PipePollScheduler {
  queueBlockedPollWaitGuestMemoryWrites(
    handle: ProcessHandle,
    worker: Worker,
    syscallNr: bigint,
    arg0: bigint,
    arg1: bigint,
    arg2: bigint,
    arg3: bigint,
  ): void;
  resumePipePollTimeoutLocally(
    handle: ProcessHandle,
    worker: Worker,
    blocked: BlockedWorkerState,
    timeoutMs: number,
  ): void;
  blockedOrCurrentSyscallNr(handle: ProcessHandle, vmPtr: number): bigint | null;
  pollWaitTimeoutMs(handle: ProcessHandle, status: number, vmPtr: number): number;
  blockedPollPipeHasBufferedData(handle: ProcessHandle, vmPtr: number): boolean;
  pollPipeWriteOpenForThreadStep(handle: ProcessHandle, vmPtr: number): number;
  pipeWaitShouldRemainPassive(
    handle: ProcessHandle,
    status: number,
    vmPtr: number,
    pollTimeoutMs: number,
    timeoutDeadlineMs: number | null,
    readyCount: number | undefined,
    pipeChildExitResumeKey: string | null,
  ): boolean;
  pipeWaitShouldParkBehindActiveChild(
    handle: ProcessHandle,
    blocked: BlockedWorkerState,
    pollTimeoutMs: number,
    readyCount: number | undefined,
  ): boolean;
  schedulePipePollTimeoutRetry(
    handle: ProcessHandle,
    worker: Worker,
    blocked: { timeoutDeadlineMs: number | null },
    timeoutMs: number,
  ): void;
  formatFirstPipeWaitDebug(handle: ProcessHandle, vmPtr: number): string;
  formatBlockedPollFdSet(handle: ProcessHandle, vmPtr: number, limit?: number): string;
}

function pipeDebugPreviewEnabled(): boolean {
  return ((globalThis as {
    process?: { env?: Record<string, string | undefined> };
  }).process?.env?.TIDEMARK_PIPE_DEBUG_PREVIEW ?? "") === "1";
}

function formatPipeDebugAscii(bytes: Uint8Array, maxLen = 96): string {
  let out = "";
  for (const byte of bytes.subarray(0, maxLen)) {
    if (byte === 0x0a) {
      out += "\\n";
    } else if (byte === 0x0d) {
      out += "\\r";
    } else if (byte === 0x09) {
      out += "\\t";
    } else if (byte >= 0x20 && byte <= 0x7e && byte !== 0x5c && byte !== 0x22) {
      out += String.fromCharCode(byte);
    } else {
      out += `\\x${byte.toString(16).padStart(2, "0")}`;
    }
  }
  if (bytes.length > maxLen) {
    out += "...";
  }
  return out;
}

function formatPipeWaitDataDebug(handle: ProcessHandle, slotBase: number, head: number, tail: number): string {
  if (!pipeDebugPreviewEnabled()) {
    return "";
  }
  const len = tail >= head
    ? tail - head
    : PIPE_BUF_SIZE - head + tail;
  if (len <= 0 || len > PIPE_BUF_SIZE) {
    return `:data_len=${len}`;
  }
  const bytes = new Uint8Array(handle.memory.buffer);
  const dataBase = slotBase + PIPE_DATA_START;
  const previewLen = Math.min(len, 96);
  const preview = new Uint8Array(previewLen);
  for (let i = 0; i < previewLen; i += 1) {
    preview[i] = bytes[dataBase + ((head + i) % PIPE_BUF_SIZE)];
  }
  return `:data_len=${len}:data_ascii=${JSON.stringify(formatPipeDebugAscii(preview))}`;
}

function syscallMatches(
  syscallNr: bigint,
  abiSyscall: (() => bigint | number) | undefined,
): boolean {
  const value = abiSyscall?.();
  return value !== undefined && syscallNr === BigInt(value);
}

export function createPipePollScheduler(deps: PipePollSchedulerDeps): PipePollScheduler {
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
    const writeLen = Math.min(requested, PIPE_BUFFER_BYTES);
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

  function queueBlockedPollWaitGuestMemoryWrites(
    handle: ProcessHandle,
    worker: Worker,
    syscallNr: bigint,
    arg0: bigint,
    arg1: bigint,
    arg2: bigint,
    arg3: bigint,
  ): void {
    const maxFds = Math.max(0, handle.kernel.abi_max_fds?.() ?? 1024);
    if (syscallMatches(syscallNr, handle.kernel.abi_sys_ppoll)) {
      const nfds = Math.min(Number(arg1), maxFds);
      if (Number.isFinite(nfds) && nfds > 0) {
        queueGuestBufferWrite(handle, worker, arg0, nfds * POLLFD_BYTES);
      }
      return;
    }
    if (syscallMatches(syscallNr, handle.kernel.abi_sys_pselect6)) {
      const nfds = Math.min(Number(arg0), maxFds);
      if (!Number.isFinite(nfds) || nfds <= 0) {
        return;
      }
      const setBytes = Math.floor((nfds + 63) / 64) * 8;
      queueGuestBufferWrite(handle, worker, arg1, setBytes);
      queueGuestBufferWrite(handle, worker, arg2, setBytes);
      queueGuestBufferWrite(handle, worker, arg3, setBytes);
    }
  }

  function resumePipePollTimeoutLocally(
    handle: ProcessHandle,
    worker: Worker,
    blocked: BlockedWorkerState,
    timeoutMs: number,
  ): void {
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `pipe-retry-timeout-local:${handle.pid}`);
    attachKernelRuntimeState(handle.kernel, handle.kernelState);
    const blockedSyscallNr = handle.kernel.thread_blocked_syscall_nr?.(blocked.vmPtr) ?? 0n;
    const blockedArg0 = handle.kernel.thread_blocked_syscall_arg?.(blocked.vmPtr, 0) ?? 0n;
    const blockedArg1 = handle.kernel.thread_blocked_syscall_arg?.(blocked.vmPtr, 1) ?? 0n;
    const blockedArg2 = handle.kernel.thread_blocked_syscall_arg?.(blocked.vmPtr, 2) ?? 0n;
    const blockedArg3 = handle.kernel.thread_blocked_syscall_arg?.(blocked.vmPtr, 3) ?? 0n;
    deps.advanceGuestTimeByMs(handle, timeoutMs);
    handle.kernel.thread_resume_poll_wait_timeout?.(blocked.vmPtr);
    queueBlockedPollWaitGuestMemoryWrites(
      handle,
      worker,
      BigInt(blockedSyscallNr),
      BigInt(blockedArg0),
      BigInt(blockedArg1),
      BigInt(blockedArg2),
      BigInt(blockedArg3),
    );
    blocked.timeoutExpired = false;
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    deps.handleBlockedRetryResult(
      handle,
      worker,
      blocked.vmPtr,
      handle.kernel.thread_status(blocked.vmPtr),
    );
  }

  function blockedOrCurrentSyscallNr(handle: ProcessHandle, vmPtr: number): bigint | null {
    const blockedSyscall = handle.kernel.thread_blocked_syscall_nr?.(vmPtr);
    if (blockedSyscall !== undefined && BigInt(blockedSyscall) !== 0n) {
      return BigInt(blockedSyscall);
    }
    const currentSyscall = handle.kernel.thread_a7?.(vmPtr);
    return currentSyscall === undefined ? null : BigInt(currentSyscall);
  }

  function pollWaitTimeoutMs(handle: ProcessHandle, status: number, vmPtr: number): number {
    if (status !== handle.kernel.abi_status_pipe_wait()) {
      return -1;
    }
    const syscall = blockedOrCurrentSyscallNr(handle, vmPtr);
    if (syscall === null) {
      return -1;
    }
    const ppoll = handle.kernel.abi_sys_ppoll?.();
    const pselect6 = handle.kernel.abi_sys_pselect6?.();
    if (
      (ppoll === undefined || syscall !== BigInt(ppoll))
      && (pselect6 === undefined || syscall !== BigInt(pselect6))
    ) {
      return -1;
    }
    const timeout = handle.kernel.thread_poll_wait_timeout?.(vmPtr);
    return timeout === undefined ? -1 : Number(timeout);
  }

  function pipeWaitShouldRemainPassive(
    handle: ProcessHandle,
    status: number,
    vmPtr: number,
    pollTimeoutMs: number,
    timeoutDeadlineMs: number | null,
    readyCount: number | undefined,
    pipeChildExitResumeKey: string | null,
  ): boolean {
    const syscall = blockedOrCurrentSyscallNr(handle, vmPtr);
    if (syscall === null) {
      return false;
    }
    const read = handle.kernel.abi_sys_read?.();
    const readv = handle.kernel.abi_sys_readv?.();
    const ppoll = handle.kernel.abi_sys_ppoll?.();
    const pselect6 = handle.kernel.abi_sys_pselect6?.();
    const isPollLike = (ppoll !== undefined && syscall === BigInt(ppoll))
      || (pselect6 !== undefined && syscall === BigInt(pselect6));
    const canPark = (
      (read !== undefined && syscall === BigInt(read))
      || (readv !== undefined && syscall === BigInt(readv))
      || isPollLike
    );
    const blockedReadIsReady = (
      ((read !== undefined && syscall === BigInt(read)) || (readv !== undefined && syscall === BigInt(readv)))
      && handle.kernel.thread_blocked_pipe_read_ready?.(vmPtr) === 1
    );
    const blockedPollPipeIsReady = blockedPollPipeHasBufferedData(handle, vmPtr);
    const pollLikePipeHasNoWriter = isPollLike
      && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) !== 1
      && !blockedPollPipeIsReady
      && pollPipeWriteOpenForThreadStep(handle, vmPtr) <= 0;
    const childExitRecordsAlreadyResumed = deps.pipeChildExitRecordsAlreadyResumedForWait(
      handle,
      vmPtr,
      pipeChildExitResumeKey,
    );
    return status === handle.kernel.abi_status_pipe_wait()
      && canPark
      && childExitRecordsAlreadyResumed
      && !blockedReadIsReady
      && !blockedPollPipeIsReady
      && !pollLikePipeHasNoWriter
      && pollTimeoutMs < 0
      && timeoutDeadlineMs === null
      && (readyCount === undefined || readyCount === 0)
      && handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) !== 1
      && handle.kernel.thread_poll_hup_sync_pending?.(vmPtr) !== 1;
  }

  function pipeWaitHasImmediateWakeReason(
    handle: ProcessHandle,
    vmPtr: number,
    readyCount: number | undefined,
  ): boolean {
    return (readyCount !== undefined && readyCount !== 0)
      || handle.kernel.thread_blocked_pipe_read_ready?.(vmPtr) === 1
      || handle.kernel.thread_blocked_pipe_write_ready?.(vmPtr) === 1
      || blockedPollPipeHasBufferedData(handle, vmPtr)
      || handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) === 1
      || handle.kernel.thread_poll_hup_sync_pending?.(vmPtr) === 1;
  }

  function pipeWaitShouldParkBehindActiveChild(
    handle: ProcessHandle,
    blocked: BlockedWorkerState,
    pollTimeoutMs: number,
    readyCount: number | undefined,
  ): boolean {
    return blocked.status === handle.kernel.abi_status_pipe_wait()
      && pollTimeoutMs < 0
      && blocked.timeoutDeadlineMs === null
      && !blocked.timeoutExpired
      && deps.pipeChildExitRecordsAlreadyResumedForWait(handle, blocked.vmPtr)
      && handle.pendingChildExitRecords === 0
      && handle.pendingChildSpawnRecords === 0
      && handle.kernel.thread_poll_wait_has_socket?.(blocked.vmPtr) !== 1
      && deps.hasActiveChildProcess(handle.pid)
      && !pipeWaitHasImmediateWakeReason(handle, blocked.vmPtr, readyCount);
  }

  function schedulePipePollTimeoutRetry(
    handle: ProcessHandle,
    worker: Worker,
    blocked: { timeoutDeadlineMs: number | null },
    timeoutMs: number,
  ): void {
    if (timeoutMs < 0) {
      blocked.timeoutDeadlineMs = null;
      deps.scheduleBlockedRetry(handle, worker, 1);
      return;
    }
    const now = Date.now();
    if (blocked.timeoutDeadlineMs === null) {
      blocked.timeoutDeadlineMs = now + timeoutMs;
    }
    const remaining = blocked.timeoutDeadlineMs - now;
    if (remaining <= 0) {
      deps.scheduleBlockedRetry(handle, worker, 0, true);
      return;
    }
    deps.scheduleBlockedRetry(handle, worker, remaining);
  }

  function blockedPollPipeHasBufferedData(handle: ProcessHandle, vmPtr: number): boolean {
    const rawMask = handle.kernel.thread_poll_wait_pipe_slots_mask?.(vmPtr);
    const mask = rawMask === undefined
      ? 0n
      : typeof rawMask === "bigint" ? rawMask : BigInt(rawMask);
    if (mask !== 0n) {
      const maxPipes = Math.min(handle.kernel.abi_max_pipes?.() ?? 64, 64);
      const view = new DataView(handle.memory.buffer);
      for (let pipeSlot = 1; pipeSlot < maxPipes; pipeSlot += 1) {
        if ((mask & (1n << BigInt(pipeSlot))) === 0n) {
          continue;
        }
        const slotBase = handle.kernelState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
        const head = view.getUint32(slotBase + PIPE_OFF_HEAD, true);
        const tail = view.getUint32(slotBase + PIPE_OFF_TAIL, true);
        if (head !== tail) {
          return true;
        }
      }
    }
    const packed = handle.kernel.thread_poll_wait_first_pipe_revents?.(vmPtr) ?? 0;
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
      ofdId === 0
      || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
    ) {
      return false;
    }
    const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
    if (pipeSlot <= 0 || pipeSlot >= handle.kernel.abi_max_pipes()) {
      return false;
    }
    const slotBase = handle.kernelState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
    const view = new DataView(handle.memory.buffer);
    const head = view.getUint32(slotBase + PIPE_OFF_HEAD, true);
    const tail = view.getUint32(slotBase + PIPE_OFF_TAIL, true);
    return head !== tail;
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
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, packed & 0xff);
    if (
      ofdId > 0
      && ofdId < handle.kernel.abi_max_fds()
      && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_pipe()
    ) {
      const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
      if (pipeSlot > 0 && pipeSlot < maxPipes) {
        pipeSlots.add(pipeSlot);
      }
    }
    return pipeSlots;
  }

  function pollPipeWriteOpenForThreadStep(handle: ProcessHandle, vmPtr: number): number {
    let writeOpen = 0;
    const view = new DataView(handle.memory.buffer);
    for (const pipeSlot of pollPipeSlotsForThreadStep(handle, vmPtr)) {
      const slotBase = handle.kernelState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
      if (slotBase < 0 || slotBase + PIPE_OFF_WRITE_OPEN + 4 > view.byteLength) {
        continue;
      }
      writeOpen = Math.max(writeOpen, view.getInt32(slotBase + PIPE_OFF_WRITE_OPEN, true));
    }
    return writeOpen;
  }

  function formatPipeSlotHolders(handle: ProcessHandle, pipeSlot: number): string {
    if (pipeSlot <= 0 || pipeSlot >= handle.kernel.abi_max_pipes()) {
      return "n/a";
    }
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
        const hostFd = member.kernel.kernel_ofd_host_fd(ofdId);
        holders.push(`${member.pid}/${fd}/${ofdId}/${hostFd >= 0 ? "r" : "w"}/h${hostFd}`);
      }
    }
    return holders.join(".") || "none";
  }

  function formatFirstPipeWaitDebug(handle: ProcessHandle, vmPtr: number): string {
    const packed = handle.kernel.thread_poll_wait_first_pipe_revents?.(vmPtr);
    if (packed === undefined) {
      return "pipe=n/a";
    }
    const fd = packed & 0xff;
    const interest = (packed >>> 8) & 0xffff;
    const revents = (packed >>> 24) & 0xff;
    try {
      const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
      if (
        ofdId <= 0
        || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
      ) {
        return `pipe=fd${fd}/i0x${interest.toString(16)}/r0x${revents.toString(16)}/ofd${ofdId}`;
      }
      const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
      if (pipeSlot <= 0 || pipeSlot >= handle.kernel.abi_max_pipes()) {
        return `pipe=fd${fd}/i0x${interest.toString(16)}/r0x${revents.toString(16)}/ofd${ofdId}/slot${pipeSlot}`;
      }
      const slotBase = handle.kernelState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
      const view = new DataView(handle.memory.buffer);
      const head = view.getUint32(slotBase + PIPE_OFF_HEAD, true);
      const tail = view.getUint32(slotBase + PIPE_OFF_TAIL, true);
      const readOpen = view.getInt32(slotBase + PIPE_OFF_READ_OPEN, true);
      const writeOpen = view.getInt32(slotBase + PIPE_OFF_WRITE_OPEN, true);
      return `pipe=fd${fd}/i0x${interest.toString(16)}/r0x${revents.toString(16)}/ofd${ofdId}/slot${pipeSlot}/h${head}/t${tail}/ro${readOpen}/wo${writeOpen}${formatPipeWaitDataDebug(handle, slotBase, head, tail)}/holders=${formatPipeSlotHolders(handle, pipeSlot)}`;
    } catch (error) {
      return `pipe=fd${fd}/i0x${interest.toString(16)}/r0x${revents.toString(16)}/err=${JSON.stringify(error instanceof Error ? error.message : String(error))}`;
    }
  }

  function formatPollFdTarget(handle: ProcessHandle, vmPtr: number, fd: number): string {
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    const maxFds = handle.kernel.abi_max_fds();
    if (ofdId <= 0 || ofdId >= maxFds) {
      return `ofd${ofdId}`;
    }
    const type = handle.kernel.kernel_ofd_fd_type(ofdId);
    const ino = handle.kernel.kernel_ofd_ino(ofdId);
    const hostFd = handle.kernel.kernel_ofd_host_fd(ofdId);
    if (type !== handle.kernel.abi_fd_type_pipe()) {
      return `ofd${ofdId}/ty${type}/ino${ino}/host${hostFd}`;
    }
    if (ino <= 0 || ino >= handle.kernel.abi_max_pipes()) {
      return `ofd${ofdId}/ty${type}/slot${ino}/host${hostFd}`;
    }
    const slotBase = handle.kernelState.pipeBase + ino * PIPE_SLOT_BYTES;
    const view = new DataView(handle.memory.buffer);
    const head = view.getUint32(slotBase + PIPE_OFF_HEAD, true);
    const tail = view.getUint32(slotBase + PIPE_OFF_TAIL, true);
    const readOpen = view.getInt32(slotBase + PIPE_OFF_READ_OPEN, true);
    const writeOpen = view.getInt32(slotBase + PIPE_OFF_WRITE_OPEN, true);
    return `ofd${ofdId}/ty${type}/slot${ino}/h${head}/t${tail}/ro${readOpen}/wo${writeOpen}/host${hostFd}/holders=${formatPipeSlotHolders(handle, ino)}`;
  }

  function guestAbsoluteRange(handle: ProcessHandle, ptr: bigint, byteLength: number): number | null {
    const guestPtr = Number(ptr);
    if (!Number.isFinite(guestPtr) || guestPtr <= 0 || byteLength <= 0) {
      return null;
    }
    const absolute = handle.guestBase + guestPtr;
    if (absolute < 0 || absolute + byteLength > handle.memory.buffer.byteLength) {
      return null;
    }
    return absolute;
  }

  function formatBlockedPollFdSet(handle: ProcessHandle, vmPtr: number, limit = 16): string {
    const syscall = blockedOrCurrentSyscallNr(handle, vmPtr);
    if (syscall === null) {
      return "pollfds=n/a";
    }
    const ppoll = handle.kernel.abi_sys_ppoll?.();
    const pselect6 = handle.kernel.abi_sys_pselect6?.();
    const maxFds = handle.kernel.abi_max_fds();
    const view = new DataView(handle.memory.buffer);
    try {
      if (ppoll !== undefined && syscall === BigInt(ppoll)) {
        const fdsPtr = BigInt(handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 0) ?? 0n);
        const nfds = Number(handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 1) ?? 0n);
        if (!Number.isFinite(nfds) || nfds <= 0) {
          return "pollfds=ppoll:none";
        }
        const count = Math.min(nfds, maxFds, limit);
        const absolute = guestAbsoluteRange(handle, fdsPtr, count * POLLFD_BYTES);
        if (absolute === null) {
          return `pollfds=ppoll:invalid:ptr=0x${fdsPtr.toString(16)}:nfds=${nfds}`;
        }
        const entries: string[] = [];
        for (let index = 0; index < count; index += 1) {
          const offset = absolute + index * POLLFD_BYTES;
          const fd = view.getInt32(offset, true);
          const events = view.getUint16(offset + 4, true);
          const revents = view.getUint16(offset + 6, true);
          if (fd < 0 || events === 0) {
            continue;
          }
          entries.push(`${index}=fd${fd}/ev0x${events.toString(16)}/rev0x${revents.toString(16)}/${formatPollFdTarget(handle, vmPtr, fd)}`);
        }
        return `pollfds=ppoll:${entries.join(",") || "none"}`;
      }
      if (pselect6 !== undefined && syscall === BigInt(pselect6)) {
        const nfds = Number(handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 0) ?? 0n);
        const readPtr = BigInt(handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 1) ?? 0n);
        const writePtr = BigInt(handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 2) ?? 0n);
        const exceptPtr = BigInt(handle.kernel.thread_blocked_syscall_arg?.(vmPtr, 3) ?? 0n);
        if (!Number.isFinite(nfds) || nfds <= 0) {
          return "pollfds=pselect:none";
        }
        const count = Math.min(nfds, maxFds, limit);
        const setBytes = Math.floor((Math.min(nfds, maxFds) + 63) / 64) * 8;
        const readBase = readPtr === 0n ? null : guestAbsoluteRange(handle, readPtr, setBytes);
        const writeBase = writePtr === 0n ? null : guestAbsoluteRange(handle, writePtr, setBytes);
        const exceptBase = exceptPtr === 0n ? null : guestAbsoluteRange(handle, exceptPtr, setBytes);
        const hasBit = (base: number | null, fd: number): boolean => {
          if (base === null) {
            return false;
          }
          return (view.getUint8(base + Math.floor(fd / 8)) & (1 << (fd % 8))) !== 0;
        };
        const entries: string[] = [];
        for (let fd = 0; fd < count; fd += 1) {
          const interest = `${hasBit(readBase, fd) ? "r" : ""}${hasBit(writeBase, fd) ? "w" : ""}${hasBit(exceptBase, fd) ? "e" : ""}`;
          if (interest === "") {
            continue;
          }
          entries.push(`fd${fd}/${interest}/${formatPollFdTarget(handle, vmPtr, fd)}`);
        }
        return `pollfds=pselect:${entries.join(",") || "none"}`;
      }
    } catch (error) {
      return `pollfds=err:${JSON.stringify(error instanceof Error ? error.message : String(error))}`;
    }
    return "pollfds=not-poll";
  }

  return {
    queueBlockedPollWaitGuestMemoryWrites,
    resumePipePollTimeoutLocally,
    blockedOrCurrentSyscallNr,
    pollWaitTimeoutMs,
    blockedPollPipeHasBufferedData,
    pollPipeWriteOpenForThreadStep,
    pipeWaitShouldRemainPassive,
    pipeWaitShouldParkBehindActiveChild,
    schedulePipePollTimeoutRetry,
    formatFirstPipeWaitDebug,
    formatBlockedPollFdSet,
  };
}
