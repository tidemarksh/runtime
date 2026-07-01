import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
} from "../../abi.js";
import { resumeBlockedSyscallBySupervisor } from "../../supervisor-blocked-resume.js";
import {
  formatPipeSlotControlDebug,
  pipeSlotHasBufferedData,
  pipeSlotWriteOpen,
  PIPE_WAIT_GUEST_MEMORY_WRITE_BYTES,
} from "../ownership/pipe-control.js";
import { POLLFD_BYTES } from "../../layout/poll.js";
import { appendPendingGuestMemoryWrites } from "../guest-memory-writes.js";
import type {
  BlockedWorkerState,
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";

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
    blocked: { timeoutDeadlineMs: number | null; supervisorTimerArmed?: boolean },
    timeoutMs: number,
  ): void;
  formatFirstPipeWaitDebug(handle: ProcessHandle, vmPtr: number): string;
  formatBlockedPollFdSet(handle: ProcessHandle, vmPtr: number, limit?: number): string;
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
    const writeLen = Math.min(requested, PIPE_WAIT_GUEST_MEMORY_WRITE_BYTES);
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
    resumeBlockedSyscallBySupervisor(handle.kernel, blocked.vmPtr, true);
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
    const pollPipeSlots = isPollLike
      ? pollPipeSlotsForThreadStep(handle, vmPtr)
      : null;
    const pollLikeHasNoWatchedFds = isPollLike
      && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) !== 1
      && pollWaitHasNoWatchedFds(handle, vmPtr, syscall);
    const pollLikePipeHasNoWriter = isPollLike
      && handle.kernel.thread_poll_wait_has_socket?.(vmPtr) !== 1
      && (pollPipeSlots?.size ?? 0) > 0
      && !blockedPollPipeIsReady
      && pollPipeWriteOpenForThreadStep(handle, vmPtr) <= 0;
    const childExitRecordsAlreadyResumed = deps.pipeChildExitRecordsAlreadyResumedForWait(
      handle,
      vmPtr,
      pipeChildExitResumeKey,
    );
    const childWorkStillPending = handle.pendingChildExitRecords > 0
      || handle.pendingChildSpawnRecords > 0
      || deps.hasActiveChildProcess(handle.pid);
    const childWakeRecordPending = (
      handle.childExitSyncRecords.size > 0
      && !childExitRecordsAlreadyResumed
    )
      || handle.pendingChildExitRecords > 0
      || handle.pendingChildSpawnRecords > 0;
    return status === handle.kernel.abi_status_pipe_wait()
      && canPark
      && (
        (childExitRecordsAlreadyResumed && childWorkStillPending)
        || (pollLikeHasNoWatchedFds && !childWakeRecordPending)
      )
      && !blockedReadIsReady
      && !blockedPollPipeIsReady
      && !pollLikePipeHasNoWriter
      && pollTimeoutMs < 0
      && timeoutDeadlineMs === null
      && (readyCount === undefined || readyCount === 0)
      && handle.kernel.thread_pipe_read_eof_sync_pending?.(vmPtr) !== 1
      && handle.kernel.thread_poll_hup_sync_pending?.(vmPtr) !== 1;
  }

  function pollWaitHasNoWatchedFds(
    handle: ProcessHandle,
    vmPtr: number,
    syscall: bigint,
  ): boolean {
    const ppoll = handle.kernel.abi_sys_ppoll?.();
    const pselect6 = handle.kernel.abi_sys_pselect6?.();
    const maxFds = handle.kernel.abi_max_fds?.() ?? 1024;
    const memory = handle.memory?.buffer;
    if (!memory) {
      return false;
    }
    const view = new DataView(memory);
    if (ppoll !== undefined && syscall === BigInt(ppoll)) {
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
    if (pselect6 !== undefined && syscall === BigInt(pselect6)) {
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
    blocked: { timeoutDeadlineMs: number | null; supervisorTimerArmed?: boolean },
    timeoutMs: number,
  ): void {
    if (blocked.supervisorTimerArmed === true) {
      blocked.timeoutDeadlineMs = null;
      return;
    }
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
      for (let pipeSlot = 1; pipeSlot < maxPipes; pipeSlot += 1) {
        if ((mask & (1n << BigInt(pipeSlot))) === 0n) {
          continue;
        }
        if (pipeSlotHasBufferedData(handle, pipeSlot)) {
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
    return pipeSlotHasBufferedData(handle, pipeSlot);
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
    for (const pipeSlot of pollPipeSlotsForThreadStep(handle, vmPtr)) {
      const slotWriteOpen = pipeSlotWriteOpen(handle, pipeSlot);
      if (slotWriteOpen === null) {
        continue;
      }
      writeOpen = Math.max(writeOpen, slotWriteOpen);
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
      return `pipe=fd${fd}/i0x${interest.toString(16)}/r0x${revents.toString(16)}/ofd${ofdId}/${formatPipeSlotControlDebug(handle, pipeSlot)}/holders=${formatPipeSlotHolders(handle, pipeSlot)}`;
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
    return `ofd${ofdId}/ty${type}/${formatPipeSlotControlDebug(handle, ino)}/host${hostFd}/holders=${formatPipeSlotHolders(handle, ino)}`;
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
