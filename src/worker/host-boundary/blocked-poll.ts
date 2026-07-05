import { formatPipeSlotControlDebug } from "../ownership/pipe-control.js";
import { POLLFD_BYTES } from "../../layout/poll.js";
import type {
  ProcessHandle,
} from "../types.js";
import {
  supervisorBlockedPollWaitTimeoutMs,
  type SupervisorBlockedWaitRetryFlags,
} from "../../supervisor-blocked-resume.js";
import { processExitInProgress } from "../process-lifecycle-state.js";
import {
  supervisorProcessPipeHostFdForFd,
  supervisorProcessPipeOfdForFd,
  supervisorProcessPipeSlotForFd,
} from "../ownership/pipe-slot.js";

export interface PipePollSchedulerDeps {
  appendRuntimeLifecycleEvent(runtimeId: number, event: string): void;
  scheduleSupervisorBlockedRetry(
    handle: ProcessHandle,
    worker: Worker,
    flags?: SupervisorBlockedWaitRetryFlags,
  ): void;
}

export interface PipePollScheduler {
  blockedOrCurrentSyscallNr(handle: ProcessHandle, vmPtr: number): bigint | null;
  pollWaitTimeoutMs(handle: ProcessHandle, status: number, vmPtr: number): number;
  formatFirstPipeWaitDebug(handle: ProcessHandle, vmPtr: number): string;
  formatBlockedPollFdSet(handle: ProcessHandle, vmPtr: number, limit?: number): string;
}

export function createPipePollScheduler(deps: PipePollSchedulerDeps): PipePollScheduler {
  function blockedOrCurrentSyscallNr(handle: ProcessHandle, vmPtr: number): bigint | null {
    const blockedSyscall = handle.kernel.thread_blocked_syscall_nr?.(vmPtr);
    if (blockedSyscall !== undefined && BigInt(blockedSyscall) !== 0n) {
      return BigInt(blockedSyscall);
    }
    const currentSyscall = handle.kernel.thread_a7?.(vmPtr);
    return currentSyscall === undefined ? null : BigInt(currentSyscall);
  }

  function pollWaitTimeoutMs(handle: ProcessHandle, _status: number, vmPtr: number): number {
    return supervisorBlockedPollWaitTimeoutMs(handle.kernel, vmPtr);
  }

  function formatPipeSlotHolders(handle: ProcessHandle, pipeSlot: number): string {
    if (pipeSlot <= 0 || pipeSlot >= handle.kernel.abi_max_pipes()) {
      return "n/a";
    }
    const holders: string[] = [];
    for (const member of handle.kernelShare.members) {
      if (member.vmPtr === 0 || processExitInProgress(member)) {
        continue;
      }
      const maxFds = member.kernel.abi_max_fds();
      for (let fd = 0; fd < maxFds; fd += 1) {
        const ofdId = supervisorProcessPipeOfdForFd(member, member.vmPtr, fd);
        if (ofdId === 0 || supervisorProcessPipeSlotForFd(member, member.vmPtr, fd) !== pipeSlot) {
          continue;
        }
        const hostFd = supervisorProcessPipeHostFdForFd(member, member.vmPtr, fd);
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
      const ofdId = supervisorProcessPipeOfdForFd(handle, vmPtr, fd);
      if (ofdId === 0) {
        return `pipe=fd${fd}/i0x${interest.toString(16)}/r0x${revents.toString(16)}/ofd${ofdId}`;
      }
      const pipeSlot = supervisorProcessPipeSlotForFd(handle, vmPtr, fd);
      if (pipeSlot <= 0 || pipeSlot >= handle.kernel.abi_max_pipes()) {
        return `pipe=fd${fd}/i0x${interest.toString(16)}/r0x${revents.toString(16)}/ofd${ofdId}/slot${pipeSlot}`;
      }
      return `pipe=fd${fd}/i0x${interest.toString(16)}/r0x${revents.toString(16)}/ofd${ofdId}/${formatPipeSlotControlDebug(handle, pipeSlot)}/holders=${formatPipeSlotHolders(handle, pipeSlot)}`;
    } catch (error) {
      return `pipe=fd${fd}/i0x${interest.toString(16)}/r0x${revents.toString(16)}/err=${JSON.stringify(error instanceof Error ? error.message : String(error))}`;
    }
  }

  function formatPollFdTarget(handle: ProcessHandle, vmPtr: number, fd: number): string {
    const ofdId = supervisorProcessPipeOfdForFd(handle, vmPtr, fd);
    if (ofdId === 0) {
      return "pipe=none";
    }
    const pipeSlot = supervisorProcessPipeSlotForFd(handle, vmPtr, fd);
    const hostFd = supervisorProcessPipeHostFdForFd(handle, vmPtr, fd);
    if (pipeSlot <= 0 || pipeSlot >= handle.kernel.abi_max_pipes()) {
      return `ofd${ofdId}/slot${pipeSlot}/host${hostFd}`;
    }
    return `ofd${ofdId}/${formatPipeSlotControlDebug(handle, pipeSlot)}/host${hostFd}/holders=${formatPipeSlotHolders(handle, pipeSlot)}`;
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
    blockedOrCurrentSyscallNr,
    pollWaitTimeoutMs,
    formatFirstPipeWaitDebug,
    formatBlockedPollFdSet,
  };
}
