import type {
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
} from "../../messages.js";
import type { ProcessHandle } from "../types.js";
import {
  supervisorOfdFdType,
  supervisorOfdIno,
  supervisorOfdRefcount,
} from "../ownership/ofd-metadata.js";

const SCHEDULER_QUEUE_DEBUG_LOG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_SCHEDULER_QUEUE_DEBUG_LOG ?? "") === "1";
const SCHEDULER_QUEUE_DEBUG_LOG_LIMIT = 256;
const SCHEDULER_QUEUE_DEBUG_LOG_PREFIXES = [
  "pump-",
  "ready:",
  "runnable-",
  "step:",
] as const;
const HOT_PC_COUNTER_LIMIT = 32;

export function formatSchedulerStepDebugSuffix(handle: ProcessHandle, vmPtr: number): string {
  const parts: string[] = [];
  const pid = handle.kernel.process_current_pid?.(vmPtr);
  if (pid !== undefined) {
    parts.push(`pid=${pid}`);
  }
  const tid = handle.kernel.thread_tid?.(vmPtr);
  if (tid !== undefined) {
    parts.push(`tid=${tid}`);
  }
  const pc = handle.kernel.thread_pc?.(vmPtr);
  if (pc !== undefined) {
    parts.push(`pc=0x${pc.toString(16)}`);
  }
  const ra = handle.kernel.thread_ra?.(vmPtr);
  if (ra !== undefined) {
    parts.push(`ra=0x${ra.toString(16)}`);
  }
  const sp = handle.kernel.thread_sp?.(vmPtr);
  if (sp !== undefined) {
    parts.push(`sp=0x${sp.toString(16)}`);
    for (const byteOffset of [32, 24, 16, 8]) {
      const stackWord = handle.kernel.thread_stack_minus?.(vmPtr, byteOffset);
      if (stackWord !== undefined) {
        parts.push(`stkm${byteOffset}=0x${stackWord.toString(16)}`);
      }
    }
  }
  for (let index = 0; index <= 32; index += 1) {
    const stackWord = handle.kernel.thread_stack_word?.(vmPtr, index);
    if (stackWord !== undefined) {
      parts.push(`stk${index}=0x${stackWord.toString(16)}`);
    }
  }
  for (const byteOffset of [8, 40, 56, 88]) {
    const wordIndex = byteOffset / 8;
    const stackWord = handle.kernel.thread_stack_word?.(vmPtr, wordIndex);
    if (stackWord !== undefined) {
      parts.push(`slot${byteOffset}=0x${stackWord.toString(16)}`);
    }
  }
  const a0 = handle.kernel.thread_a0?.(vmPtr);
  if (a0 !== undefined) {
    parts.push(`a0=${a0.toString()}`);
  }
  const a1 = handle.kernel.thread_a1?.(vmPtr);
  if (a1 !== undefined) {
    parts.push(`a1=${a1.toString()}`);
  }
  const a2 = handle.kernel.thread_reg?.(vmPtr, 12);
  if (a2 !== undefined) {
    parts.push(`a2=${a2.toString()}`);
  }
  const a3 = handle.kernel.thread_reg?.(vmPtr, 13);
  if (a3 !== undefined) {
    parts.push(`a3=${a3.toString()}`);
  }
  const gp = handle.kernel.thread_gp?.(vmPtr);
  if (gp !== undefined) {
    parts.push(`gp=0x${gp.toString(16)}`);
  }
  const tp = handle.kernel.thread_tp?.(vmPtr);
  if (tp !== undefined) {
    parts.push(`tp=0x${tp.toString(16)}`);
  }
  for (const [label, reg] of [["s0", 8], ["s1", 9], ["s2", 18], ["s3", 19], ["s4", 20], ["s5", 21], ["s6", 22]] as const) {
    const value = handle.kernel.thread_reg?.(vmPtr, reg);
    if (value !== undefined) {
      parts.push(`${label}=0x${value.toString(16)}`);
    }
  }
  const s11 = handle.kernel.thread_s11?.(vmPtr);
  if (s11 !== undefined) {
    parts.push(`s11=0x${s11.toString(16)}`);
  }
  return parts.length > 0 ? `:${parts.join(":")}` : "";
}

function tryFormatOfdType(handle: ProcessHandle, ofdId: number): string {
  try {
    const fdType = supervisorOfdFdType(handle, ofdId);
    if (fdType === handle.kernel.abi_fd_type_file?.()) {
      return "file";
    }
    if (fdType === handle.kernel.abi_fd_type_pipe?.()) {
      return "pipe";
    }
    if (fdType === handle.kernel.abi_fd_type_epoll?.()) {
      return "epoll";
    }
    if (fdType === handle.kernel.abi_fd_type_eventfd?.()) {
      return "eventfd";
    }
    if (fdType === handle.kernel.abi_fd_type_socket?.()) {
      return "socket";
    }
    return String(fdType);
  } catch {
    return "err";
  }
}

export function formatSchedulerStepFdSnapshot(
  handle: ProcessHandle,
  vmPtr: number | undefined,
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
  ofdSlots: readonly KernelWorkerOfdSlotSnapshot[] | undefined,
): string {
  if (vmPtr === undefined || vmPtr === 0 || fdEntries === undefined) {
    return "fds=none";
  }
  const focusFds = new Set([9, 10, 11, 12]);
  const selectedFdEntries = fdEntries.filter((entry) => entry.ofdId !== 0 || focusFds.has(entry.fd));
  const fdSummary = selectedFdEntries
    .slice(0, 24)
    .map((entry) => {
      if (entry.ofdId === 0) {
        return `${entry.fd}=0`;
      }
      let ino = "err";
      let refcount = "err";
      try {
        ino = String(supervisorOfdIno(handle, entry.ofdId));
      } catch {
        // The diagnostic must survive older or partial harness exports.
      }
      try {
        refcount = String(supervisorOfdRefcount(handle, entry.ofdId));
      } catch {
        // The diagnostic must survive older or partial harness exports.
      }
      return `${entry.fd}=${entry.ofdId}/${tryFormatOfdType(handle, entry.ofdId)}/ino${ino}/ref${refcount}/fl${entry.fdFlags}`;
    });
  if (selectedFdEntries.length > 24) {
    fdSummary.push(`more=${selectedFdEntries.length - 24}`);
  }
  const ofdSlotIds = ofdSlots?.map((slot) => slot.ofdId).sort((left, right) => left - right) ?? [];
  return `fds=${fdSummary.join(",") || "empty"}:ofdSlots=${ofdSlotIds.join(",") || "none"}`;
}

export function hasBrokenSchedulerThreadState(handle: ProcessHandle, vmPtr: number): boolean {
  const pc = handle.kernel.thread_pc?.(vmPtr) ?? 0n;
  const sp = handle.kernel.thread_sp?.(vmPtr) ?? 0n;
  const s11 = handle.kernel.thread_s11?.(vmPtr) ?? 0n;
  return pc < 0x1000n || sp === 0n || s11 === 0n;
}

export function appendSchedulerQueueDebugLog(
  handle: ProcessHandle,
  event: string,
): void {
  if (!SCHEDULER_QUEUE_DEBUG_LOG_ENABLED) {
    return;
  }
  const queueDebugCount = handle.workerDebugLog.reduce(
    (count, entry) => count + (
      entry.startsWith("pump-") || entry.startsWith("step:") || entry.startsWith("ready:") || entry.startsWith("runnable-")
        ? 1
        : 0
    ),
    0,
  );
  if (queueDebugCount >= SCHEDULER_QUEUE_DEBUG_LOG_LIMIT) {
    const oldestQueueDebugIndex = handle.workerDebugLog.findIndex((entry) =>
      SCHEDULER_QUEUE_DEBUG_LOG_PREFIXES.some((prefix) => entry.startsWith(prefix)));
    if (oldestQueueDebugIndex !== -1) {
      handle.workerDebugLog.splice(oldestQueueDebugIndex, 1);
    } else {
      handle.workerDebugLog.shift();
    }
  }
  handle.workerDebugLog.push(event);
}

export function incrementBoundedSchedulerCounter(
  counters: Record<string, number>,
  key: string,
  maxEntries = HOT_PC_COUNTER_LIMIT,
): void {
  const existing = counters[key];
  if (existing !== undefined) {
    counters[key] = existing + 1;
    return;
  }
  const entries = Object.keys(counters);
  if (entries.length < maxEntries) {
    counters[key] = 1;
    return;
  }
  for (const entry of entries) {
    const next = (counters[entry] ?? 0) - 1;
    if (next <= 0) {
      delete counters[entry];
      continue;
    }
    counters[entry] = next;
  }
}
