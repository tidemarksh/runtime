import {
  setWorkerErrorHandler,
  setWorkerMessageErrorHandler,
  setWorkerMessageHandler,
} from "../../bridge/compat.js";
import { publishRuntimeBridgeEpoch } from "../../bridge/runtime-state.js";
import type { ProcessHandle, ThreadWorkerState } from "../types.js";
import type { ThreadWorkerOutbound } from "../../messages.js";
import type {
  ThreadWorkerInbound,
  WorkerSchedulerQueueHelpers,
} from "./types.js";

const SCHEDULER_STEP_BUDGET = typeof window === "undefined" ? 20_000 : 2_000;

interface WorkerSchedulerQueueDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  handleWorkerMessage(handle: ProcessHandle, worker: Worker, msg: ThreadWorkerOutbound): void;
}

function readKernelAddress(value: bigint | number | undefined): number {
  if (typeof value === "bigint") {
    return Number(value);
  }
  return typeof value === "number" ? value : 0;
}

function formatStepDebugSuffix(handle: ProcessHandle, vmPtr: number): string {
  const parts: string[] = [];
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
  }
  for (let index = 0; index <= 32; index += 1) {
    const stackWord = handle.kernel.thread_stack_word?.(vmPtr, index);
    if (stackWord !== undefined) {
      parts.push(`stk${index}=0x${stackWord.toString(16)}`);
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

function hasBrokenThreadState(handle: ProcessHandle, vmPtr: number): boolean {
  const pc = handle.kernel.thread_pc?.(vmPtr) ?? 0n;
  const sp = handle.kernel.thread_sp?.(vmPtr) ?? 0n;
  const s11 = handle.kernel.thread_s11?.(vmPtr) ?? 0n;
  return pc < 0x1000n || sp === 0n || s11 === 0n;
}

export function createWorkerSchedulerQueueHelpers(
  deps: WorkerSchedulerQueueDeps,
): WorkerSchedulerQueueHelpers {
  function liveGuestBytes(handle: ProcessHandle): number {
    const guestCapacity = Math.max(0, handle.memory.buffer.byteLength - handle.guestBase);
    const fallback = Math.max(0, Math.min(handle.ramSize, guestCapacity));
    let used = Math.max(
      0,
      readKernelAddress(handle.kernel.debug_memory_brk_current?.()),
      readKernelAddress(handle.kernel.debug_memory_mmap_next?.()),
    );
    const vmaLen = handle.kernel.debug_memory_vma_len?.() ?? 0;
    for (let index = 0; index < vmaLen; index += 1) {
      used = Math.max(used, readKernelAddress(handle.kernel.debug_memory_vma_end?.(index)));
    }
    if (used === 0) {
      used = fallback;
    }
    return Math.max(0, Math.min(used, guestCapacity));
  }

  function hasExclusiveVforkWorker(handle: ProcessHandle): boolean {
    return handle.vforkSnapshot !== null && handle.vforkWorker !== null;
  }

  function attachThreadWorker(handle: ProcessHandle, worker: Worker, vmPtr: number): void {
    handle.workers.add(worker);
    deps.appendWorkerDebugLog(handle, `attach:0x${vmPtr.toString(16)}`);
    deps.threadWorkerStates.set(worker, {
      vmPtr,
      initialized: false,
      waitingInWorkerStatus: null,
      resumeWorkersOnReady: [],
    });
    setWorkerMessageHandler(worker, (event: MessageEvent<ThreadWorkerOutbound>) => {
      deps.handleWorkerMessage(handle, worker, event.data);
    });
    setWorkerErrorHandler(worker, (event) => {
      const detail = [
        event.message,
        event.filename ? `at ${event.filename}:${event.lineno}:${event.colno}` : null,
        event.error instanceof Error ? (event.error.stack ?? event.error.message) : null,
      ].filter((part): part is string => !!part).join(" ");
      deps.appendWorkerDebugLog(handle, `worker-error:0x${vmPtr.toString(16)}:${JSON.stringify(detail || "unknown worker error")}`);
      deps.failProcess(handle, new Error(
        `thread worker error vm=0x${vmPtr.toString(16)} ${detail || "unknown worker error"}`,
      ));
      event.preventDefault?.();
    });
    setWorkerMessageErrorHandler(worker, (event) => {
      deps.appendWorkerDebugLog(handle, `worker-messageerror:0x${vmPtr.toString(16)}`);
      deps.failProcess(handle, new Error(
        `thread worker messageerror vm=0x${vmPtr.toString(16)}`,
      ));
      event.preventDefault?.();
    });
    const runtimeEpoch = publishRuntimeBridgeEpoch(handle.runtimeBridge);
    const guestBytes = liveGuestBytes(handle);
    const heapBase = Math.max(0, Math.trunc(handle.kernelState.heapBase));
    const heapPtr = Math.max(heapBase, Math.trunc(handle.kernelState.heapPtr));
    const preserveKernelHeapBase = Math.min(heapBase, handle.memory.buffer.byteLength);
    const preserveKernelHeapBytes = Math.max(
      0,
      Math.min(handle.memory.buffer.byteLength, heapPtr) - preserveKernelHeapBase,
    );

    worker.postMessage({
      type: "init",
      wasmModule: handle.wasmModule,
      memory: handle.memory,
      vmPtr,
      guestBase: handle.guestBase,
      guestBytes,
      preserveKernelHeapBase,
      preserveKernelHeapBytes,
      runtimeEpoch,
      guestWriteWatchRegions: handle.guestWriteWatchRegions,
      kernelState: handle.kernelState,
      sabPageCache: handle.sabPageCache,
      runtimeNotify: handle.runtimeBridge.notifySab,
      runtimeState: handle.runtimeBridge.stateSab,
    } satisfies ThreadWorkerInbound);
  }

  function enqueueWorker(handle: ProcessHandle, worker: Worker): void {
    if (handle.exitInProgress) {
      return;
    }
    if (!handle.readyWorkers.includes(worker)) {
      const vmPtr = deps.threadWorkerStates.get(worker)?.vmPtr;
      if (vmPtr !== undefined) {
        deps.appendWorkerDebugLog(handle, `ready:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`);
        if (hasBrokenThreadState(handle, vmPtr)) {
          deps.appendWorkerDebugLog(
            handle,
            `ready_break:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`,
          );
        }
      }
      handle.readyWorkers.push(worker);
    }
    pumpProcess(handle);
  }

  function pumpProcess(handle: ProcessHandle): void {
    if (handle.exitInProgress) {
      return;
    }
    if (handle.runningWorker !== null) {
      return;
    }
    let next: Worker | undefined;
    if (hasExclusiveVforkWorker(handle)) {
      const index = handle.readyWorkers.indexOf(handle.vforkWorker!);
      if (index === -1) {
        return;
      }
      next = handle.readyWorkers.splice(index, 1)[0];
    } else {
      next = handle.readyWorkers.shift();
    }
    if (!next) {
      return;
    }
    handle.runningWorker = next;
    const vmPtr = deps.threadWorkerStates.get(next)?.vmPtr;
    if (vmPtr !== undefined) {
      deps.appendWorkerDebugLog(handle, `step:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`);
      if (hasBrokenThreadState(handle, vmPtr)) {
        deps.appendWorkerDebugLog(
          handle,
          `step_break:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`,
        );
      }
    }
    publishRuntimeBridgeEpoch(handle.runtimeBridge);
    next.postMessage({
      type: "step",
      budget: SCHEDULER_STEP_BUDGET,
      kernelState: handle.kernelState,
    } satisfies ThreadWorkerInbound);
  }

  return {
    attachThreadWorker,
    enqueueWorker,
    pumpProcess,
  };
}
