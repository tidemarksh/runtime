import type {
  KernelShareGroup,
  ProcessHandle,
  ThreadWorkerState,
} from "./types.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function formatThreadDebug(handle: ProcessHandle, vmPtr: number): string {
  const tid = handle.kernel.thread_tid?.(vmPtr) ?? 0;
  const pc = handle.kernel.thread_pc?.(vmPtr) ?? 0n;
  const ra = handle.kernel.thread_ra?.(vmPtr) ?? 0n;
  const sp = handle.kernel.thread_sp?.(vmPtr) ?? 0n;
  const a0 = handle.kernel.thread_a0?.(vmPtr) ?? 0n;
  const a1 = handle.kernel.thread_a1?.(vmPtr) ?? 0n;
  const sigPending = handle.kernel.thread_sig_pending?.(vmPtr) ?? 0n;
  const parts = [
    `tid=${tid}`,
    `pc=0x${pc.toString(16)}`,
    `ra=0x${ra.toString(16)}`,
    `sp=0x${sp.toString(16)}`,
    `a0=${a0.toString()}`,
    `a1=${a1.toString()}`,
    `sig_pending=0x${sigPending.toString(16)}`,
  ];
  const syscallNr = handle.kernel.debug_syscall_trace_nr?.();
  if (syscallNr !== undefined && syscallNr !== 0n) {
    parts.push(`syscall=${syscallNr.toString()}`);
    parts.push(`status=${handle.kernel.debug_syscall_trace_status?.() ?? "n/a"}`);
    parts.push(`result=${handle.kernel.debug_syscall_trace_result?.().toString() ?? "n/a"}`);
  }
  const signalDeliverySig = handle.kernel.debug_signal_delivery_sig?.();
  if (signalDeliverySig !== undefined && signalDeliverySig !== 0) {
    parts.push(`sig_deliver=${signalDeliverySig}`);
    parts.push(`sig_pc=0x${(handle.kernel.debug_signal_delivery_pc?.() ?? 0n).toString(16)}`);
  }
  return parts.join(" ");
}

export function debugRuntimeProcessHandles(
  runtimeId: number,
  processHandles: Map<number, ProcessHandle>,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
  runtimeLifecycleEvents: Map<number, string[]>,
): string {
  const lines = [...processHandles.values()]
    .filter((handle) => handle.runtimeId === runtimeId)
    .sort((left, right) => left.pid - right.pid)
    .map((handle) => {
      const processPtr = handle.kernel.thread_process_ptr(handle.vmPtr);
      const blocked = [...handle.blockedWorkers.values()]
        .map((state) => `${state.status}@0x${state.vmPtr.toString(16)}`)
        .join(",");
      const attachedWorkerVmPtrs = [...handle.workers]
        .map((worker) => threadWorkerStates.get(worker)?.vmPtr ?? 0)
        .filter((vmPtr) => vmPtr !== 0)
        .map((vmPtr) => `0x${vmPtr.toString(16)}`)
        .join(",");
      const workerStates = [...handle.workers]
        .map((worker) => {
          const state = threadWorkerStates.get(worker);
          if (!state) {
            return null;
          }
          let ownerState = "idle";
          if (handle.runningWorker === worker) {
            ownerState = "running";
          } else if (handle.readyWorkers.includes(worker)) {
            ownerState = "ready";
          } else if (handle.blockedWorkers.has(worker)) {
            ownerState = "blocked";
          } else if (state.waitingInWorkerStatus !== null) {
            ownerState = `wait:${state.waitingInWorkerStatus}`;
          } else if (!state.initialized) {
            ownerState = "init";
          }
          return `0x${state.vmPtr.toString(16)}:${ownerState}`;
        })
        .filter(Boolean)
        .join(",");
      const liveThreadVmPtrs = Array.from({ length: handle.kernel.abi_max_threads() }, (_value, slot) =>
        handle.kernel.process_thread_vm_ptr(handle.vmPtr, slot))
        .filter((vmPtr) => vmPtr !== 0)
        .map((vmPtr) => `0x${vmPtr.toString(16)}`)
        .join(",");
      const threadStates = Array.from({ length: handle.kernel.abi_max_threads() }, (_value, slot) => {
        const threadVmPtr = handle.kernel.process_thread_vm_ptr(handle.vmPtr, slot);
        if (threadVmPtr === 0) {
          return null;
        }
        return `${slot}:${handle.kernel.process_thread_tid(handle.vmPtr, slot)}/${handle.kernel.process_thread_state(handle.vmPtr, slot)}/0x${threadVmPtr.toString(16)}`;
      }).filter(Boolean).join(",");
      const fdSummary = Array.from({ length: 16 }, (_, fd) => {
        const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
        if (ofdId === 0) {
          return null;
        }
        return `${fd}->${ofdId}/${handle.kernel.process_fd_flags(handle.vmPtr, fd)}/${handle.kernel.kernel_ofd_fd_type(ofdId)}/${handle.kernel.kernel_ofd_ino(ofdId)}/${handle.kernel.kernel_ofd_host_fd(ofdId)}`;
      }).filter(Boolean).join(" ");
      const ofdSummary = Array.from({ length: 16 }, (_value, ofdId) => {
        if (ofdId === 0) {
          return null;
        }
        const type = handle.kernel.kernel_ofd_fd_type(ofdId);
        if (type === 0) {
          return null;
        }
        return `${ofdId}:${type}/${handle.kernel.kernel_ofd_ino(ofdId)}/${handle.kernel.kernel_ofd_host_fd(ofdId)}/rc=${handle.kernel.kernel_ofd_refcount(ofdId)}`;
      }).filter(Boolean).join(" ");
      let procSelfExe = "unavailable";
      try {
        const pathBytes = encoder.encode("/proc/self/exe");
        const scratchBase = handle.kernel.kernel_scratch_base?.();
        if (scratchBase === undefined) {
          throw new Error("kernel_scratch_base unavailable");
        }
        const pathPtr = scratchBase;
        const bufPtr = scratchBase + 0x200;
        new Uint8Array(handle.kernel.memory.buffer, pathPtr, pathBytes.length).set(pathBytes);
        const ino = handle.kernel.memfs_resolve_path(pathPtr, pathBytes.length);
        if (ino === 0 || (handle.kernel.memfs_inode_mode(ino) & 0o170000) !== 0o120000) {
          procSelfExe = "missing";
        } else {
          const len = Number(handle.kernel.memfs_read_link(ino, bufPtr, 1024));
          if (len <= 0) {
            procSelfExe = "empty";
          } else {
            procSelfExe = JSON.stringify(
              decoder.decode(new Uint8Array(handle.kernel.memory.buffer.slice(bufPtr, bufPtr + len))),
            );
          }
        }
      } catch {
        procSelfExe = "unavailable";
      }
      const runningState = handle.runningWorker ? threadWorkerStates.get(handle.runningWorker) : undefined;
      const runningVmPtr = runningState?.vmPtr ?? 0;
      const runningDebug = runningVmPtr !== 0 ? formatThreadDebug(handle, runningVmPtr) : "n/a";
      const allThreadDebug = Array.from({ length: handle.kernel.abi_max_threads() }, (_value, slot) => {
        const threadVmPtr = handle.kernel.process_thread_vm_ptr(handle.vmPtr, slot);
        if (threadVmPtr === 0) {
          return null;
        }
        const tid = handle.kernel.process_thread_tid(handle.vmPtr, slot);
        return `${tid}:${JSON.stringify(formatThreadDebug(handle, threadVmPtr))}`;
      }).filter(Boolean).join(" ");
      return [
        `pid=${handle.pid}`,
        `cmd=${JSON.stringify(handle.debugCommand)}`,
        `ppid=${handle.parentPid ?? 0}`,
        `vm=0x${handle.vmPtr.toString(16)}`,
        `process=0x${processPtr.toString(16)}`,
        `workers=${handle.workers.size}`,
        `worker_vms=[${attachedWorkerVmPtrs}]`,
        `worker_states=[${workerStates}]`,
        `thread_vms=[${liveThreadVmPtrs}]`,
        `thread_states=[${threadStates}]`,
        `ready=${handle.readyWorkers.length}`,
        `running=${handle.runningWorker ? "1" : "0"}`,
        `blocked=[${blocked}]`,
        `vfork=${handle.vforkSnapshot ? "1" : "0"}`,
        `stdio_pipes=${handle.stdinPipe}/${handle.stdoutPipe}/${handle.stderrPipe}`,
        `shared_ofds=[${[...handle.kernelShare.sharedOfdIds].sort((left, right) => left - right).join(",")}]`,
        `ofd_table=0x${handle.kernelState.ofdTablePtr.toString(16)}`,
        `memfs=0x${handle.kernelState.memfsPtr.toString(16)}`,
        `pipe=0x${handle.kernelState.pipeBase.toString(16)}`,
        `process_runtime=0x${handle.kernelState.processRuntimeStatePtr.toString(16)}`,
        `worker_log=[${handle.workerDebugLog.join(", ")}]`,
        `fds=[${fdSummary}]`,
        `ofds=[${ofdSummary}]`,
        `proc_self_exe=${procSelfExe}`,
        `running_vm=${runningVmPtr === 0 ? "0x0" : `0x${runningVmPtr.toString(16)}`}`,
        `running_debug=${JSON.stringify(runningDebug)}`,
        `thread_debugs=[${allThreadDebug}]`,
        `debug=${handle.lastDebugMessage ?? "n/a"}`,
      ].join(" ");
    });
  const lifecycle = runtimeLifecycleEvents.get(runtimeId);
  if (lifecycle && lifecycle.length > 0) {
    lines.push(`lifecycle=[${lifecycle.join(", ")}]`);
  }
  return lines.join("\n");
}

export function appendRuntimeLifecycleEvent(
  runtimeId: number,
  event: string,
  runtimeLifecycleEvents: Map<number, string[]>,
): void {
  const events = runtimeLifecycleEvents.get(runtimeId) ?? [];
  events.push(event);
  if (events.length > 64) {
    events.splice(0, events.length - 64);
  }
  runtimeLifecycleEvents.set(runtimeId, events);
}

export function formatAllLiveThreadDebug(
  handle: ProcessHandle,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
): string {
  const vmPtrs = new Set<number>();
  if (handle.vmPtr !== 0) {
    vmPtrs.add(handle.vmPtr);
  }
  vmPtrs.add(threadWorkerStates.get(handle.worker)?.vmPtr ?? 0);
  for (const worker of handle.workers) {
    vmPtrs.add(threadWorkerStates.get(worker)?.vmPtr ?? 0);
  }
  return [...vmPtrs]
    .filter((vmPtr) => vmPtr !== 0)
    .map((vmPtr) => {
      const tid = handle.kernel.thread_tid?.(vmPtr) ?? 0;
      return `${tid}:${JSON.stringify(formatThreadDebug(handle, vmPtr))}`;
    })
    .join(" ");
}

export function assertProcessStandardFdIntegrity(
  handle: ProcessHandle,
  context: string,
  debugSnapshot: string,
  formatPipeCounterDebug: (handle: ProcessHandle, fd: number) => string,
): void {
  if (handle.pid <= 0) {
    return;
  }
  for (const fd of [0, 1, 2]) {
    const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
    if (ofdId === 0) {
      continue;
    }
    const type = handle.kernel.kernel_ofd_fd_type(ofdId);
    if (type <= 0 || type > 64) {
      throw new Error(
        `${context}: pid=${handle.pid} fd=${fd} ofd=${ofdId} type=${type}\n${debugSnapshot}`,
      );
    }
    if (type !== handle.kernel.abi_fd_type_pipe()) {
      continue;
    }
    const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
    const base = handle.kernelState.pipeBase + pipeSlot * (32 + 4096);
    const i32 = new Int32Array(handle.memory.buffer);
    const readOpen = Atomics.load(i32, (base + 8) >>> 2);
    const writeOpen = Atomics.load(i32, (base + 12) >>> 2);
    if (readOpen >= 0 && writeOpen >= 0) {
      continue;
    }
    throw new Error(
      `${context}: pid=${handle.pid} fd=${fd} ofd=${ofdId} pipe=${pipeSlot} ro=${readOpen} wo=${writeOpen}\n`
      + `${formatPipeCounterDebug(handle, fd)}\n`
      + debugSnapshot,
    );
  }
}

export function collectRuntimeKernelShareRepresentatives(
  runtimeId: number,
  processHandles: Map<number, ProcessHandle>,
  kernelShareRepresentative: (handle: ProcessHandle) => ProcessHandle | null,
): ProcessHandle[] {
  if (runtimeId <= 0) {
    return [];
  }

  const visited = new Set<KernelShareGroup>();
  const representatives: ProcessHandle[] = [];
  for (const handle of processHandles.values()) {
    if (handle.runtimeId !== runtimeId) {
      continue;
    }
    const representative = kernelShareRepresentative(handle) ?? handle;
    const group = representative.kernelShare;
    if (visited.has(group)) {
      continue;
    }
    visited.add(group);
    representatives.push(representative);
  }
  return representatives;
}
