import type {
  KernelShareGroup,
  ProcessHandle,
  ThreadWorkerState,
} from "./types.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function debugScratchBase(handle: ProcessHandle): number {
  return (handle.kernel.kernel_scratch_base?.() ?? (handle.kernel.kernel_heap_ptr() + 0x10000));
}

function formatThreadDebug(handle: ProcessHandle, vmPtr: number): string {
  const tid = handle.kernel.thread_tid?.(vmPtr) ?? 0;
  const pc = handle.kernel.thread_pc?.(vmPtr) ?? 0n;
  const ra = handle.kernel.thread_ra?.(vmPtr) ?? 0n;
  const sp = handle.kernel.thread_sp?.(vmPtr) ?? 0n;
  const a0 = handle.kernel.thread_a0?.(vmPtr) ?? 0n;
  const a1 = handle.kernel.thread_a1?.(vmPtr) ?? 0n;
  const exitCode = handle.kernel.thread_exit_code?.(vmPtr) ?? 0;
  const termSignal = handle.kernel.thread_term_signal?.(vmPtr) ?? 0;
  const faultPc = handle.kernel.thread_fault_pc?.(vmPtr) ?? 0n;
  const faultAddr = handle.kernel.thread_fault_addr?.(vmPtr) ?? 0n;
  const sigPending = handle.kernel.thread_sig_pending?.(vmPtr) ?? 0n;
  const parts = [
    `tid=${tid}`,
    `pc=0x${pc.toString(16)}`,
    `ra=0x${ra.toString(16)}`,
    `sp=0x${sp.toString(16)}`,
    `a0=${a0.toString()}`,
    `a1=${a1.toString()}`,
    `exit=${exitCode}`,
    `sig=${termSignal}`,
    `fault_pc=0x${faultPc.toString(16)}`,
    `fault_addr=0x${faultAddr.toString(16)}`,
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

function readGuestCString(handle: ProcessHandle, addr: bigint | number): string | null {
  const ptr = typeof addr === "bigint" ? Number(addr) : addr;
  if (!Number.isFinite(ptr) || ptr <= 0) {
    return null;
  }
  const bytes = new Uint8Array(handle.memory.buffer);
  const out: number[] = [];
  for (let i = 0; i < 256; i += 1) {
    const value = bytes[handle.guestBase + ptr + i];
    if (value === undefined) {
      return null;
    }
    if (value === 0) {
      return decoder.decode(new Uint8Array(out));
    }
    out.push(value);
  }
  return null;
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
      const epollTable = new Uint32Array(
        handle.memory.buffer,
        handle.kernelState.epollTablePtr,
        handle.kernel.kernel_epoll_table_bytes() >>> 2,
      );
      const epollSummary = Array.from({ length: 16 }, (_value, ofdId) => {
        if (ofdId === 0) {
          return null;
        }
        const type = handle.kernel.kernel_ofd_fd_type(ofdId);
        if (type !== handle.kernel.abi_fd_type_epoll()) {
          return null;
        }
        const ino = handle.kernel.kernel_ofd_ino(ofdId);
        const ptr = ino >= 0 && ino < epollTable.length ? epollTable[ino] ?? 0 : 0;
        return `${ofdId}:${ino}->0x${ptr.toString(16)}`;
      }).filter(Boolean).join(" ");
      let procSelfExe = "unavailable";
      try {
        const pathBytes = encoder.encode("/proc/self/exe");
        const scratchBase = debugScratchBase(handle);
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
      const stackWord0 = handle.kernel.thread_stack_word?.(handle.vmPtr, 0) ?? 0n;
      const stackWord1 = handle.kernel.thread_stack_word?.(handle.vmPtr, 1) ?? 0n;
      const stackWord2 = handle.kernel.thread_stack_word?.(handle.vmPtr, 2) ?? 0n;
      const stackWord3 = handle.kernel.thread_stack_word?.(handle.vmPtr, 3) ?? 0n;
      const stackWord4 = handle.kernel.thread_stack_word?.(handle.vmPtr, 4) ?? 0n;
      const stackWord5 = handle.kernel.thread_stack_word?.(handle.vmPtr, 5) ?? 0n;
      const argv0 = readGuestCString(handle, stackWord1);
      const argv1 = readGuestCString(handle, stackWord2);
      const env0 = readGuestCString(handle, stackWord4);
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
        `memfs_data=0x${handle.kernel.kernel_memfs_data_ptr?.().toString(16) ?? "n/a"}`,
        `memfs_data_bytes=${handle.kernel.kernel_memfs_data_bytes?.().toString() ?? "n/a"}`,
        `mem_state=0x${handle.kernelState.memStatePtr.toString(16)}`,
        `pipe=0x${handle.kernelState.pipeBase.toString(16)}`,
        `process_runtime=0x${handle.kernelState.processRuntimeStatePtr.toString(16)}`,
        `mem_brk_base=${handle.kernel.debug_memory_brk_base?.().toString() ?? "n/a"}`,
        `mem_brk_current=${handle.kernel.debug_memory_brk_current?.().toString() ?? "n/a"}`,
        `mem_mmap_next=${handle.kernel.debug_memory_mmap_next?.().toString() ?? "n/a"}`,
        `stack0=${stackWord0.toString()}`,
        `stack1=0x${stackWord1.toString(16)}`,
        `stack2=0x${stackWord2.toString(16)}`,
        `stack3=0x${stackWord3.toString(16)}`,
        `stack4=0x${stackWord4.toString(16)}`,
        `stack5=0x${stackWord5.toString(16)}`,
        `argv0=${argv0 === null ? "n/a" : JSON.stringify(argv0)}`,
        `argv1=${argv1 === null ? "n/a" : JSON.stringify(argv1)}`,
        `env0=${env0 === null ? "n/a" : JSON.stringify(env0)}`,
        `worker_log=[${handle.workerDebugLog.join(", ")}]`,
        `fds=[${fdSummary}]`,
        `ofds=[${ofdSummary}]`,
        `epolls=[${epollSummary}]`,
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
