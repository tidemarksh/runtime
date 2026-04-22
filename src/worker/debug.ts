import type {
  ProcessHandle,
  ThreadWorkerState,
} from "./types.js";

export interface WorkerLogFocus {
  vmPtr: number;
  ownerState: string;
  waitingInWorkerStatus: number | null;
  threadState: string | null;
  lastCredibleState: string | null;
  lastStatus: string | null;
  lastStatusDebug: string | null;
  lastReady: string | null;
  lastReadyDebug: string | null;
  lastStep: string | null;
  lastWait20: string | null;
  lastResume20: string | null;
  lastWrite: string | null;
  lastSleep: string | null;
  lastYield: string | null;
  lastFaultDebug: string | null;
  execTrace: string | null;
  eventTail: string[];
  statusTail: string[];
}

export interface RuntimeProcessWorkerLogFocus {
  pid: number;
  cmd: string;
  workers: WorkerLogFocus[];
}

const decoder = new TextDecoder();

function formatU64(value: bigint): string {
  return BigInt.asUintN(64, value).toString(16);
}

function readWorkerOwnerState(
  handle: Pick<ProcessHandle, "runningWorker" | "readyWorkers" | "blockedWorkers">,
  worker: Worker,
  state: ThreadWorkerState,
): string {
  if (handle.runningWorker === worker) {
    return "running";
  }
  if (handle.readyWorkers.includes(worker)) {
    return "ready";
  }
  if (handle.blockedWorkers.has(worker)) {
    return "blocked";
  }
  if (state.waitingInWorkerStatus !== null) {
    return `wait:${state.waitingInWorkerStatus}`;
  }
  if (!state.initialized) {
    return "init";
  }
  return "idle";
}

function findLastWorkerLogEntry(
  workerDebugLog: readonly string[],
  prefix: string,
  vmPtr: number,
): string | null {
  const vmTag = `0x${vmPtr.toString(16)}`;
  for (let index = workerDebugLog.length - 1; index >= 0; index -= 1) {
    const entry = workerDebugLog[index];
    if (entry?.startsWith(`${prefix}:${vmTag}`)) {
      return entry;
    }
  }
  return null;
}

function collectWorkerEventTail(
  workerDebugLog: readonly string[],
  vmPtr: number,
): string[] {
  const vmTag = `0x${vmPtr.toString(16)}`;
  const prefixes = [
    "status",
    "ready",
    "step",
    "wait",
    "resume",
    "write",
    "sleep",
    "yield",
    "fault_debug",
  ];
  const out: string[] = [];
  for (let index = workerDebugLog.length - 1; index >= 0; index -= 1) {
    const entry = workerDebugLog[index];
    if (!entry?.includes(vmTag)) {
      continue;
    }
    if (!prefixes.some((prefix) => entry.startsWith(`${prefix}:${vmTag}`))) {
      continue;
    }
    if (entry.startsWith(`write:${vmTag}`) && !entry.includes("write_data=")) {
      continue;
    }
    out.push(entry);
    if (out.length >= 12) {
      break;
    }
  }
  return out.reverse();
}

function collectWorkerStatusTail(
  workerDebugLog: readonly string[],
  vmPtr: number,
): string[] {
  const vmTag = `0x${vmPtr.toString(16)}`;
  const prefixes = [
    "status",
    "status_debug",
    "ready",
    "ready-debug",
    "step",
    "wait",
    "resume",
    "fault_debug",
  ];
  const out: string[] = [];
  for (let index = workerDebugLog.length - 1; index >= 0; index -= 1) {
    const entry = workerDebugLog[index];
    if (!entry?.includes(vmTag)) {
      continue;
    }
    if (!prefixes.some((prefix) => entry.startsWith(`${prefix}:${vmTag}`))) {
      continue;
    }
    out.push(entry);
    if (out.length >= 20) {
      break;
    }
  }
  return out.reverse();
}

function isSuspiciousLowStateWorkerLogEntry(entry: string | null): boolean {
  if (!entry) {
    return false;
  }
  return /(?:^|:)pc=0x[0-9a-f]{1,3}(?::|$)/i.test(entry)
    || /(?:^|:)sp=0x0(?::|$)/.test(entry)
    || /(?:^|:)s11=0x0(?::|$)/.test(entry)
    || /(?:^|[\s:])fault_pc=0x[0-9a-f]{1,3}(?::|[\s]|$)/i.test(entry);
}

function findLastCredibleWorkerState(
  workerDebugLog: readonly string[],
  vmPtr: number,
): string | null {
  const vmTag = `0x${vmPtr.toString(16)}`;
  for (let index = workerDebugLog.length - 1; index >= 0; index -= 1) {
    const entry = workerDebugLog[index];
    if (!entry?.includes(vmTag)) {
      continue;
    }
    if (!(entry.startsWith(`status:${vmTag}`) || entry.startsWith(`ready:${vmTag}`) || entry.startsWith(`step:${vmTag}`))) {
      continue;
    }
    if (!isSuspiciousLowStateWorkerLogEntry(entry)) {
      return entry;
    }
  }
  return null;
}

function collectLoggedWorkerVmPtrs(
  workerDebugLog: readonly string[],
): number[] {
  const vmPtrs = new Set<number>();
  for (const entry of workerDebugLog) {
    const match = entry.match(/^[a-z_-]+:0x([0-9a-f]+)/);
    if (!match) {
      continue;
    }
    const vmPtr = Number.parseInt(match[1] ?? "", 16);
    if (Number.isFinite(vmPtr) && vmPtr !== 0) {
      vmPtrs.add(vmPtr);
    }
  }
  return [...vmPtrs];
}

function formatWorkerExecTrace(
  handle: Pick<ProcessHandle, "kernel">,
  vmPtr: number,
): string | null {
  const len = handle.kernel.debug_exec_trace_len?.() ?? 0;
  if (len <= 0) {
    return null;
  }
  const targetEpoch = handle.kernel.debug_exec_trace_epoch_for_vm?.(vmPtr) ?? 0;
  const entries: string[] = [];
  for (let index = 0; index < len; index += 1) {
    const entryVmPtr = handle.kernel.debug_exec_trace_entry_vm_ptr?.(index) ?? 0;
    if (entryVmPtr !== vmPtr) {
      continue;
    }
    const entryEpoch = handle.kernel.debug_exec_trace_entry_epoch?.(index) ?? 0;
    if (targetEpoch !== 0 && entryEpoch !== targetEpoch) {
      continue;
    }
    const tid = handle.kernel.debug_exec_trace_entry_tid?.(index) ?? 0;
    const pc = handle.kernel.debug_exec_trace_entry_pc?.(index) ?? 0n;
    const raw = handle.kernel.debug_exec_trace_entry_raw?.(index) ?? 0;
    const step = handle.kernel.debug_exec_trace_entry_step?.(index) ?? 0;
    const sp = handle.kernel.debug_exec_trace_entry_sp?.(index) ?? 0n;
    const ra = handle.kernel.debug_exec_trace_entry_ra?.(index) ?? 0n;
    const a0 = handle.kernel.debug_exec_trace_entry_a0?.(index) ?? 0n;
    const s11 = handle.kernel.debug_exec_trace_entry_s11?.(index) ?? 0n;
    entries.push(
      `${index}:tid=${tid}:pc=0x${formatU64(pc)}:raw=0x${raw.toString(16)}:step=${step}:sp=0x${formatU64(sp)}:ra=0x${formatU64(ra)}:a0=0x${formatU64(a0)}:s11=0x${formatU64(s11)}`,
    );
  }
  if (entries.length === 0) {
    const faultLen = handle.kernel.debug_fault_exec_trace_len_for_vm?.(vmPtr) ?? 0;
    if (faultLen <= 0) {
      return null;
    }
    const faultEntries: string[] = [];
    for (let index = 0; index < faultLen; index += 1) {
      const pc = handle.kernel.debug_fault_exec_trace_entry_pc_for_vm?.(vmPtr, index) ?? 0n;
      const raw = handle.kernel.debug_fault_exec_trace_entry_raw_for_vm?.(vmPtr, index) ?? 0;
      const step = handle.kernel.debug_fault_exec_trace_entry_step_for_vm?.(vmPtr, index) ?? 0;
      const sp = handle.kernel.debug_fault_exec_trace_entry_sp_for_vm?.(vmPtr, index) ?? 0n;
      const ra = handle.kernel.debug_fault_exec_trace_entry_ra_for_vm?.(vmPtr, index) ?? 0n;
      const a0 = handle.kernel.debug_fault_exec_trace_entry_a0_for_vm?.(vmPtr, index) ?? 0n;
      const s11 = handle.kernel.debug_fault_exec_trace_entry_s11_for_vm?.(vmPtr, index) ?? 0n;
      faultEntries.push(
        `${index}:pc=0x${formatU64(pc)}:raw=0x${raw.toString(16)}:step=${step}:sp=0x${formatU64(sp)}:ra=0x${formatU64(ra)}:a0=0x${formatU64(a0)}:s11=0x${formatU64(s11)}`,
      );
    }
    return faultEntries.join(" ");
  }
  return entries.slice(-32).join(" ");
}

function formatWorkerThreadState(
  handle: Pick<ProcessHandle, "kernel">,
  vmPtr: number,
): string | null {
  const tid = handle.kernel.thread_tid?.(vmPtr);
  if (tid === undefined) {
    return null;
  }
  const pc = handle.kernel.thread_pc(vmPtr);
  const ra = handle.kernel.thread_ra?.(vmPtr) ?? 0n;
  const sp = handle.kernel.thread_sp(vmPtr);
  const a0 = handle.kernel.thread_a0(vmPtr);
  const a1 = handle.kernel.thread_a1(vmPtr);
  const a2 = handle.kernel.thread_reg?.(vmPtr, 12) ?? 0n;
  const a3 = handle.kernel.thread_reg?.(vmPtr, 13) ?? 0n;
  const gp = handle.kernel.thread_gp?.(vmPtr) ?? 0n;
  const tp = handle.kernel.thread_tp?.(vmPtr) ?? 0n;
  const s11 = handle.kernel.thread_s11?.(vmPtr) ?? 0n;
  const sigMask = handle.kernel.thread_sig_mask?.(vmPtr) ?? 0n;
  const sigPending = handle.kernel.thread_sig_pending(vmPtr);
  const sigCode = handle.kernel.thread_sig_info_code?.(vmPtr) ?? 0;
  return [
    `tid=${tid}`,
    `pc=0x${formatU64(pc)}`,
    `ra=0x${formatU64(ra)}`,
    `sp=0x${formatU64(sp)}`,
    `a0=${a0.toString()}`,
    `a1=${a1.toString()}`,
    `a2=0x${formatU64(a2)}`,
    `a3=0x${formatU64(a3)}`,
    `gp=0x${formatU64(gp)}`,
    `tp=0x${formatU64(tp)}`,
    `s11=0x${formatU64(s11)}`,
    `sig_mask=0x${formatU64(sigMask)}`,
    `sig_pending=0x${formatU64(sigPending)}`,
    `sig_code=${sigCode}`,
  ].join(":");
}

export function summarizeProcessWorkerLogFocus(
  handle: Pick<
    ProcessHandle,
    | "workers"
    | "runningWorker"
    | "readyWorkers"
    | "blockedWorkers"
    | "workerDebugLog"
    | "kernel"
  >,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
): WorkerLogFocus[] {
  const attachedStates = [...handle.workers]
    .map((worker) => {
      const state = threadWorkerStates.get(worker);
      if (!state || state.vmPtr === 0) {
        return null;
      }
      return {
        vmPtr: state.vmPtr,
        ownerState: readWorkerOwnerState(handle, worker, state),
        waitingInWorkerStatus: state.waitingInWorkerStatus,
      };
    })
    .filter((entry): entry is {
      vmPtr: number;
      ownerState: string;
      waitingInWorkerStatus: number | null;
    } => entry !== null);
  const attachedVmPtrs = new Set(attachedStates.map((entry) => entry.vmPtr));
  const detachedStates = collectLoggedWorkerVmPtrs(handle.workerDebugLog)
    .filter((vmPtr) => !attachedVmPtrs.has(vmPtr))
    .map((vmPtr) => ({
      vmPtr,
      ownerState: "detached",
      waitingInWorkerStatus: null,
    }));

  return [...attachedStates, ...detachedStates]
    .map((state) => {
      const eventTail = collectWorkerEventTail(handle.workerDebugLog, state.vmPtr);
      const statusTail = collectWorkerStatusTail(handle.workerDebugLog, state.vmPtr);
      return {
        vmPtr: state.vmPtr,
        ownerState: state.ownerState,
        waitingInWorkerStatus: state.waitingInWorkerStatus,
        threadState: formatWorkerThreadState(handle, state.vmPtr),
        lastCredibleState: findLastCredibleWorkerState(handle.workerDebugLog, state.vmPtr),
        lastStatus: findLastWorkerLogEntry(handle.workerDebugLog, "status", state.vmPtr),
        lastStatusDebug: findLastWorkerLogEntry(handle.workerDebugLog, "status_debug", state.vmPtr),
        lastReady: findLastWorkerLogEntry(handle.workerDebugLog, "ready", state.vmPtr),
        lastReadyDebug: findLastWorkerLogEntry(handle.workerDebugLog, "ready-debug", state.vmPtr),
        lastStep: findLastWorkerLogEntry(handle.workerDebugLog, "step", state.vmPtr),
        lastWait20: findLastWorkerLogEntry(handle.workerDebugLog, "wait", state.vmPtr),
        lastResume20: findLastWorkerLogEntry(handle.workerDebugLog, "resume", state.vmPtr),
        lastWrite: findLastWorkerLogEntry(handle.workerDebugLog, "write", state.vmPtr),
        lastSleep: findLastWorkerLogEntry(handle.workerDebugLog, "sleep", state.vmPtr),
        lastYield: findLastWorkerLogEntry(handle.workerDebugLog, "yield", state.vmPtr),
        lastFaultDebug: findLastWorkerLogEntry(handle.workerDebugLog, "fault_debug", state.vmPtr),
        execTrace: formatWorkerExecTrace(handle, state.vmPtr),
        eventTail,
        statusTail,
      } satisfies WorkerLogFocus;
    })
    .filter((entry): entry is WorkerLogFocus => entry !== null)
    .sort((left, right) => left.vmPtr - right.vmPtr);
}

export function debugRuntimeWorkerLogFocus(
  runtimeId: number,
  processHandles: Map<number, ProcessHandle>,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
): RuntimeProcessWorkerLogFocus[] {
  return [...processHandles.values()]
    .filter((handle) => handle.runtimeId === runtimeId)
    .sort((left, right) => left.pid - right.pid)
    .map((handle) => ({
      pid: handle.pid,
      cmd: handle.debugCommand,
      workers: summarizeProcessWorkerLogFocus(handle, threadWorkerStates),
    }));
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
  const cloneFlags = handle.kernel.thread_clone_flags?.(vmPtr) ?? 0n;
  const cloneChildTidptr = handle.kernel.thread_clone_child_tidptr?.(vmPtr) ?? 0n;
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
    `clone_flags=0x${cloneFlags.toString(16)}`,
    `clone_child_tidptr=0x${cloneChildTidptr.toString(16)}`,
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
  const spawnKind = handle.kernel.thread_spawn_kind?.(vmPtr) ?? 0;
  if (spawnKind !== 0) {
    const spawnPath = handle.kernel.thread_spawn_path?.(vmPtr) ?? 0n;
    const spawnArgv = handle.kernel.thread_spawn_argv?.(vmPtr) ?? 0n;
    const spawnEnvp = handle.kernel.thread_spawn_envp?.(vmPtr) ?? 0n;
    parts.push(`spawn_kind=${spawnKind}`);
    parts.push(`spawn_path=0x${spawnPath.toString(16)}:${JSON.stringify(readGuestCString(handle, spawnPath) ?? "")}`);
    parts.push(`spawn_argv=0x${spawnArgv.toString(16)}:${formatGuestStrvec(handle, spawnArgv, 4)}`);
    parts.push(`spawn_envp=0x${spawnEnvp.toString(16)}:${formatGuestStrvec(handle, spawnEnvp, 4)}`);
  }
  const execveStage = handle.kernel.debug_execve_stage?.() ?? 0;
  if (execveStage !== 0) {
    parts.push(`execve_stage=${execveStage}`);
    parts.push(`execve_v0=${handle.kernel.debug_execve_value0?.().toString() ?? "n/a"}`);
    parts.push(`execve_v1=${handle.kernel.debug_execve_value1?.().toString() ?? "n/a"}`);
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

function readAbsoluteCString(handle: ProcessHandle, ptr: number): string | null {
  if (!Number.isFinite(ptr) || ptr <= 0 || ptr >= handle.memory.buffer.byteLength) {
    return null;
  }
  const bytes = new Uint8Array(handle.memory.buffer);
  const out: number[] = [];
  for (let i = 0; i < 256; i += 1) {
    const value = bytes[ptr + i];
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

function readGuestU64(handle: ProcessHandle, addr: bigint | number): bigint | null {
  const ptr = typeof addr === "bigint" ? Number(addr) : addr;
  if (!Number.isFinite(ptr) || ptr < 0) {
    return null;
  }
  const absolute = handle.guestBase + ptr;
  if (absolute < 0 || absolute + 8 > handle.memory.buffer.byteLength) {
    return null;
  }
  return new DataView(handle.memory.buffer).getBigUint64(absolute, true);
}

function formatGuestStrvec(handle: ProcessHandle, vecAddr: bigint, maxEntries: number): string {
  if (vecAddr === 0n) {
    return "[]";
  }
  const entries: string[] = [];
  for (let index = 0; index < maxEntries; index += 1) {
    const ptr = readGuestU64(handle, vecAddr + BigInt(index * 8));
    if (ptr === null) {
      entries.push(`@${index}=unreadable`);
      break;
    }
    if (ptr === 0n) {
      break;
    }
    entries.push(`@${index}=0x${ptr.toString(16)}:${JSON.stringify(readGuestCString(handle, ptr) ?? "")}`);
  }
  return `[${entries.join(",")}]`;
}

function formatProcessSyscallTrace(handle: ProcessHandle): string {
  const len = handle.kernel.debug_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return "";
  }
  const entries: string[] = [];
  for (let index = 0; index < len; index += 1) {
    const traceVmPtr = handle.kernel.debug_syscall_trace_entry_vm_ptr?.(index) ?? 0;
    const nr = handle.kernel.debug_syscall_trace_entry_nr?.(index);
    const status = handle.kernel.debug_syscall_trace_entry_status?.(index) ?? 0;
    const result = handle.kernel.debug_syscall_trace_entry_result?.(index);
    entries.push(
      `${index}:vm=0x${traceVmPtr.toString(16)}:nr=${nr?.toString() ?? "n/a"}:status=${status}:result=${result?.toString() ?? "n/a"}`,
    );
  }
  return entries.join(" ");
}

function formatProcessRiscvSyscallTrace(handle: ProcessHandle): string {
  const len = handle.kernel.debug_riscv_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return "";
  }
  const entries: string[] = [];
  for (let index = 0; index < len; index += 1) {
    const traceVmPtr = handle.kernel.debug_riscv_syscall_trace_entry_vm_ptr?.(index) ?? 0;
    const nr = handle.kernel.debug_riscv_syscall_trace_entry_nr?.(index);
    const arg0 = handle.kernel.debug_riscv_syscall_trace_entry_arg0?.(index);
    const arg1 = handle.kernel.debug_riscv_syscall_trace_entry_arg1?.(index);
    const arg2 = handle.kernel.debug_riscv_syscall_trace_entry_arg2?.(index);
    const arg3 = handle.kernel.debug_riscv_syscall_trace_entry_arg3?.(index);
    const arg4 = handle.kernel.debug_riscv_syscall_trace_entry_arg4?.(index);
    const arg5 = handle.kernel.debug_riscv_syscall_trace_entry_arg5?.(index);
    const result = handle.kernel.debug_riscv_syscall_trace_entry_result?.(index);
    const status = handle.kernel.debug_riscv_syscall_trace_entry_status?.(index) ?? 0;
    entries.push(
      `${index}:vm=0x${traceVmPtr.toString(16)}:nr=${nr?.toString() ?? "n/a"}`
      + `:a0=${arg0?.toString() ?? "n/a"}:a1=${arg1?.toString() ?? "n/a"}`
      + `:a2=${arg2?.toString() ?? "n/a"}:a3=${arg3?.toString() ?? "n/a"}`
      + `:a4=${arg4?.toString() ?? "n/a"}:a5=${arg5?.toString() ?? "n/a"}`
      + `:status=${status}:result=${result?.toString() ?? "n/a"}`,
    );
  }
  return entries.join(" ");
}

function formatProcessPathTrace(handle: ProcessHandle): string {
  const len = handle.kernel.debug_path_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return "";
  }
  const entries: string[] = [];
  for (let index = 0; index < len; index += 1) {
    const traceVmPtr = handle.kernel.debug_path_syscall_trace_vm_ptr?.(index) ?? 0;
    const nr = handle.kernel.debug_path_syscall_trace_nr?.(index);
    const dirfd = handle.kernel.debug_path_syscall_trace_dirfd?.(index);
    const status = handle.kernel.debug_path_syscall_trace_status?.(index) ?? 0;
    const result = handle.kernel.debug_path_syscall_trace_result?.(index);
    const path = readAbsoluteCString(handle, handle.kernel.debug_path_syscall_trace_path_ptr?.(index) ?? 0);
    entries.push(
      `${index}:vm=0x${traceVmPtr.toString(16)}:nr=${nr?.toString() ?? "n/a"}:dirfd=${dirfd?.toString() ?? "n/a"}:status=${status}:result=${result?.toString() ?? "n/a"}:path=${JSON.stringify(path ?? "")}`,
    );
  }
  return entries.join(" ");
}

export function debugRuntimeProcessHandles(
  runtimeId: number,
  processHandles: Map<number, ProcessHandle>,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
  runtimeLifecycleEvents: Map<number, string[]>,
  formatPipeCounterDebug: (handle: ProcessHandle, fd: number) => string,
): string {
  const lines = [...processHandles.values()]
    .filter((handle) => handle.runtimeId === runtimeId)
    .sort((left, right) => left.pid - right.pid)
    .map((handle) => {
      try {
        const processPtr = handle.kernel.thread_process_ptr(handle.vmPtr);
        const blocked = [...handle.blockedWorkers.values()]
          .map((state) => `${state.status}@0x${state.vmPtr.toString(16)}`)
          .join(",");
        const workerStates = [...handle.workers]
          .map((worker) => {
            const state = threadWorkerStates.get(worker);
            if (!state) {
              return null;
            }
            return `0x${state.vmPtr.toString(16)}:${readWorkerOwnerState(handle, worker, state)}`;
          })
          .filter(Boolean)
          .join(",");
        const workerFocusSummary = summarizeProcessWorkerLogFocus(handle, threadWorkerStates)
          .map((worker) => [
            `0x${worker.vmPtr.toString(16)}`,
            worker.ownerState,
            worker.lastCredibleState ? `credible=${JSON.stringify(worker.lastCredibleState)}` : null,
            worker.lastWait20 ? `wait=${JSON.stringify(worker.lastWait20)}` : null,
            worker.lastResume20 ? `resume=${JSON.stringify(worker.lastResume20)}` : null,
            worker.lastSleep ? `sleep=${JSON.stringify(worker.lastSleep)}` : null,
            worker.lastFaultDebug ? `fault=${JSON.stringify(worker.lastFaultDebug)}` : null,
          ].filter(Boolean).join(":"))
          .join(" ");
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
          return `${fd}->${ofdId}/${handle.kernel.process_fd_flags(handle.vmPtr, fd)}/${handle.kernel.kernel_ofd_fd_type(ofdId)}`;
        }).filter(Boolean).join(" ");

        return [
          `pid=${handle.pid}`,
          `cmd=${JSON.stringify(handle.debugCommand)}`,
          `ppid=${handle.parentPid ?? 0}`,
          `vm=0x${handle.vmPtr.toString(16)}`,
          `process=0x${processPtr.toString(16)}`,
          `workers=${handle.workers.size}`,
          `worker_states=[${workerStates}]`,
          `worker_focus=[${workerFocusSummary}]`,
          `thread_states=[${threadStates}]`,
          `ready=${handle.readyWorkers.length}`,
          `running=${handle.runningWorker ? "1" : "0"}`,
          `blocked=[${blocked}]`,
          `vfork=${handle.vforkSnapshot ? "1" : "0"}`,
          `stdio_pipes=${handle.stdinPipe}/${handle.stdoutPipe}/${handle.stderrPipe}`,
          `stdio_pipe_counters=[${[0, 1, 2, 10]
            .map((fd) => formatPipeCounterDebug(handle, fd))
            .join(" ")}]`,
          `shared_ofds=[${[...handle.kernelShare.sharedOfdIds].sort((left, right) => left - right).join(",")}]`,
          `fds=[${fdSummary}]`,
          `debug=${handle.lastDebugMessage ?? "n/a"}`,
          `syscalls=[${formatProcessSyscallTrace(handle)}]`,
          `riscv_syscalls=[${formatProcessRiscvSyscallTrace(handle)}]`,
          `path_syscalls=[${formatProcessPathTrace(handle)}]`,
        ].join(" ");
      } catch (error) {
        const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
        return [
          `pid=${handle.pid}`,
          `cmd=${JSON.stringify(handle.debugCommand)}`,
          `vm=0x${handle.vmPtr.toString(16)}`,
          `debug_error=${JSON.stringify(detail)}`,
          `syscalls=[${formatProcessSyscallTrace(handle)}]`,
          `riscv_syscalls=[${formatProcessRiscvSyscallTrace(handle)}]`,
          `path_syscalls=[${formatProcessPathTrace(handle)}]`,
        ].join(" ");
      }
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
