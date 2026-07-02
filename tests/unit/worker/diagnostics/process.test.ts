import { expect, test, vi } from "vite-plus/test";
import type { KernelExports } from "../../../../src/abi.js";
import type { ProcessHandle, ThreadWorkerState } from "../../../../src/worker/types.js";
import { debugRuntimeProcessHandles, formatAllLiveThreadDebug } from "../../../../src/worker/diagnostics/debug.js";

function makeDiagnosticsKernel(overrides: Partial<KernelExports> = {}): KernelExports {
  return new Proxy(overrides, {
    get(target, prop) {
      if (prop in target) {
        return target[prop as keyof KernelExports];
      }
      if (typeof prop !== "string") {
        return undefined;
      }
      if (prop.startsWith("abi_max_")) {
        return () => 0;
      }
      if (prop.startsWith("abi_status_")) {
        return () => 0;
      }
      if (prop.endsWith("_len") || prop.endsWith("_count")) {
        return () => 0;
      }
      if (
        prop === "thread_pc"
        || prop === "thread_ra"
        || prop === "thread_sp"
        || prop === "thread_a0"
        || prop === "thread_a1"
        || prop === "thread_reg"
        || prop === "thread_gp"
        || prop === "thread_tp"
        || prop === "thread_s11"
        || prop === "thread_sig_mask"
        || prop === "thread_sig_pending"
      ) {
        return () => 0n;
      }
      if (prop.startsWith("process_") || prop.startsWith("kernel_") || prop.startsWith("thread_")) {
        return () => 0;
      }
      if (prop.startsWith("debug_")) {
        return () => 0;
      }
      return undefined;
    },
  }) as KernelExports;
}

test("debugRuntimeProcessHandles keeps riscv syscall trace when process formatting throws", () => {
  const handle = {
    pid: 1,
    runtimeId: 7,
    debugCommand: "/jdk/bin/java -version",
    vmPtr: 0x1234,
    workerDebugLog: [],
    kernel: {
      thread_process_ptr: () => {
        throw new WebAssembly.RuntimeError("memory access out of bounds");
      },
      debug_exec_trace_len: () => 0,
      debug_syscall_trace_len: () => 0,
      debug_path_syscall_trace_len: () => 0,
      debug_riscv_syscall_trace_len: () => 2,
      debug_riscv_syscall_trace_entry_vm_ptr: (index: number) => [0x1234, 0x1234][index] ?? 0,
      debug_riscv_syscall_trace_entry_nr: (index: number) => [258n, 259n][index] ?? 0n,
      debug_riscv_syscall_trace_entry_arg0: (index: number) => [0x10n, 0x20n][index] ?? 0n,
      debug_riscv_syscall_trace_entry_arg1: (index: number) => [0x11n, 0x21n][index] ?? 0n,
      debug_riscv_syscall_trace_entry_arg2: (index: number) => [0x12n, 0x22n][index] ?? 0n,
      debug_riscv_syscall_trace_entry_arg3: (index: number) => [0x13n, 0x23n][index] ?? 0n,
      debug_riscv_syscall_trace_entry_arg4: (index: number) => [0x14n, 0x24n][index] ?? 0n,
      debug_riscv_syscall_trace_entry_arg5: (index: number) => [0x15n, 0x25n][index] ?? 0n,
      debug_riscv_syscall_trace_entry_result: (index: number) => [0n, -1n][index] ?? 0n,
      debug_riscv_syscall_trace_entry_status: (index: number) => [0, 3][index] ?? 0,
    },
  } as never;

  const snapshot = debugRuntimeProcessHandles(
    7,
    new Map([[1, handle]]),
    new WeakMap(),
    new Map(),
    () => "",
  );

  expect(snapshot).toContain("debug_error=");
  expect(snapshot).toContain("riscv_syscalls=[");
  expect(snapshot).toContain("nr=258");
  expect(snapshot).toContain("nr=259");
});

test("debugRuntimeProcessHandles includes running worker scheduler timing details", () => {
  vi.setSystemTime(new Date(20_000));
  const worker = {} as Worker;
  const thread: ThreadWorkerState = {
    vmPtr: 0x2000,
    ownerPid: 1,
    generation: 3,
    runningGeneration: 3,
    reinitCount: 0,
    stepCount: 7,
    totalStepBudget: 0,
    totalStepWallMs: 0,
    maxStepWallMs: 0,
    lastStepDurationMs: null,
    statusCount: 6,
    statusCounts: {},
    lastStatus: 0,
    lastStepAtMs: 17_500,
    lastStatusAtMs: 16_000,
    lastStepProcessPtr: null,
    lastStepPc: 0x1234n,
    lastStepSp: null,
    lastStepS11: null,
    hotStepPcs: {},
    lastStatusPc: 0x5678n,
    lastStatusSp: null,
    lastStatusS11: null,
    forkStackSnapshot: undefined,
    execveProcessState: undefined,
    hotStatusPcs: {},
    initialized: true,
    waitingInWorkerStatus: null,
    ownerAsyncStatus: null,
    resumeHostBlockedSyscallOnNextStep: false,
    pendingPipeSlotsForNextStep: [],
    pendingGuestMemoryWrites: [],
    resumeWorkersOnReady: [],
  };
  const handle = {
    pid: 1,
    runtimeId: 7,
    debugCommand: "/usr/bin/rustc",
    parentPid: null,
    vmPtr: 0x2000,
    workers: new Set([worker]),
    runningWorker: worker,
    vforkReadyWorkers: [],
    hostBlockedWorkers: new Map(),
    workerDebugLog: [],
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(16),
      stateSab: new SharedArrayBuffer(16),
    },
    kernelShare: {
      sharedOfdIds: new Set(),
      forkSharedSyncPromise: null,
    },
    networkConnections: new Map(),
    networkClosedFds: new Set(),
    pipeSyncPromise: null,
    pipeSyncQueued: false,
    forkSharedSyncPromise: null,
    vforkSnapshot: null,
    stdinPipe: 0,
    stdoutPipe: 1,
    stderrPipe: 2,
    lastDebugMessage: null,
    kernelState: {
      netBufBase: 0,
    },
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    kernel: makeDiagnosticsKernel({
      debug_thread_step_vm_ptr: () => 0x2000,
      thread_process_ptr: () => 0x3000,
      process_current_pid: () => 1,
      thread_tid: () => 11,
      thread_status: () => 0,
      process_stdin_pipe: () => 0,
      process_stdout_pipe: () => 1,
      process_stderr_pipe: () => 2,
    }),
  } as ProcessHandle;

  const snapshot = debugRuntimeProcessHandles(
    7,
    new Map([[1, handle]]),
    new WeakMap([[worker, thread]]),
    new Map(),
    () => "",
  );

  expect(snapshot).toContain("worker_details=[0x2000:owner=running");
  expect(snapshot).toContain("unansweredStep=1");
  expect(snapshot).toContain("lastStepAgeMs=2500");
  expect(snapshot).toContain("lastStatusAgeMs=4000");
  expect(snapshot).toContain("activeStepVm=0x2000");
  expect(snapshot).toContain("activeStepSelf=1");
});

test("formatAllLiveThreadDebug keeps execve summary without investigation traces", () => {
  const worker = {} as Worker;
  const handle = {
    worker,
    vmPtr: 0x1234,
    workers: new Set([worker]),
    kernel: {
      thread_tid: () => 11,
      thread_pc: () => 0x2000n,
      thread_ra: () => 0x3000n,
      thread_sp: () => 0x4000n,
      thread_a0: () => 1n,
      thread_a1: () => 2n,
      thread_exit_code: () => 0,
      thread_term_signal: () => 0,
      thread_fault_pc: () => 0n,
      thread_fault_addr: () => 0n,
      thread_sig_pending: () => 0n,
      thread_clone_flags: () => 0n,
      thread_clone_child_tidptr: () => 0n,
      thread_spawn_kind: () => 0,
      debug_syscall_trace_nr: () => 0n,
      debug_signal_delivery_sig: () => 0,
      debug_execve_stage: () => 12,
      debug_execve_value0: () => 0x55n,
      debug_execve_value1: () => 0x66n,
    },
  } as never;

  const out = formatAllLiveThreadDebug(handle, new WeakMap([[worker, { vmPtr: 0x1234 } as never]]));

  expect(out).toContain("execve_stage=12");
  expect(out).toContain("execve_v0=85");
  expect(out).toContain("execve_v1=102");
  expect(out).not.toContain("execve_approach_trace=");
  expect(out).not.toContain("execve_dispatch_trace=");
});
