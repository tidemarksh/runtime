import { expect, test, vi } from "vite-plus/test";
import type { KernelExports } from "../../../../src/abi.js";
import type { ProcessHandle, ThreadWorkerState } from "../../../../src/worker/types.js";
import { debugRuntimeProcessHandles, formatAllLiveThreadDebug } from "../../../../src/worker/diagnostics/debug.js";
import {
  setProcessDebugCommand,
  setProcessLastDebugMessage,
} from "../../../../src/worker/process-debug-state.js";
import { getThreadActivityState } from "../../../../src/worker/thread-activity-state.js";
import {
  setThreadHostIdentity,
  setThreadInitialized,
  setThreadOwnerAsyncStatus,
  setThreadReinitCount,
  setThreadRunningGeneration,
} from "../../../../src/worker/thread-host-state.js";
import { seedProcessHandleStateFixture } from "../support/process-handle-state-fixture.js";

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
  setProcessDebugCommand(handle, "/jdk/bin/java -version");

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
    hotStatusPcs: {},
    waitingInWorkerStatus: null,
    ownerAsyncStatus: null,
  };
  Object.assign(getThreadActivityState(thread), {
    stepCount: 7,
    statusCount: 6,
    lastStatus: 0,
    lastStepAtMs: 17_500,
    lastStatusAtMs: 16_000,
    lastStepPc: 0x1234n,
    lastStatusPc: 0x5678n,
  });
  setThreadHostIdentity(thread, 1, 3);
  setThreadInitialized(thread, true);
  setThreadReinitCount(thread, 0);
  setThreadRunningGeneration(thread, 3);
  const handle = {
    pid: 1,
    runtimeId: 7,
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
    },
    vforkSnapshot: null,
    stdinPipe: 0,
    stdoutPipe: 1,
    stderrPipe: 2,
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
  setProcessDebugCommand(handle, "/usr/bin/rustc");
  setProcessLastDebugMessage(handle, null);
  seedProcessHandleStateFixture(handle);

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

test("debugRuntimeProcessHandles includes owner async phase and age", () => {
  vi.setSystemTime(new Date(20_000));
  const worker = {} as Worker;
  const thread = { vmPtr: 0x2000 } as ThreadWorkerState;
  setThreadHostIdentity(thread, 1, 3);
  setThreadInitialized(thread, true);
  vi.setSystemTime(new Date(17_500));
  setThreadOwnerAsyncStatus(thread, 0, "ok-sync:lifecycle");
  vi.setSystemTime(new Date(20_000));
  const handle = {
    pid: 1,
    runtimeId: 7,
    parentPid: null,
    vmPtr: 0x2000,
    workers: new Set([worker]),
    runningWorker: null,
    vforkReadyWorkers: [],
    hostBlockedWorkers: new Map(),
    workerDebugLog: [],
    runtimeBridge: { notifySab: new SharedArrayBuffer(16), stateSab: new SharedArrayBuffer(16) },
    kernelShare: { sharedOfdIds: new Set() },
    vforkSnapshot: null,
    stdinPipe: 0,
    stdoutPipe: 1,
    stderrPipe: 2,
    kernelState: { netBufBase: 0 },
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    kernel: makeDiagnosticsKernel({
      thread_process_ptr: () => 0x3000,
      process_current_pid: () => 1,
      thread_tid: () => 1,
      thread_status: () => 0,
    }),
  } as ProcessHandle;
  seedProcessHandleStateFixture(handle);

  const snapshot = debugRuntimeProcessHandles(
    7,
    new Map([[1, handle]]),
    new WeakMap([[worker, thread]]),
    new Map(),
    () => "",
  );

  expect(snapshot).toContain("owner=async:0:ok-sync:lifecycle");
  expect(snapshot).toContain("asyncDetail=ok-sync:lifecycle");
  expect(snapshot).toContain("asyncAgeMs=2500");
});

test("debugRuntimeProcessHandles decodes fixed-length path syscall trace entries", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  new Uint8Array(memory.buffer).set(new TextEncoder().encode("/proc/self/exe"), 0x1200);
  const handle = {
    pid: 1,
    runtimeId: 7,
    parentPid: null,
    vmPtr: 0x2000,
    workers: new Set<Worker>(),
    runningWorker: null,
    vforkReadyWorkers: [],
    hostBlockedWorkers: new Map(),
    workerDebugLog: [],
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(16),
      stateSab: new SharedArrayBuffer(16),
    },
    kernelShare: {
      sharedOfdIds: new Set(),
    },
    vforkSnapshot: null,
    stdinPipe: 0,
    stdoutPipe: 1,
    stderrPipe: 2,
    kernelState: {
      netBufBase: 0,
    },
    guestBase: 0,
    memory,
    kernel: makeDiagnosticsKernel({
      thread_process_ptr: () => 0x3000,
      process_current_pid: () => 1,
      process_stdin_pipe: () => 0,
      process_stdout_pipe: () => 1,
      process_stderr_pipe: () => 2,
      debug_path_syscall_trace_len: () => 1,
      debug_path_syscall_trace_vm_ptr: () => 0x2000,
      debug_path_syscall_trace_nr: () => 78n,
      debug_path_syscall_trace_dirfd: () => -100n,
      debug_path_syscall_trace_status: () => 0,
      debug_path_syscall_trace_result: () => -22n,
      debug_path_syscall_trace_path_ptr: () => 0x1200,
      debug_path_syscall_trace_path_len: () => "/proc/self/exe".length,
    }),
  } as ProcessHandle;
  setProcessDebugCommand(handle, "/usr/bin/ruby3.3");
  setProcessLastDebugMessage(handle, null);
  seedProcessHandleStateFixture(handle);

  const snapshot = debugRuntimeProcessHandles(
    7,
    new Map([[1, handle]]),
    new WeakMap(),
    new Map(),
    () => "",
  );

  expect(snapshot).toContain("path_syscalls=[");
  expect(snapshot).toContain("nr=78");
  expect(snapshot).toContain("path_len=14");
  expect(snapshot).toContain('path="/proc/self/exe"');
});

test("debugRuntimeProcessHandles decodes fd trace path arguments", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  new Uint8Array(memory.buffer).set(new TextEncoder().encode("/usr/lib/ruby/site_ruby\0"), 0x1300);
  const handle = {
    pid: 1,
    runtimeId: 7,
    parentPid: null,
    vmPtr: 0x2000,
    workers: new Set<Worker>(),
    runningWorker: null,
    vforkReadyWorkers: [],
    hostBlockedWorkers: new Map(),
    workerDebugLog: [],
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(16),
      stateSab: new SharedArrayBuffer(16),
    },
    kernelShare: {
      sharedOfdIds: new Set(),
    },
    vforkSnapshot: null,
    stdinPipe: 0,
    stdoutPipe: 1,
    stderrPipe: 2,
    kernelState: {
      netBufBase: 0,
    },
    guestBase: 0,
    memory,
    kernel: makeDiagnosticsKernel({
      thread_process_ptr: () => 0x3000,
      process_current_pid: () => 1,
      process_stdin_pipe: () => 0,
      process_stdout_pipe: () => 1,
      process_stderr_pipe: () => 2,
      debug_fd_syscall_trace_len: () => 1,
      debug_fd_syscall_trace_vm_ptr: () => 0x2000,
      debug_fd_syscall_trace_nr: () => 79n,
      debug_fd_syscall_trace_arg0: () => -100n,
      debug_fd_syscall_trace_arg1: () => 0x1300n,
      debug_fd_syscall_trace_arg2: () => 0x4000n,
      debug_fd_syscall_trace_arg3: () => 0n,
      debug_fd_syscall_trace_result: () => -2n,
      debug_fd_syscall_trace_status: () => 0,
    }),
  } as ProcessHandle;
  setProcessDebugCommand(handle, "/usr/bin/ruby3.3");
  setProcessLastDebugMessage(handle, null);
  seedProcessHandleStateFixture(handle);

  const snapshot = debugRuntimeProcessHandles(
    7,
    new Map([[1, handle]]),
    new WeakMap(),
    new Map(),
    () => "",
  );

  expect(snapshot).toContain("fd_syscalls=[");
  expect(snapshot).toContain("nr=79");
  expect(snapshot).toContain('path="/usr/lib/ruby/site_ruby"');
});

test("formatAllLiveThreadDebug keeps execve summary without investigation traces", () => {
  const worker = {} as Worker;
  const handle = {
    worker,
    vmPtr: 0x1234,
    workers: new Set([worker]),
    guestBase: 0,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
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
  seedProcessHandleStateFixture(handle);

  const out = formatAllLiveThreadDebug(handle, new WeakMap([[worker, { vmPtr: 0x1234 } as never]]));

  expect(out).toContain("execve_stage=12");
  expect(out).toContain("execve_v0=85");
  expect(out).toContain("execve_v1=102");
  expect(out).not.toContain("execve_approach_trace=");
  expect(out).not.toContain("execve_dispatch_trace=");
});
