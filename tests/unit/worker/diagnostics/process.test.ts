import { expect, test } from "vite-plus/test";
import { debugRuntimeProcessHandles, formatAllLiveThreadDebug } from "../../../../src/worker/diagnostics/debug.js";

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
