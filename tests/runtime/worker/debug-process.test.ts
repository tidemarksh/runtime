import { expect, test } from "vite-plus/test";
import { debugRuntimeProcessHandles } from "../../../src/worker/debug.js";

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
      debug_execve_approach_trace_len: () => 0,
      debug_execve_dispatch_trace_len: () => 0,
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
