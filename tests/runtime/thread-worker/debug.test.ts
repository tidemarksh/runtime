import { expect, test } from "vite-plus/test";
import { formatWorkerError } from "../../../src/thread-worker/debug.js";

function makeThreadWorkerDebugExports() {
  const memory = new WebAssembly.Memory({ initial: 1 });

  return {
    memory,
    kernel_scratch_base: () => 0x100,
    thread_process_ptr: () => 0,
    thread_process_thread_count: () => 0,
    thread_pc: () => 0x170n,
    thread_sp: () => 0n,
    thread_a0: () => 0n,
    thread_a1: () => 0n,
    thread_sig_pending: () => 0n,
    thread_clone_flags: () => 0n,
    thread_status: () => 0,
    abi_max_fds: () => 0,
    process_fd_ofd_id: () => 0,
    process_fd_flags: () => 0,
    thread_fault_pc: () => 0x250n,
    debug_signal_delivery_sig: () => 11,
    debug_signal_delivery_pc: () => 0xfd7286n,
    debug_signal_delivery_frame_base: () => 0n,
    debug_signal_delivery_ra: () => 0n,
    debug_signal_delivery_sp: () => 0n,
    debug_signal_delivery_s11: () => 0n,
    debug_memory_brk_base: () => 0x200,
    debug_memory_brk_current: () => 0x400,
    debug_memory_mmap_next: () => 0x800,
    debug_memory_vma_len: () => 3,
    debug_memory_vma_start: (index: number) => [0x100, 0x200, 0x400][index] ?? 0,
    debug_memory_vma_end: (index: number) => [0x180, 0x300, 0x500][index] ?? 0,
    debug_memory_vma_prot: (index: number) => [5, 3, 1][index] ?? 0,
    debug_memory_vma_flags: (index: number) => [2, 6, 2][index] ?? 0,
    debug_fd_syscall_trace_len: () => {
      throw new WebAssembly.RuntimeError("operation does not support unaligned accesses");
    },
  } as const;
}

test("formatWorkerError preserves signal delivery evidence when debug collection faults", () => {
  const detail = formatWorkerError(
    makeThreadWorkerDebugExports() as never,
    0x1234,
    0,
    new WebAssembly.RuntimeError("memory access out of bounds"),
  );

  expect(detail).toContain("RuntimeError: memory access out of bounds");
  expect(detail).toContain("sig_deliver=11");
  expect(detail).toContain("sig_pc=0xfd7286");
  expect(detail).toContain("pc_vma=0x100-0x180:5/2");
  expect(detail).toContain("fault_vma=0x200-0x300:3/6");
  expect(detail).toContain('debug_collect_error="RuntimeError: operation does not support unaligned accesses');
});

test("formatWorkerError marks pc and fault pc when no containing vma exists", () => {
  const detail = formatWorkerError(
    {
      ...makeThreadWorkerDebugExports(),
      thread_pc: () => 0x900n,
      thread_fault_pc: () => 0x980n,
    } as never,
    0x1234,
    0,
    new Error("boom"),
  );

  expect(detail).toContain("pc_vma=none");
  expect(detail).toContain("fault_vma=none");
  expect(detail).toContain("pc_vma_prev=0x400-0x500:1/2");
  expect(detail).toContain("pc_vma_next=none");
  expect(detail).toContain("fault_vma_prev=0x400-0x500:1/2");
  expect(detail).toContain("fault_vma_next=none");
});
