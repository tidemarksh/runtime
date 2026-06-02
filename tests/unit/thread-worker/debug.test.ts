// Thread-worker tests in this file validate read-only debug observations.
import { expect, test, vi } from "vite-plus/test";
import {
  collectDebugParts,
  formatExecTraceEntries,
  formatWorkerError,
  THREAD_WORKER_DEBUG_OBSERVATION_MODE,
  THREAD_WORKER_SYSCALL_TRACE_CLEAR_MODE,
} from "../../../src/thread-worker/debug.js";

test("debug observations are marked read-only and do not own state repair", () => {
  expect(THREAD_WORKER_DEBUG_OBSERVATION_MODE).toBe("read-only-observation");
});

test("syscall trace clearing is classified as trace lifecycle cleanup", () => {
  expect(THREAD_WORKER_SYSCALL_TRACE_CLEAR_MODE).toBe("trace-lifecycle-cleanup");
});

function makeThreadWorkerDebugExports() {
  const memory = new WebAssembly.Memory({ initial: 1 });

  return {
    memory,
    kernel_scratch_base: () => 0x100,
    thread_process_ptr: () => 0,
    thread_process_thread_count: () => 0,
    thread_pc: () => 0x170n,
    thread_guest_memory_policy_limit: () => 0x2000000n,
    thread_guest_address_profile: () => 1,
    thread_guest_address_limit: () => 0xffff_ffffn,
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

test("debug collection and error formatting do not invoke repair or resume exports", () => {
  const repairExports = {
    thread_step: vi.fn(),
    thread_resume_blocked_syscall: vi.fn(),
    thread_resume_futex_wait: vi.fn(),
    thread_resume_futex_wait_result: vi.fn(),
    thread_resume_epoll_wait_timeout: vi.fn(),
    debug_clear_syscall_trace: vi.fn(),
    debug_clear_signal_trace: vi.fn(),
    debug_clear_cpu_trace: vi.fn(),
  };
  const exports = {
    ...makeThreadWorkerDebugExports(),
    ...repairExports,
    debug_fd_syscall_trace_len: () => 0,
    debug_exec_trace_len: () => 0,
    debug_thread_step_exec_trace_len_for_vm: () => 0,
  } as never;

  collectDebugParts(exports, 0x1234, 0);
  formatWorkerError(exports, 0x1234, 0, new Error("boom"));

  for (const fn of Object.values(repairExports)) {
    expect(fn).not.toHaveBeenCalled();
  }
});

test("formatWorkerError recovers frozen trace when thread state debug faults early", () => {
  const detail = formatWorkerError(
    {
      ...makeThreadWorkerDebugExports(),
      thread_guest_base: () => {
        throw new WebAssembly.RuntimeError("operation does not support unaligned accesses");
      },
      debug_exec_trace_len: () => 0,
      debug_thread_step_exec_trace_len_for_vm: (vmPtr: number) => vmPtr === 0x1234 ? 2 : 0,
      debug_thread_step_exec_trace_entry_pc_for_vm: (_vmPtr: number, index: number) => [0x83336n, 0x83340n][index] ?? 0n,
      debug_thread_step_exec_trace_entry_raw_for_vm: (_vmPtr: number, index: number) => [0x00a00563, 0xfe013283][index] ?? 0,
      debug_thread_step_exec_trace_entry_step_for_vm: (_vmPtr: number, index: number) => [4, 4][index] ?? 0,
      debug_thread_step_exec_trace_entry_sp_for_vm: (_vmPtr: number, index: number) => [0x37d6c000n, 0x37d6c000n][index] ?? 0n,
      debug_thread_step_exec_trace_entry_ra_for_vm: (_vmPtr: number, index: number) => [0x4d9b4n, 0x4d9b4n][index] ?? 0n,
      debug_thread_step_exec_trace_entry_a0_for_vm: (_vmPtr: number, index: number) => [0n, 0n][index] ?? 0n,
      debug_thread_step_exec_trace_entry_s11_for_vm: (_vmPtr: number, index: number) => [0xbfdc80n, 0xbfdc80n][index] ?? 0n,
    } as never,
    0x1234,
    0,
    new WebAssembly.RuntimeError("operation does not support unaligned accesses"),
  );

  expect(detail).toContain("thread_step_exec_trace=[0:pc=0x83336:raw=0xa00563");
  expect(detail).toContain("1:pc=0x83340:raw=0xfe013283");
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

test("collectDebugParts prefers per-vm cpu trace over global last trace", () => {
  const parts = collectDebugParts(
    {
      ...makeThreadWorkerDebugExports(),
      debug_fd_syscall_trace_len: () => 0,
      thread_tid: () => 3,
      thread_gp: () => 0n,
      thread_tp: () => 0n,
      thread_s11: () => 0n,
      thread_sig_mask: () => 0n,
      debug_zero_reg_pc: () => 0x111n,
      debug_zero_reg_tid: () => 1,
      debug_zero_reg_raw: () => 0x1234,
      debug_zero_reg_index: () => 27,
      debug_zero_reg_prev_value: () => 0x55n,
      debug_zero_reg_sp: () => 0x66n,
      debug_zero_reg_s11: () => 0n,
      debug_zero_reg_for_vm_pc: (vmPtr: number) => vmPtr === 0x1234 ? 0x222n : 0n,
      debug_zero_reg_for_vm_tid: (vmPtr: number) => vmPtr === 0x1234 ? 3 : 0,
      debug_zero_reg_for_vm_raw: (vmPtr: number) => vmPtr === 0x1234 ? 0x5678 : 0,
      debug_zero_reg_for_vm_index: (vmPtr: number) => vmPtr === 0x1234 ? 2 : 0,
      debug_zero_reg_for_vm_prev_value: (vmPtr: number) => vmPtr === 0x1234 ? 0x77n : 0n,
      debug_zero_reg_for_vm_sp: (vmPtr: number) => vmPtr === 0x1234 ? 0x88n : 0n,
      debug_zero_reg_for_vm_s11: (vmPtr: number) => vmPtr === 0x1234 ? 0x99n : 0n,
      debug_s11_write_pc: () => 0x333n,
      debug_s11_write_tid: () => 1,
      debug_s11_write_raw: () => 0x2222,
      debug_s11_write_source: () => 1,
      debug_s11_write_prev_value: () => 0x11n,
      debug_s11_write_value: () => 0x22n,
      debug_s11_write_for_vm_pc: (vmPtr: number) => vmPtr === 0x1234 ? 0x444n : 0n,
      debug_s11_write_for_vm_tid: (vmPtr: number) => vmPtr === 0x1234 ? 3 : 0,
      debug_s11_write_for_vm_raw: (vmPtr: number) => vmPtr === 0x1234 ? 0x9999 : 0,
      debug_s11_write_for_vm_source: (vmPtr: number) => vmPtr === 0x1234 ? 2 : 0,
      debug_s11_write_for_vm_prev_value: (vmPtr: number) => vmPtr === 0x1234 ? 0x33n : 0n,
      debug_s11_write_for_vm_value: (vmPtr: number) => vmPtr === 0x1234 ? 0x44n : 0n,
      debug_sp_write_pc: () => 0x555n,
      debug_sp_write_tid: () => 1,
      debug_sp_write_raw: () => 0x3333,
      debug_sp_write_source: () => 1,
      debug_sp_write_prev_value: () => 0x12n,
      debug_sp_write_value: () => 0x34n,
      debug_sp_write_for_vm_pc: (vmPtr: number) => vmPtr === 0x1234 ? 0x666n : 0n,
      debug_sp_write_for_vm_tid: (vmPtr: number) => vmPtr === 0x1234 ? 3 : 0,
      debug_sp_write_for_vm_raw: (vmPtr: number) => vmPtr === 0x1234 ? 0xaaaa : 0,
      debug_sp_write_for_vm_source: (vmPtr: number) => vmPtr === 0x1234 ? 2 : 0,
      debug_sp_write_for_vm_prev_value: (vmPtr: number) => vmPtr === 0x1234 ? 0x56n : 0n,
      debug_sp_write_for_vm_value: (vmPtr: number) => vmPtr === 0x1234 ? 0x78n : 0n,
    } as never,
    0x1234,
    0,
  ).join(" ");

  expect(parts).toContain("zero_reg_tid=3");
  expect(parts).toContain("guest_memory_policy_limit=33554432");
  expect(parts).toContain("zero_reg_pc=0x222");
  expect(parts).toContain("zero_reg_pc_vma=0x200-0x300:3/6");
  expect(parts).toContain("zero_reg_raw=0x5678");
  expect(parts).toContain("s11_write_tid=3");
  expect(parts).toContain("s11_write_pc=0x444");
  expect(parts).toContain("s11_write_pc_vma=0x400-0x500:1/2");
  expect(parts).toContain("sp_write_tid=3");
  expect(parts).toContain("sp_write_pc=0x666");
  expect(parts).toContain("sp_write_pc_vma=none");
  expect(parts).toContain("sp_write_pc_vma_prev=0x400-0x500:1/2");
  expect(parts).toContain("sp_write_pc_vma_next=none");
  expect(parts).not.toContain("zero_reg_pc=0x111");
  expect(parts).not.toContain("s11_write_pc=0x333");
  expect(parts).not.toContain("sp_write_pc=0x555");
});

test("collectDebugParts includes per-vm thread_step return trace", () => {
  const parts = collectDebugParts(
    {
      ...makeThreadWorkerDebugExports(),
      debug_fd_syscall_trace_len: () => 0,
      debug_thread_step_vm_ptr: () => 0xabcd,
      debug_thread_step_pc: () => 0x111n,
      debug_thread_step_tid: () => 1,
      debug_thread_step_status: () => 7,
      debug_thread_step_budget: () => 99,
      debug_thread_step_remaining: () => 77,
      debug_thread_step_ra: () => 0x22n,
      debug_thread_step_sp: () => 0x33n,
      debug_thread_step_s11: () => 0x44n,
      debug_thread_step_for_vm_pc: (vmPtr: number) => vmPtr === 0x1234 ? 0x222n : 0n,
      debug_thread_step_for_vm_tid: (vmPtr: number) => vmPtr === 0x1234 ? 3 : 0,
      debug_thread_step_for_vm_status: (vmPtr: number) => vmPtr === 0x1234 ? 0 : 0,
      debug_thread_step_for_vm_budget: (vmPtr: number) => vmPtr === 0x1234 ? 20000 : 0,
      debug_thread_step_for_vm_remaining: (vmPtr: number) => vmPtr === 0x1234 ? 19999 : 0,
      debug_thread_step_for_vm_ra: (vmPtr: number) => vmPtr === 0x1234 ? 0x55n : 0n,
      debug_thread_step_for_vm_sp: (vmPtr: number) => vmPtr === 0x1234 ? 0x66n : 0n,
      debug_thread_step_for_vm_s11: (vmPtr: number) => vmPtr === 0x1234 ? 0x77n : 0n,
    } as never,
    0x1234,
    0,
  ).join(" ");

  expect(parts).toContain("thread_step_tid=3");
  expect(parts).toContain("thread_step_vm=0x1234");
  expect(parts).toContain("thread_step_status=0");
  expect(parts).toContain("thread_step_budget=20000");
  expect(parts).toContain("thread_step_remaining=19999");
  expect(parts).toContain("thread_step_pc=0x222");
  expect(parts).toContain("thread_step_ra=0x55");
  expect(parts).toContain("thread_step_sp=0x66");
  expect(parts).toContain("thread_step_s11=0x77");
  expect(parts).not.toContain("thread_step_pc=0x111");
});

test("collectDebugParts prefers per-vm jalr trace over global jalr trace", () => {
  const parts = collectDebugParts(
    {
      ...makeThreadWorkerDebugExports(),
      debug_fd_syscall_trace_len: () => 0,
      debug_jalr_pc: () => 0x111n,
      debug_jalr_rs1: () => 1,
      debug_jalr_rd: () => 0,
      debug_jalr_rs1_value: () => 0x222n,
      debug_jalr_target: () => 0x333n,
      debug_jalr_for_vm_pc: (vmPtr: number) => vmPtr === 0x1234 ? 0x444n : 0n,
      debug_jalr_for_vm_rs1: (vmPtr: number) => vmPtr === 0x1234 ? 27 : 0,
      debug_jalr_for_vm_rd: (vmPtr: number) => vmPtr === 0x1234 ? 1 : 0,
      debug_jalr_for_vm_rs1_value: (vmPtr: number) => vmPtr === 0x1234 ? 0x555n : 0n,
      debug_jalr_for_vm_target: (vmPtr: number) => vmPtr === 0x1234 ? 0x0n : 0n,
    } as never,
    0x1234,
    0,
  ).join(" ");

  expect(parts).toContain("jalr_pc=0x444");
  expect(parts).toContain("jalr_rs1=27");
  expect(parts).toContain("jalr_rd=1");
  expect(parts).toContain("jalr_rs1v=0x555");
  expect(parts).toContain("jalr_target=0x0");
  expect(parts).not.toContain("jalr_pc=0x111");
});

test("collectDebugParts includes dedicated riscv syscall trace entries", () => {
  const parts = collectDebugParts(
    {
      ...makeThreadWorkerDebugExports(),
      debug_fd_syscall_trace_len: () => 0,
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
    } as never,
    0x1234,
    0,
  ).join(" ");

  expect(parts).toContain("riscv_syscalls=[");
  expect(parts).toContain("0x1234:258=0/0(0x10,0x11,0x12,0x13,0x14,0x15)");
  expect(parts).toContain("0x1234:259=-1/3(0x20,0x21,0x22,0x23,0x24,0x25)");
});

test("collectDebugParts prefers per-vm signal delivery trace over global trace", () => {
  const parts = collectDebugParts(
    {
      ...makeThreadWorkerDebugExports(),
      debug_fd_syscall_trace_len: () => 0,
      debug_signal_delivery_sig: () => 11,
      debug_signal_delivery_pc: () => 0x4n,
      debug_signal_delivery_frame_base: () => 0x100n,
      debug_signal_delivery_ra: () => 0x200n,
      debug_signal_delivery_sp: () => 0x300n,
      debug_signal_delivery_gp: () => 0x400n,
      debug_signal_delivery_tp: () => 0x500n,
      debug_signal_delivery_s11: () => 0x0n,
      debug_signal_delivery_for_vm_sig: (vmPtr: number) => vmPtr === 0x1234 ? 11 : 0,
      debug_signal_delivery_for_vm_pc: (vmPtr: number) => vmPtr === 0x1234 ? 0x83258n : 0n,
      debug_signal_delivery_for_vm_frame_base: (vmPtr: number) => vmPtr === 0x1234 ? 0x37d61b90n : 0n,
      debug_signal_delivery_for_vm_ra: (vmPtr: number) => vmPtr === 0x1234 ? 0x37d61ff8n : 0n,
      debug_signal_delivery_for_vm_sp: (vmPtr: number) => vmPtr === 0x1234 ? 0x37d61b90n : 0n,
      debug_signal_delivery_for_vm_gp: (vmPtr: number) => vmPtr === 0x1234 ? 0x0n : 0n,
      debug_signal_delivery_for_vm_tp: (vmPtr: number) => vmPtr === 0x1234 ? 0x0n : 0n,
      debug_signal_delivery_for_vm_s11: (vmPtr: number) => vmPtr === 0x1234 ? 0x0n : 0n,
    } as never,
    0x1234,
    0,
  ).join(" ");

  expect(parts).toContain("sig_deliver=11");
  expect(parts).toContain("sig_pc=0x83258");
  expect(parts).toContain("sig_frame=0x37d61b90");
  expect(parts).toContain("sig_ra=0x37d61ff8");
  expect(parts).not.toContain("sig_pc=0x4");
});

test("collectDebugParts includes per-vm thread_step entry trace", () => {
  const parts = collectDebugParts(
    {
      ...makeThreadWorkerDebugExports(),
      debug_fd_syscall_trace_len: () => 0,
      debug_thread_step_entry_for_vm_tid: (vmPtr: number) => vmPtr === 0x1234 ? 3 : 0,
      debug_thread_step_entry_for_vm_status: (vmPtr: number) => vmPtr === 0x1234 ? 20 : 0,
      debug_thread_step_entry_for_vm_budget: (vmPtr: number) => vmPtr === 0x1234 ? 20000 : 0,
      debug_thread_step_entry_for_vm_pc: (vmPtr: number) => vmPtr === 0x1234 ? 0x83336n : 0n,
      debug_thread_step_entry_for_vm_ra: (vmPtr: number) => vmPtr === 0x1234 ? 0x4d9b4n : 0n,
      debug_thread_step_entry_for_vm_sp: (vmPtr: number) => vmPtr === 0x1234 ? 0x37d6c000n : 0n,
      debug_thread_step_entry_for_vm_s11: (vmPtr: number) => vmPtr === 0x1234 ? 0xbfdc80n : 0n,
    } as never,
    0x1234,
    0,
  ).join(" ");

  expect(parts).toContain("thread_step_entry_tid=3");
  expect(parts).toContain("thread_step_entry_status=20");
  expect(parts).toContain("thread_step_entry_budget=20000");
  expect(parts).toContain("thread_step_entry_pc=0x83336");
  expect(parts).toContain("thread_step_entry_ra=0x4d9b4");
  expect(parts).toContain("thread_step_entry_sp=0x37d6c000");
  expect(parts).toContain("thread_step_entry_s11=0xbfdc80");
});

test("collectDebugParts includes per-vm thread_step phase trace", () => {
  const parts = collectDebugParts(
    {
      ...makeThreadWorkerDebugExports(),
      debug_fd_syscall_trace_len: () => 0,
      debug_thread_step_phase_for_vm: (vmPtr: number) => vmPtr === 0x1234 ? 4 : 0,
      debug_thread_step_phase_for_vm_status: (vmPtr: number) => vmPtr === 0x1234 ? 0 : 0,
      debug_thread_step_phase_for_vm_budget: (vmPtr: number) => vmPtr === 0x1234 ? 20000 : 0,
      debug_thread_step_phase_for_vm_remaining: (vmPtr: number) => vmPtr === 0x1234 ? 19999 : 0,
      debug_thread_step_phase_for_vm_pc: (vmPtr: number) => vmPtr === 0x1234 ? 0x4n : 0n,
      debug_thread_step_phase_for_vm_ra: (vmPtr: number) => vmPtr === 0x1234 ? 0x0n : 0n,
      debug_thread_step_phase_for_vm_sp: (vmPtr: number) => vmPtr === 0x1234 ? 0x0n : 0n,
      debug_thread_step_phase_for_vm_s11: (vmPtr: number) => vmPtr === 0x1234 ? 0x0n : 0n,
    } as never,
    0x1234,
    0,
  ).join(" ");

  expect(parts).toContain("thread_step_phase=4");
  expect(parts).toContain("thread_step_phase_status=0");
  expect(parts).toContain("thread_step_phase_budget=20000");
  expect(parts).toContain("thread_step_phase_remaining=19999");
  expect(parts).toContain("thread_step_phase_pc=0x4");
  expect(parts).toContain("thread_step_phase_ra=0x0");
  expect(parts).toContain("thread_step_phase_sp=0x0");
  expect(parts).toContain("thread_step_phase_s11=0x0");
});

test("formatExecTraceEntries keeps only entries for the requested vm", () => {
  const entries = formatExecTraceEntries({
    ...makeThreadWorkerDebugExports(),
    debug_exec_trace_len: () => 3,
    debug_exec_trace_entry_vm_ptr: (index: number) => [0x1, 0x1234, 0x1234][index] ?? 0,
    debug_exec_trace_entry_tid: (index: number) => [1, 3, 3][index] ?? 0,
    debug_exec_trace_entry_pc: (index: number) => [0x10n, 0x20n, 0x24n][index] ?? 0n,
    debug_exec_trace_entry_raw: (index: number) => [0x1, 0x2, 0x3][index] ?? 0,
    debug_exec_trace_entry_step: (index: number) => [1, 2, 4][index] ?? 0,
    debug_exec_trace_entry_sp: (index: number) => [0x30n, 0x40n, 0x44n][index] ?? 0n,
    debug_exec_trace_entry_ra: (index: number) => [0x50n, 0x60n, 0x64n][index] ?? 0n,
    debug_exec_trace_entry_a0: (index: number) => [0x70n, 0x80n, 0x84n][index] ?? 0n,
    debug_exec_trace_entry_s11: (index: number) => [0x90n, 0xa0n, 0xa4n][index] ?? 0n,
  } as never, 0x1234);

  expect(entries).toBe(
    "1:tid=3:pc=0x20:raw=0x2:step=2:sp=0x40:ra=0x60:a0=0x80:s11=0xa0"
    + " 2:tid=3:pc=0x24:raw=0x3:step=4:sp=0x44:ra=0x64:a0=0x84:s11=0xa4",
  );
});

test("formatExecTraceEntries filters to the current vm epoch when available", () => {
  const entries = formatExecTraceEntries({
    ...makeThreadWorkerDebugExports(),
    debug_exec_trace_len: () => 4,
    debug_exec_trace_epoch_for_vm: (vmPtr: number) => vmPtr === 0x1234 ? 2 : 0,
    debug_exec_trace_entry_vm_ptr: (index: number) => [0x1234, 0x1234, 0x1234, 0x1][index] ?? 0,
    debug_exec_trace_entry_epoch: (index: number) => [1, 2, 2, 2][index] ?? 0,
    debug_exec_trace_entry_tid: (index: number) => [3, 3, 3, 1][index] ?? 0,
    debug_exec_trace_entry_pc: (index: number) => [0x10n, 0x20n, 0x24n, 0x30n][index] ?? 0n,
    debug_exec_trace_entry_raw: (index: number) => [0x1, 0x2, 0x3, 0x4][index] ?? 0,
    debug_exec_trace_entry_step: (index: number) => [1, 2, 4, 4][index] ?? 0,
    debug_exec_trace_entry_sp: (index: number) => [0x30n, 0x40n, 0x44n, 0x50n][index] ?? 0n,
    debug_exec_trace_entry_ra: (index: number) => [0x50n, 0x60n, 0x64n, 0x70n][index] ?? 0n,
    debug_exec_trace_entry_a0: (index: number) => [0x70n, 0x80n, 0x84n, 0x90n][index] ?? 0n,
    debug_exec_trace_entry_s11: (index: number) => [0x90n, 0xa0n, 0xa4n, 0xb0n][index] ?? 0n,
  } as never, 0x1234);

  expect(entries).toBe(
    "1:tid=3:pc=0x20:raw=0x2:step=2:sp=0x40:ra=0x60:a0=0x80:s11=0xa0"
    + " 2:tid=3:pc=0x24:raw=0x3:step=4:sp=0x44:ra=0x64:a0=0x84:s11=0xa4",
  );
});

test("formatExecTraceEntries falls back to frozen fault exec trace for the vm", () => {
  const entries = formatExecTraceEntries({
    ...makeThreadWorkerDebugExports(),
    debug_exec_trace_len: () => 2,
    debug_exec_trace_epoch_for_vm: () => 3,
    debug_exec_trace_entry_vm_ptr: () => 0x1111,
    debug_fault_exec_trace_len_for_vm: (vmPtr: number) => vmPtr === 0x1234 ? 2 : 0,
    debug_fault_exec_trace_entry_pc_for_vm: (_vmPtr: number, index: number) => [0x83336n, 0x83340n][index] ?? 0n,
    debug_fault_exec_trace_entry_raw_for_vm: (_vmPtr: number, index: number) => [0xa00563, 0xfe013283][index] ?? 0,
    debug_fault_exec_trace_entry_step_for_vm: (_vmPtr: number, index: number) => [4, 4][index] ?? 0,
    debug_fault_exec_trace_entry_sp_for_vm: (_vmPtr: number, index: number) => [0x37d6c000n, 0x37d6c000n][index] ?? 0n,
    debug_fault_exec_trace_entry_ra_for_vm: (_vmPtr: number, index: number) => [0x4d9b4n, 0x4d9b4n][index] ?? 0n,
    debug_fault_exec_trace_entry_a0_for_vm: (_vmPtr: number, index: number) => [0n, 0n][index] ?? 0n,
    debug_fault_exec_trace_entry_s11_for_vm: (_vmPtr: number, index: number) => [0xbfdc80n, 0xbfdc80n][index] ?? 0n,
  } as never, 0x1234);

  expect(entries).toBe(
    "0:pc=0x83336:raw=0xa00563:step=4:sp=0x37d6c000:ra=0x4d9b4:a0=0x0:s11=0xbfdc80"
    + " 1:pc=0x83340:raw=0xfe013283:step=4:sp=0x37d6c000:ra=0x4d9b4:a0=0x0:s11=0xbfdc80",
  );
});

test("formatSyncFaultExecTraceEntries formats frozen sync fault exec trace for the vm", async () => {
  const { formatSyncFaultExecTraceEntries } = await import("../../../src/thread-worker/debug.ts");
  const entries = formatSyncFaultExecTraceEntries({
    ...makeThreadWorkerDebugExports(),
    debug_sync_fault_exec_trace_len_for_vm: (vmPtr: number) => vmPtr === 0x1234 ? 2 : 0,
    debug_sync_fault_exec_trace_entry_pc_for_vm: (_vmPtr: number, index: number) => [0x83350n, 0x0n][index] ?? 0n,
    debug_sync_fault_exec_trace_entry_raw_for_vm: (_vmPtr: number, index: number) => [0x0b200893, 0x0][index] ?? 0,
    debug_sync_fault_exec_trace_entry_step_for_vm: (_vmPtr: number, index: number) => [4, 0][index] ?? 0,
    debug_sync_fault_exec_trace_entry_sp_for_vm: (_vmPtr: number, index: number) => [0x37d6c000n, 0x0n][index] ?? 0n,
    debug_sync_fault_exec_trace_entry_ra_for_vm: (_vmPtr: number, index: number) => [0x4d9b4n, 0x0n][index] ?? 0n,
    debug_sync_fault_exec_trace_entry_a0_for_vm: (_vmPtr: number, index: number) => [0x4d2n, 0x0n][index] ?? 0n,
    debug_sync_fault_exec_trace_entry_s11_for_vm: (_vmPtr: number, index: number) => [0xbfdc80n, 0x0n][index] ?? 0n,
  } as never, 0x1234);

  expect(entries).toBe(
    "0:pc=0x83350:raw=0xb200893:step=4:sp=0x37d6c000:ra=0x4d9b4:a0=0x4d2:s11=0xbfdc80"
    + " 1:pc=0x0:raw=0x0:step=0:sp=0x0:ra=0x0:a0=0x0:s11=0x0",
  );
});

test("formatExecTraceEntries falls back to frozen thread_step exec trace for the vm", () => {
  const entries = formatExecTraceEntries({
    ...makeThreadWorkerDebugExports(),
    debug_exec_trace_len: () => 0,
    debug_thread_step_exec_trace_len_for_vm: (vmPtr: number) => vmPtr === 0x1234 ? 2 : 0,
    debug_thread_step_exec_trace_entry_pc_for_vm: (_vmPtr: number, index: number) => [0x83336n, 0x83340n][index] ?? 0n,
    debug_thread_step_exec_trace_entry_raw_for_vm: (_vmPtr: number, index: number) => [0x00a00563, 0xfe013283][index] ?? 0,
    debug_thread_step_exec_trace_entry_step_for_vm: (_vmPtr: number, index: number) => [4, 4][index] ?? 0,
    debug_thread_step_exec_trace_entry_sp_for_vm: (_vmPtr: number, index: number) => [0x37d6c000n, 0x37d6c000n][index] ?? 0n,
    debug_thread_step_exec_trace_entry_ra_for_vm: (_vmPtr: number, index: number) => [0x4d9b4n, 0x4d9b4n][index] ?? 0n,
    debug_thread_step_exec_trace_entry_a0_for_vm: (_vmPtr: number, index: number) => [0n, 0n][index] ?? 0n,
    debug_thread_step_exec_trace_entry_s11_for_vm: (_vmPtr: number, index: number) => [0xbfdc80n, 0xbfdc80n][index] ?? 0n,
  } as never, 0x1234);

  expect(entries).toBe(
    "0:pc=0x83336:raw=0xa00563:step=4:sp=0x37d6c000:ra=0x4d9b4:a0=0x0:s11=0xbfdc80"
    + " 1:pc=0x83340:raw=0xfe013283:step=4:sp=0x37d6c000:ra=0x4d9b4:a0=0x0:s11=0xbfdc80",
  );
});

test("collectDebugParts falls back to global thread_step trace when per-vm lookup is empty", () => {
  const parts = collectDebugParts({
    ...makeThreadWorkerDebugExports(),
    debug_fd_syscall_trace_len: () => 0,
    debug_thread_step_tid: () => 7,
    debug_thread_step_status: () => 0,
    debug_thread_step_budget: () => 20000,
    debug_thread_step_remaining: () => 19998,
    debug_thread_step_pc: () => 0x5en,
    debug_thread_step_ra: () => 0x6fn,
    debug_thread_step_sp: () => 0x70n,
    debug_thread_step_s11: () => 0n,
    debug_thread_step_for_vm_tid: () => 0,
    debug_thread_step_for_vm_status: () => 0,
    debug_thread_step_for_vm_budget: () => 0,
    debug_thread_step_for_vm_remaining: () => 0,
    debug_thread_step_for_vm_pc: () => 0n,
    debug_thread_step_for_vm_ra: () => 0n,
    debug_thread_step_for_vm_sp: () => 0n,
    debug_thread_step_for_vm_s11: () => 0n,
  } as never, 0x1234, 0).join(" ");

  expect(parts).toContain("thread_step_tid=7");
  expect(parts).toContain("thread_step_pc=0x5e");
  expect(parts).toContain("thread_step_ra=0x6f");
  expect(parts).toContain("thread_step_sp=0x70");
});

test("collectDebugParts includes thread_step trace ring when per-vm lookup stays empty", () => {
  const parts = collectDebugParts({
    ...makeThreadWorkerDebugExports(),
    debug_fd_syscall_trace_len: () => 0,
    debug_thread_step_tid: () => 0,
    debug_thread_step_status: () => 0,
    debug_thread_step_budget: () => 0,
    debug_thread_step_remaining: () => 0,
    debug_thread_step_pc: () => 0n,
    debug_thread_step_ra: () => 0n,
    debug_thread_step_sp: () => 0n,
    debug_thread_step_s11: () => 0n,
    debug_thread_step_for_vm_tid: () => 0,
    debug_thread_step_for_vm_status: () => 0,
    debug_thread_step_for_vm_budget: () => 0,
    debug_thread_step_for_vm_remaining: () => 0,
    debug_thread_step_for_vm_pc: () => 0n,
    debug_thread_step_for_vm_ra: () => 0n,
    debug_thread_step_for_vm_sp: () => 0n,
    debug_thread_step_for_vm_s11: () => 0n,
    debug_thread_step_trace_len: () => 2,
    debug_thread_step_trace_entry_vm_ptr: (index: number) => [0x1111, 0x1234][index] ?? 0,
    debug_thread_step_trace_entry_tid: (index: number) => [1, 3][index] ?? 0,
    debug_thread_step_trace_entry_status: (index: number) => [20, 0][index] ?? 0,
    debug_thread_step_trace_entry_budget: (index: number) => [20000, 20000][index] ?? 0,
    debug_thread_step_trace_entry_remaining: (index: number) => [19999, 19998][index] ?? 0,
    debug_thread_step_trace_entry_pc: (index: number) => [0x111n, 0x222n][index] ?? 0n,
    debug_thread_step_trace_entry_ra: (index: number) => [0x333n, 0x444n][index] ?? 0n,
    debug_thread_step_trace_entry_sp: (index: number) => [0x555n, 0x666n][index] ?? 0n,
    debug_thread_step_trace_entry_s11: (index: number) => [0x777n, 0x888n][index] ?? 0n,
  } as never, 0x1234, 0).join(" ");

  expect(parts).toContain("thread_step_traces=[");
  expect(parts).toContain("0:vm=0x1111:tid=1:status=20");
  expect(parts).toContain("1*:vm=0x1234:tid=3:status=0");
  expect(parts).toContain("pc=0x222");
});

test("collectDebugParts includes per-vm fault dispatch trace", () => {
  const parts = collectDebugParts(
    {
      ...makeThreadWorkerDebugExports(),
      debug_fd_syscall_trace_len: () => 0,
      debug_fault_dispatch_for_vm_phase: (vmPtr: number) => vmPtr === 0x1234 ? 2 : 0,
      debug_fault_dispatch_for_vm_tid: (vmPtr: number) => vmPtr === 0x1234 ? 3 : 0,
      debug_fault_dispatch_for_vm_pc: (vmPtr: number) => vmPtr === 0x1234 ? 0x4n : 0n,
      debug_fault_dispatch_for_vm_ra: (vmPtr: number) => vmPtr === 0x1234 ? 0x55n : 0n,
      debug_fault_dispatch_for_vm_sp: (vmPtr: number) => vmPtr === 0x1234 ? 0x66n : 0n,
      debug_fault_dispatch_for_vm_s11: (vmPtr: number) => vmPtr === 0x1234 ? 0n : 0n,
      debug_fault_dispatch_for_vm_addr: (vmPtr: number) => vmPtr === 0x1234 ? 0x4n : 0n,
    } as never,
    0x1234,
    0,
  ).join(" ");

  expect(parts).toContain("fault_dispatch_tid=3");
  expect(parts).toContain("fault_dispatch_phase=2");
  expect(parts).toContain("fault_dispatch_pc=0x4");
  expect(parts).toContain("fault_dispatch_ra=0x55");
  expect(parts).toContain("fault_dispatch_sp=0x66");
  expect(parts).toContain("fault_dispatch_s11=0x0");
  expect(parts).toContain("fault_dispatch_addr=0x4");
});

test("collectDebugParts includes per-vm exec trace tail when fault dispatch is present", () => {
  const parts = collectDebugParts(
    {
      ...makeThreadWorkerDebugExports(),
      debug_fd_syscall_trace_len: () => 0,
      debug_fault_dispatch_for_vm_phase: (vmPtr: number) => vmPtr === 0x1234 ? 2 : 0,
      debug_fault_dispatch_for_vm_tid: (vmPtr: number) => vmPtr === 0x1234 ? 3 : 0,
      debug_fault_dispatch_for_vm_pc: (vmPtr: number) => vmPtr === 0x1234 ? 0x83258n : 0n,
      debug_fault_dispatch_for_vm_ra: (vmPtr: number) => vmPtr === 0x1234 ? 0x55n : 0n,
      debug_fault_dispatch_for_vm_sp: (vmPtr: number) => vmPtr === 0x1234 ? 0x66n : 0n,
      debug_fault_dispatch_for_vm_s11: (vmPtr: number) => vmPtr === 0x1234 ? 0n : 0n,
      debug_fault_dispatch_for_vm_addr: (vmPtr: number) => vmPtr === 0x1234 ? 0x4n : 0n,
      debug_exec_trace_len: () => 3,
      debug_exec_trace_entry_vm_ptr: (index: number) => [0x1111, 0x1234, 0x1234][index] ?? 0,
      debug_exec_trace_entry_tid: (index: number) => [1, 3, 3][index] ?? 0,
      debug_exec_trace_entry_pc: (index: number) => [0x100n, 0x83308n, 0x83258n][index] ?? 0n,
      debug_exec_trace_entry_raw: (index: number) => [0x1, 0x2, 0x3][index] ?? 0,
      debug_exec_trace_entry_step: (index: number) => [4, 4, 4][index] ?? 0,
      debug_exec_trace_entry_sp: (index: number) => [0x10n, 0x20n, 0x30n][index] ?? 0n,
      debug_exec_trace_entry_ra: (index: number) => [0x11n, 0x21n, 0x31n][index] ?? 0n,
      debug_exec_trace_entry_a0: (index: number) => [0x12n, 0x22n, 0x32n][index] ?? 0n,
      debug_exec_trace_entry_s11: (index: number) => [0x13n, 0x23n, 0x0n][index] ?? 0n,
    } as never,
    0x1234,
    0,
  ).join(" ");

  expect(parts).toContain("exec_trace=[");
  expect(parts).toContain("1:tid=3:pc=0x83308");
  expect(parts).toContain("2:tid=3:pc=0x83258");
  expect(parts).not.toContain("0:tid=1:pc=0x100");
});

test("collectDebugParts includes sync fault exec trace when present", () => {
  const parts = collectDebugParts(
    {
      ...makeThreadWorkerDebugExports(),
      debug_fd_syscall_trace_len: () => 0,
      debug_fault_dispatch_for_vm_phase: (vmPtr: number) => vmPtr === 0x1234 ? 2 : 0,
      debug_fault_dispatch_for_vm_tid: (vmPtr: number) => vmPtr === 0x1234 ? 3 : 0,
      debug_fault_dispatch_for_vm_pc: (vmPtr: number) => vmPtr === 0x1234 ? 0x83258n : 0n,
      debug_fault_dispatch_for_vm_ra: (vmPtr: number) => vmPtr === 0x1234 ? 0x37d61ff8n : 0n,
      debug_fault_dispatch_for_vm_sp: (vmPtr: number) => vmPtr === 0x1234 ? 0x37d61b90n : 0n,
      debug_fault_dispatch_for_vm_s11: (vmPtr: number) => vmPtr === 0x1234 ? 0n : 0n,
      debug_fault_dispatch_for_vm_addr: (vmPtr: number) => vmPtr === 0x1234 ? 0n : 0n,
      debug_sync_fault_exec_trace_len_for_vm: (vmPtr: number) => vmPtr === 0x1234 ? 2 : 0,
      debug_sync_fault_exec_trace_entry_pc_for_vm: (_vmPtr: number, index: number) => [0x83350n, 0x0n][index] ?? 0n,
      debug_sync_fault_exec_trace_entry_raw_for_vm: (_vmPtr: number, index: number) => [0x0b200893, 0x0][index] ?? 0,
      debug_sync_fault_exec_trace_entry_step_for_vm: (_vmPtr: number, index: number) => [4, 0][index] ?? 0,
      debug_sync_fault_exec_trace_entry_sp_for_vm: (_vmPtr: number, index: number) => [0x37d6c000n, 0x0n][index] ?? 0n,
      debug_sync_fault_exec_trace_entry_ra_for_vm: (_vmPtr: number, index: number) => [0x4d9b4n, 0x0n][index] ?? 0n,
      debug_sync_fault_exec_trace_entry_a0_for_vm: (_vmPtr: number, index: number) => [0x4d2n, 0x0n][index] ?? 0n,
      debug_sync_fault_exec_trace_entry_s11_for_vm: (_vmPtr: number, index: number) => [0xbfdc80n, 0x0n][index] ?? 0n,
    } as never,
    0x1234,
    0,
  ).join(" ");

  expect(parts).toContain("sync_fault_exec_trace=[");
  expect(parts).toContain("0:pc=0x83350");
  expect(parts).toContain("1:pc=0x0");
});
