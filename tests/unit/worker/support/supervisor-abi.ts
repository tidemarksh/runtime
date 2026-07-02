import {
  requiredSupervisorRuntimeExportNames,
  type KernelExports,
} from "../../../../src/abi.js";

const DEFAULT_NUMERIC_EXPORTS: Record<string, number> = {
  supervisor_abi_version: 1,
  supervisor_debug_snapshot_bytes: 112,
  supervisor_thread_event_record_bytes: 64,
  supervisor_thread_state_snapshot_bytes: 32,
  supervisor_resume_record_bytes: 48,
  supervisor_host_completion_record_bytes: 64,
  supervisor_host_completion_outcome_success: 1,
  supervisor_host_completion_outcome_failure: 2,
  supervisor_host_op_record_bytes: 64,
  supervisor_process_host_op_record_bytes: 96,
  supervisor_fault_exit_decision_bytes: 88,
  supervisor_fault_exit_kind_thread_exit: 1,
  supervisor_fault_exit_kind_process_exit: 2,
  supervisor_fd_control_record_bytes: 96,
  supervisor_fs_control_record_bytes: 112,
  supervisor_signal_control_record_bytes: 120,
  supervisor_resume_kind_futex_wake: 1,
  supervisor_resume_kind_runnable_thread: 2,
  supervisor_blocked_resume_delegation_direct: 0,
  supervisor_blocked_resume_delegation_kernel_worker: 1,
  supervisor_blocked_resume_delegation_wait4_kernel_worker: 2,
  supervisor_blocked_resume_delegation_unsupported: 3,
  supervisor_blocked_resume_delegation_epoll_direct: 4,
  supervisor_blocked_resume_delegation_flag_kernel_worker_available: 1,
  supervisor_blocked_wait_retry_reason_default: 0,
  supervisor_blocked_wait_retry_reason_immediate: 1,
  supervisor_blocked_wait_retry_reason_standard_defer: 2,
  supervisor_blocked_wait_retry_reason_active_child: 3,
  supervisor_blocked_wait_retry_reason_passive_wait: 4,
  supervisor_blocked_wait_retry_reason_process_gate: 5,
  supervisor_blocked_wait_retry_flag_host_progress: 1,
  supervisor_blocked_wait_retry_flag_active_child: 2,
  supervisor_blocked_wait_retry_flag_passive_wait: 4,
  supervisor_blocked_wait_retry_flag_process_gate: 8,
  supervisor_blocked_wait_retry_flag_thread_init_pending: 16,
  supervisor_blocked_wait_timer_required: 0,
  supervisor_host_op_kind_arm_host_timer: 12,
  supervisor_futex_wait_completion_woken: 1,
  supervisor_futex_wait_completion_not_equal: 2,
  supervisor_futex_wait_completion_invalid_address: 4,
  supervisor_futex_wait_completion_signal_check: 5,
};

export interface SupervisorRunnableResumeRecordFixture {
  vmPtr: number;
  tid?: number;
  processPtr?: number;
  status?: number;
  seq?: bigint;
  detail0?: bigint;
}

export interface SupervisorArmHostTimerHostOpFixture {
  vmPtr: number;
  delayMs: bigint;
  seq?: bigint;
  tid?: number;
  processPtr?: number;
  status?: number;
  deadlineTicks?: bigint;
}

export interface SupervisorFaultExitDecisionRecordFixture {
  kind?: number;
  seq?: bigint;
  vmPtr: number;
  exitVmPtr?: number;
  tid?: number;
  processPtr?: number;
  remainingThreads?: number;
  liveThreads?: number;
  exitCode?: number;
  termSignal?: number;
  status?: number;
  faultSignal?: number;
  faultKind?: number;
  faultPc?: bigint;
  faultAddr?: bigint;
}

export function makeSupervisorAbiStubs(
  overrides: Partial<KernelExports> = {},
): Partial<KernelExports> {
  const stubs = Object.fromEntries(
    requiredSupervisorRuntimeExportNames().map((name) => [
      name,
      () => DEFAULT_NUMERIC_EXPORTS[name] ?? 0,
    ]),
  );
  return {
    ...stubs,
    supervisor_blocked_resume_delegation: (vmPtr: number, flags: number) => {
      const kernelWorkerAvailable = (flags
        & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_resume_delegation_flag_kernel_worker_available) !== 0;
      if (!kernelWorkerAvailable) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_resume_delegation_direct;
      }
      if (vmPtr === 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_resume_delegation_unsupported;
      }
      return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_resume_delegation_kernel_worker;
    },
    supervisor_blocked_wait_retry_delay_ms: (_vmPtr: number, reason: number) => {
      switch (reason) {
        case DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_immediate:
          return 0;
        case DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_standard_defer:
          return 10;
        case DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_active_child:
          return 5;
        case DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_passive_wait:
          return 25;
        case DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_process_gate:
          return 50;
        default:
          return 1;
      }
    },
    supervisor_blocked_wait_retry_reason: (_vmPtr: number, flags: number) => {
      if ((flags & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_flag_process_gate) !== 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_process_gate;
      }
      if ((flags & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_flag_active_child) !== 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_active_child;
      }
      if ((flags & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_flag_passive_wait) !== 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_passive_wait;
      }
      if ((flags & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_flag_host_progress) !== 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_immediate;
      }
      if ((flags & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_flag_thread_init_pending) !== 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_standard_defer;
      }
      return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_default;
    },
    ...overrides,
  } as Partial<KernelExports>;
}

export function writeSupervisorFaultExitDecisionRecord(
  memory: WebAssembly.Memory,
  outPtr: number,
  record: SupervisorFaultExitDecisionRecordFixture,
): number {
  const view = new DataView(memory.buffer);
  view.setUint32(outPtr, 0x5846_5354, true);
  view.setUint32(outPtr + 4, 1, true);
  view.setUint32(outPtr + 8, DEFAULT_NUMERIC_EXPORTS.supervisor_fault_exit_decision_bytes, true);
  view.setUint32(
    outPtr + 12,
    record.kind ?? DEFAULT_NUMERIC_EXPORTS.supervisor_fault_exit_kind_process_exit,
    true,
  );
  view.setBigUint64(outPtr + 16, record.seq ?? 1n, true);
  view.setUint32(outPtr + 24, record.vmPtr, true);
  view.setUint32(outPtr + 28, record.exitVmPtr ?? record.vmPtr, true);
  view.setUint32(outPtr + 32, record.tid ?? 1, true);
  view.setUint32(outPtr + 36, record.processPtr ?? 0, true);
  view.setUint32(outPtr + 40, record.remainingThreads ?? 0, true);
  view.setUint32(outPtr + 44, record.liveThreads ?? 0, true);
  view.setInt32(outPtr + 48, record.exitCode ?? 0, true);
  view.setInt32(outPtr + 52, record.termSignal ?? 0, true);
  view.setUint32(outPtr + 56, record.status ?? 0, true);
  view.setUint32(outPtr + 60, record.faultSignal ?? 0, true);
  view.setUint32(outPtr + 64, record.faultKind ?? 0, true);
  view.setUint32(outPtr + 68, 0, true);
  view.setBigUint64(outPtr + 72, record.faultPc ?? 0n, true);
  view.setBigUint64(outPtr + 80, record.faultAddr ?? 0n, true);
  return DEFAULT_NUMERIC_EXPORTS.supervisor_fault_exit_decision_bytes;
}

export function writeSupervisorRunnableResumeRecord(
  memory: WebAssembly.Memory,
  outPtr: number,
  record: SupervisorRunnableResumeRecordFixture,
): number {
  const view = new DataView(memory.buffer);
  view.setUint32(outPtr, 0x5255_5354, true);
  view.setUint32(outPtr + 4, 1, true);
  view.setUint32(outPtr + 8, 48, true);
  view.setUint32(outPtr + 12, DEFAULT_NUMERIC_EXPORTS.supervisor_resume_kind_runnable_thread, true);
  view.setBigUint64(outPtr + 16, record.seq ?? 1n, true);
  view.setUint32(outPtr + 24, record.vmPtr, true);
  view.setUint32(outPtr + 28, record.tid ?? 1, true);
  view.setUint32(outPtr + 32, record.processPtr ?? 0, true);
  view.setUint32(outPtr + 36, record.status ?? 0, true);
  view.setBigUint64(outPtr + 40, record.detail0 ?? 0n, true);
  return 48;
}

export function writeSupervisorArmHostTimerHostOpRecord(
  memory: WebAssembly.Memory,
  outPtr: number,
  record: SupervisorArmHostTimerHostOpFixture,
): number {
  const view = new DataView(memory.buffer);
  view.setUint32(outPtr, 0x4f48_5354, true);
  view.setUint32(outPtr + 4, DEFAULT_NUMERIC_EXPORTS.supervisor_abi_version, true);
  view.setUint32(outPtr + 8, DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_record_bytes, true);
  view.setUint32(outPtr + 12, DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_kind_arm_host_timer, true);
  view.setBigUint64(outPtr + 16, record.seq ?? 1n, true);
  view.setUint32(outPtr + 24, record.vmPtr, true);
  view.setUint32(outPtr + 28, 0, true);
  view.setUint32(outPtr + 32, record.tid ?? 1, true);
  view.setUint32(outPtr + 36, record.processPtr ?? 0, true);
  view.setUint32(outPtr + 40, record.status ?? 0, true);
  view.setUint32(outPtr + 44, 0, true);
  view.setBigUint64(outPtr + 48, record.delayMs, true);
  view.setBigUint64(outPtr + 56, record.deadlineTicks ?? 0n, true);
  return DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_record_bytes;
}
