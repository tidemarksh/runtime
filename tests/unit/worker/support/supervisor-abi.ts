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
  supervisor_fd_control_record_bytes: 96,
  supervisor_fs_control_record_bytes: 112,
  supervisor_signal_control_record_bytes: 120,
  supervisor_resume_kind_futex_wake: 1,
  supervisor_resume_kind_runnable_thread: 2,
  supervisor_host_op_kind_arm_host_timer: 12,
  supervisor_futex_wait_completion_woken: 1,
  supervisor_futex_wait_completion_not_equal: 2,
  supervisor_futex_wait_completion_timed_out: 3,
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
    ...overrides,
  } as Partial<KernelExports>;
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
