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
  supervisor_host_op_kind_spawn_thread_worker: 1,
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

type SupervisorAbiStubOverrides = Partial<KernelExports> & {
  cloneSpawnParentVmPtr?: () => number;
  currentExports?: () => Partial<KernelExports> | undefined;
};

export function makeSupervisorAbiStubs(
  overrides: SupervisorAbiStubOverrides = {},
): Partial<KernelExports> {
  const cloneSpawnHostOpChildVmPtrs = new Map<number, number>();
  const currentExports = (receiver: KernelExports | undefined): Partial<KernelExports> =>
    receiver ?? overrides.currentExports?.() ?? overrides;
  const stubs = Object.fromEntries(
    requiredSupervisorRuntimeExportNames().map((name) => [
      name,
      () => DEFAULT_NUMERIC_EXPORTS[name] ?? 0,
    ]),
  );
  return {
    ...stubs,
    supervisor_blocked_resume_delegation(this: KernelExports | undefined, vmPtr: number, flags: number) {
      const kernelWorkerAvailable = (flags
        & DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_resume_delegation_flag_kernel_worker_available) !== 0;
      const receiver = currentExports(this);
      const status = Number(receiver?.thread_status?.(vmPtr) ?? overrides.thread_status?.(vmPtr) ?? 0);
      if (!kernelWorkerAvailable) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_resume_delegation_direct;
      }
      if (vmPtr === 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_resume_delegation_unsupported;
      }
      if (status === Number(receiver?.abi_status_wait4?.() ?? overrides.abi_status_wait4?.() ?? -1)) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_resume_delegation_wait4_kernel_worker;
      }
      return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_resume_delegation_kernel_worker;
    },
    supervisor_blocked_wait_retry_delay_ms: () => 0,
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
    supervisor_process_resume(this: KernelExports | undefined, vmPtr: number, outPtr: number, outLen: number) {
      const recordBytes = DEFAULT_NUMERIC_EXPORTS.supervisor_resume_record_bytes;
      if (outLen < recordBytes) {
        return -1;
      }
      const receiver = currentExports(this);
      const memory = receiver?.memory ?? overrides.memory;
      if (!memory) {
        return -1;
      }
      const status = Number(receiver?.thread_status?.(vmPtr) ?? overrides.thread_status?.(vmPtr) ?? 0);
      const okStatus = Number(receiver?.abi_status_ok?.() ?? overrides.abi_status_ok?.() ?? 0);
      if (status !== okStatus) {
        return 0;
      }
      return writeSupervisorRunnableResumeRecord(memory, outPtr, {
        vmPtr,
        tid: Number(receiver?.thread_tid?.(vmPtr) ?? overrides.thread_tid?.(vmPtr) ?? 1),
        processPtr: Number(receiver?.thread_process_ptr?.(vmPtr) ?? overrides.thread_process_ptr?.(vmPtr) ?? 0),
        status,
      });
    },
    supervisor_next_host_op_kind(this: KernelExports | undefined, kind: number, outPtr: number, outLen: number) {
      const receiver = currentExports(this);
      const spawnThreadKind = Number(
        receiver?.supervisor_host_op_kind_spawn_thread_worker?.()
          ?? overrides.supervisor_host_op_kind_spawn_thread_worker?.()
          ?? DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_kind_spawn_thread_worker,
      );
      if (kind !== spawnThreadKind) {
        return 0;
      }
      const parentVmPtr = overrides.cloneSpawnParentVmPtr?.();
      if (parentVmPtr === undefined) {
        return 0;
      }
      const cloneSpawnStatus = Number(
        receiver?.abi_status_clone_spawn?.() ?? overrides.abi_status_clone_spawn?.() ?? -1,
      );
      const currentStatus = Number(receiver?.thread_status?.(parentVmPtr) ?? overrides.thread_status?.(parentVmPtr) ?? 0);
      if (currentStatus !== cloneSpawnStatus) {
        return 0;
      }
      const memory = receiver?.memory ?? overrides.memory;
      if (!memory) {
        return -1;
      }
      const recordBytes = DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_record_bytes;
      if (outLen < recordBytes) {
        return -1;
      }
      let childVmPtr = cloneSpawnHostOpChildVmPtrs.get(parentVmPtr);
      if (childVmPtr === undefined) {
        childVmPtr = Number(receiver?.thread_spawn_from_clone?.(parentVmPtr) ?? overrides.thread_spawn_from_clone?.(parentVmPtr) ?? 0);
        if (childVmPtr === 0) {
          return 0;
        }
        cloneSpawnHostOpChildVmPtrs.set(parentVmPtr, childVmPtr);
      }
      return writeSupervisorSpawnThreadHostOpRecord(memory, outPtr, {
        vmPtr: parentVmPtr,
        childVmPtr,
        kind: spawnThreadKind,
        tid: Number(receiver?.thread_tid?.(childVmPtr) ?? overrides.thread_tid?.(childVmPtr) ?? 1),
        processPtr: Number(receiver?.thread_process_ptr?.(parentVmPtr) ?? overrides.thread_process_ptr?.(parentVmPtr) ?? 0),
        status: Number(receiver?.abi_status_ok?.() ?? overrides.abi_status_ok?.() ?? 0),
      });
    },
    supervisor_resume_blocked_syscall(this: KernelExports | undefined, vmPtr: number, flags: number) {
      const receiver = currentExports(this);
      const status = Number(receiver?.thread_status?.(vmPtr) ?? overrides.thread_status?.(vmPtr) ?? 0);
      const timeoutExpired = (flags & 1) !== 0;
      if (status === Number(receiver?.abi_status_wait4?.() ?? overrides.abi_status_wait4?.() ?? -1)) {
        return receiver?.thread_resume_wait4?.(vmPtr) ?? overrides.thread_resume_wait4?.(vmPtr) ?? 0n;
      }
      if (
        timeoutExpired
        && (
          status === Number(receiver?.abi_status_connect?.() ?? overrides.abi_status_connect?.() ?? -1)
          || status === Number(receiver?.abi_status_socket_recv?.() ?? overrides.abi_status_socket_recv?.() ?? -1)
          || status === Number(receiver?.abi_status_socket_send?.() ?? overrides.abi_status_socket_send?.() ?? -1)
        )
      ) {
        receiver?.thread_resume_socket_wait_timeout?.(vmPtr);
        return 0n;
      }
      if (timeoutExpired && status === Number(receiver?.abi_status_epoll_wait?.() ?? overrides.abi_status_epoll_wait?.() ?? -1)) {
        receiver?.thread_resume_epoll_wait_timeout?.(vmPtr);
        return 0n;
      }
      if (timeoutExpired && status === Number(receiver?.abi_status_pipe_wait?.() ?? overrides.abi_status_pipe_wait?.() ?? -1)) {
        receiver?.thread_resume_poll_wait_timeout?.(vmPtr);
        return 0n;
      }
      return receiver?.thread_resume_blocked_syscall?.(vmPtr) ?? overrides.thread_resume_blocked_syscall?.(vmPtr) ?? 0n;
    },
    supervisor_fault_exit_decision(this: KernelExports | undefined, vmPtr: number, outPtr: number, outLen: number) {
      const recordBytes = DEFAULT_NUMERIC_EXPORTS.supervisor_fault_exit_decision_bytes;
      if (outLen < recordBytes) {
        return -1;
      }
      const receiver = currentExports(this);
      const memory = receiver?.memory ?? overrides.memory;
      if (!memory) {
        return -1;
      }
      return writeSupervisorFaultExitDecisionRecord(memory, outPtr, {
        vmPtr,
        exitVmPtr: vmPtr,
        processPtr: Number(receiver?.thread_process_ptr?.(vmPtr) ?? overrides.thread_process_ptr?.(vmPtr) ?? 0),
        remainingThreads: 0,
        liveThreads: 0,
        exitCode: Number(receiver?.thread_exit_code?.(vmPtr) ?? overrides.thread_exit_code?.(vmPtr) ?? 0),
        termSignal: Number(receiver?.thread_term_signal?.(vmPtr) ?? overrides.thread_term_signal?.(vmPtr) ?? 0),
        status: Number(receiver?.thread_status?.(vmPtr) ?? overrides.thread_status?.(vmPtr) ?? 0),
      });
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

export interface SupervisorSpawnThreadHostOpFixture {
  vmPtr: number;
  childVmPtr: number;
  kind?: number;
  seq?: bigint;
  tid?: number;
  processPtr?: number;
  status?: number;
  flags?: bigint;
  childTidPtr?: bigint;
}

export function writeSupervisorSpawnThreadHostOpRecord(
  memory: WebAssembly.Memory,
  outPtr: number,
  record: SupervisorSpawnThreadHostOpFixture,
): number {
  const view = new DataView(memory.buffer);
  view.setUint32(outPtr, 0x4f48_5354, true);
  view.setUint32(outPtr + 4, DEFAULT_NUMERIC_EXPORTS.supervisor_abi_version, true);
  view.setUint32(outPtr + 8, DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_record_bytes, true);
  view.setUint32(outPtr + 12, record.kind ?? DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_kind_spawn_thread_worker, true);
  view.setBigUint64(outPtr + 16, record.seq ?? 1n, true);
  view.setUint32(outPtr + 24, record.vmPtr, true);
  view.setUint32(outPtr + 28, record.childVmPtr, true);
  view.setUint32(outPtr + 32, record.tid ?? 1, true);
  view.setUint32(outPtr + 36, record.processPtr ?? 0, true);
  view.setUint32(outPtr + 40, record.status ?? 0, true);
  view.setUint32(outPtr + 44, 0, true);
  view.setBigUint64(outPtr + 48, record.flags ?? 0n, true);
  view.setBigUint64(outPtr + 56, record.childTidPtr ?? 0n, true);
  return DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_record_bytes;
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
