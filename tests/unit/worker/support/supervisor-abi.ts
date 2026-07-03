import {
  requiredSupervisorRuntimeExportNames,
  type KernelExports,
} from "../../../../src/abi.js";
import {
  FS_OP_CLOSE,
  FS_OP_EXECVE,
  FS_OP_MMAP,
} from "../../../../src/layout/fs-ring.js";

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
  supervisor_host_op_kind_create_process_memory: 3,
  supervisor_host_op_kind_copy_memory_ranges: 4,
  supervisor_process_host_op_record_bytes: 96,
  supervisor_fault_exit_decision_bytes: 88,
  supervisor_fault_exit_kind_thread_exit: 1,
  supervisor_fault_exit_kind_process_exit: 2,
  supervisor_fd_control_record_bytes: 96,
  supervisor_fs_control_record_bytes: 112,
  supervisor_signal_control_record_bytes: 120,
  supervisor_fs_response_flag_adopt_kernel_state: 1,
  supervisor_fs_response_flag_apply_guest_memory_writes: 2,
  supervisor_fs_response_flag_queue_thread_worker_writes: 4,
  supervisor_fs_response_flag_apply_local_metadata: 8,
  supervisor_fs_response_flag_clear_tombstones: 16,
  supervisor_fs_response_flag_fd_response_may_clear_live_fds: 32,
  supervisor_fs_response_flag_store_pending_kernel_state: 64,
  supervisor_fs_response_flag_resolved_to_execve_spawn: 128,
  supervisor_fs_response_flag_process_owner_matches: 256,
  supervisor_fs_resident_page_evict_not_found: 0,
  supervisor_fs_resident_page_evict_dirty_pinned: 1,
  supervisor_fs_resident_page_evict_evicted: 2,
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
  supervisor_vfork_execve_handoff_failure_wait_status: 127 << 8,
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
  const callNumber = (receiver: Partial<KernelExports>, name: keyof KernelExports, ...args: unknown[]): number => {
    const fn = (receiver[name] ?? overrides[name]) as ((...params: unknown[]) => unknown) | undefined;
    const value = fn?.(...args);
    return typeof value === "bigint" ? Number(value) : Number(value ?? 0);
  };
  const addRange = (ranges: Array<{ start: number; end: number }>, start: number, end: number, limit: number): void => {
    const boundedStart = Math.max(0, Math.min(start, limit));
    const boundedEnd = Math.max(boundedStart, Math.min(end, limit));
    if (boundedEnd > boundedStart) {
      ranges.push({ start: boundedStart, end: boundedEnd });
    }
  };
  const coalesceRanges = (ranges: Array<{ start: number; end: number }>): Array<{ start: number; end: number }> => {
    const sorted = ranges
      .filter((range) => range.end > range.start)
      .sort((left, right) => left.start - right.start || left.end - right.end);
    const coalesced: Array<{ start: number; end: number }> = [];
    for (const range of sorted) {
      const last = coalesced[coalesced.length - 1];
      if (last && range.start <= last.end) {
        last.end = Math.max(last.end, range.end);
      } else {
        coalesced.push({ ...range });
      }
    }
    return coalesced;
  };
  const processGuestBase = (receiver: Partial<KernelExports>): number => {
    const addressProfile = callNumber(receiver, "thread_spawn_address_profile" as keyof KernelExports, 0)
      || callNumber(receiver, "abi_guest_address_profile_target" as keyof KernelExports);
    const base = callNumber(receiver, "process_guest_base_for_address_profile", addressProfile);
    return base || 0x1000000;
  };
  const processMemoryRanges = (
    receiver: Partial<KernelExports>,
    memory: WebAssembly.Memory,
    vmPtr: number,
  ): Array<{ start: number; end: number }> => {
    const limit = memory.buffer.byteLength;
    const ranges: Array<{ start: number; end: number }> = [];
    const spawnKind = callNumber(receiver, "thread_spawn_kind", vmPtr);
    const execveKind = callNumber(receiver, "abi_spawn_kind_execve");
    const fullMemfs = spawnKind === execveKind;
    const heapPtr = callNumber(receiver, "kernel_heap_ptr");
    addRange(ranges, 0, heapPtr, limit);
    addRange(ranges, callNumber(receiver, "kernel_ofd_table_ptr"), callNumber(receiver, "kernel_ofd_table_ptr") + callNumber(receiver, "kernel_ofd_table_bytes"), limit);
    addRange(
      ranges,
      callNumber(receiver, "kernel_memfs_ptr"),
      callNumber(receiver, "kernel_memfs_ptr")
        + callNumber(receiver, "kernel_memfs_header_bytes")
        + callNumber(receiver, "kernel_memfs_inodes_bytes")
        + callNumber(receiver, "kernel_memfs_first_child_bytes")
        + callNumber(receiver, "kernel_memfs_dir_entries_bytes")
        + (fullMemfs ? callNumber(receiver, "kernel_memfs_data_bytes") : 0),
      limit,
    );
    addRange(ranges, callNumber(receiver, "kernel_memfs_data_ptr"), callNumber(receiver, "kernel_memfs_data_ptr") + callNumber(receiver, "kernel_memfs_data_bytes"), limit);
    addRange(ranges, callNumber(receiver, "kernel_epoll_table_ptr"), callNumber(receiver, "kernel_epoll_table_ptr") + callNumber(receiver, "kernel_epoll_table_bytes"), limit);
    addRange(ranges, callNumber(receiver, "kernel_socket_table_ptr"), callNumber(receiver, "kernel_socket_table_ptr") + callNumber(receiver, "kernel_socket_table_bytes"), limit);
    addRange(ranges, callNumber(receiver, "kernel_pipe_base"), callNumber(receiver, "kernel_pipe_base") + callNumber(receiver, "kernel_pipe_region_bytes"), limit);
    addRange(ranges, callNumber(receiver, "kernel_net_buf_base"), callNumber(receiver, "kernel_net_buf_base") + callNumber(receiver, "kernel_net_buf_pool_bytes"), limit);
    addRange(ranges, callNumber(receiver, "kernel_sig_slots_ptr"), callNumber(receiver, "kernel_sig_slots_ptr") + callNumber(receiver, "kernel_sig_slots_bytes") + callNumber(receiver, "kernel_signal_action_table_bytes"), limit);
    addRange(ranges, callNumber(receiver, "kernel_process_pending_ptr"), callNumber(receiver, "kernel_process_pending_ptr") + callNumber(receiver, "kernel_process_pending_bytes"), limit);
    addRange(ranges, callNumber(receiver, "kernel_process_runtime_state_ptr"), callNumber(receiver, "kernel_process_runtime_state_ptr") + callNumber(receiver, "kernel_process_runtime_state_bytes"), limit);
    addRange(ranges, callNumber(receiver, "kernel_time_state_ptr"), callNumber(receiver, "kernel_time_state_ptr") + callNumber(receiver, "kernel_time_state_bytes"), limit);
    if (spawnKind !== execveKind) {
      const guestBase = processGuestBase(receiver);
      const processPtr = callNumber(receiver, "thread_process_ptr", vmPtr);
      const processStateBytes = callNumber(receiver, "process_state_bytes") || 4096;
      addRange(ranges, guestBase, guestBase + processPtr + processStateBytes, limit);
      const spRaw = receiver.thread_sp?.(vmPtr) ?? overrides.thread_sp?.(vmPtr) ?? 0n;
      const sp = typeof spRaw === "bigint" ? Number(spRaw) : Number(spRaw);
      if (Number.isFinite(sp) && sp > 0) {
        const stackStartGuest = Math.max(0, Math.floor(sp / 65536) * 65536 - 1024 * 1024);
        addRange(ranges, guestBase + stackStartGuest, guestBase + stackStartGuest + 8 * 1024 * 1024, limit);
      }
    }
    return coalesceRanges(ranges);
  };
  const processMemoryInitialBytes = (
    receiver: Partial<KernelExports>,
    ranges: Array<{ start: number; end: number }>,
    vmPtr: number,
  ): number => {
    const spawnKind = callNumber(receiver, "thread_spawn_kind", vmPtr);
    const execveKind = callNumber(receiver, "abi_spawn_kind_execve");
    const guestBase = processGuestBase(receiver);
    if (spawnKind === execveKind) {
      const spRaw = receiver.thread_sp?.(vmPtr) ?? overrides.thread_sp?.(vmPtr) ?? 0n;
      const sp = typeof spRaw === "bigint" ? Number(spRaw) : Number(spRaw);
      const liveStackEnd = Number.isFinite(sp) && sp > 0 ? guestBase + sp + 8 * 1024 * 1024 : 0;
      return Math.max(64 * 1024 * 1024, callNumber(receiver, "kernel_heap_ptr"), guestBase + 0x10000, liveStackEnd);
    }
    const copyEnd = ranges.reduce((end, range) => Math.max(end, range.end), 0);
    return Math.max(copyEnd + 16 * 1024 * 1024, guestBase);
  };
  const processMemoryMaxPages = (
    receiver: Partial<KernelExports>,
    initialBytes: number,
    vmPtr: number,
  ): number => {
    return callNumber(receiver, "thread_spawn_process_max_memory_pages", vmPtr)
      || callNumber(receiver, "thread_process_max_memory_pages", vmPtr)
      || Math.ceil(initialBytes / 65536)
      || 1;
  };
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
    supervisor_fs_response_disposition(
      this: KernelExports | undefined,
      vmPtr: number,
      runtimePid: number,
      opcode: number,
      result: bigint | number,
      ownerAsyncStatus: number,
      lastStatus: number,
    ) {
      const receiver = currentExports(this);
      const currentPid = callNumber(receiver, "process_current_pid", vmPtr) || runtimePid;
      const processOwnerMatches = currentPid <= 0 || runtimePid <= 0 || currentPid === runtimePid;
      const numericResult = Number(BigInt.asIntN(64, typeof result === "bigint" ? result : BigInt(result)));
      const failedExecveChildOwner = opcode === FS_OP_EXECVE && numericResult < 0 && !processOwnerMatches;
      const execveSpawn = callNumber(receiver, "abi_status_execve_spawn");
      const resolvedToExecveSpawn = opcode === FS_OP_EXECVE
        && execveSpawn !== 0
        && (
          callNumber(receiver, "thread_status", vmPtr) === execveSpawn
          || ownerAsyncStatus === execveSpawn
          || lastStatus === execveSpawn
        );
      let disposition = 0;
      if (processOwnerMatches) {
        disposition |= DEFAULT_NUMERIC_EXPORTS.supervisor_fs_response_flag_process_owner_matches;
      }
      if (failedExecveChildOwner) {
        disposition |= DEFAULT_NUMERIC_EXPORTS.supervisor_fs_response_flag_store_pending_kernel_state;
      } else {
        disposition |= DEFAULT_NUMERIC_EXPORTS.supervisor_fs_response_flag_adopt_kernel_state;
        disposition |= DEFAULT_NUMERIC_EXPORTS.supervisor_fs_response_flag_apply_guest_memory_writes;
        disposition |= DEFAULT_NUMERIC_EXPORTS.supervisor_fs_response_flag_clear_tombstones;
      }
      if (!failedExecveChildOwner && processOwnerMatches) {
        disposition |= DEFAULT_NUMERIC_EXPORTS.supervisor_fs_response_flag_apply_local_metadata;
      }
      if (!failedExecveChildOwner && opcode !== FS_OP_MMAP) {
        disposition |= DEFAULT_NUMERIC_EXPORTS.supervisor_fs_response_flag_queue_thread_worker_writes;
      }
      if (opcode === FS_OP_CLOSE || opcode === FS_OP_EXECVE) {
        disposition |= DEFAULT_NUMERIC_EXPORTS.supervisor_fs_response_flag_fd_response_may_clear_live_fds;
      }
      if (resolvedToExecveSpawn) {
        disposition |= DEFAULT_NUMERIC_EXPORTS.supervisor_fs_response_flag_resolved_to_execve_spawn;
      }
      return disposition;
    },
    supervisor_fs_resident_page_slot_decision(_existingSlot: number, _emptySlot: number, _cleanEvictSlot: number) {
      if (_existingSlot >= 0) {
        return _existingSlot;
      }
      if (_emptySlot >= 0) {
        return _emptySlot;
      }
      if (_cleanEvictSlot >= 0) {
        return _cleanEvictSlot;
      }
      return -1;
    },
    supervisor_fs_resident_page_evict_status(_recordFound: number, _dirty: number) {
      if (_recordFound === 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_fs_resident_page_evict_not_found;
      }
      if (_dirty !== 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_fs_resident_page_evict_dirty_pinned;
      }
      return DEFAULT_NUMERIC_EXPORTS.supervisor_fs_resident_page_evict_evicted;
    },
    supervisor_blocked_wait_retry_delay_ms: () => 0,
    supervisor_blocked_wait_active_child_pid(this: KernelExports | undefined, vmPtr: number) {
      const receiver = currentExports(this);
      const currentPid = Number(receiver?.process_current_pid?.(vmPtr) ?? overrides.process_current_pid?.(vmPtr) ?? 0);
      return Number(receiver?.process_active_child_pid?.(currentPid) ?? overrides.process_active_child_pid?.(currentPid) ?? 0);
    },
    supervisor_blocked_wait_retry_reason(this: KernelExports | undefined, vmPtr: number, flags: number) {
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
      const activeChildPid = Number(this?.supervisor_blocked_wait_active_child_pid?.(vmPtr) ?? 0);
      if (activeChildPid > 0) {
        return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_active_child;
      }
      return DEFAULT_NUMERIC_EXPORTS.supervisor_blocked_wait_retry_reason_default;
    },
    supervisor_vfork_execve_rollback(this: KernelExports | undefined, vmPtr: number, childPid: number) {
      const receiver = currentExports(this);
      const waitStatus = Number(
        receiver?.supervisor_vfork_execve_handoff_failure_wait_status?.()
          ?? overrides.supervisor_vfork_execve_handoff_failure_wait_status?.()
          ?? DEFAULT_NUMERIC_EXPORTS.supervisor_vfork_execve_handoff_failure_wait_status,
      );
      receiver?.process_record_child_exit?.(childPid, waitStatus);
      receiver?.thread_restore_vfork_parent_without_cleanup?.(vmPtr, childPid);
      receiver?.thread_clear_vfork_runtime_state?.();
      return waitStatus;
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
    supervisor_process_memory_host_op(this: KernelExports | undefined, vmPtr: number, index: number, outPtr: number, outLen: number) {
      const recordBytes = DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_record_bytes;
      const receiver = currentExports(this);
      const memory = receiver?.memory ?? overrides.memory;
      if (!memory) {
        return -1;
      }
      if (outLen < recordBytes) {
        return -1;
      }
      const ranges = processMemoryRanges(receiver, memory, vmPtr);
      if (index === 0) {
        const initialBytes = processMemoryInitialBytes(receiver, ranges, vmPtr);
        return writeSupervisorSpawnThreadHostOpRecord(memory, outPtr, {
          vmPtr,
          childVmPtr: 0,
          kind: DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_kind_create_process_memory,
          tid: Number(receiver?.thread_spawn_child_pid?.(vmPtr) ?? overrides.thread_spawn_child_pid?.(vmPtr) ?? 1),
          processPtr: Number(receiver?.thread_process_ptr?.(vmPtr) ?? overrides.thread_process_ptr?.(vmPtr) ?? 0),
          status: Number(receiver?.thread_status?.(vmPtr) ?? overrides.thread_status?.(vmPtr) ?? 0),
          flags: BigInt(initialBytes),
          childTidPtr: BigInt(processMemoryMaxPages(receiver, initialBytes, vmPtr)),
        });
      }
      const range = ranges[index - 1];
      if (range !== undefined) {
        return writeSupervisorSpawnThreadHostOpRecord(memory, outPtr, {
          vmPtr,
          childVmPtr: 0,
          kind: DEFAULT_NUMERIC_EXPORTS.supervisor_host_op_kind_copy_memory_ranges,
          tid: Number(receiver?.thread_spawn_child_pid?.(vmPtr) ?? overrides.thread_spawn_child_pid?.(vmPtr) ?? 1),
          processPtr: Number(receiver?.thread_process_ptr?.(vmPtr) ?? overrides.thread_process_ptr?.(vmPtr) ?? 0),
          status: Number(receiver?.thread_spawn_kind?.(vmPtr) ?? overrides.thread_spawn_kind?.(vmPtr) ?? 0),
          flags: BigInt(range.start),
          childTidPtr: BigInt(range.end),
        });
      }
      return 0;
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
