import { afterEach, beforeEach, expect, test, vi } from "vite-plus/test";
import {
  createRuntimeStateBridge,
  readPublishedRuntimeBridgeEpoch,
} from "../../../../src/bridge/runtime-state.js";
import { RUNTIME_BRIDGE_NOTIFY_INDEX } from "../../../../src/layout/runtime-state.js";

vi.mock("../../../../src/worker/pool.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../../../src/worker/pool.js")>();
  return {
    ...actual,
    kernelStackSlotForWorker: () => 2,
  };
});

import { createWorkerScheduler } from "../../../../src/worker/scheduler.js";
import { registerForkSharedSyncPromise } from "../../../../src/worker/fork-shared-sync-debug.js";
import { createWorkerStatusTransitionHandler } from "../../../../src/worker/message-handler/status/transitions.js";
import { createWorkerSchedulerQueueHelpers } from "../../../../src/worker/scheduler/queue.js";
import { createRunLoopScheduler } from "../../../../src/worker/scheduler/run-loop.js";
import { createTransferDebugStats, createWorkerMessageDebugStats } from "../../../../src/worker/types.js";
import type { ProcessHandle, ThreadWorkerState } from "../../../../src/worker/types.js";
import { installSupervisorBlockedResumeStub } from "../support/supervisor-blocked-resume.js";
import {
  writeSupervisorArmHostTimerHostOpRecord,
  writeSupervisorBlockedRetryHostOpRecord,
} from "../support/supervisor-abi.js";

function makeKernel() {
  const threadResumeWait4 = vi.fn();
  let monotonicTicks = 1n;
  const pipeSyncBeforeRetryRequested = new Set<number>();
  const kernel = {
    threadResumeWait4,
    exports: {
      abi_status_ok: () => 0,
      abi_status_futex_wait: () => 20,
      abi_status_sleep: () => 26,
      abi_status_wait4: () => 31,
      abi_status_epoll_wait: () => 7,
      abi_status_vfork_wait: () => 8,
      abi_status_execve_spawn: () => 9,
      abi_status_clone_spawn: () => 10,
      abi_status_fault: () => 3,
      abi_status_pipe_wait: () => 6,
      abi_status_connect: () => 28,
      abi_status_socket_recv: () => 30,
      abi_status_socket_send: () => 29,
      abi_status_flock_wait: () => 32,
      abi_status_fs: () => 33,
      abi_max_threads: () => 64,
      abi_max_sockets: () => 1,
      thread_resume_wait4: threadResumeWait4,
      thread_resume_blocked_syscall: vi.fn(),
      thread_resume_sleep: vi.fn(),
      thread_resume_epoll_wait_timeout: vi.fn(),
      thread_resume_poll_wait_timeout: vi.fn(),
      thread_resume_socket_wait_timeout: vi.fn(),
      supervisor_complete_futex_wait: vi.fn(() => 0),
      supervisor_blocked_resume_delegation: (vmPtr: number, flags: number) => {
        const kernelWorkerAvailable = (flags & 1) !== 0;
        const status = kernel.exports.thread_status(vmPtr);
        if (kernelWorkerAvailable) {
          if (status === kernel.exports.abi_status_wait4()) {
            return 2;
          }
          if (
            status === kernel.exports.abi_status_pipe_wait()
            || status === kernel.exports.abi_status_connect()
            || status === kernel.exports.abi_status_socket_recv()
            || status === kernel.exports.abi_status_socket_send()
            || status === kernel.exports.abi_status_flock_wait()
          ) {
            return 1;
          }
          return 3;
        }
        return status === kernel.exports.abi_status_epoll_wait() ? 4 : 0;
      },
      supervisor_blocked_resume_delegation_flag_kernel_worker_available: () => 1,
      supervisor_blocked_resume_delegation_direct: () => 0,
      supervisor_blocked_resume_delegation_kernel_worker: () => 1,
      supervisor_blocked_resume_delegation_wait4_kernel_worker: () => 2,
      supervisor_blocked_resume_delegation_unsupported: () => 3,
      supervisor_blocked_resume_delegation_epoll_direct: () => 4,
      supervisor_blocked_pipe_resume_action_defer: () => 0,
      supervisor_blocked_pipe_resume_action_direct: () => 1,
      supervisor_blocked_pipe_resume_action: (vmPtr: number) => {
        if (kernel.exports.thread_status(vmPtr) !== kernel.exports.abi_status_pipe_wait()) {
          return 0;
        }
        if (kernel.exports.supervisor_blocked_wait_active_child_pid(vmPtr) > 0) {
          return 0;
        }
        if ((kernel.exports.thread_poll_wait_ready_count?.(vmPtr) ?? 0) > 0) {
          return 1;
        }
        if (
          kernel.exports.thread_blocked_pipe_read_ready?.(vmPtr) === 1
          || kernel.exports.thread_blocked_pipe_write_ready?.(vmPtr) === 1
        ) {
          return 1;
        }
        const syscallNr = kernel.exports.thread_blocked_syscall_nr?.(vmPtr)
          ?? kernel.exports.thread_a7?.(vmPtr);
        const readSyscall = kernel.exports.abi_sys_read?.();
        if (
          syscallNr === undefined
          || readSyscall === undefined
          || BigInt(syscallNr) !== BigInt(readSyscall)
          || kernel.exports.thread_poll_wait_has_socket?.(vmPtr) === 1
        ) {
          return 0;
        }
        const bufferedLen = kernel.exports.thread_blocked_pipe_buffered_len?.(vmPtr);
        const writeOpen = kernel.exports.thread_blocked_pipe_write_open?.(vmPtr);
        const readReady = bufferedLen !== undefined
          && writeOpen !== undefined
          && (bufferedLen > 0 || writeOpen <= 0);
        return readReady ? 1 : 0;
      },
      supervisor_blocked_pipe_sync_before_retry_action_none: () => 0,
      supervisor_blocked_pipe_sync_before_retry_action_request_sync: () => 1,
      supervisor_blocked_pipe_sync_before_retry_action_request_sync_and_drain_output: () => 2,
      supervisor_blocked_pipe_sync_before_retry_action_schedule_retry: () => 3,
      supervisor_blocked_pipe_sync_before_retry_flag_kernel_worker_available: () => 1,
      supervisor_blocked_pipe_sync_before_retry_action: (vmPtr: number, flags: number) => {
        const kernelWorkerAvailable = (flags & 1) !== 0;
        const timeoutMs = Number(kernel.exports.thread_poll_wait_timeout?.(vmPtr) ?? 0n);
        const readyCount = Number(kernel.exports.thread_poll_wait_ready_count?.(vmPtr) ?? 0);
        const readinessSyncPending = kernel.exports.thread_pipe_read_eof_sync_pending?.(vmPtr) === 1
          || kernel.exports.thread_poll_hup_sync_pending?.(vmPtr) === 1;
        const timedNoReady = timeoutMs >= 0 && readyCount <= 0 && !readinessSyncPending;
        if (!pipeSyncBeforeRetryRequested.has(vmPtr) && kernelWorkerAvailable) {
          pipeSyncBeforeRetryRequested.add(vmPtr);
          return timedNoReady ? 1 : 2;
        }
        return timedNoReady ? 3 : 0;
      },
      supervisor_blocked_pipe_sync_before_retry_requested: (vmPtr: number) =>
        pipeSyncBeforeRetryRequested.has(vmPtr) ? 1 : 0,
      supervisor_blocked_pipe_sync_before_retry_clear: (vmPtr: number) => {
        pipeSyncBeforeRetryRequested.delete(vmPtr);
        return 0;
      },
      supervisor_blocked_wait_retry_delay_ms: (vmPtr: number, reason: number) => {
        switch (reason) {
          case 1:
            return 0;
          case 2:
            return 10;
          case 3:
            return 5;
          case 4:
            return 25;
          case 5:
            return 50;
          default:
            return kernel.exports.thread_status(vmPtr) === kernel.exports.abi_status_wait4() ? 10 : 1;
        }
      },
      supervisor_blocked_wait_retry_reason_default: () => 0,
      supervisor_blocked_wait_retry_reason_immediate: () => 1,
      supervisor_blocked_wait_retry_reason_standard_defer: () => 2,
      supervisor_blocked_wait_retry_reason_active_child: () => 3,
      supervisor_blocked_wait_retry_reason_passive_wait: () => 4,
      supervisor_blocked_wait_retry_reason_process_gate: () => 5,
      supervisor_blocked_wait_retry_flag_host_progress: () => 1,
      supervisor_blocked_wait_retry_flag_active_child: () => 2,
      supervisor_blocked_wait_retry_flag_passive_wait: () => 4,
      supervisor_blocked_wait_retry_flag_process_gate: () => 8,
      supervisor_blocked_wait_retry_flag_thread_init_pending: () => 16,
      supervisor_blocked_wait_wake_source_pipe_sync: () => 1,
      supervisor_blocked_wait_wake_source_epoll_sync: () => 2,
      supervisor_blocked_wait_wake_source_socket_sync: () => 3,
      supervisor_blocked_wait_wake_source_pipe_and_epoll_sync: () => 4,
      supervisor_blocked_wait_wake_target_pipe_wait: () => 1,
      supervisor_blocked_wait_wake_target_epoll_wait: () => 2,
      supervisor_blocked_wait_wake_target_socket_recv: () => 4,
      supervisor_blocked_wait_wake_target_socket_send: () => 8,
      supervisor_blocked_wait_wake_mask_for_sync: (source: number) => {
        switch (source) {
          case 1:
            return 1;
          case 2:
            return 2;
          case 3:
            return 1 | 2 | 4 | 8;
          case 4:
            return 1 | 2;
          default:
            return -22;
        }
      },
      supervisor_blocked_wait_status_matches_wake_source: (status: number, source: number) => {
        const mask = kernel.exports.supervisor_blocked_wait_wake_mask_for_sync(source);
        if (mask < 0) {
          return mask;
        }
        let target = 0;
        if (status === kernel.exports.abi_status_pipe_wait()) {
          target = kernel.exports.supervisor_blocked_wait_wake_target_pipe_wait();
        } else if (status === kernel.exports.abi_status_epoll_wait()) {
          target = kernel.exports.supervisor_blocked_wait_wake_target_epoll_wait();
        } else if (status === kernel.exports.abi_status_socket_recv()) {
          target = kernel.exports.supervisor_blocked_wait_wake_target_socket_recv();
        } else if (status === kernel.exports.abi_status_socket_send()) {
          target = kernel.exports.supervisor_blocked_wait_wake_target_socket_send();
        }
        return target !== 0 && (mask & target) !== 0 ? 1 : 0;
      },
      supervisor_blocked_epoll_watches_pipe: () => 0,
      supervisor_blocked_epoll_watched_ofd: () => 0,
      supervisor_blocked_epoll_dependent_sync_ofd: (vmPtr: number, index: number) => {
        let cursor = 0;
        const maxFds = kernel.exports.abi_max_fds();
        const eventfdType = kernel.exports.abi_fd_type_eventfd?.() ?? 9;
        const timerfdType = kernel.exports.abi_fd_type_timerfd?.() ?? 10;
        for (let watchedIndex = 0; watchedIndex < maxFds; watchedIndex += 1) {
          const ofdId = kernel.exports.supervisor_blocked_epoll_watched_ofd(vmPtr, watchedIndex);
          if (ofdId === 0) {
            return 0;
          }
          const fdType = kernel.exports.kernel_ofd_fd_type(ofdId);
          if (fdType !== eventfdType && fdType !== timerfdType) {
            continue;
          }
          if (cursor === index) {
            return ofdId;
          }
          cursor += 1;
        }
        return 0;
      },
      supervisor_blocked_epoll_wait_retry_required: () => 0,
      supervisor_blocked_wait_active_child_pid: (vmPtr: number) => {
        const status = kernel.exports.thread_status(vmPtr);
        if (
          status === kernel.exports.abi_status_pipe_wait()
          && (
            (kernel.exports.thread_poll_wait_timeout?.(vmPtr) ?? -1n) >= 0n
            || kernel.exports.thread_poll_wait_has_socket?.(vmPtr) === 1
          )
        ) {
          return 0;
        }
        if (status !== kernel.exports.abi_status_pipe_wait() && status !== kernel.exports.abi_status_wait4()) {
          return 0;
        }
        const pid = kernel.exports.process_current_pid?.(vmPtr) ?? 0;
        return kernel.exports.process_active_child_pid?.(pid) ?? 0;
      },
      supervisor_blocked_wait_retry_reason: (vmPtr: number, flags: number) => {
        if ((flags & 8) !== 0) {
          return 5;
        }
        if ((flags & 2) !== 0) {
          return 3;
        }
        if ((flags & 4) !== 0) {
          return 4;
        }
        if ((flags & 1) !== 0) {
          return 1;
        }
        if ((flags & 16) !== 0) {
          return 2;
        }
        if (kernel.exports.supervisor_blocked_wait_active_child_pid(vmPtr) > 0) {
          return 3;
        }
        if (
          kernel.exports.thread_status(vmPtr) === kernel.exports.abi_status_epoll_wait()
          && kernel.exports.supervisor_blocked_epoll_wait_retry_required(vmPtr) !== 0
        ) {
          return 2;
        }
        if (
          kernel.exports.thread_status(vmPtr) === kernel.exports.abi_status_pipe_wait()
          && (kernel.exports.thread_poll_wait_ready_count?.(vmPtr) ?? 0) > 0
        ) {
          return 1;
        }
        return 0;
      },
      supervisor_futex_wait_completion_woken: () => 1,
      supervisor_futex_wait_completion_not_equal: () => 2,
      supervisor_futex_wait_completion_invalid_address: () => 4,
      supervisor_futex_wait_completion_signal_check: () => 5,
      thread_futex_wait_addr: () => 0n,
      thread_record_blocked_addr: () => 0n,
      thread_status: () => 0,
      process_current_pid: () => 1,
      supervisor_process_identity_matches: (vmPtr: number, runtimePid: number, processPtr: number) =>
        kernel.exports.process_current_pid(vmPtr) === runtimePid
        && kernel.exports.thread_process_ptr(vmPtr) === processPtr
          ? 1
          : 0,
      process_active_child_pid: () => 0,
      thread_exit_code: () => 0,
      thread_term_signal: () => 0,
      monotonic_ticks_current: () => monotonicTicks,
      sync_monotonic_ticks_floor: vi.fn((ticks: bigint | number) => {
        const nextTicks = BigInt(ticks);
        if (nextTicks > monotonicTicks) {
          monotonicTicks = nextTicks;
        }
      }),
      thread_sleep_deadline_ticks: vi.fn(() => monotonicTicks),
      thread_epoll_timerfd_deadline_ticks: vi.fn(() => 0n),
      thread_sleep_seconds: () => 0n,
      thread_sleep_nanoseconds: () => 0n,
      thread_epoll_wait_timeout: () => 0n,
      thread_poll_wait_timeout: () => -1n,
      thread_poll_wait_ready_count: () => 0,
      thread_poll_wait_has_socket: () => 0,
      thread_poll_wait_first_socket_revents: () => 0,
      thread_poll_wait_first_pipe_revents: () => 0,
      thread_a0: () => 0n,
      thread_a7: () => 0n,
      thread_blocked_fd: () => 0,
      thread_blocked_ofd_id: () => 0,
      thread_blocked_syscall_nr: () => 0n,
      thread_blocked_syscall_arg: () => 0n,
      thread_pipe_read_eof_sync_pending: () => 0,
      thread_poll_hup_sync_pending: () => 0,
      thread_socket_wait_timeout: () => -1n,
      thread_socket_idx_for_fd: () => -1,
      thread_socket_peer_idx: () => -1,
      thread_socket_state: () => 0,
      thread_socket_external_peer: () => 0,
      thread_socket_recv_available: () => 0,
      thread_socket_recv_buf_idx: () => -1,
      thread_socket_read_bytes: () => 0n,
      thread_socket_outbound_available: () => 0,
      thread_process_thread_count: () => 1,
      thread_process_ptr: () => 0x2000,
      thread_pc: () => 0x1000n,
      thread_sp: () => 0x8000n,
      thread_s11: () => 0x9000n,
      process_current_pid: () => 1,
      process_thread_tid: () => 0,
      process_thread_state: () => 0,
      process_thread_vm_ptr: (_processVmPtr: number, slot: number) => slot === 0 ? 0x1234 : 0,
      process_record_child_exit_for_parent: vi.fn(),
      process_has_active_child: () => 0,
      abi_max_fds: () => 64,
      abi_max_pipes: () => 64,
      abi_fd_type_pipe: () => 2,
      abi_fd_type_socket: () => 4,
      kernel_heap_base: () => 0,
      kernel_heap_ptr: () => 0,
      kernel_ofd_table_ptr: () => 0,
      kernel_ofd_table_bytes: () => 0,
      kernel_ofd_fd_type: () => 0,
      kernel_ofd_ino: () => 0,
      kernel_memfs_ptr: () => 0,
      kernel_memfs_header_bytes: () => 0,
      kernel_memfs_inodes_bytes: () => 0,
      kernel_memfs_first_child_bytes: () => 0,
      kernel_memfs_dir_entries_bytes: () => 0,
      kernel_memfs_data_bytes: () => 0,
      kernel_mem_state_ptr: () => 0,
      kernel_epoll_table_ptr: () => 0,
      kernel_epoll_table_bytes: () => 0,
      kernel_epoll_instance_bytes: () => 0,
      kernel_socket_table_ptr: () => 0,
      kernel_socket_table_bytes: () => 32,
      kernel_socket_used: () => 0,
      kernel_pipe_base: () => 0,
      kernel_pipe_region_bytes: () => 0,
      kernel_pipe_used: () => 0n,
      kernel_net_buf_base: () => 0,
      kernel_net_buf_pool_bytes: () => 64,
      kernel_net_buf_used: () => 0n,
      kernel_sig_slots_ptr: () => 0,
      kernel_sig_slots_bytes: () => 0,
      kernel_signal_action_table_bytes: () => 0,
      kernel_process_pending_ptr: () => 0,
      kernel_process_pending_bytes: () => 0,
      kernel_process_runtime_state_ptr: () => 0,
      kernel_process_runtime_state_bytes: () => 0,
      kernel_time_state_ptr: () => 0,
      kernel_time_state_bytes: () => 0,
      kernel_attach_runtime_state: () => {},
      process_fd_ofd_id: () => 0,
      process_fd_flags: () => 0,
    } as unknown as ProcessHandle["kernel"],
  };
  installSupervisorBlockedResumeStub(kernel.exports);
  return kernel;
}

function requestPipeSyncBeforeRetry(kernel: ReturnType<typeof makeKernel>["exports"], vmPtr: number): void {
  kernel.supervisor_blocked_pipe_sync_before_retry_action!(
    vmPtr,
    kernel.supervisor_blocked_pipe_sync_before_retry_flag_kernel_worker_available!(),
  );
}

function pipeSyncBeforeRetryRequested(kernel: ReturnType<typeof makeKernel>["exports"], vmPtr: number): boolean {
  return kernel.supervisor_blocked_pipe_sync_before_retry_requested!(vmPtr) === 1;
}

async function flushMicrotasks(count: number): Promise<void> {
  for (let index = 0; index < count; index += 1) {
    await Promise.resolve();
  }
}

function makeHostBoundaryWait4ResumeOutcome() {
  return vi.fn(async (
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    _status: number,
  ) => {
    handle.kernel.supervisor_resume_blocked_syscall?.(vmPtr, 0);
    const blocked = handle.hostBlockedWorkers.get(worker);
    if (blocked && blocked.vmPtr === vmPtr) {
      blocked.rpcPending = false;
    }
    return {
      kind: "completed" as const,
      status: handle.kernel.thread_status(vmPtr),
    };
  });
}

function makeThreadState(vmPtr: number): ThreadWorkerState {
  return {
    vmPtr,
    ownerPid: 1,
    generation: 0,
    reinitCount: 0,
    stepCount: 0,
    totalStepBudget: 0,
    totalStepWallMs: 0,
    maxStepWallMs: 0,
    lastStepDurationMs: null,
    statusCount: 0,
    statusCounts: {},
    lastStatus: null,
    lastStepAtMs: null,
    lastStatusAtMs: null,
    lastStepProcessPtr: null,
    lastStepPc: null,
    lastStepSp: null,
    lastStepS11: null,
    hotStepPcs: {},
    lastStatusPc: null,
    lastStatusSp: null,
    lastStatusS11: null,
    hotStatusPcs: {},
    initialized: true,
    waitingInWorkerStatus: null,
    ownerAsyncStatus: null,
    pendingPipeSlotsForNextStep: [],
    pendingGuestMemoryWrites: [],
    resumeWorkersOnReady: [],
  };
}

const SUPERVISOR_RESUME_RECORD_BYTES = 48;
const SUPERVISOR_RESUME_KIND_RUNNABLE_THREAD = 2;
const SUPERVISOR_FAULT_EXIT_DECISION_BYTES = 88;
const SUPERVISOR_FAULT_EXIT_KIND_THREAD_EXIT = 1;
const SUPERVISOR_FAULT_EXIT_KIND_PROCESS_EXIT = 2;

function writeSupervisorRunnableResumeRecord(
  memory: WebAssembly.Memory,
  outPtr: number,
  thread: ThreadWorkerState,
  seq = 11n,
): void {
  const view = new DataView(memory.buffer);
  view.setUint32(outPtr, 0x5255_5354, true);
  view.setUint32(outPtr + 4, 1, true);
  view.setUint32(outPtr + 8, SUPERVISOR_RESUME_RECORD_BYTES, true);
  view.setUint32(outPtr + 12, SUPERVISOR_RESUME_KIND_RUNNABLE_THREAD, true);
  view.setBigUint64(outPtr + 16, seq, true);
  view.setUint32(outPtr + 24, thread.vmPtr, true);
  view.setUint32(outPtr + 28, 2, true);
  view.setUint32(outPtr + 32, 0x2000, true);
  view.setUint32(outPtr + 36, 0, true);
  view.setBigUint64(outPtr + 40, 0n, true);
}

function installTestSupervisorFaultExitDecision(
  kernel: ProcessHandle["kernel"],
  handle: ProcessHandle,
  decision: {
    exitVmPtr: number;
    exitCode: number;
    termSignal: number;
    remainingThreads: number;
    liveThreads: number;
  },
): void {
  handle.memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  kernel.memory = handle.memory;
  kernel.kernel_scratch_base = () => 0;
  kernel.supervisor_fault_exit_decision_bytes = () => SUPERVISOR_FAULT_EXIT_DECISION_BYTES;
  kernel.supervisor_fault_exit_decision_magic = () => 0x5846_5354;
  kernel.supervisor_fault_exit_kind_thread_exit = () => SUPERVISOR_FAULT_EXIT_KIND_THREAD_EXIT;
  kernel.supervisor_fault_exit_kind_process_exit = () => SUPERVISOR_FAULT_EXIT_KIND_PROCESS_EXIT;
  kernel.supervisor_fault_exit_decision = vi.fn((vmPtr: number, outPtr: number, outLen: number) => {
    if (outLen < SUPERVISOR_FAULT_EXIT_DECISION_BYTES) {
      return -1;
    }
    const view = new DataView(handle.memory.buffer);
    view.setUint32(outPtr, 0x5846_5354, true);
    view.setUint32(outPtr + 4, 1, true);
    view.setUint32(outPtr + 8, SUPERVISOR_FAULT_EXIT_DECISION_BYTES, true);
    view.setUint32(outPtr + 12, SUPERVISOR_FAULT_EXIT_KIND_PROCESS_EXIT, true);
    view.setBigUint64(outPtr + 16, 7n, true);
    view.setUint32(outPtr + 24, vmPtr, true);
    view.setUint32(outPtr + 28, decision.exitVmPtr, true);
    view.setUint32(outPtr + 32, 2, true);
    view.setUint32(outPtr + 36, 0x2000, true);
    view.setUint32(outPtr + 40, decision.remainingThreads, true);
    view.setUint32(outPtr + 44, decision.liveThreads, true);
    view.setInt32(outPtr + 48, decision.exitCode, true);
    view.setInt32(outPtr + 52, decision.termSignal, true);
    view.setUint32(outPtr + 56, kernel.abi_status_fault(), true);
    view.setUint32(outPtr + 60, 0, true);
    view.setUint32(outPtr + 64, 0, true);
    view.setUint32(outPtr + 68, 0, true);
    view.setBigUint64(outPtr + 72, 0n, true);
    view.setBigUint64(outPtr + 80, 0n, true);
    return SUPERVISOR_FAULT_EXIT_DECISION_BYTES;
  });
}

function threadHasTestSupervisorFutexBlock(
  kernel: ProcessHandle["kernel"],
  vmPtr: number,
): boolean {
  const blockedAddr = kernel.thread_record_blocked_addr?.(vmPtr) ?? 0n;
  if (blockedAddr === 0n || blockedAddr === 0xffff_ffff_ffff_ffffn) {
    return false;
  }
  if (kernel.thread_status(vmPtr) === kernel.abi_status_futex_wait()) {
    return true;
  }
  return (kernel.thread_futex_wait_addr?.(vmPtr) ?? 0n) !== 0n;
}

function selectTestSupervisorRunnableThread(
  handle: ProcessHandle,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
): ThreadWorkerState | undefined {
  const candidates = [
    ...handle.vforkReadyWorkers,
    ...[...handle.workers].filter((worker) => !handle.vforkReadyWorkers.includes(worker)),
  ];
  for (const candidate of candidates) {
    if (candidate === handle.runningWorker) {
      continue;
    }
    const thread = threadWorkerStates.get(candidate);
    if (thread === undefined) {
      continue;
    }
    if ((thread.ownerAsyncStatus ?? null) !== null || thread.waitingInWorkerStatus !== null) {
      continue;
    }
    const status = handle.kernel.thread_status(thread.vmPtr);
    if (
      status === handle.kernel.abi_status_futex_wait()
      || threadHasTestSupervisorFutexBlock(handle.kernel, thread.vmPtr)
    ) {
      continue;
    }
    if (status !== handle.kernel.abi_status_ok()) {
      continue;
    }
    if (handle.kernel.thread_process_ptr(thread.vmPtr) === 0) {
      continue;
    }
    return thread;
  }
  return undefined;
}

function installTestSupervisorProcessResume(
  kernel: ProcessHandle["kernel"],
  handle: ProcessHandle,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
): void {
  if (kernel.supervisor_process_resume !== undefined) {
    return;
  }
  kernel.memory = handle.memory;
  kernel.kernel_scratch_base = () => 0;
  kernel.supervisor_resume_record_bytes = () => SUPERVISOR_RESUME_RECORD_BYTES;
  kernel.supervisor_resume_kind_runnable_thread = () => SUPERVISOR_RESUME_KIND_RUNNABLE_THREAD;
  let seq = 1n;
  kernel.supervisor_process_resume = vi.fn((_vmPtr: number, outPtr: number, outLen: number) => {
    if (outLen < SUPERVISOR_RESUME_RECORD_BYTES) {
      return -1;
    }
    const selectedThread = selectTestSupervisorRunnableThread(handle, threadWorkerStates);
    if (selectedThread === undefined) {
      return 0;
    }
    const view = new DataView(handle.memory.buffer);
    view.setUint32(outPtr, 0x5255_5354, true);
    view.setUint32(outPtr + 4, 1, true);
    view.setUint32(outPtr + 8, SUPERVISOR_RESUME_RECORD_BYTES, true);
    view.setUint32(outPtr + 12, SUPERVISOR_RESUME_KIND_RUNNABLE_THREAD, true);
    view.setBigUint64(outPtr + 16, seq, true);
    view.setUint32(outPtr + 24, selectedThread.vmPtr, true);
    view.setUint32(outPtr + 28, kernel.thread_tid?.(selectedThread.vmPtr) ?? 0, true);
    view.setUint32(outPtr + 32, kernel.thread_process_ptr(selectedThread.vmPtr), true);
    view.setUint32(outPtr + 36, kernel.thread_status(selectedThread.vmPtr), true);
    view.setBigUint64(outPtr + 40, 0n, true);
    seq += 1n;
    return SUPERVISOR_RESUME_RECORD_BYTES;
  });
}

function expectTestSupervisorProcessResumeSelects(
  handle: ProcessHandle,
  expectedVmPtr: number,
): void {
  const recordBytes = handle.kernel.supervisor_resume_record_bytes?.() ?? 0;
  expect(recordBytes).toBe(SUPERVISOR_RESUME_RECORD_BYTES);
  const scratch = handle.kernel.kernel_scratch_base?.() ?? 0;
  const result = handle.kernel.supervisor_process_resume?.(handle.vmPtr, scratch, recordBytes);
  expect(result).toBe(SUPERVISOR_RESUME_RECORD_BYTES);
  const view = new DataView(handle.memory.buffer);
  expect(view.getUint32(scratch + 12, true)).toBe(SUPERVISOR_RESUME_KIND_RUNNABLE_THREAD);
  expect(view.getUint32(scratch + 24, true)).toBe(expectedVmPtr);
}

interface TestSupervisorTimerHostOp {
  vmPtr: number;
  status: number;
  delayMs: bigint;
  deadlineTicks: bigint;
  seq: bigint;
}

function testTimerfdDelayMs(kernel: ProcessHandle["kernel"], deadlineTicks: bigint): bigint {
  const currentTicks = BigInt(kernel.monotonic_ticks_current?.() ?? 0n);
  if (deadlineTicks <= currentTicks) {
    return 0n;
  }
  const ticksPerSec = 100_000_000n;
  return ((deadlineTicks - currentTicks) * 1000n + (ticksPerSec - 1n)) / ticksPerSec;
}

function selectTestSupervisorBlockedWaitTimer(
  kernel: ProcessHandle["kernel"],
  vmPtr: number,
  seq: bigint,
): TestSupervisorTimerHostOp | null {
  const status = kernel.thread_status(vmPtr);
  if (status === kernel.abi_status_epoll_wait()) {
    const timeoutMs = BigInt(kernel.thread_epoll_wait_timeout(vmPtr));
    if (timeoutMs >= 0n) {
      return { vmPtr, status, delayMs: timeoutMs, deadlineTicks: 0n, seq };
    }
    const deadlineTicks = BigInt(kernel.thread_epoll_timerfd_deadline_ticks?.(vmPtr) ?? 0n);
    return deadlineTicks === 0n
      ? null
      : { vmPtr, status, delayMs: testTimerfdDelayMs(kernel, deadlineTicks), deadlineTicks, seq };
  }
  if (status === kernel.abi_status_pipe_wait()) {
    const timeoutMs = BigInt(kernel.thread_poll_wait_timeout?.(vmPtr) ?? -1n);
    if (timeoutMs < 0n || (kernel.thread_poll_wait_ready_count?.(vmPtr) ?? 0) > 0) {
      return null;
    }
    return { vmPtr, status, delayMs: timeoutMs, deadlineTicks: 0n, seq };
  }
  if (
    status === kernel.abi_status_connect()
    || status === kernel.abi_status_socket_recv()
    || status === kernel.abi_status_socket_send()
  ) {
    const timeoutMs = BigInt(kernel.thread_socket_wait_timeout?.(vmPtr) ?? -1n);
    return timeoutMs < 0n ? null : { vmPtr, status, delayMs: timeoutMs, deadlineTicks: 0n, seq };
  }
  return null;
}

function completeTestSupervisorBlockedWaitTimer(
  kernel: ProcessHandle["kernel"],
  memory: WebAssembly.Memory,
  completionPtr: number,
  completionLen: number,
): number {
  if (completionLen < 64) {
    return -1;
  }
  const view = new DataView(memory.buffer, completionPtr, completionLen);
  const vmPtr = view.getUint32(24, true);
  const deadlineTicks = view.getBigUint64(56, true);
  if (deadlineTicks !== 0n) {
    kernel.sync_monotonic_ticks_floor?.(deadlineTicks);
  }
  const status = kernel.thread_status(vmPtr);
  if (status === kernel.abi_status_epoll_wait()) {
    if (Number(kernel.thread_epoll_wait_timeout(vmPtr)) >= 0) {
      kernel.thread_resume_epoll_wait_timeout?.(vmPtr);
    } else {
      kernel.thread_resume_blocked_syscall?.(vmPtr);
    }
  } else if (status === kernel.abi_status_pipe_wait()) {
    kernel.thread_resume_poll_wait_timeout?.(vmPtr);
  } else if (
    status === kernel.abi_status_connect()
    || status === kernel.abi_status_socket_recv()
    || status === kernel.abi_status_socket_send()
  ) {
    kernel.thread_resume_socket_wait_timeout?.(vmPtr);
  }
  return kernel.thread_status(vmPtr);
}

function installTestSupervisorBlockedWaitTimers(
  kernel: ProcessHandle["kernel"],
  handle: ProcessHandle,
): void {
  if (kernel.supervisor_next_host_op !== undefined || kernel.supervisor_enqueue_blocked_wait_timer !== undefined) {
    return;
  }
  kernel.memory = handle.memory;
  kernel.kernel_heap_ptr = () => -0x3c000;
  kernel.supervisor_abi_version = () => 1;
  kernel.supervisor_host_op_record_bytes = () => 64;
  kernel.supervisor_host_op_kind_arm_host_timer = () => 12;
  kernel.supervisor_host_op_kind_retry_blocked_worker = () => 15;
  kernel.supervisor_host_completion_outcome_success = () => 1;
  kernel.supervisor_host_completion_outcome_failure = () => 2;
  const pendingTimers: TestSupervisorTimerHostOp[] = [];
  const pendingRetryTimers: Array<{
    vmPtr: number;
    delayMs: bigint;
    reason: bigint;
    seq: bigint;
    status: number;
  }> = [];
  let seq = 1n;
  kernel.supervisor_enqueue_blocked_wait_timer = vi.fn((vmPtr: number) => {
    const timer = selectTestSupervisorBlockedWaitTimer(kernel, vmPtr, seq);
    if (timer === null) {
      return 0;
    }
    seq += 1n;
    pendingTimers.push(timer);
    return 1;
  });
  kernel.supervisor_enqueue_blocked_retry_timer = vi.fn((vmPtr: number, flags: number) => {
    const reason = kernel.supervisor_blocked_wait_retry_reason!(vmPtr, flags);
    const delayMs = kernel.supervisor_blocked_wait_retry_delay_ms!(vmPtr, reason);
    if (delayMs < 0) {
      return delayMs;
    }
    pendingRetryTimers.push({
      vmPtr,
      delayMs: BigInt(delayMs),
      reason: BigInt(reason),
      seq,
      status: kernel.thread_status(vmPtr),
    });
    seq += 1n;
    return 1;
  });
  kernel.supervisor_blocked_wait_timer_required = vi.fn((vmPtr: number) =>
    selectTestSupervisorBlockedWaitTimer(kernel, vmPtr, seq) === null ? 0 : 1);
  kernel.supervisor_blocked_epoll_wait_timeout_ms = vi.fn((vmPtr: number) => {
    if (vmPtr === 0) {
      return -14;
    }
    return kernel.thread_status(vmPtr) === kernel.abi_status_epoll_wait()
      ? Number(kernel.thread_epoll_wait_timeout(vmPtr))
      : -1;
  });
  const drainHostOp = (kind: number | null, outPtr: number, outLen: number): number => {
    if (outLen < 64) {
      return -1;
    }
    if (kind === null || kind === kernel.supervisor_host_op_kind_arm_host_timer!()) {
      const timer = pendingTimers.shift();
      if (timer !== undefined) {
        return writeSupervisorArmHostTimerHostOpRecord(handle.memory, outPtr, {
          vmPtr: timer.vmPtr,
          delayMs: timer.delayMs,
          deadlineTicks: timer.deadlineTicks,
          seq: timer.seq,
          tid: kernel.process_thread_tid?.(timer.vmPtr) ?? 0,
          processPtr: kernel.thread_process_ptr(timer.vmPtr),
          status: timer.status,
        });
      }
    }
    if (kind === null || kind === kernel.supervisor_host_op_kind_retry_blocked_worker!()) {
      const timer = pendingRetryTimers.shift();
      if (timer !== undefined) {
        return writeSupervisorBlockedRetryHostOpRecord(handle.memory, outPtr, {
          vmPtr: timer.vmPtr,
          delayMs: timer.delayMs,
          reason: timer.reason,
          seq: timer.seq,
          tid: kernel.thread_tid?.(timer.vmPtr) ?? 0,
          processPtr: kernel.thread_process_ptr(timer.vmPtr),
          status: timer.status,
        });
      }
    }
    return 0;
  };
  kernel.supervisor_next_host_op = vi.fn((outPtr: number, outLen: number) =>
    drainHostOp(null, outPtr, outLen));
  kernel.supervisor_next_host_op_kind = vi.fn((kind: number, outPtr: number, outLen: number) =>
    drainHostOp(kind, outPtr, outLen));
  kernel.supervisor_complete_host_op = vi.fn((completionPtr: number, completionLen: number) =>
    completeTestSupervisorBlockedWaitTimer(kernel, handle.memory, completionPtr, completionLen));
}

function makeHandle(
  kernel: ProcessHandle["kernel"],
  worker: Worker,
  threadWorkerStates?: WeakMap<Worker, ThreadWorkerState>,
): ProcessHandle {
  const handle: ProcessHandle = {
    runtimeId: 1,
    pid: 1,
    parentPid: null,
    vmPtr: 0x1234,
    guestMemoryPolicyLimitBytes: 0,
    maxMemoryPages: 0,
    forkMaxMemoryPages: 0,
    guestBase: 0,
    worker,
    memory: new WebAssembly.Memory({ initial: 4, maximum: 16, shared: true }),
    wasmModule: {} as WebAssembly.Module,
    kernel,
    kernelState: {
      heapBase: 0,
      heapPtr: 0,
      ofdTablePtr: 0,
      memfsPtr: 0,
      memfsHeaderBytes: 0,
      memfsInodesBytes: 0,
      memfsFirstChildBytes: 0,
      memfsDirEntriesBytes: 0,
      memfsDataBytes: 0,
      memStatePtr: 0,
      epollTablePtr: 0,
      socketTablePtr: 0,
      socketUsed: 0,
      pipeBase: 0,
      pipeUsed: 0n,
      netBufBase: 0,
      netBufUsed: 0n,
      sigSlotsPtr: 0,
      processPendingPtr: 0,
      processRuntimeStatePtr: 0,
      timeStatePtr: 0,
    },
    stdinPipe: 0,
    stdoutPipe: 0,
    stderrPipe: 0,
    ttyPtySlot: 0,
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 0,
      stderrPipe: 0,
      refCount: 1,
    },
    stdioOwner: null,
    kernelShare: {
      members: new Set(),
      sharedOfdIds: new Set(),
      ofdSlotSnapshots: new Map(),
      forkSharedSyncPromise: null,
    },
    stdinQueue: [],
    stdinFlushPromise: null,
    outputBacklog: [],
    debugCommand: "/bin/sh",
    lastDebugMessage: undefined,
    runtimeBridge: createRuntimeStateBridge(),
    sabPageCache: null,
    kernelWorkerRequest: undefined,
    workers: new Set([worker]),
    vforkReadyWorkers: [],
    runningWorker: null,
    hostBlockedWorkers: new Map(),
    pendingChildSpawnRecords: 1,
    pipeSyncPromise: null,
    pendingPipeSlotSnapshots: [],
    epollSyncPromise: null,
    forkSharedSyncPromise: null,
    processMemfsDirty: false,
    ofdSyncPromise: null,
    ofdSyncQueuedIds: new Set(),
    socketSyncPromise: null,
    networkConnections: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    networkRecvPayloadWatermarks: new Map(),
    transferDebugStats: createTransferDebugStats(),
    workerMessageDebugStats: createWorkerMessageDebugStats(),
    vforkSnapshot: null,
    vforkWorker: null,
    exitInProgress: false,
    onExit: undefined,
    onError: undefined,
    onStdout: undefined,
    workerDebugLog: [],
    guestWriteWatchRegions: [],
  };
  if (threadWorkerStates !== undefined) {
    installTestSupervisorProcessResume(kernel, handle, threadWorkerStates);
    installTestSupervisorBlockedWaitTimers(kernel, handle);
  }
  return handle;
}

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

function makeStatusTransitionDeps(
  handle: ProcessHandle,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
  overrides: Partial<Parameters<typeof createWorkerStatusTransitionHandler>[0]> = {},
): Parameters<typeof createWorkerStatusTransitionHandler>[0] {
  return {
    processHandles: new Map([[handle.pid, handle]]),
    threadWorkerStates,
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    attachThreadWorker: vi.fn(),
    blockWorker: vi.fn(),
    canonicalizeActiveVforkChildViaKernelWorker: vi.fn(async () => ({ kind: "completed" })),
    captureVforkSnapshot: vi.fn(() => ({
      vmPtr: handle.vmPtr,
      childPid: null,
      kernelState: handle.kernelState,
      stackSnapshot: null,
    }) as NonNullable<ProcessHandle["vforkSnapshot"]>),
    clearVforkSnapshot: vi.fn(),
    clearBlockedWorker: vi.fn(),
    completeProcessExit: vi.fn(async () => {}),
    commitNetworkConnect: vi.fn(async () => {}),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn((_handle, error) => {
      throw error;
    }),
    handleExecveSpawn: vi.fn(async () => {}),
    notifyWorkerStatusProgress: vi.fn(),
    markRuntimeFsDirty: vi.fn(),
    pumpProcess: vi.fn(),
    pruneDetachedProcessWorkers: vi.fn(),
    pumpNetworkConnections: vi.fn(async () => false),
    requestEpollDependentOfdStateSync: vi.fn(async () => {}),
    requestEpollStateSync: vi.fn(async () => {}),
    requestPipeStateSync: vi.fn(async () => {}),
    requestSocketStateSync: vi.fn(async () => {}),
    resolveNetworkConnect: vi.fn(async () => 0n),
    resumeConnectViaKernelWorker: vi.fn(async () => ({ kind: "completed" })),
    resumeFsViaKernelWorker: vi.fn(async () => {}),
    resumeVforkParentAfterChildExit: vi.fn(() => false),
    statusDefersOwnedStateSync: vi.fn(() => false),
    syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncForkSharedStateViaKernelWorker: vi.fn(async () => {}),
    syncProcessMemfsViaKernelWorker: vi.fn(async () => {}),
    syncProcessLifecycleViaKernelWorker: vi.fn(async () => {}),
    ...overrides,
  };
}

test("ok status defers re-enqueue while a blocked epoll retry rpc is pending", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x5860320);
  const blockedThread = makeThreadState(0x6286130);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.workers.add(blockedWorker);
  handle.hostBlockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    rpcPending: false,
    rpcGeneration: 0,
  });
  const enqueueWorker = vi.fn();
  const notifyWorkerStatusProgress = vi.fn(() => {
    const blocked = handle.hostBlockedWorkers.get(blockedWorker);
    if (blocked) {
      blocked.rpcPending = true;
    }
  });
  const handleStatusTransition = createWorkerStatusTransitionHandler(makeStatusTransitionDeps(
    handle,
    threadWorkerStates,
    {
      enqueueWorker,
      notifyWorkerStatusProgress,
    },
  ));

  handleStatusTransition({
    handle,
    worker,
    thread,
    msg: {
      type: "status",
      generation: thread.generation,
      status: kernel.abi_status_ok(),
      exitCode: 0,
      termSignal: 0,
      pc: 0x1000n,
      vmPtr: thread.vmPtr,
      kernelState: handle.kernelState,
    },
    status: kernel.abi_status_ok(),
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 0,
  });

  expect(notifyWorkerStatusProgress).toHaveBeenCalledWith(handle);
  expect(enqueueWorker).not.toHaveBeenCalled();

  await vi.advanceTimersByTimeAsync(1);
  expect(enqueueWorker).not.toHaveBeenCalled();

  const blocked = handle.hostBlockedWorkers.get(blockedWorker);
  expect(blocked).toBeDefined();
  blocked!.rpcPending = false;
  await vi.advanceTimersByTimeAsync(1);

  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);
});

test("ok status re-enqueues immediately when no blocked retry rpc is pending", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x5860320);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = (vmPtr: number) => vmPtr === thread.vmPtr
    ? kernel.abi_status_futex_wait()
    : kernel.abi_status_ok();
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  const enqueueWorker = vi.fn();
  const notifyWorkerStatusProgress = vi.fn();
  const handleStatusTransition = createWorkerStatusTransitionHandler(makeStatusTransitionDeps(
    handle,
    threadWorkerStates,
    {
      enqueueWorker,
      notifyWorkerStatusProgress,
    },
  ));

  handleStatusTransition({
    handle,
    worker,
    thread,
    msg: {
      type: "status",
      generation: thread.generation,
      status: kernel.abi_status_ok(),
      exitCode: 0,
      termSignal: 0,
      pc: 0x1000n,
      vmPtr: thread.vmPtr,
      kernelState: handle.kernelState,
    },
    status: kernel.abi_status_ok(),
    wroteToPipe: false,
    drainedPipe: false,
    completedSyncEffects: 0,
  });

  expect(notifyWorkerStatusProgress).toHaveBeenCalledWith(handle);
  expect(enqueueWorker).toHaveBeenCalledWith(handle, worker);
});

test("scheduler uses the Node step budget when Node process globals are present", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  const handle = makeHandle(kernel, worker, threadWorkerStates);

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.enqueueWorker(handle, worker);

  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: 5_000_000,
  }));
});

test("scheduler uses pending worker kernel state for one active vfork child step", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x1234) as ThreadWorkerState & {
    pendingKernelStateForNextStep?: ProcessHandle["kernelState"];
  };
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  const parentKernelState = {
    ...handle.kernelState,
    heapPtr: 0x111000,
    pipeUsed: 1n,
  };
  const childKernelState = {
    ...handle.kernelState,
    heapPtr: 0x222000,
    pipeUsed: 3n,
  };
  handle.kernelState = parentKernelState;
  handle.vforkWorker = worker;
  handle.vforkSnapshot = {
    vmPtr: 0x1234,
    parentPid: 1,
    parentProcessPtr: 0x2000,
    parentPc: 0x1000n,
    parentSp: 0x8000n,
    parentS11: 0x9000n,
    childPid: 3,
    nextPid: 4,
    pgid: 1,
    sid: 1,
    inheritedSharedOfdIds: [],
  };
  thread.pendingKernelStateForNextStep = childKernelState;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.enqueueWorker(handle, worker);

  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    kernelState: childKernelState,
  }));
  expect(thread.pendingKernelStateForNextStep).toBeUndefined();
});

test("blocked FS retry pumps after clearing owner async status", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x1234);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_fs();
  kernel.thread_status = () => status;
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  const resumeFsViaKernelWorkerOutcome = vi.fn(async () => {
    expect(thread.ownerAsyncStatus).toBe(kernel.abi_status_fs());
    status = kernel.abi_status_ok();
    return { kind: "completed" as const };
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeFsViaKernelWorkerOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => ({ kind: "completed" }),
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, kernel.abi_status_fs(), 0x1234);
  await vi.advanceTimersByTimeAsync(1);
  await Promise.resolve();

  expect(resumeFsViaKernelWorkerOutcome).toHaveBeenCalledWith(handle, worker, 0x1234);
  expect(thread.ownerAsyncStatus).toBeNull();
  expect(handle.hostBlockedWorkers.has(worker)).toBe(false);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("blocked FS retry defers owner-async enqueue until async clears", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x1234);
  thread.stepCount = 3;
  thread.statusCount = 2;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_fs();
  kernel.thread_status = () => status;
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  let scheduler: ReturnType<typeof createWorkerScheduler>;
  const resumeFsViaKernelWorkerOutcome = vi.fn(async () => {
    expect(thread.ownerAsyncStatus).toBe(kernel.abi_status_fs());
    status = kernel.abi_status_ok();
    scheduler.enqueueWorker(handle, worker);
    expect(worker.postMessage).not.toHaveBeenCalled();
    return { kind: "completed" as const };
  });

  scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeFsViaKernelWorkerOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => ({ kind: "completed" }),
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, kernel.abi_status_fs(), 0x1234);
  await vi.advanceTimersByTimeAsync(1);
  await Promise.resolve();

  expect(resumeFsViaKernelWorkerOutcome).toHaveBeenCalledWith(handle, worker, 0x1234);
  expect(thread.ownerAsyncStatus).toBeNull();
  expect(handle.hostBlockedWorkers.has(worker)).toBe(false);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("pumpProcess keeps a fresh owner-async worker parked", () => {
  vi.setSystemTime(new Date(10_000));
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x1234);
  thread.ownerAsyncStatus = 6;
  thread.stepCount = 8;
  thread.statusCount = 8;
  thread.lastStatusAtMs = 9_000;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => ({ kind: "completed" }),
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(thread.ownerAsyncStatus).toBe(6);
  expect(worker.postMessage).not.toHaveBeenCalled();
});

test("wait4 retry does not gate on local child spawn bookkeeping", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel, threadResumeWait4 } = makeKernel();
  let status = kernel.abi_status_wait4();
  kernel.thread_status = () => status;
  kernel.thread_resume_wait4 = vi.fn((vmPtr: number) => {
    threadResumeWait4(vmPtr);
    status = kernel.abi_status_ok();
  });
  const retryReason = vi.fn(kernel.supervisor_blocked_wait_retry_reason);
  kernel.supervisor_blocked_wait_retry_reason = retryReason;
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  const executeHostBlockedSyscallResumeOutcome = makeHostBoundaryWait4ResumeOutcome();

  const lifecycleEvents: string[] = [];
  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      lifecycleEvents.push(`fail:${error.message}`);
      throw error;
    },
    executeHostBlockedSyscallResumeOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_wait4(), 0x1234);
  await flushMicrotasks(2);
  expect(retryReason).not.toHaveBeenCalledWith(
    0x1234,
    kernel.supervisor_blocked_wait_retry_flag_process_gate(),
  );
  expect(executeHostBlockedSyscallResumeOutcome).toHaveBeenCalledWith(
    handle,
    worker,
    0x1234,
    handle.kernel.abi_status_wait4(),
  );
  expect(threadResumeWait4).toHaveBeenCalledWith(0x1234);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("blocked retry delegates vfork process gate retry reason to kernel", () => {
  const vforkWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const vforkThread = makeThreadState(0x1234);
  const blockedThread = makeThreadState(0x5678);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(vforkWorker, vforkThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = (vmPtr: number) => vmPtr === blockedThread.vmPtr
    ? kernel.abi_status_pipe_wait()
    : kernel.abi_status_ok();
  const retryReason = vi.fn((vmPtr: number, flags: number) => {
    expect(vmPtr).toBe(blockedThread.vmPtr);
    expect(flags).toBe(0);
    return kernel.supervisor_blocked_wait_retry_reason_process_gate();
  });
  kernel.supervisor_blocked_wait_retry_reason = retryReason;
  const handle = makeHandle(kernel, blockedWorker, threadWorkerStates);
  handle.workers.add(vforkWorker);
  handle.vforkWorker = vforkWorker;
  handle.vforkReadyWorkers = [vforkWorker];
  handle.vforkSnapshot = {
    vmPtr: vforkThread.vmPtr,
    parentPid: 1,
    parentProcessPtr: 0x2000,
    parentPc: 0x1000n,
    parentSp: 0x8000n,
    parentS11: 0x9000n,
    childPid: 2,
    nextPid: 3,
    pgid: 1,
    sid: 1,
    inheritedSharedOfdIds: [],
  };
  handle.hostBlockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_pipe_wait(),
    rpcPending: false,
    rpcGeneration: 0,
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.notifySchedulerProgress(handle);

  expect(retryReason).toHaveBeenCalledWith(blockedThread.vmPtr, 0);
  expect(blockedWorker.postMessage).not.toHaveBeenCalled();
  expect(vforkWorker.postMessage).not.toHaveBeenCalled();
});

test("wait4 retry delegates to kernel worker when kernel-worker support is available", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel, threadResumeWait4 } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_wait4();
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn(async () => ({
    type: "wait4-resumed",
    requestId: 0,
    kernelState: handle.kernelState,
  }));

  const resumeWait4ViaKernelWorker = vi.fn(async () => ({ kind: "blocked" as const, status: kernel.abi_status_wait4() }));
  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker,
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_wait4(), 0x1234);
  await vi.advanceTimersByTimeAsync(11);

  expect(resumeWait4ViaKernelWorker).toHaveBeenCalledWith(handle, worker, 0x1234);
  expect(threadResumeWait4).not.toHaveBeenCalled();
  expect(handle.hostBlockedWorkers.has(worker)).toBe(true);
});

test("wait4 retry clears stale blocked state when kernel status is already ok", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel, threadResumeWait4 } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  const resumeDelegation = vi.fn(kernel.supervisor_blocked_resume_delegation);
  kernel.supervisor_blocked_resume_delegation = resumeDelegation;
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn(async () => {
    throw new Error("stale wait4 must not delegate to the kernel worker");
  });
  const lifecycleEvents: string[] = [];
  const failProcess = vi.fn((_handle: ProcessHandle, error: Error) => {
    throw error;
  });
  const resumeWait4ViaKernelWorker = vi.fn(async () => ({ kind: "completed" as const }));

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker,
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_wait4(), 0x1234);

  expect(handle.hostBlockedWorkers.has(worker)).toBe(false);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(lifecycleEvents).toContain("wait4-retry-stale:1:vm=0x1234:status=0");
  expect(lifecycleEvents).toContain("wait4-resumed-supervisor:1");
  expect(resumeDelegation).not.toHaveBeenCalled();
  expect(resumeWait4ViaKernelWorker).not.toHaveBeenCalled();
  expect(threadResumeWait4).not.toHaveBeenCalled();
  expect(failProcess).not.toHaveBeenCalled();
});

test("epoll retry wakes a running worker instead of resuming under it", async () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedVmPtr = 0x1234;
  const runningVmPtr = 0x5678;
  const blockedThread = makeThreadState(blockedVmPtr);
  const runningThread = makeThreadState(runningVmPtr);
  runningThread.runningGeneration = runningThread.generation;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(blockedWorker, blockedThread);
  threadWorkerStates.set(runningWorker, runningThread);
  const { exports: kernel } = makeKernel();
  kernel.abi_max_threads = () => 4;
  kernel.abi_fd_type_epoll = () => 12;
  kernel.thread_status = (vmPtr: number) =>
    vmPtr === runningVmPtr ? kernel.abi_status_ok() : kernel.abi_status_epoll_wait();
  kernel.thread_epoll_wait_timeout = () => -1n;
  kernel.thread_process_thread_count = () => 2;
  kernel.supervisor_blocked_epoll_wait_retry_required = () => 1;
  kernel.process_thread_vm_ptr = (_processVmPtr: number, slot: number) => {
    if (slot === 0) {
      return blockedVmPtr;
    }
    if (slot === 1) {
      return runningVmPtr;
    }
    return 0;
  };
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => (fd === 4 ? 10 : 0);
  kernel.kernel_ofd_fd_type = (ofdId: number) => (ofdId === 10 ? kernel.abi_fd_type_epoll() : 0);
  kernel.kernel_ofd_ino = (ofdId: number) => (ofdId === 10 ? 4 : 0);
  kernel.thread_blocked_ofd_id = () => 10;
  kernel.thread_resume_blocked_syscall = vi.fn();
  const handle = makeHandle(kernel, blockedWorker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.workers = new Set([blockedWorker, runningWorker]);
  handle.runningWorker = runningWorker;
  handle.kernelWorkerRequest = vi.fn(async () => {
    throw new Error("epoll retry must not sync or resume while a worker is running");
  });
  const lifecycleEvents: string[] = [];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => ({ kind: "completed" }),
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_epoll_wait(), blockedVmPtr);
  const notify = new Int32Array(handle.runtimeBridge.notifySab);
  const notifyBefore = Atomics.load(notify, RUNTIME_BRIDGE_NOTIFY_INDEX);

  scheduler.notifySchedulerProgress(handle);
  await flushMicrotasks(4);

  expect(handle.kernelWorkerRequest).not.toHaveBeenCalled();
  expect(Atomics.load(notify, RUNTIME_BRIDGE_NOTIFY_INDEX)).toBeGreaterThan(notifyBefore);
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handle.hostBlockedWorkers.has(blockedWorker)).toBe(true);
  expect(lifecycleEvents).not.toContain("epoll-retry-result:1:vm=0x1234:status=22:a0=0");
});

test("host-local epoll retry treats timeout zero as expired", async () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(blockedWorker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_epoll_wait();
  kernel.thread_status = () => status;
  kernel.thread_epoll_wait_timeout = () => 0n;
  kernel.abi_max_threads = () => 4;
  kernel.process_thread_vm_ptr = (_processVmPtr: number, slot: number) => slot === 0 ? 0x1234 : 0;
  kernel.abi_fd_type_epoll = () => 7;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 4 ? 5 : 0;
  kernel.thread_blocked_ofd_id = () => 5;
  kernel.kernel_ofd_fd_type = (ofdId: number) => ofdId === 5 ? 7 : 0;
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    throw new Error("timeout-zero epoll wait must not re-dispatch the blocked syscall");
  });
  kernel.thread_resume_epoll_wait_timeout = vi.fn(() => {
    status = kernel.abi_status_ok();
  });
  const handle = makeHandle(kernel, blockedWorker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => ({ kind: "completed" }),
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_epoll_wait(), 0x1234);
  const blocked = handle.hostBlockedWorkers.get(blockedWorker);
  expect(blocked).toBeDefined();
  scheduler.clearHostBlockedRetryTimer(handle, blockedWorker);

  scheduler.notifySchedulerProgress(handle);
  await vi.waitFor(() => {
    expect(kernel.thread_resume_epoll_wait_timeout).toHaveBeenCalledWith(0x1234);
  });

  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handle.hostBlockedWorkers.has(blockedWorker)).toBe(false);
  expect(blockedWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
  }));
});

test("epoll kernel-worker retry completes timeout through supervisor timer HostOp", async () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(blockedWorker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_epoll_wait();
  kernel.thread_status = () => status;
  kernel.thread_epoll_wait_timeout = () => 0n;
  kernel.abi_max_threads = () => 4;
  kernel.process_thread_vm_ptr = (_processVmPtr: number, slot: number) => slot === 0 ? 0x1234 : 0;
  kernel.abi_fd_type_epoll = () => 7;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 4 ? 5 : 0;
  kernel.thread_blocked_ofd_id = () => 5;
  kernel.kernel_ofd_fd_type = (ofdId: number) => ofdId === 5 ? 7 : 0;
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    throw new Error("epoll timeout completion must not re-dispatch the blocked syscall in the scheduler");
  });
  kernel.thread_resume_epoll_wait_timeout = vi.fn(() => {
    status = kernel.abi_status_ok();
  });
  const handle = makeHandle(kernel, blockedWorker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn(async (message) => {
    expect(message.type).toBe("sync-epoll-state");
    return {
      type: "epoll-state-synced",
      requestId: 0,
      kernelState: message.kernelState,
    };
  });
  const lifecycleEvents: string[] = [];
  const executeHostBlockedSyscallResumeOutcome = vi.fn(async () => {
    status = kernel.abi_status_ok();
    return { kind: "completed" as const, status };
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    executeHostBlockedSyscallResumeOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => ({ kind: "completed" }),
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_epoll_wait(), 0x1234);
  const blocked = handle.hostBlockedWorkers.get(blockedWorker);
  expect(blocked).toBeDefined();
  scheduler.clearHostBlockedRetryTimer(handle, blockedWorker);

  scheduler.notifySchedulerProgress(handle);
  await vi.waitFor(() => {
    expect(kernel.supervisor_complete_host_op).toHaveBeenCalledWith(expect.any(Number), 64);
  });

  expect(executeHostBlockedSyscallResumeOutcome).not.toHaveBeenCalled();
  expect(kernel.thread_resume_epoll_wait_timeout).toHaveBeenCalledWith(0x1234);
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(lifecycleEvents).toContain("supervisor-host-timer-arm:1:vm=0x1234:seq=1:delay=0");
  expect(handle.hostBlockedWorkers.has(blockedWorker)).toBe(false);
});

test("epoll sync timeout zero survives a running-worker retry skip", async () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedVmPtr = 0x1234;
  const runningVmPtr = 0x5678;
  const blockedThread = makeThreadState(blockedVmPtr);
  const runningThread = makeThreadState(runningVmPtr);
  runningThread.runningGeneration = runningThread.generation;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(blockedWorker, blockedThread);
  threadWorkerStates.set(runningWorker, runningThread);
  const { exports: kernel } = makeKernel();
  let timeoutMs = -1n;
  let blockedStatus = kernel.abi_status_epoll_wait();
  kernel.abi_max_threads = () => 4;
  kernel.abi_fd_type_epoll = () => 7;
  kernel.thread_status = (vmPtr: number) =>
    vmPtr === runningVmPtr ? kernel.abi_status_ok() : blockedStatus;
  kernel.thread_epoll_wait_timeout = () => timeoutMs;
  kernel.thread_process_thread_count = () => 2;
  kernel.supervisor_blocked_epoll_wait_retry_required = () => 1;
  kernel.process_thread_vm_ptr = (_processVmPtr: number, slot: number) => {
    if (slot === 0) {
      return blockedVmPtr;
    }
    if (slot === 1) {
      return runningVmPtr;
    }
    return 0;
  };
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 4 ? 5 : 0;
  kernel.thread_blocked_ofd_id = () => 5;
  kernel.kernel_ofd_fd_type = (ofdId: number) => ofdId === 5 ? 7 : 0;
  kernel.thread_resume_blocked_syscall = vi.fn();
  kernel.thread_resume_epoll_wait_timeout = vi.fn(() => {
    blockedStatus = kernel.abi_status_ok();
  });
  const handle = makeHandle(kernel, blockedWorker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.workers = new Set([blockedWorker, runningWorker]);
  handle.kernelWorkerRequest = vi.fn(async (message) => {
    expect(message.type).toBe("sync-epoll-state");
    timeoutMs = 0n;
    handle.runningWorker = runningWorker;
    return {
      type: "epoll-state-synced",
      requestId: 0,
      kernelState: message.kernelState,
    };
  });
  const lifecycleEvents: string[] = [];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => ({ kind: "completed" }),
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_epoll_wait(), blockedVmPtr);
  const blocked = handle.hostBlockedWorkers.get(blockedWorker);
  expect(blocked).toBeDefined();
  scheduler.clearHostBlockedRetryTimer(handle, blockedWorker);

  scheduler.notifySchedulerProgress(handle);
  await vi.waitFor(() => {
    expect(lifecycleEvents.some((event) => event.startsWith("epoll-retry-skip:1:running=1:reason=standard-defer"))).toBe(true);
  });

  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(kernel.thread_resume_epoll_wait_timeout).toHaveBeenCalledWith(blockedVmPtr);
  expect(handle.hostBlockedWorkers.has(blockedWorker)).toBe(true);
  scheduler.clearHostBlockedRetryTimer(handle, blockedWorker);
});

test("epoll retry defers while clone child init is expected to release parent", async () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const parentWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const childWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedThread = makeThreadState(0x1234);
  const parentThread = makeThreadState(0x5678);
  const childThread = makeThreadState(0x9abc);
  parentThread.ownerAsyncStatus = 10;
  childThread.initialized = false;
  childThread.resumeWorkersOnReady = [parentWorker];
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(blockedWorker, blockedThread);
  threadWorkerStates.set(parentWorker, parentThread);
  threadWorkerStates.set(childWorker, childThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234
    ? kernel.abi_status_epoll_wait()
    : kernel.abi_status_ok();
  kernel.thread_epoll_wait_timeout = () => -1n;
  kernel.supervisor_blocked_epoll_wait_retry_required = () => 1;
  kernel.abi_fd_type_epoll = () => 7;
  const handle = makeHandle(kernel, blockedWorker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.workers.add(parentWorker);
  handle.workers.add(childWorker);
  handle.kernelWorkerRequest = vi.fn(async (message) => {
    if (message.type === "sync-epoll-state") {
      return {
        type: "epoll-state-synced",
        requestId: 0,
        kernelState: message.kernelState,
      };
    }
    throw new Error(`unexpected kernel worker request: ${message.type}`);
  });
  const lifecycleEvents: string[] = [];
  const executeHostBlockedSyscallResumeOutcome = vi.fn(async () => ({
    kind: "blocked" as const,
    status: kernel.abi_status_epoll_wait(),
  }));

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    executeHostBlockedSyscallResumeOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_epoll_wait(), 0x1234);
  const blocked = handle.hostBlockedWorkers.get(blockedWorker);
  expect(blocked).toBeDefined();
  scheduler.clearHostBlockedRetryTimer(handle, blockedWorker);

  scheduler.notifySchedulerProgress(handle);

  expect(lifecycleEvents).toContain("epoll-retry-skip:1:clone-child-init-pending");
  expect(handle.kernelWorkerRequest).not.toHaveBeenCalled();
  expect(vi.getTimerCount()).toBeGreaterThan(0);

  childThread.initialized = true;
  childThread.resumeWorkersOnReady = [];
  scheduler.clearHostBlockedRetryTimer(handle, blockedWorker);
  scheduler.notifySchedulerProgress(handle);

  expect(lifecycleEvents.some((event) => event.startsWith("epoll-retry-rpc:1:vm=0x1234"))).toBe(true);
  expect(handle.kernelWorkerRequest).not.toHaveBeenCalled();
  expect(executeHostBlockedSyscallResumeOutcome).not.toHaveBeenCalled();
  scheduler.clearHostBlockedRetryTimer(handle, blockedWorker);
});

test("pipe retry pumps active child processes before remaining blocked", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_pipe_wait();
  kernel.thread_a7 = () => 63n;
  kernel.thread_a0 = () => 4n;
  kernel.thread_poll_wait_timeout = () => -1n;
  kernel.process_active_child_pid = () => 7;
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  const pumpSupervisorSelectedChildProcess = vi.fn(() => true);

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    pumpSupervisorSelectedChildProcess,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  await vi.advanceTimersByTimeAsync(6);

  expect(pumpSupervisorSelectedChildProcess).toHaveBeenCalledWith(7);
});

test("finite pipe poll with active children completes through supervisor timer", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_pipe_wait();
  const resumePollTimeout = vi.fn(() => {
    status = kernel.abi_status_ok();
  });
  kernel.abi_sys_ppoll = () => 73;
  kernel.thread_a7 = () => 73n;
  kernel.thread_a0 = () => 0n;
  kernel.thread_status = () => status;
  kernel.thread_poll_wait_timeout = () => 500n;
  kernel.thread_poll_wait_ready_count = () => 0;
  kernel.thread_poll_wait_has_socket = () => 0;
  kernel.thread_resume_poll_wait_timeout = resumePollTimeout;
  kernel.process_active_child_pid = () => 7;
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  const pumpSupervisorSelectedChildProcess = vi.fn(() => true);

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    pumpSupervisorSelectedChildProcess,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  await vi.advanceTimersByTimeAsync(35);

  expect(pumpSupervisorSelectedChildProcess).not.toHaveBeenCalled();
  expect(resumePollTimeout).not.toHaveBeenCalled();

  await vi.advanceTimersByTimeAsync(500);

  expect(pumpSupervisorSelectedChildProcess).not.toHaveBeenCalled();
  expect(resumePollTimeout).toHaveBeenCalledWith(0x1234);
  expect(kernel.supervisor_complete_host_op).toHaveBeenCalledWith(expect.any(Number), 64);
});

test("pipe pselect retry parks behind a supervisor-owned active child", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  kernel.abi_sys_pselect6 = () => 72;
  kernel.thread_a7 = () => 72n;
  kernel.thread_a0 = () => 0n;
  kernel.thread_status = () => kernel.abi_status_pipe_wait();
  kernel.thread_poll_wait_timeout = () => -1n;
  kernel.thread_poll_wait_ready_count = () => 0;
  kernel.thread_poll_wait_has_socket = () => 0;
  kernel.thread_pipe_read_eof_sync_pending = () => 0;
  kernel.thread_poll_hup_sync_pending = () => 0;
  kernel.process_active_child_pid = () => 7;
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn(async () => {
    throw new Error("scheduler retry should delegate blocked resume through outcome helper");
  });
  const lifecycleEvents: string[] = [];
  const executeHostBlockedSyscallResumeOutcome = vi.fn(async () => ({
    kind: "blocked" as const,
    status: kernel.abi_status_pipe_wait(),
  }));

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    executeHostBlockedSyscallResumeOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  scheduler.notifySchedulerProgress(handle);
  await Promise.resolve();

  expect(lifecycleEvents.some((event) => event.startsWith("pipe-retry-skip:1:active-child"))).toBe(true);
  expect(executeHostBlockedSyscallResumeOutcome).not.toHaveBeenCalled();
  expect(handle.hostBlockedWorkers.has(worker)).toBe(true);
});

test("pipe pselect retry resumes after pipe sync when no writer remains", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  kernel.abi_sys_pselect6 = () => 72;
  kernel.abi_fd_type_pipe = () => 6;
  kernel.thread_a7 = () => 72n;
  kernel.thread_a0 = () => 0n;
  kernel.thread_status = () => kernel.abi_status_pipe_wait();
  kernel.thread_poll_wait_timeout = () => -1n;
  kernel.thread_poll_wait_ready_count = () => 0;
  kernel.thread_poll_wait_has_socket = () => 0;
  kernel.thread_poll_wait_first_pipe_revents = () => 4 | (0x001 << 8);
  kernel.thread_pipe_read_eof_sync_pending = () => 0;
  kernel.thread_poll_hup_sync_pending = () => 0;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 4 ? 10 : 0;
  kernel.kernel_ofd_fd_type = (ofdId: number) => ofdId === 10 ? kernel.abi_fd_type_pipe() : 0;
  kernel.kernel_ofd_ino = (ofdId: number) => ofdId === 10 ? 2 : 0;
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  kernel.memory = handle.memory;
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn(async () => {
    throw new Error("scheduler retry should delegate blocked resume through outcome helper");
  });
  const executeHostBlockedSyscallResumeOutcome = vi.fn(async () => ({
    kind: "blocked" as const,
    status: kernel.abi_status_pipe_wait(),
  }));
  const lifecycleEvents: string[] = [];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    executeHostBlockedSyscallResumeOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  const blocked = handle.hostBlockedWorkers.get(worker);
  expect(blocked).toBeDefined();
  requestPipeSyncBeforeRetry(kernel, 0x1234);
  scheduler.notifySchedulerProgress(handle);
  await Promise.resolve();

  expect(lifecycleEvents.some((event) => event.startsWith("pipe-retry:1:a7=72"))).toBe(true);
  expect(executeHostBlockedSyscallResumeOutcome).toHaveBeenCalledWith(
    handle,
    worker,
    0x1234,
    handle.kernel.abi_status_pipe_wait(),
  );
});

test("kernel-worker blocked syscall retry requires outcome helper", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_pipe_wait();
  kernel.thread_a7 = () => 999n;
  kernel.thread_a0 = () => 4n;
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn(async () => {
    throw new Error("direct kernel-worker request should not be issued by scheduler retry");
  });
  const failProcess = vi.fn();

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  const blocked = handle.hostBlockedWorkers.get(worker);
  expect(blocked).toBeDefined();
  scheduler.clearHostBlockedRetryTimer(handle, worker);
  requestPipeSyncBeforeRetry(kernel, 0x1234);
  scheduler.notifySchedulerProgress(handle);

  expect(failProcess).toHaveBeenCalledWith(handle, expect.any(Error));
  expect((failProcess.mock.calls[0]?.[1] as Error).message).toBe(
    "kernel-worker blocked syscall resume requires executeHostBlockedSyscallResumeOutcome",
  );
  expect(handle.hostBlockedWorkers.get(worker)?.rpcPending).toBe(false);
  expect(handle.hostBlockedWorkers.get(worker)?.rpcGeneration).toBe(0);
});

test("kernel-worker unsupported blocked status fails before terminal local resume", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  const unsupportedStatus = 0x4d2;
  kernel.thread_status = () => unsupportedStatus;
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    throw new Error("unsupported kernel-worker status must not use terminal local resume");
  });
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn(async () => {
    throw new Error("unsupported blocked status must not issue an ad hoc kernel-worker request");
  });
  const failProcess = vi.fn();

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, unsupportedStatus, 0x1234);
  const blocked = handle.hostBlockedWorkers.get(worker);
  expect(blocked).toBeDefined();
  scheduler.clearHostBlockedRetryTimer(handle, worker);
  scheduler.notifySchedulerProgress(handle);

  expect(failProcess).toHaveBeenCalledWith(handle, expect.any(Error));
  expect((failProcess.mock.calls[0]?.[1] as Error).message).toBe(
    "kernel supervisor rejected blocked status 1234 resume delegation",
  );
  expect(handle.kernelWorkerRequest).not.toHaveBeenCalled();
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
});

test("does not passively park synced pipe EOF", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  kernel.abi_sys_read = () => 63n;
  kernel.thread_status = () => kernel.abi_status_pipe_wait();
  kernel.thread_a7 = () => 63n;
  kernel.thread_blocked_syscall_nr = () => 63n;
  kernel.thread_blocked_pipe_buffered_len = () => 0;
  kernel.thread_blocked_pipe_write_open = () => 0;
  kernel.thread_blocked_pipe_read_ready = () => 0;
  kernel.thread_blocked_pipe_write_ready = () => 0;
  kernel.thread_poll_wait_has_socket = () => 0;
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn() as never;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, kernel.abi_status_pipe_wait(), 0x1234);
  const blocked = handle.hostBlockedWorkers.get(worker);
  expect(blocked).toBeDefined();
  scheduler.clearHostBlockedRetryTimer(handle, worker);
  requestPipeSyncBeforeRetry(kernel, 0x1234);

  scheduler.blockWorker(handle, worker, kernel.abi_status_pipe_wait(), 0x1234);

  expect(vi.getTimerCount()).toBeGreaterThan(0);
});

test("reblocking the same pipe wait preserves in-flight blocked rpc ownership", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_pipe_wait();
  kernel.thread_a7 = () => 999n;
  kernel.thread_a0 = () => 4n;
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn(async () => {
    throw new Error("same-status reblock must not start a duplicate retry");
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  const blocked = handle.hostBlockedWorkers.get(worker);
  expect(blocked).toBeDefined();
  blocked!.rpcPending = true;
  blocked!.rpcGeneration = 7;
  requestPipeSyncBeforeRetry(kernel, 0x1234);

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);

  const reblocked = handle.hostBlockedWorkers.get(worker);
  expect(reblocked?.rpcPending).toBe(true);
  expect(reblocked?.rpcGeneration).toBe(7);
  expect(pipeSyncBeforeRetryRequested(kernel, 0x1234)).toBe(true);
});

test("socket poll is resumed immediately after network pump changes state", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_pipe_wait();
  kernel.thread_status = () => status;
  kernel.thread_a7 = () => 999n;
  kernel.thread_a0 = () => 3n;
  kernel.thread_poll_wait_timeout = () => -1n;
  kernel.thread_poll_wait_ready_count = () => 0;
  kernel.thread_poll_wait_has_socket = () => 1;
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    throw new Error("socket poll resume must be delegated through host-boundary outcome helper");
  });
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.networkConnections.set(3, {} as never);
  const pumpNetworkConnections = vi.fn(async () => true);
  const executeHostBlockedSyscallResumeOutcome = vi.fn(async () => {
    status = kernel.abi_status_ok();
    return { kind: "completed" as const, status };
  });
  const failProcess = vi.fn();

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess,
    pumpNetworkConnections,
    executeHostBlockedSyscallResumeOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  await vi.advanceTimersByTimeAsync(2);

  expect(pumpNetworkConnections).toHaveBeenCalledWith(handle, 0x1234);
  expect(executeHostBlockedSyscallResumeOutcome).toHaveBeenCalledWith(
    handle,
    worker,
    0x1234,
    handle.kernel.abi_status_pipe_wait(),
  );
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handle.hostBlockedWorkers.has(worker)).toBe(false);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({ type: "step" }));
  expect(failProcess).not.toHaveBeenCalled();
});

test("socket poll pump resumes through kernel-worker RPC when available", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_pipe_wait();
  kernel.thread_status = () => status;
  kernel.thread_a7 = () => 999n;
  kernel.thread_a0 = () => 3n;
  kernel.thread_poll_wait_timeout = () => -1n;
  kernel.thread_poll_wait_ready_count = () => 0;
  kernel.thread_poll_wait_has_socket = () => 1;
  kernel.process_state_bytes = () => 0;
  kernel.kernel_ofd_table_bytes = () => 64 * 32;
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    status = kernel.abi_status_ok();
  });
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.networkConnections.set(3, {} as never);
  handle.kernelWorkerRequest = vi.fn(async (msg) => {
    switch (msg.type) {
      case "sync-socket-state":
        return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
      case "sync-pipe-state":
        return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
      default:
        throw new Error(`unexpected kernel-worker request: ${msg.type}`);
    }
  }) as ProcessHandle["kernelWorkerRequest"];
  const pumpNetworkConnections = vi.fn(async () => true);
  const executeHostBlockedSyscallResumeOutcome = vi.fn(async () => {
    status = kernel.abi_status_ok();
    return { kind: "completed" as const, status };
  });
  const failProcess = vi.fn();

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess,
    pumpNetworkConnections,
    executeHostBlockedSyscallResumeOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => ({ kind: "completed" }),
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  await vi.advanceTimersByTimeAsync(2);
  await Promise.resolve();
  await Promise.resolve();

  expect(pumpNetworkConnections).toHaveBeenCalledWith(handle, 0x1234);
  expect(failProcess).not.toHaveBeenCalled();
  expect(handle.kernelWorkerRequest).toHaveBeenCalled();
  expect(executeHostBlockedSyscallResumeOutcome).toHaveBeenCalledWith(
    handle,
    worker,
    0x1234,
    handle.kernel.abi_status_pipe_wait(),
  );
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handle.hostBlockedWorkers.has(worker)).toBe(false);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({ type: "step" }));
  expect(failProcess).not.toHaveBeenCalled();
});

test("finite pipe poll timeout completes through supervisor timer when kernel-worker exists", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_pipe_wait();
  const resumePollTimeout = vi.fn((vmPtr: number) => {
    expect(vmPtr).toBe(0x1234);
    status = kernel.abi_status_ok();
  });
  kernel.abi_sys_ppoll = () => 73;
  kernel.thread_a7 = () => 73n;
  kernel.thread_a0 = () => 0n;
  kernel.thread_status = () => status;
  kernel.thread_poll_wait_timeout = () => 100n;
  kernel.thread_poll_wait_ready_count = () => 0;
  kernel.thread_poll_wait_has_socket = () => 0;
  kernel.thread_resume_poll_wait_timeout = resumePollTimeout;
  kernel.thread_resume_blocked_syscall = vi.fn();
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn(async () => ({ type: "unexpected" }) as never);
  const executeHostBlockedSyscallResumeOutcome = vi.fn(async () => {
    status = kernel.abi_status_ok();
    return { kind: "completed" as const, status };
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    executeHostBlockedSyscallResumeOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  const blocked = handle.hostBlockedWorkers.get(worker);
  expect(blocked).toBeDefined();
  await vi.advanceTimersByTimeAsync(100);

  expect(executeHostBlockedSyscallResumeOutcome).not.toHaveBeenCalled();
  expect(resumePollTimeout).toHaveBeenCalledWith(0x1234);
  expect(kernel.supervisor_complete_host_op).toHaveBeenCalledWith(expect.any(Number), 64);
  expect(handle.hostBlockedWorkers.has(worker)).toBe(false);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("finite pipe poll timeout uses saved blocked syscall when current a7 moved on", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_pipe_wait();
  const resumePollTimeout = vi.fn((vmPtr: number) => {
    expect(vmPtr).toBe(0x1234);
    const view = new DataView(handle.memory.buffer);
    view.setBigUint64(0x20, 0x08n, true);
    view.setBigUint64(0x28, 0x10n, true);
    view.setBigUint64(0x30, 0n, true);
    status = kernel.abi_status_ok();
  });
  kernel.abi_sys_pselect6 = () => 72;
  kernel.abi_max_fds = () => 64;
  kernel.thread_a7 = () => 48n;
  kernel.thread_blocked_syscall_nr = () => 72n;
  kernel.thread_blocked_syscall_arg = (_vmPtr: number, index: number) => {
    switch (index) {
      case 0:
        return 16n;
      case 1:
        return 0x20n;
      case 2:
        return 0x28n;
      case 3:
        return 0x30n;
      default:
        return 0n;
    }
  };
  kernel.thread_a0 = () => 0n;
  kernel.thread_status = () => status;
  kernel.thread_poll_wait_timeout = () => 25n;
  kernel.thread_poll_wait_ready_count = () => 0;
  kernel.thread_poll_wait_has_socket = () => 0;
  kernel.thread_resume_poll_wait_timeout = resumePollTimeout;
  kernel.thread_resume_blocked_syscall = vi.fn();
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  await vi.advanceTimersByTimeAsync(1);
  expect(resumePollTimeout).not.toHaveBeenCalled();
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();

  await vi.advanceTimersByTimeAsync(25);

  expect(resumePollTimeout).toHaveBeenCalledWith(0x1234);
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handle.hostBlockedWorkers.has(worker)).toBe(false);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
  }));
});

test("supervisor-owned finite pipe poll timer does not run local pipe sync before completion", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_pipe_wait();
  const order: string[] = [];
  const resumePollTimeout = vi.fn(() => {
    order.push("resume");
    status = kernel.abi_status_ok();
  });
  kernel.abi_sys_ppoll = () => 73;
  kernel.thread_a7 = () => 73n;
  kernel.thread_a0 = () => 0n;
  kernel.thread_status = () => status;
  kernel.thread_poll_wait_timeout = () => 100n;
  kernel.thread_poll_wait_ready_count = () => 0;
  kernel.thread_poll_wait_has_socket = () => 0;
  kernel.thread_resume_poll_wait_timeout = resumePollTimeout;
  kernel.abi_max_fds = () => 64;
  kernel.abi_max_pipes = () => 64;
  kernel.abi_fd_type_pipe = () => 6;
  kernel.abi_fd_type_pty_master = () => 7;
  kernel.abi_fd_type_pty_slave = () => 8;
  kernel.process_fd_ofd_id = () => 0;
  kernel.process_fd_flags = () => 0;
  kernel.thread_process_thread_count = () => 1;
  kernel.process_thread_vm_ptr = (_processVmPtr, slot) => slot === 0 ? 0x1234 : 0;
  kernel.kernel_ofd_table_bytes = () => 64 * 32;
  kernel.kernel_ofd_fd_type = () => 0;
  kernel.kernel_ofd_ino = () => 0;
  kernel.kernel_ofd_host_fd = () => 0;
  kernel.kernel_pipe_region_bytes = () => 64 * (32 + 65_536);
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn(async (message) => {
    order.push("sync");
    expect(message.type).toBe("sync-pipe-state");
    return {
      type: "pipe-state-synced",
      kernelState: handle.kernelState,
      pipeSlots: [],
    };
  });

  const lifecycleEvents: string[] = [];
  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  const blocked = handle.hostBlockedWorkers.get(worker);
  expect(blocked).toBeDefined();
  scheduler.notifySchedulerProgress(handle);
  await Promise.resolve();

  expect(lifecycleEvents.some((event) => event.startsWith("pipe-sync-start:1:wake=0"))).toBe(false);
  expect(order).toEqual([]);
  expect(resumePollTimeout).not.toHaveBeenCalled();
  expect(handle.hostBlockedWorkers.has(worker)).toBe(true);

  await vi.advanceTimersByTimeAsync(100);

  expect(order).toEqual(["resume"]);
  expect(handle.kernelWorkerRequest).not.toHaveBeenCalled();
  expect(handle.hostBlockedWorkers.has(worker)).toBe(false);
});

test("supervisor-owned finite pipe poll with active child completes without pipe sync", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_pipe_wait();
  const order: string[] = [];
  const resumePollTimeout = vi.fn(() => {
    order.push("resume");
    status = kernel.abi_status_ok();
  });
  kernel.abi_sys_ppoll = () => 73;
  kernel.thread_a7 = () => 73n;
  kernel.thread_a0 = () => 0n;
  kernel.thread_status = () => status;
  kernel.thread_poll_wait_timeout = () => 100n;
  kernel.thread_poll_wait_ready_count = () => 0;
  kernel.thread_poll_wait_has_socket = () => 0;
  kernel.thread_resume_poll_wait_timeout = resumePollTimeout;
  kernel.abi_max_fds = () => 64;
  kernel.abi_max_pipes = () => 64;
  kernel.abi_fd_type_pipe = () => 6;
  kernel.abi_fd_type_pty_master = () => 7;
  kernel.abi_fd_type_pty_slave = () => 8;
  kernel.process_fd_ofd_id = () => 0;
  kernel.process_fd_flags = () => 0;
  kernel.process_thread_vm_ptr = () => 0;
  kernel.kernel_ofd_table_bytes = () => 64 * 32;
  kernel.kernel_ofd_fd_type = () => 0;
  kernel.kernel_ofd_ino = () => 0;
  kernel.kernel_ofd_host_fd = () => 0;
  kernel.kernel_pipe_region_bytes = () => 64 * (32 + 65_536);
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn(async () => {
    throw new Error("active child finite pipe poll timeout should not sync pipe state");
  });
  const executeHostBlockedSyscallResumeOutcome = vi.fn(async () => ({ kind: "completed" as const, status }));

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    executeHostBlockedSyscallResumeOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_pipe_wait(), 0x1234);
  const blocked = handle.hostBlockedWorkers.get(worker);
  expect(blocked).toBeDefined();
  scheduler.notifySchedulerProgress(handle);
  await Promise.resolve();

  expect(order).toEqual([]);
  expect(executeHostBlockedSyscallResumeOutcome).not.toHaveBeenCalled();
  expect(resumePollTimeout).not.toHaveBeenCalled();
  expect(handle.kernelWorkerRequest).not.toHaveBeenCalled();
  expect(handle.hostBlockedWorkers.has(worker)).toBe(true);

  await vi.advanceTimersByTimeAsync(100);

  expect(order).toEqual(["resume"]);
  expect(resumePollTimeout).toHaveBeenCalledWith(0x1234);
  expect(handle.hostBlockedWorkers.has(worker)).toBe(false);
});

test("wait4 retry resumes without waiting for a fork-shared sync gate", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel, threadResumeWait4 } = makeKernel();
  let status = kernel.abi_status_wait4();
  kernel.thread_status = () => status;
  kernel.thread_resume_wait4 = vi.fn((vmPtr: number) => {
    threadResumeWait4(vmPtr);
    status = kernel.abi_status_ok();
  });
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  const forkSharedSync = new Promise<void>(() => {});
  handle.forkSharedSyncPromise = forkSharedSync;
  handle.kernelShare.forkSharedSyncPromise = forkSharedSync;
  const lifecycleEvents: string[] = [];
  const executeHostBlockedSyscallResumeOutcome = makeHostBoundaryWait4ResumeOutcome();

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    executeHostBlockedSyscallResumeOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_wait4(), 0x1234);
  await vi.advanceTimersByTimeAsync(11);
  await flushMicrotasks(2);

  expect(executeHostBlockedSyscallResumeOutcome).toHaveBeenCalledWith(
    handle,
    worker,
    0x1234,
    handle.kernel.abi_status_wait4(),
  );
  expect(threadResumeWait4).toHaveBeenCalledWith(0x1234);
  expect(handle.hostBlockedWorkers.has(worker)).toBe(false);
});

test("wait4 retry clears a stale fork-shared sync source before retrying", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel, threadResumeWait4 } = makeKernel();
  let status = kernel.abi_status_wait4();
  kernel.thread_status = () => status;
  kernel.thread_process_ptr = (vmPtr: number) => vmPtr === 0x9999 ? 0 : 0x2000;
  kernel.thread_resume_wait4 = vi.fn((vmPtr: number) => {
    threadResumeWait4(vmPtr);
    status = kernel.abi_status_ok();
  });
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.kernelShare.members.add(handle);
  handle.pendingChildSpawnRecords = 0;
  const forkSharedSync = new Promise<void>(() => {});
  registerForkSharedSyncPromise(forkSharedSync, {
    ownerPid: handle.pid,
    sourceVmPtr: 0x9999,
    reason: "owned-ofd",
    targetPids: [5],
  });
  handle.forkSharedSyncPromise = forkSharedSync;
  handle.kernelShare.forkSharedSyncPromise = forkSharedSync;
  const lifecycleEvents: string[] = [];
  const executeHostBlockedSyscallResumeOutcome = makeHostBoundaryWait4ResumeOutcome();

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    executeHostBlockedSyscallResumeOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_wait4(), 0x1234);
  await vi.advanceTimersByTimeAsync(11);
  await flushMicrotasks(2);

  expect(handle.forkSharedSyncPromise).toBeNull();
  expect(handle.kernelShare.forkSharedSyncPromise).toBeNull();
  expect(lifecycleEvents.some((event) =>
    event.startsWith("wait4-retry-cleared:1:fork-shared-sync-stale-source:")
    && event.includes(":owner=1:src=0x9999:reason=owned-ofd:targets=5"),
  )).toBe(true);
  expect(executeHostBlockedSyscallResumeOutcome).toHaveBeenCalledWith(
    handle,
    worker,
    0x1234,
    handle.kernel.abi_status_wait4(),
  );
  expect(threadResumeWait4).toHaveBeenCalledWith(0x1234);
});

test("wait4 retry clears an empty-target fork-shared sync gate before retrying", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel, threadResumeWait4 } = makeKernel();
  let status = kernel.abi_status_wait4();
  kernel.thread_status = () => status;
  kernel.thread_process_ptr = () => 0x2000;
  kernel.thread_resume_wait4 = vi.fn((vmPtr: number) => {
    threadResumeWait4(vmPtr);
    status = kernel.abi_status_ok();
  });
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.kernelShare.members.add(handle);
  handle.pendingChildSpawnRecords = 0;
  const forkSharedSync = new Promise<void>(() => {});
  registerForkSharedSyncPromise(forkSharedSync, {
    ownerPid: handle.pid,
    sourceVmPtr: 0x9999,
    reason: "blocked-rpc",
    targetPids: [],
  });
  handle.forkSharedSyncPromise = forkSharedSync;
  handle.kernelShare.forkSharedSyncPromise = forkSharedSync;
  const lifecycleEvents: string[] = [];
  const executeHostBlockedSyscallResumeOutcome = makeHostBoundaryWait4ResumeOutcome();

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    executeHostBlockedSyscallResumeOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_wait4(), 0x1234);
  await vi.advanceTimersByTimeAsync(11);
  await flushMicrotasks(2);

  expect(handle.forkSharedSyncPromise).toBeNull();
  expect(handle.kernelShare.forkSharedSyncPromise).toBeNull();
  expect(lifecycleEvents.some((event) =>
    event.startsWith("wait4-retry-cleared:1:fork-shared-sync-empty-targets:")
    && event.includes(":owner=1:src=0x9999:reason=blocked-rpc:targets=none"),
  )).toBe(true);
  expect(executeHostBlockedSyscallResumeOutcome).toHaveBeenCalledWith(
    handle,
    worker,
    0x1234,
    handle.kernel.abi_status_wait4(),
  );
  expect(threadResumeWait4).toHaveBeenCalledWith(0x1234);
});

test("idle ok worker resumes after a fork-shared sync gate settles", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  let resolveSync!: () => void;
  const forkSharedSync = new Promise<void>((resolve) => {
    resolveSync = resolve;
  });
  handle.forkSharedSyncPromise = forkSharedSync;
  handle.kernelShare.forkSharedSyncPromise = forkSharedSync;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);
  expect(worker.postMessage).not.toHaveBeenCalled();

  resolveSync();
  await Promise.resolve();
  await Promise.resolve();

  expect(handle.forkSharedSyncPromise).toBeNull();
  expect(handle.kernelShare.forkSharedSyncPromise).toBeNull();
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("wait4 retry does not park behind a pending fork-shared sync gate", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel, threadResumeWait4 } = makeKernel();
  let status = kernel.abi_status_wait4();
  kernel.thread_status = () => status;
  kernel.thread_resume_wait4 = vi.fn((vmPtr: number) => {
    threadResumeWait4(vmPtr);
    status = kernel.abi_status_ok();
    return 7n;
  });
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  const forkSharedSync = new Promise<void>(() => {});
  handle.forkSharedSyncPromise = forkSharedSync;
  handle.kernelShare.forkSharedSyncPromise = forkSharedSync;
  const executeHostBlockedSyscallResumeOutcome = makeHostBoundaryWait4ResumeOutcome();

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    executeHostBlockedSyscallResumeOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_wait4(), 0x1234);
  await vi.advanceTimersByTimeAsync(11);
  await flushMicrotasks(2);

  expect(executeHostBlockedSyscallResumeOutcome).toHaveBeenCalledWith(
    handle,
    worker,
    0x1234,
    handle.kernel.abi_status_wait4(),
  );
  expect(threadResumeWait4).toHaveBeenCalledWith(0x1234);
  expect(handle.hostBlockedWorkers.has(worker)).toBe(false);
});

test("wait4 retry re-enters supervisor direct resume until wait4 is reaped", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel, threadResumeWait4 } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_wait4();
  kernel.thread_resume_wait4 = vi.fn((vmPtr: number) => {
    threadResumeWait4(vmPtr);
    return 0n;
  });
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  const lifecycleEvents: string[] = [];
  const executeHostBlockedSyscallResumeOutcome = makeHostBoundaryWait4ResumeOutcome();

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    executeHostBlockedSyscallResumeOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_wait4(), 0x1234);
  await vi.advanceTimersByTimeAsync(11);
  await flushMicrotasks(2);
  await vi.advanceTimersByTimeAsync(11);
  await flushMicrotasks(2);

  expect(threadResumeWait4.mock.calls.length).toBeGreaterThanOrEqual(2);
  expect(lifecycleEvents.filter((event) => event.startsWith("wait4-retry-host-boundary:1:0x1234:records=supervisor")).length)
    .toBeGreaterThanOrEqual(2);
});

test("epoll timerfd wait advances guest time to deadline before retrying epoll", async () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(blockedWorker, makeThreadState(0x1234));
  threadWorkerStates.set(readyWorker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_epoll_wait();
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234
    ? status
    : kernel.abi_status_ok();
  kernel.thread_epoll_wait_timeout = () => -1n;
  kernel.thread_epoll_timerfd_deadline_ticks = () => 1_000_001n;
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    status = kernel.abi_status_ok();
  });
  const handle = makeHandle(kernel, blockedWorker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.workers.add(readyWorker);
  handle.vforkReadyWorkers.push(readyWorker);

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_epoll_wait(), 0x1234);

  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  await vi.advanceTimersByTimeAsync(9);
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();

  await vi.advanceTimersByTimeAsync(1);

  expect(kernel.sync_monotonic_ticks_floor).toHaveBeenCalledWith(1_000_001n);
  expect(kernel.thread_resume_blocked_syscall).toHaveBeenCalledWith(0x1234);
  expect(kernel.thread_resume_epoll_wait_timeout).not.toHaveBeenCalled();
});

test("epoll kernel-worker retry re-queries timerfd deadline after state sync", async () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(blockedWorker, makeThreadState(0x1234));
  threadWorkerStates.set(readyWorker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_epoll_wait();
  let deadlineVisible = false;
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234
    ? status
    : kernel.abi_status_ok();
  kernel.thread_epoll_wait_timeout = () => -1n;
  kernel.thread_process_thread_count = () => 2;
  kernel.supervisor_blocked_epoll_wait_retry_required = () => 1;
  kernel.process_thread_vm_ptr = (_vmPtr: number, slot: number) => slot === 0 ? 0x1234 : 0;
  kernel.abi_fd_type_epoll = () => 7;
  kernel.abi_fd_type_timerfd = () => 10;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => {
    if (fd === 4) {
      return 5;
    }
    if (fd === 5) {
      return 6;
    }
    return 0;
  };
  kernel.thread_blocked_ofd_id = (vmPtr: number) => vmPtr === 0x1234 ? 5 : 0;
  kernel.kernel_ofd_fd_type = (ofdId: number) => {
    if (ofdId === 5) {
      return 7;
    }
    if (ofdId === 6) {
      return 10;
    }
    return 0;
  };
  kernel.supervisor_blocked_epoll_watched_ofd = vi.fn((_vmPtr: number, index: number) => (
    index === 0 ? 6 : 0
  ));
  kernel.thread_epoll_timerfd_deadline_ticks = vi.fn(() => deadlineVisible ? 1_000_001n : 0n);
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    status = kernel.abi_status_ok();
  });
  const handle = makeHandle(kernel, blockedWorker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.workers.add(readyWorker);
  handle.vforkReadyWorkers.push(readyWorker);
  handle.kernelWorkerRequest = vi.fn(async (message) => {
    if (message.type === "sync-ofd-state") {
      deadlineVisible = true;
      return {
        type: "ofd-state-synced",
        requestId: 0,
        kernelState: message.kernelState,
      };
    }
    if (message.type === "sync-epoll-state") {
      return {
        type: "epoll-state-synced",
        requestId: 0,
        kernelState: message.kernelState,
      };
    }
    throw new Error(`unexpected kernel worker request: ${message.type}`);
  });
  const lifecycleEvents: string[] = [];
  const failProcess = vi.fn();
  const executeHostBlockedSyscallResumeOutcome = vi.fn(async () => {
    status = kernel.abi_status_ok();
    return { kind: "completed" as const, status };
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess,
    executeHostBlockedSyscallResumeOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_epoll_wait(), 0x1234);

  await vi.advanceTimersByTimeAsync(10);
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();

  expect(handle.kernelWorkerRequest).toHaveBeenCalledWith(expect.objectContaining({
    type: "sync-ofd-state",
  }));
  expect(failProcess).not.toHaveBeenCalled();
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(lifecycleEvents).toContain("epoll-retry-timer:1:vm=0x1234");

  await vi.advanceTimersByTimeAsync(9);
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();

  await vi.advanceTimersByTimeAsync(1);

  expect(kernel.sync_monotonic_ticks_floor).toHaveBeenCalledWith(1_000_001n);
  expect(executeHostBlockedSyscallResumeOutcome).not.toHaveBeenCalled();
  expect(kernel.thread_resume_blocked_syscall).toHaveBeenCalledWith(0x1234);
  expect(kernel.thread_resume_epoll_wait_timeout).not.toHaveBeenCalled();
});

test("epoll kernel-worker retry syncs watched timerfd OFD after the timer fd is closed", async () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(blockedWorker, makeThreadState(0x1234));
  threadWorkerStates.set(readyWorker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_epoll_wait();
  let deadlineVisible = false;
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234
    ? status
    : kernel.abi_status_ok();
  kernel.thread_epoll_wait_timeout = () => -1n;
  kernel.thread_process_thread_count = () => 2;
  kernel.supervisor_blocked_epoll_wait_retry_required = () => 1;
  kernel.process_thread_vm_ptr = (_vmPtr: number, slot: number) => slot === 0 ? 0x1234 : 0;
  kernel.abi_fd_type_epoll = () => 7;
  kernel.abi_fd_type_timerfd = () => 10;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 4 ? 5 : 0;
  kernel.thread_blocked_ofd_id = (vmPtr: number) => vmPtr === 0x1234 ? 5 : 0;
  kernel.thread_blocked_fd = (vmPtr: number) => vmPtr === 0x1234 ? 4 : 0;
  kernel.kernel_ofd_fd_type = (ofdId: number) => {
    if (ofdId === 5) {
      return 7;
    }
    if (ofdId === 6) {
      return 10;
    }
    return 0;
  };
  kernel.supervisor_blocked_epoll_watched_ofd = vi.fn((_vmPtr: number, index: number) => (
    index === 0 ? 6 : 0
  ));
  kernel.kernel_ofd_ino = (ofdId: number) => {
    if (ofdId === 5) {
      return 0;
    }
    if (ofdId === 6) {
      return 1;
    }
    return 0;
  };
  kernel.kernel_ofd_offset = () => 0n;
  kernel.kernel_epoll_table_bytes = () => 64 * 8;
  kernel.kernel_epoll_instance_bytes = () => 64 * 32 + 4;
  kernel.thread_epoll_timerfd_deadline_ticks = vi.fn(() => deadlineVisible ? 1_000_001n : 0n);
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    status = kernel.abi_status_ok();
  });
  const handle = makeHandle(kernel, blockedWorker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.workers.add(readyWorker);
  handle.vforkReadyWorkers.push(readyWorker);
  handle.kernelState.epollTablePtr = 0x1000;
  const epollInstancePtr = 0x2000;
  const epollView = new DataView(handle.memory.buffer);
  epollView.setUint32(handle.kernelState.epollTablePtr, epollInstancePtr, true);
  epollView.setUint32(epollInstancePtr, 5, true);
  epollView.setUint32(epollInstancePtr + 4, 6, true);
  epollView.setUint32(epollInstancePtr + 8, 0x40000001, true);
  epollView.setBigUint64(epollInstancePtr + 16, 5n, true);
  epollView.setUint32(epollInstancePtr + 24, 0, true);
  epollView.setUint32(epollInstancePtr + 64 * 32, 1, true);

  handle.kernelWorkerRequest = vi.fn(async (message) => {
    if (message.type === "sync-ofd-state") {
      expect(message.ofdIds).toEqual([6]);
      deadlineVisible = true;
      return {
        type: "ofd-state-synced",
        requestId: 0,
        kernelState: message.kernelState,
      };
    }
    if (message.type === "sync-epoll-state") {
      return {
        type: "epoll-state-synced",
        requestId: 0,
        kernelState: message.kernelState,
      };
    }
    throw new Error(`unexpected kernel worker request: ${message.type}`);
  });
  const lifecycleEvents: string[] = [];
  const failProcess = vi.fn();
  const executeHostBlockedSyscallResumeOutcome = vi.fn(async () => {
    status = kernel.abi_status_ok();
    return { kind: "completed" as const, status };
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => {
      lifecycleEvents.push(event);
    },
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess,
    executeHostBlockedSyscallResumeOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_epoll_wait(), 0x1234);

  await vi.advanceTimersByTimeAsync(10);
  await Promise.resolve();
  await Promise.resolve();

  expect(handle.kernelWorkerRequest).toHaveBeenCalledWith(expect.objectContaining({
    type: "sync-ofd-state",
    ofdIds: [6],
  }));
  expect(failProcess).not.toHaveBeenCalled();
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(lifecycleEvents).toContain("epoll-retry-timer:1:vm=0x1234");

  await vi.advanceTimersByTimeAsync(10);

  expect(kernel.sync_monotonic_ticks_floor).toHaveBeenCalledWith(1_000_001n);
  expect(executeHostBlockedSyscallResumeOutcome).not.toHaveBeenCalled();
  expect(kernel.thread_resume_blocked_syscall).toHaveBeenCalledWith(0x1234);
  expect(kernel.thread_resume_epoll_wait_timeout).not.toHaveBeenCalled();
});

test("epoll kernel-worker retry syncs watched pipe, epoll table, then dependent eventfd", async () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(blockedWorker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = kernel.abi_status_epoll_wait();
  kernel.thread_status = () => status;
  kernel.thread_epoll_wait_timeout = () => -1n;
  kernel.thread_process_thread_count = () => 2;
  kernel.supervisor_blocked_epoll_wait_retry_required = () => 1;
  kernel.process_thread_vm_ptr = (_vmPtr: number, slot: number) => slot === 0 ? 0x1234 : 0;
  kernel.abi_fd_type_epoll = () => 7;
  kernel.abi_fd_type_pipe = () => 6;
  kernel.abi_fd_type_file = () => 1;
  kernel.abi_fd_type_eventfd = () => 9;
  kernel.abi_fd_type_timerfd = () => 10;
  kernel.process_fd_ofd_id = (_vmPtr: number, fd: number) => fd === 4 ? 5 : 0;
  kernel.process_fd_flags = () => 0;
  kernel.thread_blocked_ofd_id = (vmPtr: number) => vmPtr === 0x1234 ? 5 : 0;
  kernel.thread_blocked_fd = (vmPtr: number) => vmPtr === 0x1234 ? 4 : 0;
  kernel.kernel_ofd_fd_type = (ofdId: number) => {
    if (ofdId === 5) {
      return 7;
    }
    if (ofdId === 6) {
      return 6;
    }
    if (ofdId === 7) {
      return 9;
    }
    return 0;
  };
  kernel.kernel_ofd_ino = (ofdId: number) => {
    if (ofdId === 5) {
      return 0;
    }
    if (ofdId === 6) {
      return 3;
    }
    return 0;
  };
  kernel.kernel_ofd_host_fd = (ofdId: number) => ofdId === 6 ? 3 : 0;
  kernel.kernel_ofd_status_flags = () => 0;
  kernel.kernel_ofd_offset = () => 0n;
  kernel.kernel_ofd_table_bytes = () => 64 * 32;
  kernel.kernel_pipe_region_bytes = () => 64 * (32 + 65_536);
  kernel.kernel_epoll_table_bytes = () => 64 * 8;
  kernel.kernel_epoll_instance_bytes = () => 64 * 32 + 4;
  kernel.process_state_bytes = () => 0;
  kernel.supervisor_blocked_epoll_watches_pipe = vi.fn(() => 1);
  kernel.supervisor_blocked_epoll_watched_ofd = vi.fn((_vmPtr: number, index: number) => (
    index === 0 ? 7 : 0
  ));
  kernel.thread_resume_blocked_syscall = vi.fn(() => {
    throw new Error("kernel-worker epoll resume must use blocked syscall outcome");
  });

  const handle = makeHandle(kernel, blockedWorker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelState.epollTablePtr = 0x1000;
  handle.kernelState.ofdTablePtr = 0x3000;
  handle.kernelState.pipeBase = 0x4000;
  const epollTablePtr = handle.kernelState.epollTablePtr;
  const epollInstancePtr = 0x2000;
  const epollView = new DataView(handle.memory.buffer);
  epollView.setUint32(handle.kernelState.epollTablePtr, epollInstancePtr, true);
  epollView.setUint32(epollInstancePtr, 10, true);
  epollView.setUint32(epollInstancePtr + 4, 6, true);
  epollView.setUint32(epollInstancePtr + 8, 0x1, true);
  epollView.setBigUint64(epollInstancePtr + 16, 10n, true);
  epollView.setUint32(epollInstancePtr + 24, 0, true);
  epollView.setUint32(epollInstancePtr + 64 * 32, 1, true);

  const requests: string[] = [];
  handle.kernelWorkerRequest = vi.fn(async (message) => {
    requests.push(message.type);
    if (message.type === "sync-pipe-state") {
      return {
        type: "pipe-state-synced",
        requestId: 0,
        kernelState: { ...message.kernelState, epollTablePtr },
        pipeSlots: [],
      };
    }
    if (message.type === "sync-epoll-state") {
      epollView.setUint32(epollInstancePtr + 32, 12, true);
      epollView.setUint32(epollInstancePtr + 36, 7, true);
      epollView.setUint32(epollInstancePtr + 40, 0x1, true);
      epollView.setBigUint64(epollInstancePtr + 48, 12n, true);
      epollView.setUint32(epollInstancePtr + 56, 0, true);
      epollView.setUint32(epollInstancePtr + 64 * 32, 2, true);
      return {
        type: "epoll-state-synced",
        requestId: 0,
        kernelState: { ...message.kernelState, epollTablePtr },
      };
    }
    if (message.type === "sync-ofd-state") {
      expect(message.ofdIds).toEqual([7]);
      expect(message.mode).toBe("pull");
      return {
        type: "ofd-state-synced",
        requestId: 0,
        kernelState: message.kernelState,
      };
    }
    throw new Error(`unexpected kernel worker request: ${message.type}`);
  });
  const executeHostBlockedSyscallResumeOutcome = vi.fn(async () => {
    status = kernel.abi_status_ok();
    return { kind: "completed" as const, status };
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    executeHostBlockedSyscallResumeOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_epoll_wait(), 0x1234);

  await vi.advanceTimersByTimeAsync(10);
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();

  expect(requests).toEqual(["sync-pipe-state", "sync-epoll-state", "sync-ofd-state"]);
  expect(kernel.supervisor_blocked_epoll_watches_pipe).toHaveBeenCalledWith(0x1234);
  expect(kernel.supervisor_blocked_epoll_watched_ofd).toHaveBeenCalledWith(0x1234, 0);
  expect(executeHostBlockedSyscallResumeOutcome).toHaveBeenCalledWith(
    handle,
    blockedWorker,
    0x1234,
    handle.kernel.abi_status_epoll_wait(),
  );
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
});

test("wait4 retry reaches supervisor even while another worker is marked running", async () => {
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(blockedWorker, makeThreadState(0x1234));
  threadWorkerStates.set(runningWorker, makeThreadState(0x5678));
  const { exports: kernel, threadResumeWait4 } = makeKernel();
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234
    ? kernel.abi_status_wait4()
    : kernel.abi_status_ok();
  const handle = makeHandle(kernel, blockedWorker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.workers.add(runningWorker);
  handle.runningWorker = runningWorker;
  const notify = new Int32Array(handle.runtimeBridge.notifySab);
  const notifyBefore = Atomics.load(notify, 0);
  const executeHostBlockedSyscallResumeOutcome = makeHostBoundaryWait4ResumeOutcome();

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    executeHostBlockedSyscallResumeOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, blockedWorker, handle.kernel.abi_status_wait4(), 0x1234);
  await vi.advanceTimersByTimeAsync(11);
  await flushMicrotasks(2);

  expect(executeHostBlockedSyscallResumeOutcome).toHaveBeenCalledWith(
    handle,
    blockedWorker,
    0x1234,
    handle.kernel.abi_status_wait4(),
  );
  expect(threadResumeWait4).toHaveBeenCalledWith(0x1234);
  expect(Atomics.load(notify, 0)).toBe(notifyBefore);
  expect(handle.hostBlockedWorkers.has(blockedWorker)).toBe(true);
});

test("pumpProcess preserves an unanswered ok running worker that has not returned status", () => {
  vi.setSystemTime(new Date(10_000));
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x66c42c8);
  const readyThread = makeThreadState(0x64a6e20);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 1;
  runningThread.lastStepAtMs = 7_500;
  runningThread.lastStatusAtMs = null;
  runningThread.lastStatus = null;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(readyWorker, readyThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => 0;
  const handle = makeHandle(kernel, runningWorker, threadWorkerStates);
  handle.workers.add(readyWorker);
  handle.runningWorker = runningWorker;
  handle.vforkReadyWorkers = [readyWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(runningThread.runningGeneration).toBe(runningThread.generation);
  expect(handle.runningWorker).toBe(runningWorker);
  expect(readyWorker.postMessage).not.toHaveBeenCalled();
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess keeps an unanswered running worker after an active step clears", async () => {
  vi.setSystemTime(new Date(10_000));
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x66c42c8);
  const readyThread = makeThreadState(0x64a6e20);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 1;
  runningThread.lastStepAtMs = 7_500;
  runningThread.lastStatusAtMs = null;
  runningThread.lastStatus = null;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(readyWorker, readyThread);
  const { exports: kernel } = makeKernel();
  let activeStepVmPtr = runningThread.vmPtr;
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => activeStepVmPtr;
  const handle = makeHandle(kernel, runningWorker, threadWorkerStates);
  handle.workers.add(readyWorker);
  handle.runningWorker = runningWorker;
  handle.vforkReadyWorkers = [readyWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);
  await vi.advanceTimersByTimeAsync(2_000);

  expect(handle.runningWorker).toBe(runningWorker);
  expect(readyWorker.postMessage).not.toHaveBeenCalled();

  activeStepVmPtr = 0;
  await vi.advanceTimersByTimeAsync(2_000);

  expect(runningThread.runningGeneration).toBe(runningThread.generation);
  expect(handle.runningWorker).toBe(runningWorker);
  expect(readyWorker.postMessage).not.toHaveBeenCalled();
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess preserves a dispatched worker when a sibling remains queued", async () => {
  vi.setSystemTime(new Date(10_000));
  const firstWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const secondWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const firstThread = makeThreadState(0x66c42c8);
  const secondThread = makeThreadState(0x64a6e20);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(firstWorker, firstThread);
  threadWorkerStates.set(secondWorker, secondThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => 0;
  const handle = makeHandle(kernel, firstWorker, threadWorkerStates);
  handle.workers.add(secondWorker);
  handle.vforkReadyWorkers = [firstWorker, secondWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(firstWorker);
  expect(firstThread.runningGeneration).toBe(firstThread.generation);
  expect(handle.vforkReadyWorkers).toEqual([secondWorker]);

  vi.setSystemTime(new Date(12_000));
  await vi.advanceTimersByTimeAsync(2_000);

  expect(firstThread.runningGeneration).toBe(firstThread.generation);
  expect(handle.runningWorker).toBe(firstWorker);
  expect(firstWorker.postMessage).toHaveBeenCalledTimes(1);
  expect(secondWorker.postMessage).not.toHaveBeenCalled();
  expect(handle.vforkReadyWorkers).toEqual([secondWorker]);
});

test("pumpProcess runs ordinary OK ready workers under blocked I/O pressure", () => {
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyThread = makeThreadState(0x7127a90);
  const blockedThread = makeThreadState(0x7141c68);
  readyThread.lastStatus = 0;
  readyThread.stepCount = 27;
  readyThread.statusCount = 27;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(readyWorker, readyThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, readyWorker, threadWorkerStates);
  handle.workers.add(blockedWorker);
  handle.vforkReadyWorkers = [readyWorker];
  handle.hostBlockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    rpcPending: false,
    rpcGeneration: 0,
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(readyWorker);
  expect(handle.vforkReadyWorkers).toEqual([]);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: 5_000_000,
  }));
});

test("pumpProcess runs clone-spawn ready workers under blocked I/O pressure", () => {
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyThread = makeThreadState(0x5860320);
  const blockedThread = makeThreadState(0x6286130);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(readyWorker, readyThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  readyThread.lastStatus = kernel.abi_status_clone_spawn();
  readyThread.stepCount = 59;
  readyThread.statusCount = 59;
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, readyWorker, threadWorkerStates);
  handle.workers.add(blockedWorker);
  handle.vforkReadyWorkers = [readyWorker];
  handle.hostBlockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    rpcPending: false,
    rpcGeneration: 0,
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(readyWorker);
  expect(handle.vforkReadyWorkers).toEqual([]);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: 5_000_000,
  }));
  expect(blockedWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess runs ordinary OK ready workers while blocked I/O retry timer waits", () => {
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyThread = makeThreadState(0x70bf0d8);
  const blockedThread = makeThreadState(0x7141c68);
  readyThread.lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(readyWorker, readyThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, readyWorker, threadWorkerStates);
  handle.workers.add(blockedWorker);
  handle.vforkReadyWorkers = [readyWorker];
  handle.hostBlockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    rpcPending: false,
    rpcGeneration: 0,
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(readyWorker);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: 5_000_000,
  }));
});

test("pumpProcess runs yield-ready workers when no other ready worker exists", () => {
  const yieldWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const yieldThread = makeThreadState(0x70c8e98);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  yieldThread.lastStatus = 26;
  threadWorkerStates.set(yieldWorker, yieldThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  const handle = makeHandle(kernel, yieldWorker, threadWorkerStates);
  handle.vforkReadyWorkers = [yieldWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(yieldWorker);
  expect(yieldWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: 5_000_000,
  }));
});

test("pumpProcess preserves a stale unanswered ok running worker without active-step proof", async () => {
  vi.setSystemTime(new Date(10_000));
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x9ba7260);
  thread.runningGeneration = thread.generation;
  thread.stepCount = 12;
  thread.statusCount = 11;
  thread.lastStepAtMs = 7_500;
  thread.lastStatusAtMs = 7_000;
  thread.lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  delete kernel.debug_thread_step_vm_ptr;
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.runningWorker = worker;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(worker);
  expect(thread.runningGeneration).toBe(thread.generation);
  expect(thread.stepCount).toBe(12);
  expect(worker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess preserves a stale unanswered ok running worker when last trace points elsewhere", async () => {
  vi.setSystemTime(new Date(10_000));
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x57fe3d0);
  thread.runningGeneration = thread.generation;
  thread.stepCount = 101;
  thread.statusCount = 100;
  thread.lastStepAtMs = 7_500;
  thread.lastStatusAtMs = 7_500;
  thread.lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => 0;
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.runningWorker = worker;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(worker);
  expect(thread.runningGeneration).toBe(thread.generation);
  expect(thread.stepCount).toBe(101);
  expect(worker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess preserves a stale unanswered no-ready running worker with pending guest writes", async () => {
  vi.setSystemTime(new Date(10_000));
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x9ba7260);
  thread.runningGeneration = thread.generation;
  thread.stepCount = 12;
  thread.statusCount = 11;
  thread.lastStepAtMs = 7_500;
  thread.lastStatusAtMs = 7_000;
  thread.lastStatus = 0;
  thread.pendingGuestMemoryWrites.push({
    guestPtr: 0x4000,
    bytes: new Uint8Array([1, 2, 3]),
  });
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  delete kernel.debug_thread_step_vm_ptr;
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.runningWorker = worker;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(worker);
  expect(thread.runningGeneration).toBe(thread.generation);
  expect(worker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess preserves surplus unanswered running work before recovering idle work", () => {
  vi.setSystemTime(new Date(10_000));
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const idleWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x9ba7260);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 70;
  runningThread.statusCount = 11;
  runningThread.lastStepAtMs = 7_500;
  runningThread.lastStatusAtMs = 7_000;
  runningThread.lastStatus = 0;
  const idleThread = makeThreadState(0x6648420);
  idleThread.stepCount = 162;
  idleThread.statusCount = 161;
  idleThread.lastStepAtMs = 7_500;
  idleThread.lastStatusAtMs = 7_499;
  idleThread.lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(idleWorker, idleThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  delete kernel.debug_thread_step_vm_ptr;
  const handle = makeHandle(kernel, runningWorker, threadWorkerStates);
  handle.workers.add(idleWorker);
  handle.runningWorker = runningWorker;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(runningThread.runningGeneration).toBe(runningThread.generation);
  expect(runningThread.stepCount).toBe(70);
  expect(idleThread.runningGeneration).toBeUndefined();
  expect(idleThread.stepCount).toBe(162);
  expect(handle.runningWorker).toBe(runningWorker);
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
  expect(idleWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess runs an OK ready worker after an immediate epoll retry attempt", () => {
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyThread = makeThreadState(0x5860320);
  readyThread.stepCount = 42;
  readyThread.statusCount = 42;
  readyThread.lastStatus = 0;
  const blockedThread = makeThreadState(0x6286130);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(readyWorker, readyThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = (vmPtr: number) => vmPtr === blockedThread.vmPtr
    ? kernel.abi_status_epoll_wait()
    : kernel.abi_status_ok();
  const handle = makeHandle(kernel, readyWorker, threadWorkerStates);
  handle.workers.add(blockedWorker);
  handle.vforkReadyWorkers = [readyWorker];
  handle.hostBlockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    rpcPending: false,
    rpcGeneration: 0,
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(readyWorker);
  expect(readyThread.runningGeneration).toBe(readyThread.generation);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(blockedWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess preserves an unanswered running worker when the step shares a status millisecond", () => {
  vi.setSystemTime(new Date(10_000));
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x66c42c8);
  const readyThread = makeThreadState(0x64a6e20);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 3;
  runningThread.statusCount = 2;
  runningThread.lastStepAtMs = 7_500;
  runningThread.lastStatusAtMs = 7_500;
  runningThread.lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(readyWorker, readyThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => 0;
  const handle = makeHandle(kernel, runningWorker, threadWorkerStates);
  handle.workers.add(readyWorker);
  handle.runningWorker = runningWorker;
  handle.vforkReadyWorkers = [readyWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(runningThread.runningGeneration).toBe(runningThread.generation);
  expect(handle.runningWorker).toBe(runningWorker);
  expect(readyWorker.postMessage).not.toHaveBeenCalled();
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess preserves an unanswered ok running worker when status covers the step time", () => {
  vi.setSystemTime(new Date(10_000));
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x6648420);
  const readyThread = makeThreadState(0x9ba7260);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 162;
  runningThread.statusCount = 161;
  runningThread.lastStepAtMs = 7_500;
  runningThread.lastStatusAtMs = 7_500;
  runningThread.lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(readyWorker, readyThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  delete kernel.debug_thread_step_vm_ptr;
  const handle = makeHandle(kernel, runningWorker, threadWorkerStates);
  handle.workers.add(readyWorker);
  handle.runningWorker = runningWorker;
  handle.vforkReadyWorkers = [readyWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(runningThread.runningGeneration).toBe(runningThread.generation);
  expect(handle.runningWorker).toBe(runningWorker);
  expect(readyWorker.postMessage).not.toHaveBeenCalled();
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess preserves an unanswered ok running worker when the step is stale", () => {
  vi.setSystemTime(new Date(10_000));
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x6648420);
  const readyThread = makeThreadState(0x9ba7260);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 162;
  runningThread.statusCount = 161;
  runningThread.lastStepAtMs = 7_500;
  runningThread.lastStatusAtMs = 7_000;
  runningThread.lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(readyWorker, readyThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  delete kernel.debug_thread_step_vm_ptr;
  const handle = makeHandle(kernel, runningWorker, threadWorkerStates);
  handle.workers.add(readyWorker);
  handle.runningWorker = runningWorker;
  handle.vforkReadyWorkers = [readyWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(runningThread.runningGeneration).toBe(runningThread.generation);
  expect(handle.runningWorker).toBe(runningWorker);
  expect(readyWorker.postMessage).not.toHaveBeenCalled();
  expect(runningWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess completes exit when all canonical threads fault after workers were released", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  const { exports: kernel } = makeKernel();
  kernel.process_thread_vm_ptr = (_vmPtr: number, slot: number) => {
    if (slot === 0) {
      return 0x581f470;
    }
    if (slot === 1) {
      return 0x6645d48;
    }
    return 0;
  };
  kernel.thread_status = () => kernel.abi_status_fault();
  kernel.thread_exit_code = () => 1;
  kernel.thread_term_signal = () => 0;
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  installTestSupervisorFaultExitDecision(kernel, handle, {
    exitVmPtr: 0x581f470,
    exitCode: 1,
    termSignal: 0,
    remainingThreads: 2,
    liveThreads: 2,
  });
  handle.workers.clear();
  handle.vforkReadyWorkers = [];
  handle.runningWorker = null;
  handle.hostBlockedWorkers.clear();
  const appendRuntimeLifecycleEvent = vi.fn();
  const completeProcessExit = vi.fn(async () => {});

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent,
    threadWorkerStates,
    completeProcessExit,
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(completeProcessExit).toHaveBeenCalledWith(handle, 1, 0, 0x581f470);
  expect(appendRuntimeLifecycleEvent).toHaveBeenCalledWith(
    1,
    "complete-exit-request:no-workers-fault:1:vm=0x581f470:exit=1:0:seq=7:threads=2/2",
  );
  expect(worker.postMessage).not.toHaveBeenCalled();
});

test("epoll retry keeps unanswered futex running worker when last trace points elsewhere", () => {
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x7127a90);
  const blockedThread = makeThreadState(0x7141c68);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 2;
  runningThread.statusCount = 1;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  runningThread.lastStatus = kernel.abi_status_futex_wait();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => 0;
  const handle = makeHandle(kernel, runningWorker, threadWorkerStates);
  handle.workers.add(blockedWorker);
  handle.runningWorker = runningWorker;
  handle.vforkReadyWorkers = [];
  handle.hostBlockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    rpcPending: false,
    rpcGeneration: 0,
  });
  const scheduler = createRunLoopScheduler({
    threadWorkerStates,
    retryHostBlockedWorker: vi.fn(),
  });

  expect(scheduler.clearStaleRunningWorker(handle, blockedWorker)).toBe(false);

  expect(handle.runningWorker).toBe(runningWorker);
  expect(runningThread.runningGeneration).toBe(runningThread.generation);
  expect(handle.vforkReadyWorkers).not.toContain(runningWorker);
});

test("epoll retry keeps unanswered futex running worker with a blocked record", () => {
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x7127a90);
  const blockedThread = makeThreadState(0x7141c68);
  const stackPointer = 0xa467710n;
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 2;
  runningThread.statusCount = 1;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  runningThread.lastStatus = kernel.abi_status_futex_wait();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => 0;
  kernel.thread_sp = (vmPtr: number) => (
    vmPtr === runningThread.vmPtr ? stackPointer : 0x8000n
  );
  kernel.thread_futex_wait_addr = () => 0n;
  kernel.thread_record_blocked_addr = () => 0n;
  const handle = makeHandle(kernel, runningWorker, threadWorkerStates);
  handle.workers.add(blockedWorker);
  handle.runningWorker = runningWorker;
  handle.vforkReadyWorkers = [];
  handle.hostBlockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    rpcPending: false,
    rpcGeneration: 0,
  });
  const scheduler = createRunLoopScheduler({
    threadWorkerStates,
    retryHostBlockedWorker: vi.fn(),
  });

  expect(scheduler.clearStaleRunningWorker(handle, blockedWorker)).toBe(false);

  expect(handle.runningWorker).toBe(runningWorker);
  expect(runningThread.runningGeneration).toBe(runningThread.generation);
  expect(runningThread.waitingInWorkerStatus).toBeNull();
  expect(handle.vforkReadyWorkers).not.toContain(runningWorker);
});

test("epoll retry keeps active unanswered ok running worker", () => {
  vi.setSystemTime(10_000);
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeThreadState(0x585f220);
  const blockedThread = makeThreadState(0x6285030);
  runningThread.runningGeneration = runningThread.generation;
  runningThread.stepCount = 827;
  runningThread.statusCount = 825;
  runningThread.lastStatus = 0;
  runningThread.lastStatusAtMs = 7_000;
  runningThread.lastStepAtMs = 7_100;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  runningThread.lastStatus = kernel.abi_status_ok();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.debug_thread_step_vm_ptr = () => runningThread.vmPtr;
  const handle = makeHandle(kernel, runningWorker, threadWorkerStates);
  handle.workers.add(blockedWorker);
  handle.runningWorker = runningWorker;
  handle.hostBlockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    rpcPending: false,
    rpcGeneration: 0,
  });
  const scheduler = createRunLoopScheduler({
    threadWorkerStates,
    retryHostBlockedWorker: vi.fn(),
  });

  expect(scheduler.clearStaleRunningWorker(handle, blockedWorker)).toBe(false);

  expect(handle.runningWorker).toBe(runningWorker);
  expect(runningThread.runningGeneration).toBe(runningThread.generation);
});

test("pumpProcess waits for socket status publication before dispatching a ready sibling", () => {
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(runningWorker, makeThreadState(0x5678));
  threadWorkerStates.set(readyWorker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x5678
    ? kernel.abi_status_socket_recv()
    : kernel.abi_status_ok();
  const handle = makeHandle(kernel, runningWorker, threadWorkerStates);
  handle.workers.add(readyWorker);
  handle.runningWorker = runningWorker;
  handle.vforkReadyWorkers = [readyWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(runningWorker);
  expect(handle.hostBlockedWorkers.has(runningWorker)).toBe(false);
  expect(readyWorker.postMessage).not.toHaveBeenCalled();
});

test("pruning a stale running worker clears the running slot", () => {
  const staleWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const anchorWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(staleWorker, makeThreadState(0x1234));
  threadWorkerStates.set(anchorWorker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  kernel.process_thread_vm_ptr = (vmPtr: number, slot: number) => {
    if (vmPtr === 0x5678 && slot === 0) {
      return 0x5678;
    }
    return 0;
  };
  kernel.thread_process_ptr = (vmPtr: number) => vmPtr === 0x5678 ? 0x2000 : 0;
  const handle = makeHandle(kernel, staleWorker, threadWorkerStates);
  handle.workers.add(anchorWorker);
  handle.runningWorker = staleWorker;
  handle.pendingChildSpawnRecords = 0;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pruneDetachedProcessWorkers(handle, 0x5678, null);

  expect(handle.runningWorker).toBeNull();
  expect(handle.workers.has(staleWorker)).toBe(false);
  expect(handle.workers.has(anchorWorker)).toBe(true);
});

test("pruning preserves a worker whose thread state still belongs to the process", () => {
  const detachedWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const anchorWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(detachedWorker, makeThreadState(0x1234));
  threadWorkerStates.set(anchorWorker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  kernel.process_thread_vm_ptr = (vmPtr: number, slot: number) => {
    if (vmPtr === 0x5678 && slot === 0) {
      return 0x5678;
    }
    return 0;
  };
  kernel.thread_process_ptr = (vmPtr: number) => (
    vmPtr === 0x1234 || vmPtr === 0x5678 ? 0x2000 : 0
  );
  const handle = makeHandle(kernel, detachedWorker, threadWorkerStates);
  handle.workers.add(anchorWorker);
  handle.runningWorker = detachedWorker;
  handle.pendingChildSpawnRecords = 0;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pruneDetachedProcessWorkers(handle, 0x5678, null);

  expect(handle.runningWorker).toBe(detachedWorker);
  expect(handle.workers.has(detachedWorker)).toBe(true);
  expect(handle.workers.has(anchorWorker)).toBe(true);
  expect(detachedWorker.terminate).not.toHaveBeenCalled();
});

test("pruning preserves a detached worker in vfork wait", () => {
  const vforkWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const anchorWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(vforkWorker, makeThreadState(0x1234));
  threadWorkerStates.set(anchorWorker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  kernel.process_thread_vm_ptr = (vmPtr: number, slot: number) => {
    if (vmPtr === 0x5678 && slot === 0) {
      return 0x5678;
    }
    return 0;
  };
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234 ? kernel.abi_status_vfork_wait() : kernel.abi_status_ok();
  const handle = makeHandle(kernel, vforkWorker, threadWorkerStates);
  handle.workers.add(anchorWorker);
  handle.runningWorker = vforkWorker;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pruneDetachedProcessWorkers(handle, 0x5678, null);

  expect(handle.runningWorker).toBe(vforkWorker);
  expect(handle.workers.has(vforkWorker)).toBe(true);
  expect(handle.workers.has(anchorWorker)).toBe(true);
});

test("pruning preserves a detached worker during execve spawn handoff", () => {
  const execveWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const anchorWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(execveWorker, makeThreadState(0x1234));
  threadWorkerStates.set(anchorWorker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  kernel.process_thread_vm_ptr = (vmPtr: number, slot: number) => {
    if (vmPtr === 0x5678 && slot === 0) {
      return 0x5678;
    }
    return 0;
  };
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234 ? kernel.abi_status_execve_spawn() : kernel.abi_status_ok();
  const handle = makeHandle(kernel, execveWorker, threadWorkerStates);
  handle.workers.add(anchorWorker);

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pruneDetachedProcessWorkers(handle, 0x5678, null);

  expect(handle.workers.has(execveWorker)).toBe(true);
  expect(handle.workers.has(anchorWorker)).toBe(true);
});

test("pruning is deferred while child spawn records are still pending", () => {
  const staleWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const anchorWorker = {
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(staleWorker, makeThreadState(0x1234));
  threadWorkerStates.set(anchorWorker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  kernel.process_thread_vm_ptr = (vmPtr: number, slot: number) => {
    if (vmPtr === 0x5678 && slot === 0) {
      return 0x5678;
    }
    return 0;
  };
  const handle = makeHandle(kernel, staleWorker, threadWorkerStates);
  handle.workers.add(anchorWorker);
  handle.pendingChildSpawnRecords = 1;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pruneDetachedProcessWorkers(handle, 0x5678, null);

  expect(handle.workers.has(staleWorker)).toBe(true);
  expect(handle.workers.has(anchorWorker)).toBe(true);
});

test("enqueue clears a dangling running worker before stepping the next ready worker", () => {
  const staleWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const readyWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(readyWorker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  const handle = makeHandle(kernel, readyWorker, threadWorkerStates);
  handle.workers.add(staleWorker);
  handle.runningWorker = staleWorker;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  handle.workers.delete(staleWorker);
  scheduler.enqueueWorker(handle, readyWorker);

  expect(handle.runningWorker).toBe(readyWorker);
  expect(readyWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("step publishes the full process fd table to the selected worker", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x5678));
  const { exports: kernel } = makeKernel();
  kernel.abi_max_fds = () => 6;
  kernel.process_fd_ofd_id = (vmPtr: number, fd: number) => {
    if (vmPtr !== 0x5678) {
      return fd === 4 ? 99 : 0;
    }
    if (fd === 4) {
      return 7;
    }
    if (fd === 5) {
      return 8;
    }
    return 0;
  };
  kernel.process_fd_flags = (vmPtr: number, fd: number) => {
    if (vmPtr !== 0x5678) {
      return fd === 4 ? 2 : 0;
    }
    return fd === 4 ? 1 : 0;
  };
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.vforkReadyWorkers = [worker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    fdEntries: [
      { fd: 0, ofdId: 0, fdFlags: 0 },
      { fd: 1, ofdId: 0, fdFlags: 0 },
      { fd: 2, ofdId: 0, fdFlags: 0 },
      { fd: 3, ofdId: 0, fdFlags: 0 },
      { fd: 4, ofdId: 99, fdFlags: 2 },
      { fd: 5, ofdId: 0, fdFlags: 0 },
    ],
  }));
});

test("pumpProcess runs supervisor-selected ready worker while preserving a still-waiting worker", () => {
  const waitingWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runnableWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const waitingThread = makeThreadState(0x1234);
  const runnableThread = makeThreadState(0x5678);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(waitingWorker, waitingThread);
  threadWorkerStates.set(runnableWorker, runnableThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = (vmPtr: number) => vmPtr === 0x1234
    ? kernel.abi_status_futex_wait()
    : kernel.abi_status_ok();
  const handle = makeHandle(kernel, waitingWorker, threadWorkerStates);
  handle.workers.add(runnableWorker);
  handle.vforkReadyWorkers = [waitingWorker, runnableWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(runnableWorker);
  expect(waitingWorker.postMessage).not.toHaveBeenCalled();
  expect(runnableWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(handle.vforkReadyWorkers).toEqual([waitingWorker]);
});

test("pumpProcess skips a ready worker that is still in canonical futex wait", () => {
  const waitingWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runnableWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const waitingThread = makeThreadState(0x1234);
  const runnableThread = makeThreadState(0x5678);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(waitingWorker, waitingThread);
  threadWorkerStates.set(runnableWorker, runnableThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = (vmPtr: number) => vmPtr === waitingThread.vmPtr
    ? kernel.abi_status_futex_wait()
    : kernel.abi_status_ok();
  const handle = makeHandle(kernel, waitingWorker, threadWorkerStates);
  handle.workers.add(runnableWorker);
  handle.vforkReadyWorkers = [waitingWorker, runnableWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(handle.runningWorker).toBe(runnableWorker);
  expect(waitingWorker.postMessage).not.toHaveBeenCalled();
  expect(runnableWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("pumpProcess skips a ready worker whose thread record is still futex-blocked", () => {
  const waitingWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runnableWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const waitingThread = makeThreadState(0x1234);
  const runnableThread = makeThreadState(0x5678);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(waitingWorker, waitingThread);
  threadWorkerStates.set(runnableWorker, runnableThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.thread_record_blocked_addr = (vmPtr: number) => vmPtr === waitingThread.vmPtr
    ? 0xa46787cn
    : 0n;
  kernel.thread_futex_wait_addr = (vmPtr: number) => vmPtr === waitingThread.vmPtr
    ? 0xa46787cn
    : 0n;
  const handle = makeHandle(kernel, waitingWorker, threadWorkerStates);
  handle.workers.add(runnableWorker);
  handle.vforkReadyWorkers = [waitingWorker, runnableWorker];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(waitingThread.waitingInWorkerStatus).toBeNull();
  expect(handle.runningWorker).toBe(runnableWorker);
  expect(waitingWorker.postMessage).not.toHaveBeenCalled();
  expect(runnableWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("pumpProcess drains only supervisor-selected futex resumes when supervisor queue is available", () => {
  const selectedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const unselectedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const selectedThread = makeThreadState(0x1234);
  const unselectedThread = makeThreadState(0x5678);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(selectedWorker, selectedThread);
  threadWorkerStates.set(unselectedWorker, unselectedThread);
  const { exports: kernel } = makeKernel();
  let resumePolled = false;
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.thread_record_blocked_addr = () => 0n;
  kernel.thread_futex_wait_addr = () => 0n;
  kernel.supervisor_resume_record_bytes = () => 48;
  kernel.supervisor_resume_kind_futex_wake = () => 1;
  kernel.kernel_scratch_base = () => 0;
  kernel.supervisor_next_resume = vi.fn((outPtr: number, outLen: number) => {
    if (resumePolled) {
      return 0;
    }
    expect(outPtr).toBe(0);
    expect(outLen).toBe(48);
    const view = new DataView(kernel.memory.buffer);
    view.setUint32(0, 0x5255_5354, true);
    view.setUint32(4, 1, true);
    view.setUint32(8, 48, true);
    view.setUint32(12, 1, true);
    view.setBigUint64(16, 1n, true);
    view.setUint32(24, selectedThread.vmPtr, true);
    view.setUint32(28, 2, true);
    view.setUint32(32, 0x2000, true);
    view.setUint32(36, kernel.abi_status_ok(), true);
    view.setBigUint64(40, 0xfeedn, true);
    resumePolled = true;
    return 48;
  });
  const handle = makeHandle(kernel, selectedWorker, threadWorkerStates);
  kernel.memory = handle.memory;
  handle.workers.add(unselectedWorker);

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(kernel.supervisor_next_resume).toHaveBeenCalledTimes(2);
  expect(kernel.supervisor_complete_futex_wait).not.toHaveBeenCalled();
  expect(selectedThread.waitingInWorkerStatus).toBeNull();
  expect(unselectedThread.waitingInWorkerStatus).toBeNull();
  expect(handle.runningWorker).toBe(selectedWorker);
  expect(selectedWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(unselectedWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess records supervisor thread-state snapshot diagnostics for selected futex resumes", () => {
  const selectedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const unselectedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const selectedThread = makeThreadState(0x1234);
  const unselectedThread = makeThreadState(0x5678);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(selectedWorker, selectedThread);
  threadWorkerStates.set(unselectedWorker, unselectedThread);
  const { exports: kernel } = makeKernel();
  let resumePolled = false;
  kernel.thread_status = (vmPtr: number) => vmPtr === selectedThread.vmPtr
    ? kernel.abi_status_ok()
    : kernel.abi_status_futex_wait();
  kernel.thread_record_blocked_addr = (vmPtr: number) => vmPtr === unselectedThread.vmPtr ? 0xbeefn : 0n;
  kernel.thread_futex_wait_addr = (vmPtr: number) => vmPtr === unselectedThread.vmPtr ? 0xbeefn : 0n;
  kernel.supervisor_resume_record_bytes = () => 48;
  kernel.supervisor_resume_kind_futex_wake = () => 1;
  kernel.supervisor_thread_state_snapshot_bytes = () => 2080;
  kernel.kernel_scratch_base = () => 0;
  kernel.supervisor_next_resume = vi.fn((outPtr: number, outLen: number) => {
    if (resumePolled) {
      return 0;
    }
    expect(outPtr).toBe(0);
    expect(outLen).toBe(48);
    const view = new DataView(kernel.memory.buffer);
    view.setUint32(0, 0x5255_5354, true);
    view.setUint32(4, 1, true);
    view.setUint32(8, 48, true);
    view.setUint32(12, 1, true);
    view.setBigUint64(16, 1n, true);
    view.setUint32(24, selectedThread.vmPtr, true);
    view.setUint32(28, 2, true);
    view.setUint32(32, 0x2000, true);
    view.setUint32(36, kernel.abi_status_ok(), true);
    view.setBigUint64(40, 0xfeedn, true);
    resumePolled = true;
    return 48;
  });
  kernel.supervisor_thread_state_snapshot = vi.fn((vmPtr: number, outPtr: number, outLen: number) => {
    expect(vmPtr).toBe(selectedThread.vmPtr);
    expect(outPtr).toBe(0);
    expect(outLen).toBe(2080);
    const view = new DataView(kernel.memory.buffer);
    view.setUint32(0, 0x4854_5354, true);
    view.setUint32(4, 1, true);
    view.setUint32(8, 2080, true);
    view.setUint32(12, 64, true);
    view.setUint32(16, 0x2000, true);
    view.setUint32(20, selectedThread.vmPtr, true);
    view.setUint32(24, 32, true);
    view.setUint32(28, 0, true);
    view.setUint32(32, 2, true);
    view.setUint32(36, 2, true);
    view.setUint32(40, selectedThread.vmPtr, true);
    view.setUint32(44, kernel.abi_status_ok(), true);
    view.setBigUint64(48, 0n, true);
    view.setBigUint64(56, 0n, true);
    view.setUint32(64, 3, true);
    view.setUint32(68, 3, true);
    view.setUint32(72, unselectedThread.vmPtr, true);
    view.setUint32(76, kernel.abi_status_futex_wait(), true);
    view.setBigUint64(80, 0xbeefn, true);
    view.setBigUint64(88, 0x44n, true);
    return 2080;
  });
  kernel.supervisor_process_resume = vi.fn(() => 0);
  const handle = makeHandle(kernel, selectedWorker, threadWorkerStates);
  kernel.memory = handle.memory;
  handle.workers.add(unselectedWorker);
  const debugEvents: string[] = [];
  const queue = createWorkerSchedulerQueueHelpers({
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: (_handle, event) => {
      debugEvents.push(event);
    },
    blockWorker: () => {},
    clearHostBlockedRetryTimer: () => {},
    takeThreadWorkerBlockedResume: () => false,
    isSupervisorFutexWaiter: (targetHandle, worker) => {
      const thread = threadWorkerStates.get(worker);
      return thread !== undefined
        && targetHandle.kernel.thread_status(thread.vmPtr) === targetHandle.kernel.abi_status_futex_wait();
    },
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    handleWorkerMessage: () => {},
    threadWorkerStates,
  });

  queue.pumpProcess(handle);

  expect(kernel.supervisor_thread_state_snapshot).toHaveBeenCalledTimes(1);
  expect(selectedThread.waitingInWorkerStatus).toBeNull();
  expect(unselectedThread.waitingInWorkerStatus).toBeNull();
  expect(debugEvents).toContain(
    "supervisor-resume:futex-wake:observed:0x1234:tid=2:seq=1:process=0x2000:detail=0xfeed:status=0"
      + ":snapshot=process=0x2000:owner=0x1234"
      + ":threads=2/2/0x1234/0/0x0,3/3/0x5678/20/0xbeef/sig=0x44",
  );
});

test("pumpProcess runs process-scoped supervisor resume worker instead of TypeScript ready priority", () => {
  const selectedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const unselectedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const selectedThread = makeThreadState(0x1234);
  const unselectedThread = makeThreadState(0x5678);
  selectedThread.lastStatus = 26;
  unselectedThread.lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(selectedWorker, selectedThread);
  threadWorkerStates.set(unselectedWorker, unselectedThread);
  const { exports: kernel } = makeKernel();
  kernel.kernel_scratch_base = () => 0;
  kernel.supervisor_resume_record_bytes = () => 48;
  kernel.supervisor_resume_kind_runnable_thread = () => 2;
  kernel.supervisor_process_resume = vi.fn((
    vmPtr: number,
    outPtr: number,
    outLen: number,
  ) => {
    expect(vmPtr).toBe(0x1234);
    expect(outPtr).toBe(0);
    expect(outLen).toBe(48);
    const view = new DataView(kernel.memory.buffer);
    view.setUint32(outPtr, 0x5255_5354, true);
    view.setUint32(outPtr + 4, 1, true);
    view.setUint32(outPtr + 8, 48, true);
    view.setUint32(outPtr + 12, 2, true);
    view.setBigUint64(outPtr + 16, 1n, true);
    view.setUint32(outPtr + 24, selectedThread.vmPtr, true);
    view.setUint32(outPtr + 28, 2, true);
    view.setUint32(outPtr + 32, 0x2000, true);
    view.setUint32(outPtr + 36, kernel.abi_status_ok(), true);
    view.setBigUint64(outPtr + 40, 1n, true);
    return 48;
  });
  const handle = makeHandle(kernel, selectedWorker, threadWorkerStates);
  kernel.memory = handle.memory;
  handle.workers.add(unselectedWorker);
  handle.vforkReadyWorkers = [selectedWorker, unselectedWorker];
  const debugEvents: string[] = [];
  const queue = createWorkerSchedulerQueueHelpers({
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: (_handle, event) => {
      debugEvents.push(event);
    },
    blockWorker: () => {},
    clearHostBlockedRetryTimer: () => {},
    takeThreadWorkerBlockedResume: () => false,
    isSupervisorFutexWaiter: () => false,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    handleWorkerMessage: () => {},
    threadWorkerStates,
  });

  queue.pumpProcess(handle);

  expect(kernel.supervisor_process_resume).toHaveBeenCalledTimes(1);
  expect(handle.runningWorker).toBe(selectedWorker);
  expect(handle.vforkReadyWorkers).toEqual([unselectedWorker]);
  expect(selectedWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
  expect(unselectedWorker.postMessage).not.toHaveBeenCalled();
  expect(debugEvents).toContain(
    "supervisor-resume:runnable:selected:0x1234:tid=2:seq=1:process=0x2000:status=0:detail=0x1",
  );
});

test("pumpProcess does not run TypeScript continuation when supervisor process resume selects no runnable worker", () => {
  const ordinaryWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const continuationWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const ordinaryThread = makeThreadState(0x1234);
  const continuationThread = makeThreadState(0x5678);
  continuationThread.pendingPipeSlotsForNextStep.push({
    pipeSlot: 1,
    slot: new Uint8Array(16),
  } as never);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(ordinaryWorker, ordinaryThread);
  threadWorkerStates.set(continuationWorker, continuationThread);
  const { exports: kernel } = makeKernel();
  ordinaryThread.lastStatus = kernel.abi_status_ok();
  continuationThread.lastStatus = kernel.abi_status_pipe_wait();
  kernel.kernel_scratch_base = () => 0;
  kernel.supervisor_resume_record_bytes = () => 48;
  kernel.supervisor_process_resume = vi.fn((
    vmPtr: number,
    outPtr: number,
    outLen: number,
  ) => {
    expect(vmPtr).toBe(0x1234);
    expect(outPtr).toBe(0);
    expect(outLen).toBe(48);
    return 0;
  });
  const handle = makeHandle(kernel, ordinaryWorker, threadWorkerStates);
  kernel.memory = handle.memory;
  handle.workers.add(continuationWorker);
  handle.vforkReadyWorkers = [ordinaryWorker, continuationWorker];
  const debugEvents: string[] = [];
  const queue = createWorkerSchedulerQueueHelpers({
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: (_handle, event) => {
      debugEvents.push(event);
    },
    blockWorker: () => {},
    clearHostBlockedRetryTimer: () => {},
    takeThreadWorkerBlockedResume: () => false,
    isSupervisorFutexWaiter: () => false,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    handleWorkerMessage: () => {},
    threadWorkerStates,
  });

  queue.pumpProcess(handle);

  expect(kernel.supervisor_process_resume).toHaveBeenCalledTimes(1);
  expect(handle.runningWorker).toBeNull();
  expect(handle.vforkReadyWorkers).toEqual([ordinaryWorker, continuationWorker]);
  expect(ordinaryWorker.postMessage).not.toHaveBeenCalled();
  expect(continuationWorker.postMessage).not.toHaveBeenCalled();
  expect(debugEvents).toContain("supervisor-resume:runnable:none:ready=2");
});

test("pumpProcess leaves a quiescent stack-local futex waiter parked with a blocked record", () => {
  const waitingWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const waitingThread = makeThreadState(0x57fe3d0);
  const blockedThread = makeThreadState(0x7759fb0);
  const stackPointer = 0xa4676f0n;
  const stackLocalFutexAddr = 0xa467858n;
  waitingThread.stepCount = 97;
  waitingThread.statusCount = 97;
  waitingThread.lastStatus = 20;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(waitingWorker, waitingThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  let waitingStatus = kernel.abi_status_futex_wait();
  const completeSupervisorFutexWait = vi.fn((vmPtr: number, completion: number) => {
    expect(vmPtr).toBe(waitingThread.vmPtr);
    expect(completion).toBe(kernel.supervisor_futex_wait_completion_woken());
    waitingStatus = kernel.abi_status_ok();
    return waitingStatus;
  });
  kernel.supervisor_complete_futex_wait = completeSupervisorFutexWait;
  kernel.thread_status = (vmPtr: number) => {
    if (vmPtr === waitingThread.vmPtr) {
      return waitingStatus;
    }
    if (vmPtr === blockedThread.vmPtr) {
      return kernel.abi_status_epoll_wait();
    }
    return kernel.abi_status_ok();
  };
  kernel.thread_sp = (vmPtr: number) => (
    vmPtr === waitingThread.vmPtr ? stackPointer : 0x8000n
  );
  kernel.thread_futex_wait_addr = (vmPtr: number) => (
    vmPtr === waitingThread.vmPtr ? stackLocalFutexAddr : 0n
  );
  kernel.thread_record_blocked_addr = (vmPtr: number) => (
    vmPtr === waitingThread.vmPtr ? stackLocalFutexAddr : 0n
  );
  const handle = makeHandle(kernel, waitingWorker, threadWorkerStates);
  handle.workers.add(blockedWorker);
  handle.hostBlockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_epoll_wait(),
    rpcPending: false,
    rpcGeneration: 0,
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(completeSupervisorFutexWait).not.toHaveBeenCalled();
  expect(waitingThread.waitingInWorkerStatus).toBeNull();
  expect(handle.runningWorker).toBeNull();
  expect(waitingWorker.postMessage).not.toHaveBeenCalled();
  expect(blockedWorker.postMessage).not.toHaveBeenCalled();
});

test("pumpProcess leaves a quiescent worker-local futex waiter parked until its record is woken", () => {
  const waitingWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const waitingThread = makeThreadState(0x57fe3d0);
  const blockedThread = makeThreadState(0x7759fb0);
  waitingThread.stepCount = 97;
  waitingThread.statusCount = 97;
  waitingThread.lastStatus = 20;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(waitingWorker, waitingThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = (vmPtr: number) => {
    if (vmPtr === waitingThread.vmPtr) {
      return kernel.abi_status_futex_wait();
    }
    if (vmPtr === blockedThread.vmPtr) {
      return kernel.abi_status_pipe_wait();
    }
    return kernel.abi_status_ok();
  };
  kernel.thread_record_blocked_addr = (vmPtr: number) => (
    vmPtr === waitingThread.vmPtr ? 0x162c1d0n : 0n
  );
  kernel.thread_futex_wait_addr = (vmPtr: number) => (
    vmPtr === waitingThread.vmPtr ? 0x162c1d0n : 0n
  );
  const handle = makeHandle(kernel, waitingWorker, threadWorkerStates);
  handle.workers.add(blockedWorker);
  handle.hostBlockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_pipe_wait(),
    rpcPending: false,
    rpcGeneration: 0,
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.pumpProcess(handle);

  expect(kernel.supervisor_complete_futex_wait).not.toHaveBeenCalled();
  expect(waitingThread.waitingInWorkerStatus).toBeNull();
  expect(handle.runningWorker).toBeNull();
  expect(waitingWorker.postMessage).not.toHaveBeenCalled();
  expect(blockedWorker.postMessage).not.toHaveBeenCalled();
});

test("blockWorker parks futex wait as supervisor-owned worker state", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x70bf0d7);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = (vmPtr: number) => vmPtr === thread.vmPtr
    ? kernel.abi_status_futex_wait()
    : kernel.abi_status_ok();
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.runningWorker = worker;
  handle.vforkReadyWorkers = [worker];
  handle.hostBlockedWorkers.set(worker, {
    vmPtr: thread.vmPtr,
    status: kernel.abi_status_pipe_wait(),
    rpcPending: false,
    rpcGeneration: 0,
  });
  requestPipeSyncBeforeRetry(kernel, thread.vmPtr);
  requestPipeSyncBeforeRetry(kernel, thread.vmPtr);

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, kernel.abi_status_futex_wait(), thread.vmPtr);

  expect(handle.hostBlockedWorkers.has(worker)).toBe(false);
  expect(handle.vforkReadyWorkers).toEqual([]);
  expect(handle.runningWorker).toBeNull();
  expect(thread.waitingInWorkerStatus).toBeNull();
  expect(thread.runningGeneration).toBeNull();
  scheduler.enqueueWorker(handle, worker);
  expect(handle.vforkReadyWorkers).toEqual([]);
  expect(handle.runningWorker).toBeNull();
  expect(worker.postMessage).not.toHaveBeenCalled();
});

test("blocked completion parks futex result as supervisor-owned worker state", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x70bf0d9);
  thread.runningGeneration = thread.generation;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  let currentStatus = kernel.abi_status_pipe_wait();
  kernel.supervisor_blocked_pipe_sync_before_retry_requested = () => 1;
  kernel.supervisor_blocked_pipe_sync_before_retry_action = () =>
    kernel.supervisor_blocked_pipe_sync_before_retry_action_none!();
  kernel.thread_status = (vmPtr: number) => vmPtr === thread.vmPtr
    ? currentStatus
    : kernel.abi_status_ok();
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.kernelWorkerRequest = vi.fn();
  handle.pendingChildSpawnRecords = 0;
  handle.runningWorker = worker;
  handle.vforkReadyWorkers = [worker];
  handle.hostBlockedWorkers.set(worker, {
    vmPtr: thread.vmPtr,
    status: kernel.abi_status_pipe_wait(),
    rpcPending: false,
    rpcGeneration: 0,
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    executeHostBlockedSyscallResumeOutcome: async () => {
      currentStatus = kernel.abi_status_futex_wait();
      return {
        kind: "completed",
        status: currentStatus,
      };
    },
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  handle.runningWorker = null;
  scheduler.notifySchedulerProgress(handle);
  await flushMicrotasks(4);

  expect(handle.hostBlockedWorkers.has(worker)).toBe(false);
  expect(handle.vforkReadyWorkers).toEqual([]);
  expect(handle.runningWorker).toBeNull();
  expect(thread.waitingInWorkerStatus).toBeNull();
  expect(thread.runningGeneration).toBeNull();
  scheduler.enqueueWorker(handle, worker);
  expect(handle.vforkReadyWorkers).toEqual([]);
  expect(handle.runningWorker).toBeNull();
  expect(worker.postMessage).not.toHaveBeenCalled();
});

test("blocked completion arms supervisor timer instead of scheduling runtime retry", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x70bf0da);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 16, shared: true });
  let currentStatus = kernel.abi_status_pipe_wait();
  Object.assign(kernel, {
    memory,
    supervisor_blocked_pipe_sync_before_retry_requested: () => 1,
    supervisor_blocked_pipe_sync_before_retry_action: () =>
      kernel.supervisor_blocked_pipe_sync_before_retry_action_none!(),
    supervisor_abi_version: () => 1,
    supervisor_host_op_record_bytes: () => 64,
    supervisor_host_op_kind_arm_host_timer: () => 12,
    supervisor_host_completion_outcome_success: () => 1,
    supervisor_host_completion_outcome_failure: () => 2,
    kernel_heap_ptr: () => -0x3c000,
    supervisor_next_host_op: vi.fn((outPtr: number, outLen: number) => {
      if (currentStatus !== kernel.abi_status_epoll_wait()) {
        return 0;
      }
      expect(outLen).toBe(64);
      writeSupervisorArmHostTimerHostOpRecord(memory, outPtr, {
        vmPtr: thread.vmPtr,
        delayMs: 25n,
        seq: 7n,
        tid: 2,
        processPtr: 0x2000,
        status: kernel.abi_status_epoll_wait(),
        deadlineTicks: 100n,
      });
      return 64;
    }),
    supervisor_enqueue_blocked_wait_timer: vi.fn(() =>
      currentStatus === kernel.abi_status_epoll_wait() ? 1 : 0),
    supervisor_blocked_wait_timer_required: vi.fn(() =>
      currentStatus === kernel.abi_status_epoll_wait() ? 1 : 0),
    supervisor_blocked_epoll_wait_timeout_ms: vi.fn(() =>
      currentStatus === kernel.abi_status_epoll_wait() ? 25 : -1),
    supervisor_complete_host_op: vi.fn(() => 0),
    thread_status: (vmPtr: number) => vmPtr === thread.vmPtr
      ? currentStatus
      : kernel.abi_status_ok(),
    thread_epoll_wait_timeout: () => 25n,
  });
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.kernelWorkerRequest = vi.fn();
  handle.pendingChildSpawnRecords = 0;
  Object.assign(handle.kernel, { memory });
  handle.memory = memory;
  handle.hostBlockedWorkers.set(worker, {
    vmPtr: thread.vmPtr,
    status: kernel.abi_status_pipe_wait(),
    rpcPending: false,
    rpcGeneration: 0,
  });
  const runtimeEvents: string[] = [];
  const executeHostBlockedSyscallResumeOutcome = vi.fn(async () => {
    currentStatus = kernel.abi_status_epoll_wait();
    return {
      kind: "completed" as const,
      status: currentStatus,
    };
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => runtimeEvents.push(event),
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    executeHostBlockedSyscallResumeOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.notifySchedulerProgress(handle);
  await Promise.resolve();
  await Promise.resolve();

  expect(executeHostBlockedSyscallResumeOutcome).toHaveBeenCalledWith(
    handle,
    worker,
    thread.vmPtr,
    kernel.abi_status_pipe_wait(),
  );
  const blocked = handle.hostBlockedWorkers.get(worker);
  expect(blocked?.status).toBe(kernel.abi_status_epoll_wait());
  expect(kernel.supervisor_next_host_op).toHaveBeenCalledWith(expect.any(Number), 64);
  expect(runtimeEvents).toContain(
    `supervisor-host-timer-arm:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}:seq=7:delay=25`,
  );
});

test("blocked wait retry arms supervisor timer before runtime poll retry", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x70bf0db);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 16, shared: true });
  Object.assign(kernel, {
    memory,
    supervisor_abi_version: () => 1,
    supervisor_host_op_record_bytes: () => 64,
    supervisor_host_op_kind_arm_host_timer: () => 12,
    supervisor_host_completion_outcome_success: () => 1,
    supervisor_host_completion_outcome_failure: () => 2,
    kernel_heap_ptr: () => -0x3c000,
    supervisor_next_host_op: vi.fn((outPtr: number, outLen: number) => {
      expect(outLen).toBe(64);
      writeSupervisorArmHostTimerHostOpRecord(memory, outPtr, {
        vmPtr: thread.vmPtr,
        delayMs: 40n,
        seq: 7n,
        tid: 2,
        processPtr: 0x2000,
        status: kernel.abi_status_pipe_wait(),
        deadlineTicks: 100n,
      });
      return 64;
    }),
    supervisor_complete_host_op: vi.fn(() => 0),
    abi_sys_pselect6: () => 72n,
    thread_status: (vmPtr: number) => vmPtr === thread.vmPtr
      ? kernel.abi_status_pipe_wait()
      : kernel.abi_status_ok(),
    thread_blocked_syscall_nr: (vmPtr: number) => vmPtr === thread.vmPtr ? 72n : 0n,
    thread_poll_wait_timeout: () => 40n,
    thread_poll_wait_ready_count: () => 0,
    thread_poll_wait_has_socket: () => 0,
    thread_pipe_read_eof_sync_pending: () => 0,
    thread_poll_hup_sync_pending: () => 0,
  });
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  Object.assign(handle.kernel, { memory });
  handle.memory = memory;
  handle.hostBlockedWorkers.set(worker, {
    vmPtr: thread.vmPtr,
    status: kernel.abi_status_pipe_wait(),
    rpcPending: false,
    rpcGeneration: 0,
  });
  const runtimeEvents: string[] = [];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => runtimeEvents.push(event),
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.notifySchedulerProgress(handle);

  const blocked = handle.hostBlockedWorkers.get(worker);
  expect(blocked?.status).toBe(kernel.abi_status_pipe_wait());
  expect(kernel.supervisor_next_host_op).toHaveBeenCalledWith(expect.any(Number), 64);
  expect(runtimeEvents).toContain(
    `supervisor-host-timer-arm:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}:seq=7:delay=40`,
  );
});

test("reblocking an already armed supervisor timer does not enqueue a duplicate host timer", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x70bf0df);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 16, shared: true });
  Object.assign(kernel, {
    memory,
    supervisor_abi_version: () => 1,
    supervisor_host_op_record_bytes: () => 64,
    supervisor_host_op_kind_arm_host_timer: () => 12,
    supervisor_host_completion_outcome_success: () => 1,
    supervisor_host_completion_outcome_failure: () => 2,
    kernel_heap_ptr: () => -0x3c000,
    supervisor_next_host_op: vi.fn((outPtr: number, outLen: number) => {
      expect(outLen).toBe(64);
      return writeSupervisorArmHostTimerHostOpRecord(memory, outPtr, {
        vmPtr: thread.vmPtr,
        delayMs: 40n,
        seq: 7n,
        tid: 2,
        processPtr: 0x2000,
        status: kernel.abi_status_pipe_wait(),
        deadlineTicks: 100n,
      });
    }),
    supervisor_complete_host_op: vi.fn(() => 0),
    abi_sys_pselect6: () => 72n,
    thread_status: (vmPtr: number) => vmPtr === thread.vmPtr
      ? kernel.abi_status_pipe_wait()
      : kernel.abi_status_ok(),
    thread_blocked_syscall_nr: (vmPtr: number) => vmPtr === thread.vmPtr ? 72n : 0n,
    thread_poll_wait_timeout: () => 40n,
    thread_poll_wait_ready_count: () => 0,
    thread_poll_wait_has_socket: () => 0,
    thread_pipe_read_eof_sync_pending: () => 0,
    thread_poll_hup_sync_pending: () => 0,
  });
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  Object.assign(handle.kernel, { memory });
  handle.memory = memory;
  const runtimeEvents: string[] = [];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => runtimeEvents.push(event),
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, kernel.abi_status_pipe_wait(), thread.vmPtr);
  scheduler.blockWorker(handle, worker, kernel.abi_status_pipe_wait(), thread.vmPtr);

  expect(kernel.supervisor_next_host_op).toHaveBeenCalledTimes(1);
  expect(runtimeEvents.filter((event) => event.startsWith("supervisor-host-timer-arm"))).toHaveLength(1);
});

test("finite blocked wait fails closed when supervisor timer enqueue ABI is missing", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x70bf0e0);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  Object.assign(kernel, {
    supervisor_abi_version: () => 1,
    supervisor_host_op_record_bytes: () => 64,
    supervisor_host_op_kind_arm_host_timer: () => 12,
    supervisor_next_host_op: vi.fn((_outPtr: number, outLen: number) => {
      expect(outLen).toBe(64);
      return 0;
    }),
    abi_sys_pselect6: () => 72n,
    thread_status: (vmPtr: number) => vmPtr === thread.vmPtr
      ? kernel.abi_status_pipe_wait()
      : kernel.abi_status_ok(),
    thread_blocked_syscall_nr: (vmPtr: number) => vmPtr === thread.vmPtr ? 72n : 0n,
    thread_poll_wait_timeout: () => 40n,
    thread_poll_wait_ready_count: () => 0,
    thread_poll_wait_has_socket: () => 0,
    thread_pipe_read_eof_sync_pending: () => 0,
    thread_poll_hup_sync_pending: () => 0,
  });
  const handle = makeHandle(kernel, worker, threadWorkerStates);

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  expect(() => scheduler.blockWorker(handle, worker, kernel.abi_status_pipe_wait(), thread.vmPtr))
    .toThrow("kernel supervisor ABI missing supervisor_enqueue_blocked_wait_timer");
});

test("supervisor timer completion resumes the supervisor-selected runnable worker", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x70bf0dc);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 16, shared: true });
  let currentStatus = kernel.abi_status_pipe_wait();
  let resumeReady = false;
  let resumeDrained = false;
  Object.assign(kernel, {
    memory,
    supervisor_abi_version: () => 1,
    supervisor_host_op_record_bytes: () => 64,
    supervisor_host_op_kind_arm_host_timer: () => 12,
    supervisor_host_completion_outcome_success: () => 1,
    supervisor_host_completion_outcome_failure: () => 2,
    supervisor_resume_record_bytes: () => 48,
    supervisor_resume_kind_runnable_thread: () => SUPERVISOR_RESUME_KIND_RUNNABLE_THREAD,
    kernel_heap_ptr: () => -0x3c000,
    supervisor_next_host_op: vi.fn((outPtr: number, outLen: number) => {
      expect(outLen).toBe(64);
      writeSupervisorArmHostTimerHostOpRecord(memory, outPtr, {
        vmPtr: thread.vmPtr,
        delayMs: 40n,
        seq: 7n,
        tid: 2,
        processPtr: 0x2000,
        status: kernel.abi_status_pipe_wait(),
        deadlineTicks: 100n,
      });
      return 64;
    }),
    supervisor_complete_host_op: vi.fn((_completionPtr: number, _completionLen: number) => {
      currentStatus = kernel.abi_status_ok();
      resumeReady = true;
      return 0;
    }),
    supervisor_next_resume: vi.fn((outPtr: number, outLen: number) => {
      expect(outLen).toBe(SUPERVISOR_RESUME_RECORD_BYTES);
      if (!resumeReady || resumeDrained) {
        return 0;
      }
      writeSupervisorRunnableResumeRecord(memory, outPtr, thread);
      resumeDrained = true;
      return SUPERVISOR_RESUME_RECORD_BYTES;
    }),
    thread_status: (vmPtr: number) => vmPtr === thread.vmPtr
      ? currentStatus
      : kernel.abi_status_ok(),
    thread_poll_wait_timeout: () => 40n,
    thread_poll_wait_ready_count: () => 0,
    thread_poll_wait_has_socket: () => 0,
    thread_pipe_read_eof_sync_pending: () => 0,
    thread_poll_hup_sync_pending: () => 0,
  });
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  Object.assign(handle.kernel, { memory });
  handle.memory = memory;
  handle.hostBlockedWorkers.set(worker, {
    vmPtr: thread.vmPtr,
    status: kernel.abi_status_pipe_wait(),
    rpcPending: false,
    rpcGeneration: 0,
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.notifySchedulerProgress(handle);
  await vi.advanceTimersByTimeAsync(40);

  expect(kernel.supervisor_complete_host_op).toHaveBeenCalledWith(expect.any(Number), 64);
  expect(kernel.supervisor_next_resume).toHaveBeenCalled();
  expect(handle.hostBlockedWorkers.has(worker)).toBe(false);
  expect(handle.runningWorker).toBe(worker);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    budget: expect.any(Number),
  }));
});

test("blockWorker schedules initial kernel-worker pipe retry before passive parking", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x70bf0d8);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  kernel.abi_sys_read = () => 63n;
  kernel.thread_blocked_syscall_nr = (vmPtr: number) => (
    vmPtr === thread.vmPtr ? 63n : 0n
  );
  kernel.thread_blocked_pipe_read_ready = () => 0;
  kernel.thread_blocked_pipe_write_ready = () => 0;
  kernel.thread_poll_wait_has_socket = () => 0;
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.pendingChildSpawnRecords = 0;
  handle.kernelWorkerRequest = vi.fn() as never;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, kernel.abi_status_pipe_wait(), thread.vmPtr);

  expect(pipeSyncBeforeRetryRequested(kernel, thread.vmPtr)).toBe(false);
  expect(vi.getTimerCount()).toBeGreaterThan(0);
  expect(worker.postMessage).not.toHaveBeenCalled();
});

test("enqueue does not place the current running worker back on the ready queue", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.runningWorker = worker;

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.enqueueWorker(handle, worker);

  expect(handle.runningWorker).toBe(worker);
  expect(handle.vforkReadyWorkers).not.toContain(worker);
  expect(worker.postMessage).not.toHaveBeenCalled();
});

test("enqueue does not place a worker-local wait worker on the ready queue", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const thread = makeThreadState(0x1234);
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, thread);
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_futex_wait();
  const handle = makeHandle(kernel, worker, threadWorkerStates);

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.enqueueWorker(handle, worker);

  expect(handle.vforkReadyWorkers).not.toContain(worker);
  expect(worker.postMessage).not.toHaveBeenCalled();
});

test("attachThreadWorker fails the process when a browser worker emits an error before ready", () => {
  const worker = {
    postMessage: vi.fn(),
    onmessage: null,
    onerror: null,
    onmessageerror: null,
  } as unknown as Worker & {
    onmessage: ((event: MessageEvent<unknown>) => void) | null;
    onerror: ((event: ErrorEvent) => void) | null;
    onmessageerror: ((event: MessageEvent<unknown>) => void) | null;
  };
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  const { exports: kernel } = makeKernel();
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.guestMemoryPolicyLimitBytes = 16n * 1024n * 1024n;
  handle.guestBase = 0x1000;
  const failProcess = vi.fn();

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.attachThreadWorker(handle, { worker, vmPtr: 0x1234 });

  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "init",
    vmPtr: 0x1234,
  }));

  worker.onerror?.({
    message: "boot failed",
    filename: "thread-worker.mjs",
    lineno: 10,
    colno: 5,
    error: new Error("boot failed"),
    preventDefault: vi.fn(),
  } as unknown as ErrorEvent);

  expect(failProcess).toHaveBeenCalledTimes(1);
  expect(failProcess.mock.calls[0]?.[1]).toBeInstanceOf(Error);
  expect(String(failProcess.mock.calls[0]?.[1]?.message)).toContain("thread worker error vm=0x1234");
});

test("socket wait timeout resumes with timeout handler after deadline", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = 30;
  const resumeSocketTimeout = vi.fn(() => {
    status = 0;
  });
  kernel.thread_socket_wait_timeout = () => 25n;
  kernel.thread_resume_socket_wait_timeout = resumeSocketTimeout;
  kernel.thread_resume_blocked_syscall = vi.fn();
  kernel.thread_status = () => status;
  const handle = makeHandle(kernel, worker, threadWorkerStates);

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_socket_recv(), 0x1234);
  await vi.advanceTimersByTimeAsync(30);

  expect(resumeSocketTimeout).toHaveBeenCalledWith(0x1234);
});

test("notifySchedulerProgress leaves supervisor timer owned socket wait to supervisor completion", () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  kernel.thread_status = () => kernel.abi_status_socket_recv();
  kernel.thread_socket_wait_timeout = () => 25n;
  const resumeSocketTimeout = vi.fn();
  kernel.thread_resume_socket_wait_timeout = resumeSocketTimeout;
  kernel.thread_resume_blocked_syscall = vi.fn();
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  installTestSupervisorBlockedWaitTimers(kernel, handle);
  const runtimeEvents: string[] = [];

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => runtimeEvents.push(event),
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_socket_recv(), 0x1234);
  scheduler.notifySchedulerProgress(handle);

  expect(resumeSocketTimeout).not.toHaveBeenCalled();
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(handle.hostBlockedWorkers.has(worker)).toBe(true);
  expect(runtimeEvents).toContain(
    `blocked-retry-skip-supervisor-timer:${handle.pid}:status=${handle.kernel.abi_status_socket_recv()}:vm=0x1234`,
  );
});

test("connect timeout completes through supervisor timer when kernel-worker exists", async () => {
  const worker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState(0x1234));
  const { exports: kernel } = makeKernel();
  let status = 28;
  const resumeSocketTimeout = vi.fn(() => {
    status = 0;
  });
  kernel.thread_socket_wait_timeout = () => 25n;
  kernel.thread_resume_socket_wait_timeout = resumeSocketTimeout;
  kernel.thread_resume_blocked_syscall = vi.fn();
  kernel.thread_status = () => status;
  const handle = makeHandle(kernel, worker, threadWorkerStates);
  handle.kernelWorkerRequest = vi.fn(async () => ({ type: "unexpected" }) as never);
  const runtimeEvents: string[] = [];
  const executeHostBlockedSyscallResumeOutcome = vi.fn(async () => {
    status = 0;
    return { kind: "completed" as const, status };
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => runtimeEvents.push(event),
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    executeHostBlockedSyscallResumeOutcome,
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => {},
    handleWorkerMessage: () => {},
  });

  scheduler.blockWorker(handle, worker, handle.kernel.abi_status_connect(), 0x1234);
  await vi.advanceTimersByTimeAsync(30);
  await Promise.resolve();
  await Promise.resolve();

  expect(executeHostBlockedSyscallResumeOutcome).not.toHaveBeenCalled();
  expect(resumeSocketTimeout).toHaveBeenCalledWith(0x1234);
  expect(kernel.supervisor_complete_host_op).toHaveBeenCalledWith(expect.any(Number), 64);
  expect(kernel.thread_resume_blocked_syscall).not.toHaveBeenCalled();
  expect(runtimeEvents).toContain(`supervisor-host-timer-arm:${handle.pid}:vm=0x1234:seq=1:delay=25`);
  expect(handle.hostBlockedWorkers.has(worker)).toBe(false);
});
