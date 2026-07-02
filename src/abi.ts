import { ensureMemoryCapacity, kernelScratchBase } from "./worker/kernel-scratch.js";
import { normalizeGuestMemoryPolicyLimitBytes } from "./worker/memory-policy.js";

/**
 * WASM exports from the kernel module.
 * All constants are read from the WASM exports — no hand-written values.
 */
const SUPERVISOR_PROCESS_HOST_OP_RECORD_BYTES = 96;
const SUPERVISOR_PROCESS_HOST_OP_RECORD_MAGIC = 0x4850_5354;
const SUPERVISOR_PROCESS_HOST_OP_KIND_SPAWN_PROCESS_WORKER = 1;
const SUPERVISOR_PROCESS_HOST_OP_SCRATCH_OFFSET = 0x2e000;

export interface KernelExports {
  memory: WebAssembly.Memory;
  __stack_pointer?: WebAssembly.Global;
  __heap_base?: WebAssembly.Global;

  // Status codes
  abi_status_ok(): number;
  abi_status_fault(): number;
  abi_status_fs(): number;
  abi_status_futex_wait(): number;
  abi_status_pipe_wait(): number;
  abi_status_epoll_wait(): number;
  abi_status_vfork_wait(): number;
  abi_status_execve_spawn(): number;
  abi_status_clone_spawn(): number;
  abi_status_sleep(): number;
  abi_status_jit_ready(): number;
  abi_status_wait4(): number;
  abi_status_connect(): number;
  abi_status_socket_send(): number;
  abi_status_socket_recv(): number;
  abi_status_flock_wait(): number;
  abi_fault_kind_none?(): number;
  abi_fault_kind_guest_maperr?(): number;
  abi_fault_kind_guest_accerr?(): number;
  abi_fault_kind_illegal_instruction?(): number;
  abi_fault_kind_guest_bus?(): number;

  // Spawn kinds
  abi_spawn_kind_none(): number;
  abi_spawn_kind_execve(): number;
  abi_spawn_kind_fork(): number;

  // Limits
  abi_page_size(): number;
  abi_max_fds(): number;
  abi_max_threads(): number;
  abi_max_pipes(): number;
  abi_max_sockets?(): number;

  // FD types
  abi_fd_type_none(): number;
  abi_fd_type_file(): number;
  abi_fd_type_pipe(): number;
  abi_fd_type_epoll(): number;
  abi_fd_type_socket(): number;
  abi_fd_type_eventfd?(): number;
  abi_fd_type_timerfd?(): number;
  abi_fd_type_pty_master?(): number;
  abi_fd_type_pty_slave?(): number;

  // Signal numbers
  abi_sigkill(): number;
  abi_sigterm(): number;
  abi_sigchld(): number;
  abi_sigpipe(): number;
  abi_clone_parent(): bigint | number;
  abi_o_nonblock?(): number;
  abi_sys_read?(): bigint | number;
  abi_sys_openat?(): bigint | number;
  abi_sys_write?(): bigint | number;
  abi_sys_pwrite64?(): bigint | number;
  abi_sys_lseek?(): bigint | number;
  abi_sys_execve?(): bigint | number;
  abi_sys_close?(): bigint | number;
  abi_sys_close_range?(): bigint | number;
  abi_sys_pipe2?(): bigint | number;
  abi_sys_epoll_create1?(): bigint | number;
  abi_sys_epoll_ctl?(): bigint | number;
  abi_sys_dup?(): bigint | number;
  abi_sys_dup3?(): bigint | number;
  abi_sys_fcntl?(): bigint | number;
  abi_sys_ioctl?(): bigint | number;
  abi_sys_readv?(): bigint | number;
  abi_sys_writev?(): bigint | number;
  abi_sys_socket?(): bigint | number;
  abi_sys_socketpair?(): bigint | number;
  abi_sys_bind?(): bigint | number;
  abi_sys_listen?(): bigint | number;
  abi_sys_connect?(): bigint | number;
  abi_sys_accept4?(): bigint | number;
  abi_sys_sendto?(): bigint | number;
  abi_sys_recvfrom?(): bigint | number;
  abi_sys_sendmsg?(): bigint | number;
  abi_sys_recvmsg?(): bigint | number;
  abi_sys_setsockopt?(): bigint | number;
  abi_sys_shutdown?(): bigint | number;
  abi_sys_eventfd2?(): bigint | number;
  abi_sys_setpgid?(): bigint | number;
  abi_sys_setsid?(): bigint | number;
  abi_sys_kill?(): bigint | number;
  abi_sys_tkill?(): bigint | number;
  abi_sys_tgkill?(): bigint | number;
  abi_sys_ppoll?(): bigint | number;
  abi_sys_pselect6?(): bigint | number;
  debug_panic_file_ptr?(): number;
  debug_panic_file_len?(): number;
  debug_panic_line?(): number;
  debug_panic_column?(): number;
  debug_fd_syscall_trace_len?(): number;
  debug_fd_syscall_trace_vm_ptr?(index: number): number;
  debug_fd_syscall_trace_nr?(index: number): bigint | number;
  debug_fd_syscall_trace_arg0?(index: number): bigint | number;
  debug_fd_syscall_trace_arg0_ofd_id?(index: number): number;
  debug_fd_syscall_trace_arg0_ofd_type?(index: number): number;
  debug_fd_syscall_trace_arg0_ofd_offset?(index: number): bigint | number;
  debug_fd_syscall_trace_arg0_ofd_host_fd?(index: number): number;
  debug_fd_syscall_trace_arg0_ofd_status_flags?(index: number): number;
  debug_fd_syscall_trace_arg0_ofd_ino?(index: number): number;
  debug_fd_syscall_trace_arg0_inode_mode?(index: number): number;
  debug_fd_syscall_trace_arg0_pipe_generation?(index: number): number;
  debug_fd_syscall_trace_arg1?(index: number): bigint | number;
  debug_fd_syscall_trace_arg2?(index: number): bigint | number;
  debug_fd_syscall_trace_arg2_ofd_id?(index: number): number;
  debug_fd_syscall_trace_arg2_ofd_type?(index: number): number;
  debug_fd_syscall_trace_arg3?(index: number): bigint | number;
  debug_fd_syscall_trace_result?(index: number): bigint | number;
  debug_fd_syscall_trace_status?(index: number): number;
  debug_fd_syscall_trace_pid_sample0?(index: number): bigint | number;
  debug_fd_syscall_trace_pid_sample1?(index: number): bigint | number;
  debug_path_syscall_trace_len?(): number;
  debug_path_syscall_trace_vm_ptr?(index: number): number;
  debug_path_syscall_trace_nr?(index: number): bigint | number;
  debug_path_syscall_trace_dirfd?(index: number): bigint | number;
  debug_path_syscall_trace_result?(index: number): bigint | number;
  debug_path_syscall_trace_status?(index: number): number;
  debug_path_syscall_trace_path_ptr?(index: number): number;
  debug_path_syscall_trace_path_len?(index: number): number;
  debug_execve_error_errno?(): bigint | number;
  debug_execve_error_stage?(): number;
  debug_execve_error_value0?(): bigint | number;
  debug_execve_error_value1?(): bigint | number;

  // Supervisor protocol
  supervisor_abi_version?(): number;
  supervisor_debug_snapshot_bytes?(): number;
  supervisor_thread_event_record_bytes?(): number;
  supervisor_thread_event_kind_blocked?(): number;
  supervisor_thread_event_kind_ready?(): number;
  supervisor_thread_event_kind_faulted?(): number;
  supervisor_thread_event_kind_exited?(): number;
  supervisor_thread_event_kind_host_completion_arrived?(): number;
  supervisor_thread_event_kind_timer_fired?(): number;
  supervisor_thread_event_kind_network_readable?(): number;
  supervisor_thread_event_kind_backing_store_ready?(): number;
  supervisor_thread_state_snapshot_bytes?(): number;
  supervisor_resume_record_bytes?(): number;
  supervisor_host_completion_record_bytes?(): number;
  supervisor_host_completion_outcome_success?(): number;
  supervisor_host_completion_outcome_failure?(): number;
  supervisor_host_op_record_bytes?(): number;
  supervisor_process_host_op_record_bytes?(): number;
  supervisor_fault_exit_decision_bytes?(): number;
  supervisor_fd_control_record_bytes?(): number;
  supervisor_fs_control_record_bytes?(): number;
  supervisor_signal_control_record_bytes?(): number;
  supervisor_resume_kind_futex_wake?(): number;
  supervisor_resume_kind_runnable_thread?(): number;
  supervisor_host_op_kind_spawn_thread_worker?(): number;
  supervisor_host_op_kind_spawn_process_worker?(): number;
  supervisor_host_op_kind_create_process_memory?(): number;
  supervisor_host_op_kind_copy_memory_ranges?(): number;
  supervisor_host_op_kind_fill_page_from_backing?(): number;
  supervisor_host_op_kind_flush_dirty_page?(): number;
  supervisor_host_op_kind_process_fs_request?(): number;
  supervisor_host_op_kind_host_network_connect?(): number;
  supervisor_host_op_kind_host_network_read?(): number;
  supervisor_host_op_kind_host_network_write?(): number;
  supervisor_host_op_kind_host_network_close?(): number;
  supervisor_host_op_kind_arm_host_timer?(): number;
  supervisor_host_op_kind_resume_thread?(): number;
  supervisor_host_op_kind_terminate_worker?(): number;
  supervisor_process_host_op_kind_spawn_process_worker?(): number;
  supervisor_fault_exit_kind_thread_exit?(): number;
  supervisor_fault_exit_kind_process_exit?(): number;
  supervisor_fd_control_kind_pipe_created?(): number;
  supervisor_fd_control_kind_fd_closed?(): number;
  supervisor_fd_control_kind_fd_duplicated?(): number;
  supervisor_fd_control_kind_fd_flags_updated?(): number;
  supervisor_fd_control_kind_epoll_created?(): number;
  supervisor_fd_control_kind_epoll_ctl_add?(): number;
  supervisor_fd_control_kind_epoll_ctl_del?(): number;
  supervisor_fd_control_kind_epoll_ctl_mod?(): number;
  supervisor_fd_control_kind_socket_created?(): number;
  supervisor_fd_control_kind_socketpair_created?(): number;
  supervisor_fd_control_kind_socket_bound?(): number;
  supervisor_fd_control_kind_socket_listening?(): number;
  supervisor_fd_control_kind_socket_connect_started?(): number;
  supervisor_fd_control_kind_socket_connected?(): number;
  supervisor_fd_control_kind_socket_connect_failed?(): number;
  supervisor_fd_control_kind_socket_accepted?(): number;
  supervisor_pipe_slot_mode_read?(): number;
  supervisor_pipe_slot_mode_write?(): number;
  supervisor_pipe_slot_mode_control?(): number;
  supervisor_blocked_pipe_resume_action_defer?(): number;
  supervisor_blocked_pipe_resume_action_direct?(): number;
  supervisor_fs_control_kind_page_cache_miss?(): number;
  supervisor_fs_control_kind_page_cache_fill?(): number;
  supervisor_fs_control_kind_dirty_page_published?(): number;
  supervisor_fs_control_kind_dirty_page_flushed?(): number;
  supervisor_fs_dirty_page_op_write?(): number;
  supervisor_fs_dirty_page_op_truncate?(): number;
  supervisor_fs_dirty_page_op_flush?(): number;
  supervisor_fs_dirty_page_flag_update_inode?(): number;
  supervisor_fs_dirty_page_flag_publish_dirty?(): number;
  supervisor_fs_dirty_page_flag_writeback_required?(): number;
  supervisor_fs_backing_fill_mode_resident?(): number;
  supervisor_fs_backing_fill_mode_direct?(): number;
  supervisor_fs_backing_source_range_none?(): number;
  supervisor_fs_backing_source_range_read?(): number;
  supervisor_fs_clean_backing_range_none?(): number;
  supervisor_fs_clean_backing_range_read?(): number;
  supervisor_fs_clean_backing_range_read_zero_tail?(): number;
  supervisor_fs_clean_backing_range_zero?(): number;
  supervisor_fs_page_table_source_backing?(): number;
  supervisor_fs_page_table_source_resident?(): number;
  supervisor_fs_completion_flag_memfs_dirty?(): number;
  supervisor_fs_completion_flag_memfs_publish_ready?(): number;
  supervisor_fs_completion_flag_ofd_state_changed?(): number;
  supervisor_fs_completion_flag_regular_file_request?(): number;
  supervisor_signal_control_kind_boundary_check?(): number;
  supervisor_signal_control_kind_signal_delivered?(): number;
  supervisor_signal_control_kind_blocked_wait_interrupted?(): number;
  supervisor_reset_shadow_log?(): void;
  supervisor_shadow_thread_event_count?(): bigint | number;
  supervisor_shadow_host_completion_count?(): bigint | number;
  supervisor_shadow_host_op_poll_count?(): bigint | number;
  supervisor_shadow_host_op_emit_count?(): bigint | number;
  supervisor_shadow_host_op_overflow_count?(): bigint | number;
  supervisor_shadow_host_op_log_hash?(): bigint | number;
  supervisor_shadow_host_op_last_hash?(): bigint | number;
  supervisor_shadow_host_op_last_len?(): number;
  supervisor_shadow_resume_poll_count?(): bigint | number;
  supervisor_shadow_resume_enqueue_count?(): bigint | number;
  supervisor_shadow_resume_overflow_count?(): bigint | number;
  supervisor_shadow_fd_control_count?(): bigint | number;
  supervisor_shadow_fs_control_count?(): bigint | number;
  supervisor_shadow_signal_control_count?(): bigint | number;
  supervisor_shadow_thread_event_log_hash?(): bigint | number;
  supervisor_shadow_host_completion_log_hash?(): bigint | number;
  supervisor_submit_thread_event?(eventPtr: number, eventLen: number): number;
  supervisor_complete_host_op?(completionPtr: number, completionLen: number): number;
  supervisor_next_host_op?(outPtr: number, outLen: number): number;
  supervisor_next_host_op_kind?(kind: number, outPtr: number, outLen: number): number;
  supervisor_process_host_op?(vmPtr: number, outPtr: number, outLen: number): number;
  supervisor_process_memory_host_op?(vmPtr: number, index: number, outPtr: number, outLen: number): number;
  supervisor_next_resume?(outPtr: number, outLen: number): number;
  supervisor_process_resume?(vmPtr: number, outPtr: number, outLen: number): number;
  supervisor_fault_exit_decision?(vmPtr: number, outPtr: number, outLen: number): number;
  supervisor_process_held_ofd_refcount?(vmPtr: number, ofdId: number): number;
  supervisor_process_private_pipe_slot_mask?(vmPtr: number): bigint | number;
  supervisor_process_private_pipe_source_mode?(vmPtr: number, pipeSlot: number): number;
  supervisor_process_private_pipe_target_mode?(vmPtr: number, pipeSlot: number): number;
  supervisor_process_held_pipe_slot_mask?(vmPtr: number): bigint | number;
  supervisor_process_pipe_used_mask?(vmPtr: number): bigint | number;
  supervisor_process_pipe_slot_mode?(vmPtr: number, pipeSlot: number): number;
  supervisor_process_pipe_read_open_count?(vmPtr: number, pipeSlot: number): number;
  supervisor_process_pipe_write_open_count?(vmPtr: number, pipeSlot: number): number;
  supervisor_blocked_pipe_resume_action?(vmPtr: number): number;
  supervisor_process_epoll_held_ofd?(vmPtr: number, index: number): number;
  supervisor_process_epoll_dependent_ofd?(vmPtr: number, index: number): number;
  supervisor_blocked_epoll_watched_ofd?(vmPtr: number, index: number): number;
  supervisor_blocked_epoll_watches_pipe?(vmPtr: number): number;
  supervisor_process_socket_held_ofd?(vmPtr: number, index: number): number;
  supervisor_socket_connect_fd?(vmPtr: number): number;
  supervisor_socket_connect_peer_addr?(vmPtr: number, dstPtr: number, dstLen: number): number;
  supervisor_socket_connect_peer_addr_for_fd?(vmPtr: number, fd: number, dstPtr: number, dstLen: number): number;
  supervisor_socket_prepare_external_peer?(vmPtr: number, fd: number): number;
  supervisor_socket_commit_external_connect_result?(vmPtr: number, fd: number, result: bigint | number): number;
  supervisor_socket_close_external_peer?(vmPtr: number, fd: number): number;
  supervisor_socket_force_shutdown?(vmPtr: number, fd: number): number;
  supervisor_socket_bridge_active_fd?(vmPtr: number, preferredFd: number, expectedOfd: number, pendingConnect: number): number;
  supervisor_resume_blocked_syscall?(vmPtr: number, flags: number): bigint | number;
  supervisor_blocked_resume_delegation?(vmPtr: number, flags: number): number;
  supervisor_blocked_resume_delegation_flag_kernel_worker_available?(): number;
  supervisor_blocked_resume_delegation_direct?(): number;
  supervisor_blocked_resume_delegation_kernel_worker?(): number;
  supervisor_blocked_resume_delegation_wait4_kernel_worker?(): number;
  supervisor_blocked_resume_delegation_unsupported?(): number;
  supervisor_blocked_resume_delegation_epoll_direct?(): number;
  supervisor_blocked_wait_retry_reason?(vmPtr: number, flags: number): number;
  supervisor_blocked_wait_retry_delay_ms?(vmPtr: number, reason: number): number;
  supervisor_blocked_wait_active_child_pid?(vmPtr: number): number;
  supervisor_blocked_wait_retry_reason_default?(): number;
  supervisor_blocked_wait_retry_reason_immediate?(): number;
  supervisor_blocked_wait_retry_reason_standard_defer?(): number;
  supervisor_blocked_wait_retry_reason_active_child?(): number;
  supervisor_blocked_wait_retry_reason_passive_wait?(): number;
  supervisor_blocked_wait_retry_reason_process_gate?(): number;
  supervisor_blocked_wait_retry_flag_host_progress?(): number;
  supervisor_blocked_wait_retry_flag_active_child?(): number;
  supervisor_blocked_wait_retry_flag_passive_wait?(): number;
  supervisor_blocked_wait_retry_flag_process_gate?(): number;
  supervisor_blocked_wait_retry_flag_thread_init_pending?(): number;
  supervisor_enqueue_blocked_wait_timer?(vmPtr: number): number;
  supervisor_blocked_wait_timer_required?(vmPtr: number): number;
  supervisor_fd_control_record?(outPtr: number, outLen: number): number;
  supervisor_fs_control_record?(outPtr: number, outLen: number): number;
  supervisor_fs_pending_ring_request?(vmPtr: number): number;
  supervisor_fs_pending_regular_file_fd?(vmPtr: number): number;
  supervisor_fs_pending_regular_file_ofd?(vmPtr: number): number;
  supervisor_fs_pending_backing_fill_ino?(vmPtr: number): number;
  supervisor_fs_pending_backing_fill_offset?(vmPtr: number): bigint | number;
  supervisor_fs_pending_backing_fill_length?(vmPtr: number): bigint | number;
  supervisor_fs_pending_backing_fill_expected_size?(vmPtr: number): bigint | number;
  supervisor_fs_pending_backing_fill_opcode?(vmPtr: number): number;
  supervisor_fs_pending_mmap_guest_write_ptr?(vmPtr: number): bigint | number;
  supervisor_fs_pending_mmap_guest_write_len?(vmPtr: number): bigint | number;
  supervisor_fs_dirty_page_decision?(
    op: number,
    ino: number,
    offset: bigint | number,
    lengthOrSize: bigint | number,
    currentSize: bigint | number,
    currentFileObjectId: bigint | number,
    currentGeneration: bigint | number,
    existingDirtyPages: number,
    deltaDirtyPages: number,
    residentPageCapacity: number,
    pageMapNewer: number,
  ): bigint | number;
  supervisor_fs_dirty_page_decision_next_size?(): bigint | number;
  supervisor_fs_dirty_page_decision_file_object_id?(): bigint | number;
  supervisor_fs_dirty_page_decision_generation?(): bigint | number;
  supervisor_fs_dirty_page_decision_flags?(): number;
  supervisor_fs_page_table_source_decision?(
    ino: number,
    hasDirtyPages: number,
    publishedFileObjectId: bigint | number,
    publishedGeneration: bigint | number,
    publishedSize: bigint | number,
    backingFileObjectId: bigint | number,
    backingGeneration: bigint | number,
    backingSize: bigint | number,
  ): number;
  supervisor_fs_backing_fill_probe_direct?(
    opcode: number,
    alignedLength: bigint | number,
  ): number;
  supervisor_fs_backing_fill_mode_decision?(
    opcode: number,
    alignedLength: bigint | number,
    headProbeReadable: number,
    tailProbeReadable: number,
  ): number;
  supervisor_fs_backing_source_range_decision?(
    ino: number,
    fileOffset: bigint | number,
    length: bigint | number,
    expectedSize: bigint | number,
    extentOffset: bigint | number,
    extentLength: bigint | number,
    sourceByteLength: bigint | number,
    requireSourceBounds: number,
  ): number;
  supervisor_fs_backing_source_range_source_byte_length?(): bigint | number;
  supervisor_fs_backing_source_range_source_offset?(): bigint | number;
  supervisor_fs_backing_source_range_read_length?(): bigint | number;
  supervisor_fs_clean_backing_range_decision?(
    ino: number,
    offset: bigint | number,
    length: bigint | number,
    publishedSize: bigint | number,
    backingSize: bigint | number,
  ): number;
  supervisor_fs_clean_backing_range_read_offset?(): bigint | number;
  supervisor_fs_clean_backing_range_read_length?(): bigint | number;
  supervisor_fs_clean_backing_range_output_length?(): bigint | number;
  supervisor_fs_completion_decision?(
    vmPtr: number,
    opcode: number,
    effects: number,
    result: bigint | number,
    hostMemfsMutated: number,
    hostOfdStateChanged: number,
    pendingRingRequest: number,
    regularFileRequest: number,
  ): number;
  supervisor_signal_control_record?(outPtr: number, outLen: number): number;
  supervisor_debug_snapshot?(outPtr: number, outLen: number): number;
  supervisor_thread_state_snapshot?(vmPtr: number, outPtr: number, outLen: number): number;
  supervisor_futex_wait_completion_woken?(): number;
  supervisor_futex_wait_completion_not_equal?(): number;
  supervisor_futex_wait_completion_invalid_address?(): number;
  supervisor_futex_wait_completion_signal_check?(): number;
  supervisor_complete_futex_wait?(vmPtr: number, completion: number): number;
  supervisor_signal_pending_for_blocked_wait?(vmPtr: number): number;

  // Open flags
  abi_o_rdonly(): number;
  abi_o_wronly(): number;
  abi_o_rdwr(): number;
  abi_o_creat(): number;
  abi_o_trunc(): number;
  abi_o_append(): number;
  abi_o_cloexec(): number;

  // Error codes
  abi_enoent(): number;
  abi_ebadf(): number;
  abi_einval(): number;
  abi_enomem(): number;
  abi_enosys(): number;
  abi_eintr(): number;
  abi_eagain(): number;
  abi_etimedout?(): number;
  abi_einprogress?(): number;
  abi_epipe(): number;
  abi_enotsock?(): number;
  abi_enotconn?(): number;
  abi_econnrefused(): number;

  // Kernel lifecycle
  set_base_time(sec: bigint, nsec: bigint): void;
  sync_monotonic_ticks_floor?(ticks: bigint | number): void;
  monotonic_ticks_current?(): bigint | number;
  kernel_init(heap_base: number): void;
  kernel_heap_base(): number;
  kernel_heap_ptr(): number;
  kernel_ofd_table_ptr(): number;
  kernel_ofd_table_bytes(): number;
  kernel_ofd_state_ptr?(): number;
  kernel_ofd_state_bytes?(): number;
  kernel_ofd_offset_floor_ptr?(): number;
  kernel_ofd_offset_floor_bytes?(): number;
  kernel_memfs_ptr(): number;
  kernel_memfs_header_bytes(): number;
  kernel_memfs_inodes_ptr(): number;
  kernel_memfs_inodes_bytes(): number;
  kernel_memfs_first_child_ptr(): number;
  kernel_memfs_first_child_bytes(): number;
  kernel_memfs_dir_entries_ptr(): number;
  kernel_memfs_dir_entries_bytes(): number;
  kernel_memfs_data_ptr(): number;
  kernel_memfs_data_bytes(): number;
  kernel_mem_state_ptr(): number;
  kernel_epoll_table_ptr(): number;
  kernel_epoll_table_bytes(): number;
  kernel_epoll_instance_bytes(): number;
  kernel_socket_table_ptr(): number;
  kernel_socket_table_bytes(): number;
  kernel_socket_state_ptr?(): number;
  kernel_socket_state_bytes?(): number;
  kernel_socket_used(): number;
  kernel_pipe_base(): number;
  kernel_pipe_region_bytes(): number;
  kernel_pipe_used(): bigint;
  kernel_net_buf_base(): number;
  kernel_net_buf_pool_bytes(): number;
  kernel_net_buf_used(): bigint;
  kernel_sig_slots_ptr(): number;
  kernel_sig_slots_bytes(): number;
  kernel_signal_action_table_bytes(): number;
  kernel_process_pending_ptr(): number;
  kernel_process_pending_bytes(): number;
  kernel_signalfd_state_ptr?(): number;
  kernel_signalfd_state_bytes?(): number;
  kernel_process_runtime_state_ptr(): number;
  kernel_process_runtime_state_bytes(): number;
  kernel_time_state_ptr(): number;
  kernel_time_state_bytes(): number;
  debug_memory_syscall_trace_len?(): number;
  debug_memory_syscall_trace_entry_nr?(index: number): bigint;
  debug_memory_syscall_trace_entry_arg0?(index: number): bigint;
  debug_memory_syscall_trace_entry_arg1?(index: number): bigint;
  debug_memory_syscall_trace_entry_arg2?(index: number): bigint;
  debug_memory_syscall_trace_entry_arg3?(index: number): bigint;
  debug_memory_syscall_trace_entry_arg4?(index: number): bigint;
  debug_memory_syscall_trace_entry_arg5?(index: number): bigint;
  debug_memory_syscall_trace_entry_result?(index: number): bigint;
  debug_memory_syscall_trace_entry_pid?(index: number): number;
  debug_memory_syscall_trace_entry_process_ptr?(index: number): number;
  kernel_scratch_base?(): number;
  kernel_reset_memfs(): void;
  kernel_attach_runtime_state(
    heapBase: number,
    heapPtr: number,
    ofdStatePtr: number,
    memfsPtr: number,
    memStatePtr: number,
    epollTablePtr: number,
    socketStatePtr: number,
    socketUsed: number,
    pipeBase: number,
    pipeUsed: bigint,
    netBufBase: number,
    netBufUsed: bigint,
    sigSlotsPtr: number,
    processPendingPtr: number,
    processRuntimeStatePtr: number,
    timeStatePtr: number,
    signalfdStatePtr?: number,
  ): void;

  // Process creation
  process_push_arg(addr: number): void;
  process_push_env(addr: number): void;
  process_clear_args(): void;
  abi_guest_address_profile_memory64?(): number;
  abi_guest_address_profile_target?(): number;
  process_guest_base_for_address_profile?(addressProfile: number): bigint;
  process_guest_address_limit_for_profile?(addressProfile: number, guestBase: bigint): bigint;
  process_inspect_elf_brk_with_memory_policy?(
    elf_ptr: number,
    elf_len: number,
    addressProfile: number,
    guestMemoryPolicyLimit: bigint,
  ): bigint;
  process_create_with_memory_policy?(
    elf_ptr: number,
    elf_len: number,
    addressProfile: number,
    guestMemoryPolicyLimit: bigint,
    processMaxMemoryPages: number,
    stack_size: number,
  ): number;
  process_create_last_error?(): number;

  // Thread execution
  thread_step(vm_ptr: number, budget: number): number;
  debug_thread_step_no_block_cache?(vm_ptr: number, budget: number): number;
  thread_status(vm_ptr: number): number;
  thread_exit_code(vm_ptr: number): number;
  thread_term_signal(vm_ptr: number): number;
  thread_pc(vm_ptr: number): bigint;
  thread_tid?(vm_ptr: number): number;
  thread_tp?(vm_ptr: number): bigint;
  thread_gp?(vm_ptr: number): bigint;
  thread_s11?(vm_ptr: number): bigint;
  thread_reg?(vm_ptr: number, regIndex: number): bigint;
  thread_tls_base?(vm_ptr: number): bigint;
  thread_sig_mask?(vm_ptr: number): bigint;
  thread_sig_pending(vm_ptr: number): bigint;
  thread_pending_signal_interrupts_blocked_syscall?(vm_ptr: number): number;
  thread_sig_info_code?(vm_ptr: number): number;
  thread_fault_pc?(vm_ptr: number): bigint;
  thread_fault_addr?(vm_ptr: number): bigint;
  thread_fault_kind?(vm_ptr: number): number;
  thread_fault_signal?(vm_ptr: number): number;
  thread_fault_code?(vm_ptr: number): number;
  thread_ra?(vm_ptr: number): bigint;
  thread_sp(vm_ptr: number): bigint;
  thread_stack_minus?(vm_ptr: number, byteOffset: number): bigint;
  thread_a0(vm_ptr: number): bigint;
  thread_a1(vm_ptr: number): bigint;
  thread_a2?(vm_ptr: number): bigint;
  thread_a3?(vm_ptr: number): bigint;
  thread_a4?(vm_ptr: number): bigint;
  thread_a5?(vm_ptr: number): bigint;
  thread_a7?(vm_ptr: number): bigint;
  thread_stack_word?(vm_ptr: number, word_index: number): bigint;
  thread_blocked_fd(vm_ptr: number): number;
  thread_blocked_ofd_id(vm_ptr: number): number;
  thread_blocked_syscall_nr?(vm_ptr: number): bigint;
  thread_blocked_syscall_arg?(vm_ptr: number, index: number): bigint;
  // FS handling
  thread_pending_fs_effects(vm_ptr: number): number;
  thread_take_completed_sync_effects?(vm_ptr: number): number;
  thread_take_completed_pipe_sync_slots?(vm_ptr: number): bigint | number;
  thread_take_completed_pipe_sync_read_slots?(vm_ptr: number): bigint | number;
  thread_take_completed_pipe_sync_write_slots?(vm_ptr: number): bigint | number;
  thread_take_completed_pipe_fresh_slots?(vm_ptr: number): bigint | number;
  thread_completed_pipe_read_guest_ptr?(vm_ptr: number): bigint | number;
  thread_take_completed_pipe_read_len?(vm_ptr: number): number;
  thread_pending_fs_target_ino(vm_ptr: number): number;
  thread_pending_fs_opcode(vm_ptr: number): number;
  thread_pending_fs_path_ptr?(vm_ptr: number): bigint | number;
  thread_pending_fs_path_len?(vm_ptr: number): number;
  thread_pending_fs_absolute_path?(vm_ptr: number, buf_ptr: number, buf_len: number): bigint;
  thread_pending_fs_secondary_absolute_path?(vm_ptr: number, buf_ptr: number, buf_len: number): bigint;
  thread_execve_backing_miss_ino?(vm_ptr: number): number;
  thread_execve_backing_miss_offset?(vm_ptr: number): bigint | number;
  thread_execve_backing_miss_len?(vm_ptr: number): number;
  thread_execve_backing_miss_expected_size?(vm_ptr: number): number;
  thread_clear_execve_backing_miss?(vm_ptr: number): void;
  thread_fs_ring_base?(vm_ptr: number): number;
  thread_fs_ring_submit_head?(vm_ptr: number): number;
  thread_fs_ring_submit_tail?(vm_ptr: number): number;
  thread_fs_ring_complete_head?(vm_ptr: number): number;
  thread_fs_ring_complete_tail?(vm_ptr: number): number;
  thread_fs_ring_request_opcode_at?(vm_ptr: number, pos: number): number;
  thread_fs_ring_request_vm_ptr_at?(vm_ptr: number, pos: number): number;
  thread_fs_ring_completion_vm_ptr_at?(vm_ptr: number, pos: number): number;
  thread_fs_ring_completion_result_at?(vm_ptr: number, pos: number): bigint;
  thread_process_fs(vm_ptr: number): void;
  thread_resume_fs(vm_ptr: number): bigint;

  // Blocking state accessors
  thread_futex_wait_addr(vm_ptr: number): bigint;
  thread_futex_wait_timeout(vm_ptr: number): bigint;
  thread_futex_wait_expected(vm_ptr: number): number;
  thread_sleep_seconds(vm_ptr: number): bigint;
  thread_sleep_nanoseconds(vm_ptr: number): bigint;
  thread_sleep_deadline_ticks?(vm_ptr: number): bigint | number;
  thread_record_blocked_addr(vm_ptr: number): bigint;
  thread_resume_wait4(vm_ptr: number): bigint;
  thread_epoll_wait_timeout(vm_ptr: number): bigint;
  thread_epoll_timerfd_deadline_ticks?(vm_ptr: number): bigint | number;
  thread_poll_wait_timeout?(vm_ptr: number): bigint;
  thread_poll_wait_has_socket?(vm_ptr: number): number;
  thread_poll_wait_ready_count?(vm_ptr: number): number;
  thread_poll_wait_first_socket_revents?(vm_ptr: number): number;
  thread_poll_wait_first_pipe_revents?(vm_ptr: number): number;
  thread_poll_wait_pipe_slots_mask?(vm_ptr: number): bigint | number;
  thread_pipe_read_eof_sync_pending?(vm_ptr: number): number;
  thread_poll_hup_sync_pending?(vm_ptr: number): number;
  thread_blocked_pipe_buffered_len?(vm_ptr: number): number;
  thread_blocked_pipe_write_open?(vm_ptr: number): number;
  thread_blocked_pipe_read_ready?(vm_ptr: number): number;
  thread_blocked_pipe_write_ready?(vm_ptr: number): number;
  thread_resume_pipe_read_open_empty?(vm_ptr: number): number;
  thread_socket_wait_timeout?(vm_ptr: number): bigint;
  thread_connect_fd?(vm_ptr: number): number;
  thread_connect_peer_addr?(vm_ptr: number, dst_ptr: number, dst_len: number): number;
  thread_connect_peer_addr_for_fd?(vm_ptr: number, fd: number, dst_ptr: number, dst_len: number): number;
  thread_socket_drain_outbound?(vm_ptr: number, fd: number, dst_ptr: number, dst_len: number): number;
  thread_socket_close_external_peer?(vm_ptr: number, fd: number): number;
  thread_socket_force_shutdown?(vm_ptr: number, fd: number): number;
  thread_socket_mark_external_peer?(vm_ptr: number, fd: number): number;
  thread_socket_commit_external_connect?(vm_ptr: number, fd: number): number;
  thread_socket_outbound_available?(vm_ptr: number, fd: number): number;
  thread_socket_idx_for_fd?(vm_ptr: number, fd: number): number;
  thread_socket_peer_idx?(socket_idx: number): number;
  thread_socket_state?(socket_idx: number): number;
  thread_socket_type?(socket_idx: number): number;
  thread_socket_external_peer?(socket_idx: number): number;
  thread_socket_recv_buf_idx?(socket_idx: number): number;
  thread_socket_recv_available?(socket_idx: number): number;
  thread_socket_read_count?(socket_idx: number): number;
  thread_socket_read_bytes?(socket_idx: number): bigint | number;
  thread_socket_read_last_fd?(socket_idx: number): number;
  thread_socket_read_last_result?(socket_idx: number): bigint | number;
  thread_socket_read_last_before?(socket_idx: number): number;
  thread_socket_read_last_after?(socket_idx: number): number;
  thread_socket_inject_inbound?(vm_ptr: number, fd: number, src_ptr: number, src_len: number): number;
  thread_resume_epoll_wait_timeout(vm_ptr: number): void;
  thread_resume_poll_wait_timeout?(vm_ptr: number): void;
  thread_resume_socket_wait_timeout?(vm_ptr: number): void;
  thread_interrupt_epoll_wait?(vm_ptr: number, errno: number): number;
  thread_resume_sleep?(vm_ptr: number): void;
  thread_resume_connect_result(vm_ptr: number, result: bigint): void;
  thread_resume_connect_result_for_fd?(vm_ptr: number, fd: number, result: bigint): void;
  thread_vfork_child_pid(): number;
  thread_clone_flags(vm_ptr: number): bigint;
  thread_clone_child_stack?(vm_ptr: number): bigint;
  thread_clone_tls?(vm_ptr: number): bigint;
  thread_clone_child_tidptr?(vm_ptr: number): bigint;

  // Spawn handoff
  thread_spawn_kind(vm_ptr: number): number;
  thread_guest_address_profile?(vm_ptr: number): number;
  thread_guest_address_limit?(vm_ptr: number): bigint;
  thread_guest_memory_policy_limit?(vm_ptr: number): bigint;
  thread_set_guest_memory_policy_limit?(vm_ptr: number, limit: bigint): void;
  thread_process_max_memory_pages?(vm_ptr: number): number;
  thread_spawn_child_pid(vm_ptr: number): number;
  thread_spawn_path(vm_ptr: number): bigint;
  thread_spawn_argv(vm_ptr: number): bigint;
  thread_spawn_envp(vm_ptr: number): bigint;
  thread_spawn_address_profile?(vm_ptr: number): number;
  thread_spawn_guest_memory_policy_limit?(vm_ptr: number): bigint;
  thread_spawn_process_max_memory_pages?(vm_ptr: number): number;
  thread_set_execve_memory_handoff?(
    vm_ptr: number,
    processPtr: number,
    pathAddr: bigint,
    argvAddr: bigint,
    envpAddr: bigint,
    addressProfile: number,
    guestMemoryPolicyLimit: bigint,
    processMaxMemoryPages: number,
  ): void;
  thread_clear_spawn_handoff(vm_ptr: number): void;
  thread_clear_vfork_runtime_state?(): void;
  thread_resume_vfork_parent(vm_ptr: number, child_pid: number): void;
  thread_restore_vfork_parent_without_cleanup?(vm_ptr: number, child_pid: number): void;
  thread_restore_vfork_parent_proc_self_exe_snapshot?(): void;
  thread_complete_execve_handoff(vm_ptr: number): bigint;
  thread_spawn_from_clone(parent_vm_ptr: number): number;
  thread_process_thread_count(vm_ptr: number): number;
  thread_guest_base?(vm_ptr: number): number;
  thread_process_ptr(vm_ptr: number): number;
  process_state_bytes(): number;
  process_thread_tid(vm_ptr: number, slot: number): number;
  process_thread_state(vm_ptr: number, slot: number): number;
  process_thread_vm_ptr(vm_ptr: number, slot: number): number;
  process_record_child_exit(pid: number, wait_status: number): void;
  process_record_child_exit_for_parent?(parent_pid: number, pid: number, wait_status: number): void;
  process_has_active_child?(parent_pid: number): number;
  process_active_child_pid?(parent_pid: number): number;
  process_child_ppid?(pid: number): number;
  process_child_pgid?(pid: number): number;
  process_child_sid?(pid: number): number;
  process_next_pid?(): number;
  process_current_pid(vm_ptr: number): number;
  process_current_ppid(vm_ptr: number): number;
  process_current_pgid(vm_ptr: number): number;
  process_current_sid(vm_ptr: number): number;
  process_current_did_exec(vm_ptr: number): number;
  process_rewrite_child_pid(vm_ptr: number, oldPid: number, newPid: number, nextPid: number): void;
  process_canonicalize_child_pid?(vmPtr: number, provisionalChildPid: number, nextPidFloor: number): number;
  process_reset_lifecycle(
    vm_ptr: number,
    pid: number,
    ppid: number,
    nextPid: number,
    pgid: number,
    sid: number,
    didExec: number,
  ): void;
  supervisor_vfork_execve_handoff_failure_wait_status?(): number;
  supervisor_vfork_execve_rollback?(vmPtr: number, childPid: number): number;
  thread_prepare_fork_child?(vm_ptr: number, pid: number): void;
  thread_restore_fork_parent?(vm_ptr: number, processPtr: number, pid: number, ppid: number): void;
  process_upsert_process_entry(pid: number, ppid: number, pgid: number, sid: number, didExec: number): void;
  process_remove_process_entry(pid: number): void;
  process_stdin_pipe(vm_ptr: number): number;
  process_stdout_pipe(vm_ptr: number): number;
  process_stderr_pipe(vm_ptr: number): number;
  process_tty_pty_slot?(vm_ptr: number): number;
  process_use_pipe_stdio?(vm_ptr: number): number;
  process_close_host_stdin?(vm_ptr: number): void;
  process_host_write_stdin?(vm_ptr: number, srcPtr: number, srcLen: number): number;
  process_set_host_stdio(
    vm_ptr: number,
    stdinPipeSlot: number,
    stdoutPipeSlot: number,
    stderrPipeSlot: number,
  ): void;
  process_cwd_ino(vm_ptr: number): number;
  process_set_cwd_ino(vm_ptr: number, ino: number): void;
  process_install_procfs_snapshot?(
    target_ptr: number,
    target_len: number,
    guestMemoryPolicyLimit: bigint,
  ): void;
  process_fd_ofd_id(vm_ptr: number, fd: number): number;
  process_fd_flags(vm_ptr: number, fd: number): number;
  process_fd_generation?(vm_ptr: number, fd: number): number;
  process_set_fd_entry(vm_ptr: number, fd: number, ofdId: number, fdFlags: number): void;
  kernel_retain_ofd(ofdId: number): void;
  kernel_release_ofd(ofdId: number): void;
  kernel_reserve_ofd?(ofdId: number): void;
  kernel_unreserve_ofd?(ofdId: number): void;
  kernel_ofd_reserved?(ofdId: number): number;
  kernel_ofd_reserved_ptr?(): number;
  kernel_ofd_reserved_bytes?(): number;
  kernel_ofd_fd_type(ofdId: number): number;
  kernel_pty_input_pipe?(ptyIdx: number): number;
  kernel_pty_output_pipe?(ptyIdx: number): number;
  kernel_ofd_host_fd(ofdId: number): number;
  kernel_ofd_status_flags(ofdId: number): number;
  kernel_ofd_ino(ofdId: number): number;
  kernel_ofd_refcount(ofdId: number): number;
  kernel_ofd_offset(ofdId: number): bigint;
  process_release_host_stdio(vm_ptr: number): void;
  process_inject_http(vm_ptr: number, port: number, req_ptr: number, req_len: number): number;
  process_read_http_response(vm_ptr: number, conn_id: number, dst_ptr: number, dst_len: number): number;
  process_close_http_connection(vm_ptr: number, conn_id: number): void;
  debug_clear_syscall_trace?(): void;
  debug_clear_signal_trace?(): void;
  debug_clear_cpu_trace?(): void;
  debug_cpu_profile_block_hits?(): bigint | number;
  debug_cpu_profile_block_misses?(): bigint | number;
  debug_cpu_profile_block_builds?(): bigint | number;
  debug_cpu_profile_block_build_failures?(): bigint | number;
  debug_cpu_profile_block_build_ops?(): bigint | number;
  debug_cpu_profile_block_build_budget?(): bigint | number;
  debug_cpu_profile_block_budget_skips?(): bigint | number;
  debug_cpu_profile_block_cache_unsupported?(): bigint | number;
  debug_cpu_profile_block_exec_ops?(): bigint | number;
  debug_cpu_profile_block_exec_unsupported?(): bigint | number;
  debug_cpu_profile_block_exec_op_sample_interval?(): bigint | number;
  debug_cpu_profile_block_exec_sampled_blocks?(): bigint | number;
  debug_cpu_profile_block_miss_sample_interval?(): bigint | number;
  debug_cpu_profile_block_miss_sampled?(): bigint | number;
  debug_cpu_profile_block_miss_sample_count?(): number;
  debug_cpu_profile_block_miss_sample_pc?(index: number): bigint | number;
  debug_cpu_profile_block_miss_sample_evicted_pc?(index: number): bigint | number;
  debug_cpu_profile_block_miss_sample_cache_index?(index: number): number;
  debug_cpu_profile_block_miss_sample_count_at?(index: number): bigint | number;
  debug_cpu_profile_interpreter_ops?(): bigint | number;
  debug_cpu_profile_block_build_op_count?(opId: number): bigint | number;
  debug_cpu_profile_block_exec_op_count?(opId: number): bigint | number;
  debug_cpu_profile_interpreter_op_count?(opId: number): bigint | number;
  debug_cpu_profile_unsupported_op_count?(opId: number): bigint | number;
  debug_memory_permit_checks?(): bigint | number;
  debug_memory_permit_cache_hits?(): bigint | number;
  debug_memory_permit_cover_hits?(): bigint | number;
  debug_memory_permit_denied?(): bigint | number;
  debug_memory_permit_checks_by_prot?(prot: number): bigint | number;
  debug_memory_permit_cache_hits_by_prot?(prot: number): bigint | number;
  debug_memory_permit_cover_hits_by_prot?(prot: number): bigint | number;
  debug_memory_permit_denied_by_prot?(prot: number): bigint | number;
  debug_clear_guest_write_watch_trace?(): void;
  debug_configure_guest_write_watch?(slot: number, start: bigint | number, end: bigint | number): void;
  debug_guest_write_overlap_trace_len?(): number;
  debug_guest_write_overlap_trace_entry_slot?(index: number): number;
  debug_guest_write_overlap_trace_entry_op?(index: number): number;
  debug_guest_write_overlap_trace_entry_tid?(index: number): number;
  debug_guest_write_overlap_trace_entry_pc?(index: number): bigint | number;
  debug_guest_write_overlap_trace_entry_addr?(index: number): bigint | number;
  debug_guest_write_overlap_trace_entry_len?(index: number): number;
  debug_guest_write_overlap_trace_entry_sample_len?(index: number): number;
  debug_guest_write_overlap_trace_entry_sample?(index: number): bigint | number;
  debug_guest_write_overlap_trace_entry_execve_stage?(index: number): number;
  debug_guest_range_permit_denied_trace_len?(): number;
  debug_guest_range_permit_denied_trace_entry_vm_ptr?(index: number): number;
  debug_guest_range_permit_denied_trace_entry_tid?(index: number): number;
  debug_guest_range_permit_denied_trace_entry_pc?(index: number): bigint | number;
  debug_guest_range_permit_denied_trace_entry_a0?(index: number): bigint | number;
  debug_guest_range_permit_denied_trace_entry_a1?(index: number): bigint | number;
  debug_guest_range_permit_denied_trace_entry_a2?(index: number): bigint | number;
  debug_guest_range_permit_denied_trace_entry_a3?(index: number): bigint | number;
  debug_guest_range_permit_denied_trace_entry_a4?(index: number): bigint | number;
  debug_guest_range_permit_denied_trace_entry_a5?(index: number): bigint | number;
  debug_guest_range_permit_denied_trace_entry_s1?(index: number): bigint | number;
  debug_guest_range_permit_denied_trace_entry_s2?(index: number): bigint | number;
  debug_guest_range_permit_denied_trace_entry_s3?(index: number): bigint | number;
  debug_guest_range_permit_denied_trace_entry_s11?(index: number): bigint | number;
  debug_guest_range_permit_denied_trace_entry_start?(index: number): bigint | number;
  debug_guest_range_permit_denied_trace_entry_len?(index: number): bigint | number;
  debug_guest_range_permit_denied_trace_entry_end?(index: number): bigint | number;
  debug_guest_range_permit_denied_trace_entry_guest_limit?(index: number): bigint | number;
  debug_guest_range_permit_denied_trace_entry_prot?(index: number): number;
  debug_guest_range_permit_denied_trace_entry_cover_start?(index: number): bigint | number;
  debug_guest_range_permit_denied_trace_entry_cover_end?(index: number): bigint | number;
  debug_guest_range_permit_denied_trace_entry_cover_prot?(index: number): number;
  debug_guest_range_permit_denied_trace_entry_reason?(index: number): number;
  debug_syscall_trace_nr?(): bigint;
  debug_syscall_trace_result?(): bigint;
  debug_syscall_trace_status?(): number;
  debug_syscall_trace_len?(): number;
  debug_syscall_trace_entry_vm_ptr?(index: number): number;
  debug_syscall_trace_entry_nr?(index: number): bigint | number;
  debug_syscall_trace_entry_arg0?(index: number): bigint | number;
  debug_syscall_trace_entry_arg1?(index: number): bigint | number;
  debug_syscall_trace_entry_arg2?(index: number): bigint | number;
  debug_syscall_trace_entry_arg3?(index: number): bigint | number;
  debug_syscall_trace_entry_arg4?(index: number): bigint | number;
  debug_syscall_trace_entry_arg5?(index: number): bigint | number;
  debug_syscall_trace_entry_result?(index: number): bigint | number;
  debug_syscall_trace_entry_status?(index: number): number;
  debug_syscall_trace_entry_pid_sample0?(index: number): bigint | number;
  debug_syscall_trace_entry_pid_sample1?(index: number): bigint | number;
  debug_syscall_profile_count?(nr: number): bigint | number;
  debug_syscall_profile_non_ok_status_count?(nr: number): bigint | number;
  debug_syscall_profile_error_count?(nr: number): bigint | number;
  debug_futex_trace_len?(): number;
  debug_futex_trace_entry_vm_ptr?(index: number): number;
  debug_futex_trace_entry_tid?(index: number): number;
  debug_futex_trace_entry_pc?(index: number): bigint | number;
  debug_futex_trace_entry_ra?(index: number): bigint | number;
  debug_futex_trace_entry_sp?(index: number): bigint | number;
  debug_futex_trace_entry_s0?(index: number): bigint | number;
  debug_futex_trace_entry_s1?(index: number): bigint | number;
  debug_futex_trace_entry_s2?(index: number): bigint | number;
  debug_futex_trace_entry_s3?(index: number): bigint | number;
  debug_futex_trace_entry_a0?(index: number): bigint | number;
  debug_futex_trace_entry_a1?(index: number): bigint | number;
  debug_futex_trace_entry_a2?(index: number): bigint | number;
  debug_futex_trace_entry_a3?(index: number): bigint | number;
  debug_futex_trace_entry_a4?(index: number): bigint | number;
  debug_futex_trace_entry_a5?(index: number): bigint | number;
  debug_futex_trace_entry_op?(index: number): number;
  debug_futex_trace_entry_addr?(index: number): bigint | number;
  debug_futex_trace_entry_addr2?(index: number): bigint | number;
  debug_futex_trace_entry_val?(index: number): bigint | number;
  debug_futex_trace_entry_val3?(index: number): bigint | number;
  debug_futex_trace_entry_current?(index: number): number;
  debug_futex_trace_entry_local?(index: number): number;
  debug_futex_trace_entry_external?(index: number): number;
  debug_futex_trace_entry_requeued?(index: number): number;
  debug_futex_trace_entry_result?(index: number): bigint | number;
  debug_futex_trace_entry_status?(index: number): number;
  debug_riscv_syscall_trace_len?(): number;
  debug_riscv_syscall_trace_entry_vm_ptr?(index: number): number;
  debug_riscv_syscall_trace_entry_nr?(index: number): bigint | number;
  debug_riscv_syscall_trace_entry_arg0?(index: number): bigint | number;
  debug_riscv_syscall_trace_entry_arg1?(index: number): bigint | number;
  debug_riscv_syscall_trace_entry_arg2?(index: number): bigint | number;
  debug_riscv_syscall_trace_entry_arg3?(index: number): bigint | number;
  debug_riscv_syscall_trace_entry_arg4?(index: number): bigint | number;
  debug_riscv_syscall_trace_entry_arg5?(index: number): bigint | number;
  debug_riscv_syscall_trace_entry_result?(index: number): bigint | number;
  debug_riscv_syscall_trace_entry_status?(index: number): number;
  debug_completed_sync_effects?(): number;
  debug_memory_trace_nr?(): bigint;
  debug_memory_trace_arg0?(): bigint;
  debug_memory_trace_arg1?(): bigint;
  debug_memory_trace_arg2?(): bigint;
  debug_memory_trace_arg3?(): bigint;
  debug_memory_trace_result?(): bigint;
  debug_memory_trace_len?(): number;
  debug_memory_trace_entry_nr?(index: number): bigint | number;
  debug_memory_trace_entry_arg0?(index: number): bigint | number;
  debug_memory_trace_entry_arg1?(index: number): bigint | number;
  debug_memory_trace_entry_arg2?(index: number): bigint | number;
  debug_memory_trace_entry_arg3?(index: number): bigint | number;
  debug_memory_trace_entry_result?(index: number): bigint | number;
  debug_memory_brk_base?(): bigint | number;
  debug_memory_brk_current?(): bigint | number;
  debug_memory_mmap_next?(): bigint | number;
  debug_memory_vma_len?(): number;
  debug_memory_vma_start?(index: number): bigint | number;
  debug_memory_vma_end?(index: number): bigint | number;
  debug_memory_vma_prot?(index: number): number;
  debug_memory_vma_flags?(index: number): number;
  debug_memory_vma_fd?(index: number): number;
  debug_memory_vma_offset?(index: number): bigint | number;
  sig_slots_ptr?(): number;
  process_pending_ptr?(): number;
  debug_signal_delivery_sig?(): number;
  debug_signal_delivery_for_vm_sig?(vmPtr: number): number;
  debug_signal_delivery_frame_base?(): bigint;
  debug_signal_delivery_for_vm_frame_base?(vmPtr: number): bigint;
  debug_signal_delivery_pc?(): bigint;
  debug_signal_delivery_for_vm_pc?(vmPtr: number): bigint;
  debug_signal_delivery_ra?(): bigint;
  debug_signal_delivery_for_vm_ra?(vmPtr: number): bigint;
  debug_signal_delivery_sp?(): bigint;
  debug_signal_delivery_for_vm_sp?(vmPtr: number): bigint;
  debug_signal_delivery_gp?(): bigint;
  debug_signal_delivery_for_vm_gp?(vmPtr: number): bigint;
  debug_signal_delivery_tp?(): bigint;
  debug_signal_delivery_for_vm_tp?(vmPtr: number): bigint;
  debug_signal_delivery_s11?(): bigint;
  debug_signal_delivery_for_vm_s11?(vmPtr: number): bigint;
  debug_signal_delivery_trace_len?(): number;
  debug_signal_delivery_trace_entry_vm_ptr?(index: number): number;
  debug_signal_delivery_trace_entry_sig?(index: number): number;
  debug_signal_delivery_trace_entry_code?(index: number): number;
  debug_signal_delivery_trace_entry_frame_base?(index: number): bigint | number;
  debug_signal_delivery_trace_entry_pc?(index: number): bigint | number;
  debug_signal_delivery_trace_entry_ra?(index: number): bigint | number;
  debug_signal_delivery_trace_entry_sp?(index: number): bigint | number;
  debug_signal_delivery_trace_entry_s11?(index: number): bigint | number;
  debug_signal_delivery_trace_entry_fault_pc?(index: number): bigint | number;
  debug_signal_delivery_trace_entry_fault_addr?(index: number): bigint | number;
  debug_default_signal_sig?(): number;
  debug_default_signal_code?(): number;
  debug_default_signal_vm_ptr?(): number;
  debug_default_signal_pc?(): bigint;
  debug_default_signal_ra?(): bigint;
  debug_default_signal_sp?(): bigint;
  debug_default_signal_gp?(): bigint;
  debug_default_signal_tp?(): bigint;
  debug_default_signal_s11?(): bigint;
  debug_default_signal_fault_pc?(): bigint;
  debug_default_signal_fault_addr?(): bigint;
  debug_illegal_instruction_vm_ptr?(): number;
  debug_illegal_instruction_pc?(): bigint;
  debug_illegal_instruction_raw?(): number;
  debug_illegal_instruction_ra?(): bigint;
  debug_illegal_instruction_sp?(): bigint;
  debug_illegal_instruction_a0?(): bigint;
  debug_illegal_instruction_a4?(): bigint;
  debug_illegal_instruction_s11?(): bigint;
  debug_illegal_instruction_jalr_pc?(): bigint;
  debug_illegal_instruction_jalr_rs1_value?(): bigint;
  debug_illegal_instruction_jalr_target?(): bigint;
  debug_illegal_instruction_jalr_rd?(): number;
  debug_illegal_instruction_jalr_rs1?(): number;
  debug_illegal_instruction_signal_frame_base?(): bigint;
  debug_illegal_instruction_signal_handler_pc?(): bigint;
  debug_illegal_instruction_signal_code?(): number;
  debug_illegal_instruction_signal_addr?(): bigint;
  debug_illegal_instruction_signal_uc_pc?(): bigint;
  debug_illegal_instruction_signal_uc_ra?(): bigint;
  debug_illegal_instruction_signal_uc_sp?(): bigint;
  debug_illegal_instruction_signal_uc_a0?(): bigint;
  debug_signal_send_caller_vm_ptr?(): number;
  debug_signal_send_kind?(): number;
  debug_signal_send_pid?(): number;
  debug_signal_send_tid?(): number;
  debug_signal_send_sig?(): number;
  debug_signal_send_result?(): bigint;
  debug_sync_fault_vm_ptr?(): number;
  debug_sync_fault_tid?(): number;
  debug_sync_fault_pc?(): bigint;
  debug_sync_fault_ra?(): bigint;
  debug_sync_fault_sp?(): bigint;
  debug_sync_fault_s11?(): bigint;
  debug_sync_fault_a0?(): bigint;
  debug_sync_fault_a1?(): bigint;
  debug_sync_fault_a2?(): bigint;
  debug_sync_fault_a3?(): bigint;
  debug_sync_fault_a4?(): bigint;
  debug_sync_fault_a5?(): bigint;
  debug_sync_fault_a6?(): bigint;
  debug_sync_fault_a7?(): bigint;
  debug_sync_fault_s1?(): bigint;
  debug_sync_fault_s2?(): bigint;
  debug_sync_fault_s3?(): bigint;
  debug_sync_fault_s4?(): bigint;
  debug_sync_fault_s5?(): bigint;
  debug_sync_fault_s6?(): bigint;
  debug_sync_fault_s7?(): bigint;
  debug_sync_fault_addr?(): bigint;
  debug_sync_fault_code?(): number;
  debug_signal_send_trace_len?(): number;
  debug_signal_send_trace_entry_caller_vm_ptr?(index: number): number;
  debug_signal_send_trace_entry_kind?(index: number): number;
  debug_signal_send_trace_entry_pid?(index: number): number;
  debug_signal_send_trace_entry_tid?(index: number): number;
  debug_signal_send_trace_entry_sig?(index: number): number;
  debug_signal_send_trace_entry_result?(index: number): bigint | number;
  debug_sigreturn_frame_base?(): bigint;
  debug_sigreturn_pc?(): bigint;
  debug_sigreturn_ra?(): bigint;
  debug_sigreturn_sp?(): bigint;
  debug_sigreturn_gp?(): bigint;
  debug_sigreturn_tp?(): bigint;
  debug_sigreturn_s11?(): bigint;
  debug_clone_spawn_tid?(): number;
  debug_clone_spawn_pc?(): bigint;
  debug_clone_spawn_ra?(): bigint;
  debug_clone_spawn_gp?(): bigint;
  debug_clone_spawn_sp?(): bigint;
  debug_clone_spawn_s11?(): bigint;
  debug_clone_spawn_stack_m32?(): bigint;
  debug_clone_spawn_stack_m24?(): bigint;
  debug_clone_spawn_stack_m16?(): bigint;
  debug_clone_spawn_stack_m8?(): bigint;
  debug_signal_trampoline_hit_tid?(): number;
  debug_signal_trampoline_hit_pc?(): bigint;
  debug_sigreturn_count?(): number;
  debug_zero_reg_tid?(): number;
  debug_zero_reg_for_vm_tid?(vmPtr: number): number;
  debug_zero_reg_pc?(): bigint;
  debug_zero_reg_for_vm_pc?(vmPtr: number): bigint;
  debug_zero_reg_raw?(): number;
  debug_zero_reg_for_vm_raw?(vmPtr: number): number;
  debug_zero_reg_index?(): number;
  debug_zero_reg_for_vm_index?(vmPtr: number): number;
  debug_zero_reg_prev_value?(): bigint;
  debug_zero_reg_for_vm_prev_value?(vmPtr: number): bigint;
  debug_zero_reg_sp?(): bigint;
  debug_zero_reg_for_vm_sp?(vmPtr: number): bigint;
  debug_zero_reg_s11?(): bigint;
  debug_zero_reg_for_vm_s11?(vmPtr: number): bigint;
  debug_s11_write_tid?(): number;
  debug_s11_write_for_vm_tid?(vmPtr: number): number;
  debug_s11_write_pc?(): bigint;
  debug_s11_write_for_vm_pc?(vmPtr: number): bigint;
  debug_s11_write_raw?(): number;
  debug_s11_write_for_vm_raw?(vmPtr: number): number;
  debug_s11_write_source?(): number;
  debug_s11_write_for_vm_source?(vmPtr: number): number;
  debug_s11_write_prev_value?(): bigint;
  debug_s11_write_for_vm_prev_value?(vmPtr: number): bigint;
  debug_s11_write_value?(): bigint;
  debug_s11_write_for_vm_value?(vmPtr: number): bigint;
  debug_sp_write_pc?(): bigint;
  debug_sp_write_for_vm_pc?(vmPtr: number): bigint;
  debug_sp_write_raw?(): number;
  debug_sp_write_for_vm_raw?(vmPtr: number): number;
  debug_sp_write_source?(): number;
  debug_sp_write_for_vm_source?(vmPtr: number): number;
  debug_sp_write_prev_value?(): bigint;
  debug_sp_write_for_vm_prev_value?(vmPtr: number): bigint;
  debug_sp_write_value?(): bigint;
  debug_sp_write_for_vm_value?(vmPtr: number): bigint;
  debug_sp_write_tid?(): number;
  debug_sp_write_for_vm_tid?(vmPtr: number): number;
  debug_jalr_pc?(): bigint;
  debug_clone_jalr_pc?(): bigint;
  debug_jalr_for_vm_pc?(vmPtr: number): bigint;
  debug_jalr_rs1_value?(): bigint;
  debug_clone_jalr_rs1_value?(): bigint;
  debug_jalr_for_vm_rs1_value?(vmPtr: number): bigint;
  debug_jalr_target?(): bigint;
  debug_clone_jalr_target?(): bigint;
  debug_jalr_for_vm_target?(vmPtr: number): bigint;
  debug_jalr_rd?(): number;
  debug_clone_jalr_rd?(): number;
  debug_jalr_for_vm_rd?(vmPtr: number): number;
  debug_jalr_rs1?(): number;
  debug_clone_jalr_rs1?(): number;
  debug_jalr_for_vm_rs1?(vmPtr: number): number;
  debug_clone_startup_trace_len?(): number;
  debug_clone_startup_trace_entry_vm_ptr?(index: number): number;
  debug_clone_startup_trace_entry_tid?(index: number): number;
  debug_clone_startup_trace_entry_pc?(index: number): bigint;
  debug_clone_startup_trace_entry_raw?(index: number): number;
  debug_clone_startup_trace_entry_a0?(index: number): bigint;
  debug_clone_startup_trace_entry_t1?(index: number): bigint;
  debug_clone_startup_trace_entry_t2?(index: number): bigint;
  debug_clone_startup_trace_entry_sp?(index: number): bigint;
  debug_clone_startup_trace_entry_s11?(index: number): bigint;
  debug_execve_stage?(): number;
  debug_execve_value0?(): bigint;
  debug_execve_value1?(): bigint;
  debug_execve_stage_trace_len?(): number;
  debug_execve_stage_trace_stage?(index: number): number;
  debug_execve_stage_trace_value0?(index: number): bigint;
  debug_execve_stage_trace_value1?(index: number): bigint;
  debug_exec_trace_len?(): number;
  debug_exec_trace_entry_vm_ptr?(index: number): number;
  debug_exec_trace_entry_epoch?(index: number): number;
  debug_exec_trace_entry_tid?(index: number): number;
  debug_exec_trace_entry_pc?(index: number): bigint;
  debug_exec_trace_entry_raw?(index: number): number;
  debug_exec_trace_entry_step?(index: number): number;
  debug_exec_trace_entry_sp?(index: number): bigint;
  debug_exec_trace_entry_ra?(index: number): bigint;
  debug_exec_trace_entry_a0?(index: number): bigint;
  debug_exec_trace_entry_s11?(index: number): bigint;
  debug_exec_trace_epoch_for_vm?(vmPtr: number): number;
  debug_fault_exec_trace_len_for_vm?(vmPtr: number): number;
  debug_fault_exec_trace_entry_pc_for_vm?(vmPtr: number, index: number): bigint;
  debug_fault_exec_trace_entry_raw_for_vm?(vmPtr: number, index: number): number;
  debug_fault_exec_trace_entry_step_for_vm?(vmPtr: number, index: number): number;
  debug_fault_exec_trace_entry_sp_for_vm?(vmPtr: number, index: number): bigint;
  debug_fault_exec_trace_entry_ra_for_vm?(vmPtr: number, index: number): bigint;
  debug_fault_exec_trace_entry_a0_for_vm?(vmPtr: number, index: number): bigint;
  debug_fault_exec_trace_entry_s11_for_vm?(vmPtr: number, index: number): bigint;
  debug_sync_fault_exec_trace_len_for_vm?(vmPtr: number): number;
  debug_sync_fault_exec_trace_entry_pc_for_vm?(vmPtr: number, index: number): bigint;
  debug_sync_fault_exec_trace_entry_raw_for_vm?(vmPtr: number, index: number): number;
  debug_sync_fault_exec_trace_entry_step_for_vm?(vmPtr: number, index: number): number;
  debug_sync_fault_exec_trace_entry_sp_for_vm?(vmPtr: number, index: number): bigint;
  debug_sync_fault_exec_trace_entry_ra_for_vm?(vmPtr: number, index: number): bigint;
  debug_sync_fault_exec_trace_entry_a0_for_vm?(vmPtr: number, index: number): bigint;
  debug_sync_fault_exec_trace_entry_s11_for_vm?(vmPtr: number, index: number): bigint;
  debug_thread_step_exec_trace_len_for_vm?(vmPtr: number): number;
  debug_thread_step_exec_trace_entry_pc_for_vm?(vmPtr: number, index: number): bigint;
  debug_thread_step_exec_trace_entry_raw_for_vm?(vmPtr: number, index: number): number;
  debug_thread_step_exec_trace_entry_step_for_vm?(vmPtr: number, index: number): number;
  debug_thread_step_exec_trace_entry_sp_for_vm?(vmPtr: number, index: number): bigint;
  debug_thread_step_exec_trace_entry_ra_for_vm?(vmPtr: number, index: number): bigint;
  debug_thread_step_exec_trace_entry_a0_for_vm?(vmPtr: number, index: number): bigint;
  debug_thread_step_exec_trace_entry_s11_for_vm?(vmPtr: number, index: number): bigint;
  debug_break_exec_trace_len_for_vm?(vmPtr: number): number;
  debug_break_exec_trace_entry_pc_for_vm?(vmPtr: number, index: number): bigint;
  debug_break_exec_trace_entry_raw_for_vm?(vmPtr: number, index: number): number;
  debug_break_exec_trace_entry_step_for_vm?(vmPtr: number, index: number): number;
  debug_break_exec_trace_entry_sp_for_vm?(vmPtr: number, index: number): bigint;
  debug_break_exec_trace_entry_ra_for_vm?(vmPtr: number, index: number): bigint;
  debug_break_exec_trace_entry_a0_for_vm?(vmPtr: number, index: number): bigint;
  debug_break_exec_trace_entry_s11_for_vm?(vmPtr: number, index: number): bigint;
  debug_thread_step_trace_len?(): number;
  debug_thread_step_trace_entry_vm_ptr?(index: number): number;
  debug_thread_step_trace_entry_tid?(index: number): number;
  debug_thread_step_trace_entry_status?(index: number): number;
  debug_thread_step_trace_entry_budget?(index: number): number;
  debug_thread_step_trace_entry_remaining?(index: number): number;
  debug_thread_step_trace_entry_pc?(index: number): bigint;
  debug_thread_step_trace_entry_ra?(index: number): bigint;
  debug_thread_step_trace_entry_sp?(index: number): bigint;
  debug_thread_step_trace_entry_s11?(index: number): bigint;
  debug_thread_step_tid?(): number;
  debug_thread_step_vm_ptr?(): number;
  debug_thread_step_entry_for_vm_tid?(vmPtr: number): number;
  debug_thread_step_entry_for_vm_status?(vmPtr: number): number;
  debug_thread_step_entry_for_vm_budget?(vmPtr: number): number;
  debug_thread_step_entry_for_vm_pc?(vmPtr: number): bigint;
  debug_thread_step_entry_for_vm_ra?(vmPtr: number): bigint;
  debug_thread_step_entry_for_vm_sp?(vmPtr: number): bigint;
  debug_thread_step_entry_for_vm_s11?(vmPtr: number): bigint;
  debug_thread_step_for_vm_tid?(vmPtr: number): number;
  debug_thread_step_status?(): number;
  debug_thread_step_for_vm_status?(vmPtr: number): number;
  debug_thread_step_budget?(): number;
  debug_thread_step_for_vm_budget?(vmPtr: number): number;
  debug_thread_step_remaining?(): number;
  debug_thread_step_for_vm_remaining?(vmPtr: number): number;
  debug_thread_step_pc?(): bigint;
  debug_thread_step_for_vm_pc?(vmPtr: number): bigint;
  debug_thread_step_ra?(): bigint;
  debug_thread_step_for_vm_ra?(vmPtr: number): bigint;
  debug_thread_step_sp?(): bigint;
  debug_thread_step_for_vm_sp?(vmPtr: number): bigint;
  debug_thread_step_s11?(): bigint;
  debug_thread_step_for_vm_s11?(vmPtr: number): bigint;
  debug_thread_step_phase?(): number;
  debug_thread_step_phase_vm_ptr?(): number;
  debug_thread_step_phase_for_vm?(vmPtr: number): number;
  debug_thread_step_phase_status?(): number;
  debug_thread_step_phase_for_vm_status?(vmPtr: number): number;
  debug_thread_step_phase_budget?(): number;
  debug_thread_step_phase_for_vm_budget?(vmPtr: number): number;
  debug_thread_step_phase_remaining?(): number;
  debug_thread_step_phase_for_vm_remaining?(vmPtr: number): number;
  debug_thread_step_phase_pc?(): bigint;
  debug_thread_step_phase_for_vm_pc?(vmPtr: number): bigint;
  debug_thread_step_phase_ra?(): bigint;
  debug_thread_step_phase_for_vm_ra?(vmPtr: number): bigint;
  debug_thread_step_phase_sp?(): bigint;
  debug_thread_step_phase_for_vm_sp?(vmPtr: number): bigint;
  debug_thread_step_phase_s11?(): bigint;
  debug_thread_step_phase_for_vm_s11?(vmPtr: number): bigint;
  debug_fault_dispatch_for_vm_phase?(vmPtr: number): number;
  debug_fault_dispatch_for_vm_tid?(vmPtr: number): number;
  debug_fault_dispatch_for_vm_pc?(vmPtr: number): bigint;
  debug_fault_dispatch_for_vm_ra?(vmPtr: number): bigint;
  debug_fault_dispatch_for_vm_sp?(vmPtr: number): bigint;
  debug_fault_dispatch_for_vm_s11?(vmPtr: number): bigint;
  debug_fault_dispatch_for_vm_addr?(vmPtr: number): bigint;
  // MemFS operations (kernel Worker only)
  memfs_resolve_path(path_ptr: number, path_len: number): number;
  memfs_lookup(parent_ino: number, name_ptr: number, name_len: number): number;
  memfs_inode_count(): number;
  memfs_dir_entry_count(): number;
  memfs_data_used(): number;
  memfs_data_cap(): number;
  memfs_last_create_symlink_stage(): number;
  memfs_last_create_symlink_arg0(): bigint;
  memfs_last_create_symlink_arg1(): bigint;
  memfs_create_file(parent_ino: number, name_ptr: number, name_len: number): number;
  memfs_unlink(parent_ino: number, name_ptr: number, name_len: number): bigint;
  memfs_rmdir(parent_ino: number, name_ptr: number, name_len: number): bigint;
  memfs_reserve_inode_until(target_ino: number): bigint;
  memfs_write_file_at(ino: number, offset: bigint, data_ptr: number, data_len: number): bigint;
  memfs_read_file_to(ino: number, offset: bigint, buf_ptr: number, buf_len: number): bigint;
  memfs_file_size(ino: number): bigint;
  memfs_truncate_file(ino: number, size: bigint): bigint;
  memfs_set_file_size_shallow(ino: number, size: bigint): bigint;
  memfs_set_file_external(ino: number, size: bigint): bigint;
  memfs_inode_mode(ino: number): number;
  memfs_inode_nlink(ino: number): number;
  memfs_inode_uid(ino: number): number;
  memfs_inode_gid(ino: number): number;
  memfs_read_link(ino: number, buf_ptr: number, buf_len: number): bigint;
  memfs_ensure_dir(parent_ino: number, name_ptr: number, name_len: number, mode: number): number;
  memfs_set_mode(ino: number, mode: number): bigint;
  memfs_create_symlink(
    parent_ino: number,
    name_ptr: number,
    name_len: number,
    target_ptr: number,
    target_len: number,
  ): number;
  memfs_create_file_with_data(
    parent_ino: number,
    name_ptr: number,
    name_len: number,
    data_ptr: number,
    data_len: number,
  ): number;
  memfs_readdir_names(dir_ino: number, buf_ptr: number, buf_len: number): bigint;
}

const REQUIRED_SUPERVISOR_RUNTIME_EXPORTS = [
  "supervisor_abi_version",
  "supervisor_debug_snapshot_bytes",
  "supervisor_thread_event_record_bytes",
  "supervisor_thread_event_kind_blocked",
  "supervisor_thread_event_kind_ready",
  "supervisor_thread_event_kind_faulted",
  "supervisor_thread_event_kind_exited",
  "supervisor_thread_event_kind_host_completion_arrived",
  "supervisor_thread_event_kind_timer_fired",
  "supervisor_thread_event_kind_network_readable",
  "supervisor_thread_event_kind_backing_store_ready",
  "supervisor_thread_state_snapshot_bytes",
  "supervisor_resume_record_bytes",
  "supervisor_host_completion_record_bytes",
  "supervisor_host_completion_outcome_success",
  "supervisor_host_completion_outcome_failure",
  "supervisor_host_op_record_bytes",
  "supervisor_process_host_op_record_bytes",
  "supervisor_fault_exit_decision_bytes",
  "supervisor_fd_control_record_bytes",
  "supervisor_fs_control_record_bytes",
  "supervisor_signal_control_record_bytes",
  "supervisor_resume_kind_futex_wake",
  "supervisor_resume_kind_runnable_thread",
  "supervisor_host_op_kind_spawn_thread_worker",
  "supervisor_host_op_kind_spawn_process_worker",
  "supervisor_host_op_kind_create_process_memory",
  "supervisor_host_op_kind_copy_memory_ranges",
  "supervisor_host_op_kind_fill_page_from_backing",
  "supervisor_host_op_kind_flush_dirty_page",
  "supervisor_host_op_kind_process_fs_request",
  "supervisor_host_op_kind_host_network_connect",
  "supervisor_host_op_kind_host_network_read",
  "supervisor_host_op_kind_host_network_write",
  "supervisor_host_op_kind_host_network_close",
  "supervisor_host_op_kind_arm_host_timer",
  "supervisor_host_op_kind_resume_thread",
  "supervisor_host_op_kind_terminate_worker",
  "supervisor_process_host_op_kind_spawn_process_worker",
  "supervisor_fault_exit_kind_thread_exit",
  "supervisor_fault_exit_kind_process_exit",
  "supervisor_fd_control_kind_pipe_created",
  "supervisor_fd_control_kind_fd_closed",
  "supervisor_fd_control_kind_fd_duplicated",
  "supervisor_fd_control_kind_fd_flags_updated",
  "supervisor_fd_control_kind_epoll_created",
  "supervisor_fd_control_kind_epoll_ctl_add",
  "supervisor_fd_control_kind_epoll_ctl_del",
  "supervisor_fd_control_kind_epoll_ctl_mod",
  "supervisor_fd_control_kind_socket_created",
  "supervisor_fd_control_kind_socketpair_created",
  "supervisor_fd_control_kind_socket_bound",
  "supervisor_fd_control_kind_socket_listening",
  "supervisor_fd_control_kind_socket_connect_started",
  "supervisor_fd_control_kind_socket_connected",
  "supervisor_fd_control_kind_socket_connect_failed",
  "supervisor_fd_control_kind_socket_accepted",
  "supervisor_pipe_slot_mode_read",
  "supervisor_pipe_slot_mode_write",
  "supervisor_pipe_slot_mode_control",
  "supervisor_blocked_pipe_resume_action_defer",
  "supervisor_blocked_pipe_resume_action_direct",
  "supervisor_fs_control_kind_page_cache_miss",
  "supervisor_fs_control_kind_page_cache_fill",
  "supervisor_fs_control_kind_dirty_page_published",
  "supervisor_fs_control_kind_dirty_page_flushed",
  "supervisor_fs_dirty_page_op_write",
  "supervisor_fs_dirty_page_op_truncate",
  "supervisor_fs_dirty_page_op_flush",
  "supervisor_fs_dirty_page_flag_update_inode",
  "supervisor_fs_dirty_page_flag_publish_dirty",
  "supervisor_fs_dirty_page_flag_writeback_required",
  "supervisor_fs_backing_fill_mode_resident",
  "supervisor_fs_backing_fill_mode_direct",
  "supervisor_fs_backing_source_range_none",
  "supervisor_fs_backing_source_range_read",
  "supervisor_fs_clean_backing_range_none",
  "supervisor_fs_clean_backing_range_read",
  "supervisor_fs_clean_backing_range_read_zero_tail",
  "supervisor_fs_clean_backing_range_zero",
  "supervisor_fs_page_table_source_backing",
  "supervisor_fs_page_table_source_resident",
  "supervisor_signal_control_kind_boundary_check",
  "supervisor_signal_control_kind_signal_delivered",
  "supervisor_signal_control_kind_blocked_wait_interrupted",
  "supervisor_reset_shadow_log",
  "supervisor_shadow_thread_event_count",
  "supervisor_shadow_host_completion_count",
  "supervisor_shadow_host_op_poll_count",
  "supervisor_shadow_host_op_emit_count",
  "supervisor_shadow_host_op_overflow_count",
  "supervisor_shadow_host_op_log_hash",
  "supervisor_shadow_host_op_last_hash",
  "supervisor_shadow_host_op_last_len",
  "supervisor_shadow_resume_poll_count",
  "supervisor_shadow_resume_enqueue_count",
  "supervisor_shadow_resume_overflow_count",
  "supervisor_shadow_fd_control_count",
  "supervisor_shadow_fs_control_count",
  "supervisor_shadow_signal_control_count",
  "supervisor_shadow_thread_event_log_hash",
  "supervisor_shadow_host_completion_log_hash",
  "supervisor_submit_thread_event",
  "supervisor_complete_host_op",
  "supervisor_next_host_op",
  "supervisor_next_host_op_kind",
  "supervisor_process_host_op",
  "supervisor_process_memory_host_op",
  "supervisor_next_resume",
  "supervisor_process_resume",
  "supervisor_fault_exit_decision",
  "supervisor_process_held_ofd_refcount",
  "supervisor_process_private_pipe_slot_mask",
  "supervisor_process_private_pipe_source_mode",
  "supervisor_process_private_pipe_target_mode",
  "supervisor_process_held_pipe_slot_mask",
  "supervisor_process_pipe_used_mask",
  "supervisor_process_pipe_slot_mode",
  "supervisor_process_pipe_read_open_count",
  "supervisor_process_pipe_write_open_count",
  "supervisor_blocked_pipe_resume_action",
  "supervisor_process_epoll_held_ofd",
  "supervisor_process_epoll_dependent_ofd",
  "supervisor_blocked_epoll_watched_ofd",
  "supervisor_blocked_epoll_watches_pipe",
  "supervisor_process_socket_held_ofd",
  "supervisor_socket_connect_fd",
  "supervisor_socket_connect_peer_addr",
  "supervisor_socket_connect_peer_addr_for_fd",
  "supervisor_socket_prepare_external_peer",
  "supervisor_socket_commit_external_connect_result",
  "supervisor_socket_close_external_peer",
  "supervisor_socket_force_shutdown",
  "supervisor_socket_bridge_active_fd",
  "supervisor_resume_blocked_syscall",
  "supervisor_blocked_resume_delegation",
  "supervisor_blocked_resume_delegation_flag_kernel_worker_available",
  "supervisor_blocked_resume_delegation_direct",
  "supervisor_blocked_resume_delegation_kernel_worker",
  "supervisor_blocked_resume_delegation_wait4_kernel_worker",
  "supervisor_blocked_resume_delegation_unsupported",
  "supervisor_blocked_resume_delegation_epoll_direct",
  "supervisor_blocked_wait_retry_reason",
  "supervisor_blocked_wait_retry_delay_ms",
  "supervisor_blocked_wait_active_child_pid",
  "supervisor_blocked_wait_retry_reason_default",
  "supervisor_blocked_wait_retry_reason_immediate",
  "supervisor_blocked_wait_retry_reason_standard_defer",
  "supervisor_blocked_wait_retry_reason_active_child",
  "supervisor_blocked_wait_retry_reason_passive_wait",
  "supervisor_blocked_wait_retry_reason_process_gate",
  "supervisor_blocked_wait_retry_flag_host_progress",
  "supervisor_blocked_wait_retry_flag_active_child",
  "supervisor_blocked_wait_retry_flag_passive_wait",
  "supervisor_blocked_wait_retry_flag_process_gate",
  "supervisor_blocked_wait_retry_flag_thread_init_pending",
  "supervisor_enqueue_blocked_wait_timer",
  "supervisor_blocked_wait_timer_required",
  "supervisor_vfork_execve_handoff_failure_wait_status",
  "supervisor_vfork_execve_rollback",
  "supervisor_fd_control_record",
  "supervisor_fs_control_record",
  "supervisor_fs_pending_backing_fill_ino",
  "supervisor_fs_pending_backing_fill_offset",
  "supervisor_fs_pending_backing_fill_length",
  "supervisor_fs_pending_backing_fill_expected_size",
  "supervisor_fs_pending_backing_fill_opcode",
  "supervisor_fs_pending_mmap_guest_write_ptr",
  "supervisor_fs_pending_mmap_guest_write_len",
  "supervisor_fs_dirty_page_decision",
  "supervisor_fs_dirty_page_decision_next_size",
  "supervisor_fs_dirty_page_decision_file_object_id",
  "supervisor_fs_dirty_page_decision_generation",
  "supervisor_fs_dirty_page_decision_flags",
  "supervisor_fs_page_table_source_decision",
  "supervisor_fs_backing_fill_probe_direct",
  "supervisor_fs_backing_fill_mode_decision",
  "supervisor_fs_backing_source_range_decision",
  "supervisor_fs_backing_source_range_source_byte_length",
  "supervisor_fs_backing_source_range_source_offset",
  "supervisor_fs_backing_source_range_read_length",
  "supervisor_fs_clean_backing_range_decision",
  "supervisor_fs_clean_backing_range_read_offset",
  "supervisor_fs_clean_backing_range_read_length",
  "supervisor_fs_clean_backing_range_output_length",
  "supervisor_signal_control_record",
  "supervisor_debug_snapshot",
  "supervisor_thread_state_snapshot",
  "supervisor_futex_wait_completion_woken",
  "supervisor_futex_wait_completion_not_equal",
  "supervisor_futex_wait_completion_invalid_address",
  "supervisor_futex_wait_completion_signal_check",
  "supervisor_complete_futex_wait",
  "supervisor_signal_pending_for_blocked_wait",
] as const;

export function requiredSupervisorRuntimeExportNames(): readonly string[] {
  return REQUIRED_SUPERVISOR_RUNTIME_EXPORTS;
}

export function assertSupervisorRuntimeAbi(exports: KernelExports): void {
  const missing = REQUIRED_SUPERVISOR_RUNTIME_EXPORTS.filter((name) =>
    typeof (exports as unknown as Record<string, unknown>)[name] !== "function");
  if (missing.length > 0) {
    throw new Error(`kernel supervisor ABI missing required export(s): ${missing.join(", ")}`);
  }
}

/** Cached STATUS constants read from WASM exports. */
export interface StatusCodes {
  OK: number;
  FAULT: number;
  FS: number;
  FUTEX_WAIT: number;
  PIPE_WAIT: number;
  EPOLL_WAIT: number;
  VFORK_WAIT: number;
  EXECVE_SPAWN: number;
  CLONE_SPAWN: number;
  SLEEP: number;
  JIT_READY: number;
  WAIT4: number;
  CONNECT: number;
  SOCKET_SEND: number;
  SOCKET_RECV: number;
  FLOCK_WAIT: number;
}

/** Cached spawn kind constants. */
export interface SpawnKindCodes {
  NONE: number;
  EXECVE: number;
  FORK: number;
}

export interface KernelRuntimeState {
  heapBase: number;
  heapPtr: number;
  ofdTablePtr: number;
  ofdStatePtr?: number;
  ofdStateBytes?: number;
  ofdOffsetFloorPtr?: number;
  ofdOffsetFloorBytes?: number;
  memfsPtr: number;
  memfsHeaderBytes: number;
  memfsInodesBytes: number;
  memfsFirstChildBytes: number;
  memfsDirEntriesBytes: number;
  memfsDataBytes: number;
  memStatePtr: number;
  epollTablePtr: number;
  epollTableBytes?: number;
  socketTablePtr: number;
  socketStatePtr?: number;
  socketStateBytes?: number;
  socketUsed: number;
  pipeBase: number;
  pipeRegionBytes?: number;
  pipeUsed: bigint;
  netBufBase: number;
  netBufPoolBytes?: number;
  netBufUsed: bigint;
  sigSlotsPtr: number;
  sigSlotsBytes?: number;
  processPendingPtr: number;
  processPendingBytes?: number;
  signalfdStatePtr?: number;
  signalfdStateBytes?: number;
  processRuntimeStatePtr: number;
  processRuntimeStateBytes?: number;
  timeStatePtr: number;
  timeStateBytes?: number;
}

export function kernelOfdStatePtr(state: KernelRuntimeState): number {
  return state.ofdStatePtr !== undefined && state.ofdStatePtr > 0 ? state.ofdStatePtr : state.ofdTablePtr;
}

export function kernelOfdStateBytes(
  exports: Partial<Pick<KernelExports, "kernel_ofd_table_bytes" | "kernel_ofd_state_bytes">>,
  state?: KernelRuntimeState | null,
): number {
  const exportedBytes = exports.kernel_ofd_state_bytes?.();
  if (exportedBytes !== undefined && exportedBytes > 0) {
    return exportedBytes;
  }
  if (state?.ofdStateBytes !== undefined && state.ofdStateBytes > 0) {
    return state.ofdStateBytes;
  }
  return exports.kernel_ofd_table_bytes?.() ?? 0;
}

export function kernelSocketStatePtr(state: KernelRuntimeState): number {
  return state.socketStatePtr !== undefined && state.socketStatePtr > 0 ? state.socketStatePtr : state.socketTablePtr;
}

export function kernelSocketStateBytes(
  exports: Partial<Pick<KernelExports, "kernel_socket_table_bytes" | "kernel_socket_state_bytes">>,
  state?: KernelRuntimeState | null,
): number {
  const exportedBytes = exports.kernel_socket_state_bytes?.();
  if (exportedBytes !== undefined && exportedBytes > 0) {
    return exportedBytes;
  }
  if (state?.socketStateBytes !== undefined && state.socketStateBytes > 0) {
    return state.socketStateBytes;
  }
  return exports.kernel_socket_table_bytes?.() ?? 0;
}

export interface ProcessSpawnHandoff {
  kind: number;
  childPid: number;
  path: bigint;
  argv: bigint;
  envp: bigint;
  addressProfile: number;
  guestMemoryPolicyLimit: bigint;
  processMaxMemoryPages: number;
  supervisorHostOp?: boolean;
  supervisorHostOpId?: bigint;
}

export function guestAddressProfileForProcessCreation(exports: KernelExports): number {
  if (!exports.abi_guest_address_profile_target) {
    throw new Error("kernel ABI missing abi_guest_address_profile_target");
  }
  if (!exports.process_guest_base_for_address_profile) {
    throw new Error("kernel ABI missing process_guest_base_for_address_profile");
  }
  const targetProfile = exports.abi_guest_address_profile_target();
  if (targetProfile === 0) {
    throw new Error("kernel ABI returned invalid target guest address profile");
  }
  if (exports.process_guest_base_for_address_profile(targetProfile) === 0n) {
    throw new Error("kernel ABI returned invalid target guest address base");
  }
  return targetProfile;
}

export function processGuestBaseForAddressProfile(
  exports: KernelExports,
  addressProfile: number,
): number {
  const base = exports.process_guest_base_for_address_profile?.(addressProfile);
  if (base === undefined) {
    throw new Error("kernel ABI missing process_guest_base_for_address_profile");
  }
  return Number(base);
}

export function processInspectElfBrkWithMemoryPolicy(
  exports: KernelExports,
  elfPtr: number,
  elfLen: number,
  addressProfile: number,
  guestMemoryPolicyLimit: bigint,
): bigint {
  if (!exports.process_inspect_elf_brk_with_memory_policy) {
    throw new Error("kernel ABI missing process_inspect_elf_brk_with_memory_policy");
  }
  return exports.process_inspect_elf_brk_with_memory_policy(
    elfPtr,
    elfLen,
    addressProfile,
    guestMemoryPolicyLimit,
  );
}

export function processCreateWithMemoryPolicy(
  exports: KernelExports,
  elfPtr: number,
  elfLen: number,
  addressProfile: number,
  guestMemoryPolicyLimit: bigint,
  processMaxMemoryPages: number,
  stackSize: number,
): number {
  if (!exports.process_create_with_memory_policy) {
    throw new Error("kernel ABI missing process_create_with_memory_policy");
  }
  return exports.process_create_with_memory_policy(
    elfPtr,
    elfLen,
    addressProfile,
    guestMemoryPolicyLimit,
    processMaxMemoryPages,
    stackSize,
  );
}

export function setGuestMemoryPolicyLimit(
  exports: KernelExports,
  vmPtr: number,
  limit: number | bigint,
): void {
  if (!exports.thread_set_guest_memory_policy_limit) {
    throw new Error("kernel ABI missing thread_set_guest_memory_policy_limit");
  }
  exports.thread_set_guest_memory_policy_limit(vmPtr, normalizeGuestMemoryPolicyLimitBytes(limit));
}

export function setExecveMemoryHandoff(
  exports: KernelExports,
  vmPtr: number,
  processPtr: number,
  pathAddr: bigint,
  argvAddr: bigint,
  envpAddr: bigint,
  addressProfile: number,
  guestMemoryPolicyLimit: bigint,
  processMaxMemoryPages: number,
): void {
  if (!exports.thread_set_execve_memory_handoff) {
    throw new Error("kernel ABI missing thread_set_execve_memory_handoff");
  }
  exports.thread_set_execve_memory_handoff(
    vmPtr,
    processPtr,
    pathAddr,
    argvAddr,
    envpAddr,
    addressProfile,
    guestMemoryPolicyLimit,
    processMaxMemoryPages,
  );
}

export function readStatusCodes(exports: KernelExports): StatusCodes {
  return {
    OK: exports.abi_status_ok(),
    FAULT: exports.abi_status_fault(),
    FS: exports.abi_status_fs(),
    FUTEX_WAIT: exports.abi_status_futex_wait(),
    PIPE_WAIT: exports.abi_status_pipe_wait(),
    EPOLL_WAIT: exports.abi_status_epoll_wait(),
    VFORK_WAIT: exports.abi_status_vfork_wait(),
    EXECVE_SPAWN: exports.abi_status_execve_spawn(),
    CLONE_SPAWN: exports.abi_status_clone_spawn(),
    SLEEP: exports.abi_status_sleep(),
    JIT_READY: exports.abi_status_jit_ready(),
    WAIT4: exports.abi_status_wait4(),
    CONNECT: exports.abi_status_connect(),
    SOCKET_SEND: exports.abi_status_socket_send(),
    SOCKET_RECV: exports.abi_status_socket_recv(),
    FLOCK_WAIT: exports.abi_status_flock_wait(),
  };
}

export function readSpawnKindCodes(exports: KernelExports): SpawnKindCodes {
  return {
    NONE: exports.abi_spawn_kind_none(),
    EXECVE: exports.abi_spawn_kind_execve(),
    FORK: exports.abi_spawn_kind_fork(),
  };
}

export function readKernelRuntimeState(exports: KernelExports): KernelRuntimeState {
  const memfsExports = exports as Partial<Pick<
    KernelExports,
    | "kernel_memfs_header_bytes"
    | "kernel_memfs_inodes_bytes"
    | "kernel_memfs_first_child_bytes"
    | "kernel_memfs_dir_entries_bytes"
    | "kernel_memfs_data_bytes"
  >>;
  const state: KernelRuntimeState = {
    heapBase: exports.kernel_heap_base(),
    heapPtr: exports.kernel_heap_ptr(),
    ofdTablePtr: exports.kernel_ofd_table_ptr(),
    memfsPtr: exports.kernel_memfs_ptr(),
    memfsHeaderBytes: memfsExports.kernel_memfs_header_bytes?.() ?? 0,
    memfsInodesBytes: memfsExports.kernel_memfs_inodes_bytes?.() ?? 0,
    memfsFirstChildBytes: memfsExports.kernel_memfs_first_child_bytes?.() ?? 0,
    memfsDirEntriesBytes: memfsExports.kernel_memfs_dir_entries_bytes?.() ?? 0,
    memfsDataBytes: memfsExports.kernel_memfs_data_bytes?.() ?? 0,
    memStatePtr: exports.kernel_mem_state_ptr(),
    epollTablePtr: exports.kernel_epoll_table_ptr(),
    socketTablePtr: exports.kernel_socket_table_ptr(),
    socketUsed: exports.kernel_socket_used(),
    pipeBase: exports.kernel_pipe_base(),
    pipeUsed: exports.kernel_pipe_used(),
    netBufBase: exports.kernel_net_buf_base(),
    netBufUsed: exports.kernel_net_buf_used(),
    sigSlotsPtr: exports.kernel_sig_slots_ptr(),
    processPendingPtr: exports.kernel_process_pending_ptr(),
    processRuntimeStatePtr: exports.kernel_process_runtime_state_ptr(),
    timeStatePtr: exports.kernel_time_state_ptr(),
  };
  const ofdStatePtr = exports.kernel_ofd_state_ptr?.();
  const ofdStateBytes = exports.kernel_ofd_state_bytes?.();
  const ofdOffsetFloorPtr = exports.kernel_ofd_offset_floor_ptr?.();
  const ofdOffsetFloorBytes = exports.kernel_ofd_offset_floor_bytes?.();
  const socketStatePtr = exports.kernel_socket_state_ptr?.();
  const socketStateBytes = exports.kernel_socket_state_bytes?.();
  const signalfdStatePtr = exports.kernel_signalfd_state_ptr?.();
  const signalfdStateBytes = exports.kernel_signalfd_state_bytes?.();
  state.epollTableBytes = exports.kernel_epoll_table_bytes?.();
  state.pipeRegionBytes = exports.kernel_pipe_region_bytes?.();
  state.netBufPoolBytes = exports.kernel_net_buf_pool_bytes?.();
  state.sigSlotsBytes = exports.kernel_sig_slots_bytes?.();
  state.processPendingBytes = exports.kernel_process_pending_bytes?.();
  state.processRuntimeStateBytes = exports.kernel_process_runtime_state_bytes?.();
  state.timeStateBytes = exports.kernel_time_state_bytes?.();
  if (ofdStatePtr !== undefined && ofdStatePtr > 0) {
    state.ofdStatePtr = ofdStatePtr;
  }
  if (ofdStateBytes !== undefined && ofdStateBytes > 0) {
    state.ofdStateBytes = ofdStateBytes;
  }
  if (ofdOffsetFloorPtr !== undefined && ofdOffsetFloorPtr > 0) {
    state.ofdOffsetFloorPtr = ofdOffsetFloorPtr;
  }
  if (ofdOffsetFloorBytes !== undefined && ofdOffsetFloorBytes > 0) {
    state.ofdOffsetFloorBytes = ofdOffsetFloorBytes;
  }
  if (socketStatePtr !== undefined && socketStatePtr > 0) {
    state.socketStatePtr = socketStatePtr;
  }
  if (socketStateBytes !== undefined && socketStateBytes > 0) {
    state.socketStateBytes = socketStateBytes;
  }
  if (signalfdStatePtr !== undefined && signalfdStatePtr > 0) {
    state.signalfdStatePtr = signalfdStatePtr;
  }
  if (signalfdStateBytes !== undefined && signalfdStateBytes > 0) {
    state.signalfdStateBytes = signalfdStateBytes;
  }
  return state;
}

export function attachKernelRuntimeState(
  exports: KernelExports,
  state: KernelRuntimeState,
): void {
  exports.kernel_attach_runtime_state(
    state.heapBase,
    state.heapPtr,
    kernelOfdStatePtr(state),
    state.memfsPtr,
    state.memStatePtr,
    state.epollTablePtr,
    kernelSocketStatePtr(state),
    state.socketUsed,
    state.pipeBase,
    state.pipeUsed,
    state.netBufBase,
    state.netBufUsed,
    state.sigSlotsPtr,
    state.processPendingPtr,
    state.processRuntimeStatePtr,
    state.timeStatePtr,
    state.signalfdStatePtr ?? 0,
  );
}

function supervisorProcessHostOpKindSpawnProcessWorker(exports: KernelExports): number {
  return exports.supervisor_process_host_op_kind_spawn_process_worker?.()
    ?? SUPERVISOR_PROCESS_HOST_OP_KIND_SPAWN_PROCESS_WORKER;
}

function readSupervisorProcessSpawnHandoff(
  exports: KernelExports,
  vmPtr: number,
): ProcessSpawnHandoff | null | undefined {
  const readHostOp = exports.supervisor_process_host_op;
  const recordBytesExport = exports.supervisor_process_host_op_record_bytes;
  if (readHostOp === undefined || recordBytesExport === undefined) {
    return undefined;
  }
  const recordBytes = recordBytesExport();
  if (recordBytes < SUPERVISOR_PROCESS_HOST_OP_RECORD_BYTES) {
    throw new Error(`supervisor process HostOp record bytes too small: ${recordBytes}`);
  }
  const scratch = kernelScratchBase(exports) + SUPERVISOR_PROCESS_HOST_OP_SCRATCH_OFFSET;
  ensureMemoryCapacity(exports.memory, scratch + recordBytes);
  const result = readHostOp(vmPtr, scratch, recordBytes);
  if (result === 0) {
    return null;
  }
  if (result < 0 || result > recordBytes) {
    throw new Error(`supervisor process HostOp poll failed: ${result}`);
  }
  if (result < SUPERVISOR_PROCESS_HOST_OP_RECORD_BYTES) {
    throw new Error(`supervisor process HostOp record too short: ${result}`);
  }
  const view = new DataView(exports.memory.buffer, scratch, result);
  const magic = view.getUint32(0, true);
  const version = view.getUint32(4, true);
  const bytes = view.getUint32(8, true);
  const kind = view.getUint32(12, true);
  if (magic !== SUPERVISOR_PROCESS_HOST_OP_RECORD_MAGIC) {
    throw new Error(`supervisor process HostOp bad magic: 0x${magic.toString(16)}`);
  }
  const expectedVersion = exports.supervisor_abi_version?.() ?? version;
  if (version !== expectedVersion) {
    throw new Error(`supervisor process HostOp bad version: ${version}`);
  }
  if (bytes < SUPERVISOR_PROCESS_HOST_OP_RECORD_BYTES || bytes > view.byteLength) {
    throw new Error(`supervisor process HostOp bad size: ${bytes}`);
  }
  if (kind !== supervisorProcessHostOpKindSpawnProcessWorker(exports)) {
    throw new Error(`supervisor process HostOp bad kind: ${kind}`);
  }
  const parentVmPtr = view.getUint32(24, true);
  if (parentVmPtr !== vmPtr) {
    throw new Error(
      `supervisor process HostOp parent mismatch: `
        + `0x${parentVmPtr.toString(16)} != 0x${vmPtr.toString(16)}`,
    );
  }
  return {
    kind: view.getUint32(28, true),
    childPid: view.getInt32(32, true),
    path: view.getBigUint64(64, true),
    argv: view.getBigUint64(72, true),
    envp: view.getBigUint64(80, true),
    addressProfile: view.getUint32(44, true),
    guestMemoryPolicyLimit: view.getBigUint64(48, true),
    processMaxMemoryPages: view.getUint32(56, true),
    supervisorHostOp: true,
    supervisorHostOpId: view.getBigUint64(16, true),
  };
}

export function readProcessSpawnHandoff(
  exports: KernelExports,
  vmPtr: number,
): ProcessSpawnHandoff {
  const supervisorHandoff = readSupervisorProcessSpawnHandoff(exports, vmPtr);
  if (supervisorHandoff !== undefined && supervisorHandoff !== null) {
    return supervisorHandoff;
  }
  return {
    kind: exports.thread_spawn_kind(vmPtr),
    childPid: exports.thread_spawn_child_pid(vmPtr),
    path: exports.thread_spawn_path(vmPtr),
    argv: exports.thread_spawn_argv(vmPtr),
    envp: exports.thread_spawn_envp(vmPtr),
    addressProfile: exports.thread_spawn_address_profile?.(vmPtr) ?? 0,
    guestMemoryPolicyLimit: exports.thread_spawn_guest_memory_policy_limit?.(vmPtr) ?? 0n,
    processMaxMemoryPages: exports.thread_spawn_process_max_memory_pages?.(vmPtr) ?? 0,
  };
}
