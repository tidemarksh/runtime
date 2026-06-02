/**
 * WASM exports from the kernel module.
 * All constants are read from the WASM exports — no hand-written values.
 */
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
  abi_fd_type_pty_master?(): number;
  abi_fd_type_pty_slave?(): number;

  // Signal numbers
  abi_sigkill(): number;
  abi_sigterm(): number;
  abi_sigchld(): number;
  abi_sigpipe(): number;
  abi_clone_parent(): bigint | number;
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
  ): void;

  // Process creation
  process_push_arg(addr: number): void;
  process_push_env(addr: number): void;
  process_clear_args(): void;
  abi_guest_address_profile_wasm32_compatible?(): number;
  abi_guest_address_profile_memory64?(): number;
  process_guest_base_for_address_profile?(addressProfile: number): bigint;
  process_guest_address_limit_for_profile?(addressProfile: number, guestBase: bigint): bigint;
  process_guest_base(ram_size: number): number;
  process_inspect_elf_brk_with_memory_policy?(
    elf_ptr: number,
    elf_len: number,
    addressProfile: number,
    guestMemoryPolicyLimit: bigint,
  ): bigint;
  process_inspect_elf_brk(elf_ptr: number, elf_len: number, ram_size: number): bigint;
  process_create_with_memory_policy?(
    elf_ptr: number,
    elf_len: number,
    addressProfile: number,
    guestMemoryPolicyLimit: bigint,
    processMaxMemoryPages: number,
    stack_size: number,
  ): number;
  process_create(elf_ptr: number, elf_len: number, ram_size: number, stack_size: number): number;
  process_create_last_error?(): number;

  // Thread execution
  thread_step(vm_ptr: number, budget: number): number;
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
  thread_pending_fs_path_ptr?(vm_ptr: number): number;
  thread_pending_fs_path_len?(vm_ptr: number): number;
  thread_pending_fs_absolute_path?(vm_ptr: number, buf_ptr: number, buf_len: number): bigint;
  thread_pending_fs_secondary_absolute_path?(vm_ptr: number, buf_ptr: number, buf_len: number): bigint;
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
  thread_resume_futex_wait(vm_ptr: number): void;
  thread_resume_futex_wait_result(vm_ptr: number, result: bigint): void;
  thread_sleep_seconds(vm_ptr: number): bigint;
  thread_sleep_nanoseconds(vm_ptr: number): bigint;
  thread_sleep_deadline_ticks?(vm_ptr: number): bigint | number;
  thread_record_blocked_addr(vm_ptr: number): number;
  thread_resume_wait4(vm_ptr: number): bigint;
  thread_epoll_wait_timeout(vm_ptr: number): bigint;
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
  thread_resume_blocked_syscall(vm_ptr: number): bigint;
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
  thread_guest_ram_limit(vm_ptr: number): number;
  thread_set_guest_ram_limit(vm_ptr: number, ramSize: number): void;
  thread_guest_address_profile?(vm_ptr: number): number;
  thread_guest_address_limit?(vm_ptr: number): bigint;
  thread_guest_memory_policy_limit?(vm_ptr: number): bigint;
  thread_set_guest_memory_policy_limit?(vm_ptr: number, limit: bigint): void;
  thread_process_max_memory_pages?(vm_ptr: number): number;
  thread_spawn_child_pid(vm_ptr: number): number;
  thread_spawn_path(vm_ptr: number): bigint;
  thread_spawn_argv(vm_ptr: number): bigint;
  thread_spawn_envp(vm_ptr: number): bigint;
  thread_spawn_ram_size(vm_ptr: number): number;
  thread_spawn_address_profile?(vm_ptr: number): number;
  thread_spawn_guest_memory_policy_limit?(vm_ptr: number): bigint;
  thread_spawn_process_max_memory_pages?(vm_ptr: number): number;
  thread_set_execve_handoff(
    vm_ptr: number,
    processPtr: number,
    pathAddr: bigint,
    argvAddr: bigint,
    envpAddr: bigint,
    ramSize: number,
  ): void;
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
  process_current_pid(vm_ptr: number): number;
  process_current_ppid(vm_ptr: number): number;
  process_current_pgid(vm_ptr: number): number;
  process_current_sid(vm_ptr: number): number;
  process_current_did_exec(vm_ptr: number): number;
  process_rewrite_child_pid(vm_ptr: number, oldPid: number, newPid: number, nextPid: number): void;
  process_reset_lifecycle(
    vm_ptr: number,
    pid: number,
    ppid: number,
    nextPid: number,
    pgid: number,
    sid: number,
    didExec: number,
  ): void;
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
  process_install_procfs_snapshot?(target_ptr: number, target_len: number): void;
  process_fd_ofd_id(vm_ptr: number, fd: number): number;
  process_fd_flags(vm_ptr: number, fd: number): number;
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
  socketTablePtr: number;
  socketStatePtr?: number;
  socketStateBytes?: number;
  socketUsed: number;
  pipeBase: number;
  pipeUsed: bigint;
  netBufBase: number;
  netBufUsed: bigint;
  sigSlotsPtr: number;
  processPendingPtr: number;
  processRuntimeStatePtr: number;
  timeStatePtr: number;
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
}

export function guestAddressProfileWasm32Compatible(exports: KernelExports): number {
  if (!exports.abi_guest_address_profile_wasm32_compatible) {
    throw new Error("kernel ABI missing guest address profile export");
  }
  return exports.abi_guest_address_profile_wasm32_compatible();
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

function normalizeMemoryPolicyLimitBigInt(limit: number | bigint): bigint {
  if (typeof limit === "bigint") {
    return limit < 0n ? 0n : limit;
  }
  if (!Number.isFinite(limit)) {
    return 0n;
  }
  return BigInt(Math.max(Math.trunc(limit), 0));
}

export function setGuestMemoryPolicyLimit(
  exports: KernelExports,
  vmPtr: number,
  limit: number | bigint,
): void {
  if (!exports.thread_set_guest_memory_policy_limit) {
    throw new Error("kernel ABI missing thread_set_guest_memory_policy_limit");
  }
  exports.thread_set_guest_memory_policy_limit(vmPtr, normalizeMemoryPolicyLimitBigInt(limit));
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
  );
}

export function readProcessSpawnHandoff(
  exports: KernelExports,
  vmPtr: number,
): ProcessSpawnHandoff {
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
