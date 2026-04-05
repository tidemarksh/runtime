/**
 * WASM exports from the kernel module.
 * All constants are read from the WASM exports — no hand-written values.
 */
export interface KernelExports {
  memory: WebAssembly.Memory;

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

  // Spawn kinds
  abi_spawn_kind_none(): number;
  abi_spawn_kind_execve(): number;
  abi_spawn_kind_fork(): number;

  // Limits
  abi_page_size(): number;
  abi_max_fds(): number;
  abi_max_threads(): number;
  abi_max_pipes(): number;

  // FD types
  abi_fd_type_none(): number;
  abi_fd_type_file(): number;
  abi_fd_type_pipe(): number;
  abi_fd_type_epoll(): number;
  abi_fd_type_socket(): number;

  // Signal numbers
  abi_sigkill(): number;
  abi_sigterm(): number;
  abi_sigchld(): number;
  abi_sigpipe(): number;
  abi_clone_parent(): bigint | number;
  abi_sys_read?(): bigint | number;
  abi_sys_write?(): bigint | number;
  abi_sys_lseek?(): bigint | number;
  abi_sys_execve?(): bigint | number;
  abi_sys_close?(): bigint | number;
  abi_sys_pipe2?(): bigint | number;
  abi_sys_epoll_ctl?(): bigint | number;
  abi_sys_dup?(): bigint | number;
  abi_sys_dup3?(): bigint | number;
  abi_sys_fcntl?(): bigint | number;
  abi_sys_readv?(): bigint | number;
  abi_sys_writev?(): bigint | number;
  abi_sys_socket?(): bigint | number;
  abi_sys_bind?(): bigint | number;
  abi_sys_listen?(): bigint | number;
  abi_sys_accept4?(): bigint | number;
  abi_sys_eventfd2?(): bigint | number;
  abi_sys_setpgid?(): bigint | number;
  abi_sys_setsid?(): bigint | number;
  abi_sys_kill?(): bigint | number;
  abi_sys_tkill?(): bigint | number;
  abi_sys_tgkill?(): bigint | number;
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
  debug_fd_syscall_trace_arg1?(index: number): bigint | number;
  debug_fd_syscall_trace_arg2?(index: number): bigint | number;
  debug_fd_syscall_trace_arg2_ofd_id?(index: number): number;
  debug_fd_syscall_trace_arg2_ofd_type?(index: number): number;
  debug_fd_syscall_trace_arg3?(index: number): bigint | number;
  debug_fd_syscall_trace_result?(index: number): bigint | number;
  debug_fd_syscall_trace_status?(index: number): number;
  debug_path_syscall_trace_len?(): number;
  debug_path_syscall_trace_vm_ptr?(index: number): number;
  debug_path_syscall_trace_nr?(index: number): bigint | number;
  debug_path_syscall_trace_dirfd?(index: number): bigint | number;
  debug_path_syscall_trace_result?(index: number): bigint | number;
  debug_path_syscall_trace_status?(index: number): number;
  debug_path_syscall_trace_path_ptr?(index: number): number;
  debug_path_syscall_trace_path_len?(index: number): number;
  debug_thread_count_trace_len?(): number;
  debug_thread_count_trace_kind?(index: number): number;
  debug_thread_count_trace_vm_ptr?(index: number): number;
  debug_thread_count_trace_tid?(index: number): number;
  debug_thread_count_trace_process_ptr?(index: number): number;
  debug_thread_count_trace_before?(index: number): number;
  debug_thread_count_trace_after?(index: number): number;

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
  abi_epipe(): number;
  abi_econnrefused(): number;

  // Kernel lifecycle
  set_base_time(sec: bigint, nsec: bigint): void;
  kernel_init(heap_base: number): void;
  kernel_heap_base(): number;
  kernel_heap_ptr(): number;
  kernel_ofd_table_ptr(): number;
  kernel_ofd_table_bytes(): number;
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
  kernel_scratch_base?(): number;
  kernel_reset_memfs(): void;
  kernel_attach_runtime_state(
    heapBase: number,
    heapPtr: number,
    ofdTablePtr: number,
    memfsPtr: number,
    memStatePtr: number,
    epollTablePtr: number,
    socketTablePtr: number,
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
  process_guest_base(ram_size: number): number;
  process_inspect_elf_brk(elf_ptr: number, elf_len: number, ram_size: number): bigint;
  process_create(elf_ptr: number, elf_len: number, ram_size: number, stack_size: number): number;

  // Thread execution
  thread_step(vm_ptr: number, budget: number): number;
  thread_status(vm_ptr: number): number;
  thread_exit_code(vm_ptr: number): number;
  thread_term_signal(vm_ptr: number): number;
  thread_pc(vm_ptr: number): bigint;
  thread_tid?(vm_ptr: number): number;
  thread_tp?(vm_ptr: number): bigint;
  thread_gp?(vm_ptr: number): bigint;
  thread_x27?(vm_ptr: number): bigint;
  thread_reg?(vm_ptr: number, regIndex: number): bigint;
  thread_tls_base?(vm_ptr: number): bigint;
  thread_sig_mask?(vm_ptr: number): bigint;
  thread_sig_pending(vm_ptr: number): bigint;
  thread_sig_info_code?(vm_ptr: number): number;
  thread_fault_pc?(vm_ptr: number): bigint;
  thread_fault_addr?(vm_ptr: number): bigint;
  thread_ra?(vm_ptr: number): bigint;
  thread_sp(vm_ptr: number): bigint;
  thread_a0(vm_ptr: number): bigint;
  thread_a1(vm_ptr: number): bigint;
  thread_stack_word?(vm_ptr: number, word_index: number): bigint;
  thread_blocked_fd(vm_ptr: number): number;
  thread_blocked_ofd_id(vm_ptr: number): number;

  // FS handling
  thread_pending_fs_effects(vm_ptr: number): number;
  thread_pending_fs_target_ino(vm_ptr: number): number;
  thread_pending_fs_opcode(vm_ptr: number): number;
  thread_process_fs(vm_ptr: number): void;
  thread_resume_fs(vm_ptr: number): number;

  // Blocking state accessors
  thread_futex_wait_addr(vm_ptr: number): bigint;
  thread_futex_wait_timeout(vm_ptr: number): bigint;
  thread_futex_wait_expected(vm_ptr: number): number;
  thread_resume_futex_wait(vm_ptr: number): void;
  thread_resume_futex_wait_result(vm_ptr: number, result: bigint): void;
  thread_sleep_seconds(vm_ptr: number): bigint;
  thread_sleep_nanoseconds(vm_ptr: number): bigint;
  thread_record_blocked_addr(vm_ptr: number): number;
  thread_resume_wait4(vm_ptr: number): bigint;
  thread_epoll_wait_timeout(vm_ptr: number): bigint;
  thread_resume_epoll_wait_timeout(vm_ptr: number): void;
  thread_interrupt_epoll_wait?(vm_ptr: number, errno: number): number;
  thread_resume_blocked_syscall(vm_ptr: number): bigint;
  thread_resume_connect_result(vm_ptr: number, result: bigint): void;
  thread_vfork_child_pid(): number;
  thread_clone_flags(vm_ptr: number): bigint;
  thread_clone_child_stack?(vm_ptr: number): bigint;
  thread_clone_tls?(vm_ptr: number): bigint;
  thread_clone_child_tidptr?(vm_ptr: number): bigint;

  // Spawn handoff
  thread_spawn_kind(vm_ptr: number): number;
  thread_spawn_child_pid(vm_ptr: number): number;
  thread_spawn_path(vm_ptr: number): bigint;
  thread_spawn_argv(vm_ptr: number): bigint;
  thread_spawn_envp(vm_ptr: number): bigint;
  thread_set_execve_handoff(
    vm_ptr: number,
    processPtr: number,
    pathAddr: bigint,
    argvAddr: bigint,
    envpAddr: bigint,
  ): void;
  thread_clear_spawn_handoff(vm_ptr: number): void;
  thread_resume_vfork_parent(vm_ptr: number, child_pid: number): void;
  thread_restore_vfork_parent_without_cleanup?(vm_ptr: number, child_pid: number): void;
  thread_complete_execve_handoff(vm_ptr: number): bigint;
  thread_spawn_from_clone(parent_vm_ptr: number): number;
  thread_process_thread_count(vm_ptr: number): number;
  thread_process_ptr(vm_ptr: number): number;
  process_state_bytes(): number;
  process_thread_tid(vm_ptr: number, slot: number): number;
  process_thread_state(vm_ptr: number, slot: number): number;
  process_thread_vm_ptr(vm_ptr: number, slot: number): number;
  process_record_child_exit(pid: number, wait_status: number): void;
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
  process_upsert_process_entry(pid: number, ppid: number, pgid: number, sid: number, didExec: number): void;
  process_remove_process_entry(pid: number): void;
  process_stdin_pipe(vm_ptr: number): number;
  process_stdout_pipe(vm_ptr: number): number;
  process_stderr_pipe(vm_ptr: number): number;
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
  kernel_ofd_fd_type(ofdId: number): number;
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
  debug_clear_memory_trace?(): void;
  debug_clear_write_trace?(): void;
  debug_write_trace_write_calls?(): number;
  debug_write_trace_writev_calls?(): number;
  debug_write_trace_last_fd?(): bigint;
  debug_write_trace_last_nr?(): bigint;
  debug_write_trace_last_buf_addr?(): bigint;
  debug_write_trace_last_count?(): bigint;
  debug_write_trace_last_result?(): bigint;
  debug_write_trace_last_ofd_id?(): number;
  debug_write_trace_last_ofd_type?(): number;
  debug_write_trace_last_ofd_ino?(): number;
  debug_write_trace_last_status_flags?(): number;
  debug_write_trace_last_pipe_read_open?(): number;
  debug_write_trace_last_pipe_write_open?(): number;
  debug_write_trace_ptr?(): number;
  debug_write_trace_len?(): number;
  debug_pipe_read_open?(slot: number): number;
  debug_pipe_write_open?(slot: number): number;
  debug_last_pipe_close_slot?(): number;
  debug_last_pipe_close_is_read_end?(): number;
  debug_last_pipe_close_ofd_id?(): number;
  debug_last_pipe_close_read_open?(): number;
  debug_last_pipe_close_write_open?(): number;
  debug_syscall_trace_vm_ptr?(): number;
  debug_syscall_trace_nr?(): bigint;
  debug_syscall_trace_result?(): bigint;
  debug_syscall_trace_status?(): number;
  debug_completed_sync_effects?(): number;
  debug_memory_trace_nr?(): bigint;
  debug_memory_trace_arg0?(): bigint;
  debug_memory_trace_arg1?(): bigint;
  debug_memory_trace_arg2?(): bigint;
  debug_memory_trace_arg3?(): bigint;
  debug_memory_trace_result?(): bigint;
  debug_memory_brk_base?(): bigint | number;
  debug_memory_brk_current?(): bigint | number;
  debug_memory_mmap_next?(): bigint | number;
  debug_memory_vma_len?(): number;
  debug_memory_vma_start?(index: number): bigint | number;
  debug_memory_vma_end?(index: number): bigint | number;
  debug_memory_vma_prot?(index: number): number;
  debug_memory_vma_flags?(index: number): number;
  debug_epoll_wait_epfd?(): bigint | number;
  debug_epoll_wait_ofd_id?(): number;
  debug_epoll_wait_ofd_type?(): number;
  debug_epoll_wait_idx?(): number;
  debug_epoll_wait_ptr?(): number;
  debug_epoll_wait_count?(): number;
  debug_epoll_wait_result?(): bigint | number;
  debug_epoll_wait_reason?(): number;
  debug_guest_mem_last_addr?(): bigint;
  debug_guest_mem_last_len?(): number;
  debug_guest_mem_last_kind?(): number;
  debug_signal_delivery_sig?(): number;
  debug_signal_delivery_frame_base?(): bigint;
  debug_signal_delivery_pc?(): bigint;
  debug_signal_delivery_ra?(): bigint;
  debug_signal_delivery_sp?(): bigint;
  debug_signal_delivery_x27?(): bigint;
  debug_sigreturn_frame_base?(): bigint;
  debug_sigreturn_pc?(): bigint;
  debug_sigreturn_ra?(): bigint;
  debug_sigreturn_sp?(): bigint;
  debug_sigreturn_x27?(): bigint;
  debug_clone_spawn_tid?(): number;
  debug_clone_spawn_pc?(): bigint;
  debug_clone_spawn_ra?(): bigint;
  debug_clone_spawn_sp?(): bigint;
  debug_clone_spawn_x27?(): bigint;
  debug_signal_trampoline_hit_tid?(): number;
  debug_signal_trampoline_hit_pc?(): bigint;
  debug_sigreturn_count?(): number;
  debug_zero_reg_pc?(): bigint;
  debug_zero_reg_raw?(): number;
  debug_zero_reg_index?(): number;
  debug_zero_reg_prev_value?(): bigint;
  debug_zero_reg_sp?(): bigint;
  debug_zero_reg_x27?(): bigint;
  debug_x27_write_pc?(): bigint;
  debug_x27_write_raw?(): number;
  debug_x27_write_source?(): number;
  debug_x27_write_prev_value?(): bigint;
  debug_x27_write_value?(): bigint;
  debug_tp_write_tid?(): number;
  debug_tp_write_pc?(): bigint;
  debug_tp_write_raw?(): number;
  debug_tp_write_source?(): number;
  debug_tp_write_prev_value?(): bigint;
  debug_tp_write_value?(): bigint;
  debug_tp_write_rs1?(): number;
  debug_tp_write_rs1_value?(): bigint;
  debug_sp_write_pc?(): bigint;
  debug_sp_write_raw?(): number;
  debug_sp_write_source?(): number;
  debug_sp_write_prev_value?(): bigint;
  debug_sp_write_value?(): bigint;
  debug_jalr_pc?(): bigint;
  debug_jalr_rs1_value?(): bigint;
  debug_jalr_target?(): bigint;
  debug_jalr_rd?(): number;
  debug_jalr_rs1?(): number;
  debug_execve_stage?(): number;
  debug_execve_value0?(): bigint;
  debug_execve_value1?(): bigint;

  // MemFS operations (kernel Worker only)
  memfs_resolve_path(path_ptr: number, path_len: number): number;
  memfs_create_file(parent_ino: number, name_ptr: number, name_len: number): number;
  memfs_reserve_inode_until(target_ino: number): bigint;
  memfs_write_file_at(ino: number, offset: bigint, data_ptr: number, data_len: number): bigint;
  memfs_read_file_to(ino: number, offset: bigint, buf_ptr: number, buf_len: number): bigint;
  memfs_file_size(ino: number): bigint;
  memfs_truncate_file(ino: number, size: bigint): bigint;
  memfs_set_file_size_shallow(ino: number, size: bigint): bigint;
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
  memfsPtr: number;
  memStatePtr: number;
  epollTablePtr: number;
  socketTablePtr: number;
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

export interface ProcessSpawnHandoff {
  kind: number;
  childPid: number;
  path: bigint;
  argv: bigint;
  envp: bigint;
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
  return {
    heapBase: exports.kernel_heap_base(),
    heapPtr: exports.kernel_heap_ptr(),
    ofdTablePtr: exports.kernel_ofd_table_ptr(),
    memfsPtr: exports.kernel_memfs_ptr(),
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
}

export function attachKernelRuntimeState(
  exports: KernelExports,
  state: KernelRuntimeState,
): void {
  exports.kernel_attach_runtime_state(
    state.heapBase,
    state.heapPtr,
    state.ofdTablePtr,
    state.memfsPtr,
    state.memStatePtr,
    state.epollTablePtr,
    state.socketTablePtr,
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
  };
}
