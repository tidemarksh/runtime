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
  abi_eagain(): number;
  abi_epipe(): number;

  // Kernel lifecycle
  set_base_time(sec: bigint, nsec: bigint): void;
  kernel_init(heap_base: number): void;
  kernel_heap_base(): number;
  kernel_heap_ptr(): number;
  kernel_ofd_table_ptr(): number;
  kernel_memfs_ptr(): number;
  kernel_mem_state_ptr(): number;
  kernel_epoll_table_ptr(): number;
  kernel_socket_table_ptr(): number;
  kernel_socket_used(): number;
  kernel_pipe_base(): number;
  kernel_pipe_used(): bigint;
  kernel_net_buf_base(): number;
  kernel_net_buf_used(): bigint;
  kernel_sig_slots_ptr(): number;
  kernel_process_pending_ptr(): number;
  kernel_process_runtime_state_ptr(): number;
  kernel_time_state_ptr(): number;
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
  process_create(elf_ptr: number, elf_len: number, ram_size: number, stack_size: number): number;

  // Thread execution
  thread_step(vm_ptr: number, budget: number): number;
  thread_status(vm_ptr: number): number;
  thread_exit_code(vm_ptr: number): number;
  thread_pc(vm_ptr: number): bigint;
  thread_a0(vm_ptr: number): bigint;

  // FS handling
  thread_process_fs(vm_ptr: number): void;
  thread_resume_fs(vm_ptr: number): number;

  // Blocking state accessors
  thread_futex_wait_addr(vm_ptr: number): bigint;
  thread_futex_wait_timeout(vm_ptr: number): bigint;
  thread_resume_futex_wait(vm_ptr: number): void;
  thread_resume_futex_wait_result(vm_ptr: number, result: bigint): void;
  thread_sleep_seconds(vm_ptr: number): bigint;
  thread_sleep_nanoseconds(vm_ptr: number): bigint;
  thread_record_blocked_addr(vm_ptr: number): number;
  thread_resume_wait4(vm_ptr: number): bigint;
  thread_vfork_child_pid(): number;

  // Spawn handoff
  thread_spawn_kind(vm_ptr: number): number;
  thread_spawn_child_pid(vm_ptr: number): number;
  thread_spawn_path(vm_ptr: number): bigint;
  thread_spawn_argv(vm_ptr: number): bigint;
  thread_spawn_envp(vm_ptr: number): bigint;
  thread_clear_spawn_handoff(vm_ptr: number): void;
  thread_resume_vfork_parent(vm_ptr: number, child_pid: number): void;
  thread_spawn_from_clone(parent_vm_ptr: number): number;
  thread_process_thread_count(vm_ptr: number): number;

  // MemFS operations (kernel Worker only)
  memfs_resolve_path(path_ptr: number, path_len: number): number;
  memfs_create_file(parent_ino: number, name_ptr: number, name_len: number): number;
  memfs_write_file_at(ino: number, offset: bigint, data_ptr: number, data_len: number): bigint;
  memfs_read_file_to(ino: number, offset: bigint, buf_ptr: number, buf_len: number): bigint;
  memfs_file_size(ino: number): bigint;
  memfs_ensure_dir(parent_ino: number, name_ptr: number, name_len: number, mode: number): number;
  memfs_create_file_with_data(
    parent_ino: number,
    name_ptr: number,
    name_len: number,
    data_ptr: number,
    data_len: number,
  ): number;
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
