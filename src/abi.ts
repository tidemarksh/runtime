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

  // Lifecycle
  kernel_init(heap_base: number): void;
  process_create(elf_ptr: number, elf_len: number, ram_size: number, stack_size: number): number;

  // Thread execution
  thread_step(vm_ptr: number, budget: number): number;
  thread_status(vm_ptr: number): number;
  thread_exit_code(vm_ptr: number): number;
  thread_pc(vm_ptr: number): bigint;
  thread_a0(vm_ptr: number): bigint;
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
  };
}
