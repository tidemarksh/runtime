/**
 * WASM exports from the kernel module.
 * All constants are read from the WASM exports — no hand-written values.
 */
export interface KernelExports {
  memory: WebAssembly.Memory;

  // Constants (read once at init)
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
  abi_page_size(): number;
  abi_max_fds(): number;

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
