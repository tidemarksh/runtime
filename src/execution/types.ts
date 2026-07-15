import type { KernelExports, StatusCodes } from "../abi.js";

export type ExecutionEngineKind = "interpreter" | "jit";

export type ExecutionControlBoundary =
  | "continue"
  | "syscall"
  | "fault"
  | "blocking"
  | "invalidation";

export type ExecutionKernelExports = Pick<
  KernelExports,
  | "memory"
  | "thread_step"
  | "thread_step_jit"
  | "thread_jit_module_ptr"
  | "thread_jit_module_len"
  | "thread_jit_install"
  | "thread_jit_compiled_id"
  | "thread_jit_complete"
  | "thread_jit_clear_pending"
  | "debug_thread_step_no_block_cache"
  | "thread_status"
>;

export interface ExecutionStepRequest {
  vmPtr: number;
  budget: number;
}

export interface ExecutionStepResult {
  engine: ExecutionEngineKind;
  status: number;
  boundary: ExecutionControlBoundary;
  returnedBudget: number;
}

export interface ExecutionEngine {
  readonly kind: ExecutionEngineKind;
  step(request: ExecutionStepRequest): ExecutionStepResult;
}

export type ExecutionBoundaryStatusCodes = Pick<
  StatusCodes,
  | "OK"
  | "FAULT"
  | "FS"
  | "FUTEX_WAIT"
  | "PIPE_WAIT"
  | "EPOLL_WAIT"
  | "VFORK_WAIT"
  | "EXECVE_SPAWN"
  | "CLONE_SPAWN"
  | "SLEEP"
  | "JIT_READY"
  | "WAIT4"
  | "CONNECT"
  | "SOCKET_SEND"
  | "SOCKET_RECV"
  | "FLOCK_WAIT"
>;
