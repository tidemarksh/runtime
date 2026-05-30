import type { KernelExports, StatusCodes } from "../abi.js";

export type ExecutionEngineKind = "interpreter";

export type ExecutionControlBoundary =
  | "continue"
  | "syscall"
  | "fault"
  | "blocking"
  | "invalidation";

export type ExecutionKernelExports = Pick<
  KernelExports,
  "thread_step" | "thread_status"
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

