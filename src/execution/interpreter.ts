import type {
  ExecutionBoundaryStatusCodes,
  ExecutionControlBoundary,
  ExecutionEngine,
  ExecutionKernelExports,
  ExecutionStepRequest,
  ExecutionStepResult,
} from "./types.js";

export interface InterpreterExecutionEngineOptions {
  exports: ExecutionKernelExports;
  statusCodes: ExecutionBoundaryStatusCodes;
}

export function classifyExecutionBoundary(
  status: number,
  statusCodes: ExecutionBoundaryStatusCodes,
): ExecutionControlBoundary {
  if (status === statusCodes.OK) {
    return "continue";
  }
  if (status === statusCodes.FAULT) {
    return "fault";
  }
  if (status === statusCodes.JIT_READY) {
    return "invalidation";
  }
  if (
    status === statusCodes.FUTEX_WAIT
    || status === statusCodes.PIPE_WAIT
    || status === statusCodes.EPOLL_WAIT
    || status === statusCodes.VFORK_WAIT
    || status === statusCodes.SLEEP
    || status === statusCodes.WAIT4
    || status === statusCodes.CONNECT
    || status === statusCodes.SOCKET_RECV
    || status === statusCodes.FLOCK_WAIT
  ) {
    return "blocking";
  }
  if (
    status === statusCodes.FS
    || status === statusCodes.EXECVE_SPAWN
    || status === statusCodes.CLONE_SPAWN
    || status === statusCodes.SOCKET_SEND
  ) {
    return "syscall";
  }
  return "syscall";
}

export function createInterpreterExecutionEngine(
  options: InterpreterExecutionEngineOptions,
): ExecutionEngine {
  const { exports, statusCodes } = options;
  return {
    kind: "interpreter",
    step(request: ExecutionStepRequest): ExecutionStepResult {
      const returnedBudget = exports.thread_step(request.vmPtr, request.budget);
      const status = exports.thread_status(request.vmPtr);
      return {
        engine: "interpreter",
        status,
        boundary: classifyExecutionBoundary(status, statusCodes),
        returnedBudget,
      };
    },
  };
}

export function executeInterpreterStep(
  options: InterpreterExecutionEngineOptions & ExecutionStepRequest,
): ExecutionStepResult {
  const engine = createInterpreterExecutionEngine(options);
  return engine.step(options);
}

