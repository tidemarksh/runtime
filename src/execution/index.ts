export {
  classifyExecutionBoundary,
  createInterpreterExecutionEngine,
  executeInterpreterStep,
  type InterpreterExecutionEngineOptions,
} from "./interpreter.js";
export {
  createJitExecutionEngine,
  hasJitExecutionAbi,
  type JitModuleFactory,
} from "./jit.js";
export type {
  ExecutionBoundaryStatusCodes,
  ExecutionControlBoundary,
  ExecutionEngine,
  ExecutionEngineKind,
  ExecutionKernelExports,
  ExecutionStepRequest,
  ExecutionStepResult,
} from "./types.js";
