import { classifyExecutionBoundary } from "./interpreter.js";
import type {
  ExecutionBoundaryStatusCodes,
  ExecutionEngine,
  ExecutionKernelExports,
  ExecutionStepRequest,
  ExecutionStepResult,
} from "./types.js";

interface JitModuleExports extends WebAssembly.Exports {
  run(vmPtr: number): number;
}

interface CompiledJitModule {
  run(vmPtr: number): number;
}

export type JitModuleFactory = (
  bytes: Uint8Array<ArrayBuffer>,
  memory: WebAssembly.Memory,
) => CompiledJitModule;

export interface JitExecutionEngineOptions {
  exports: ExecutionKernelExports;
  statusCodes: ExecutionBoundaryStatusCodes;
  moduleFactory?: JitModuleFactory;
}

type JitKernelExports = ExecutionKernelExports & Required<Pick<
  ExecutionKernelExports,
  | "memory"
  | "thread_step_jit"
  | "thread_jit_module_ptr"
  | "thread_jit_module_len"
  | "thread_jit_install"
  | "thread_jit_compiled_id"
  | "thread_jit_complete"
  | "thread_jit_clear_pending"
>>;

function defaultModuleFactory(
  bytes: Uint8Array<ArrayBuffer>,
  memory: WebAssembly.Memory,
): CompiledJitModule {
  const module = new WebAssembly.Module(bytes);
  const instance = new WebAssembly.Instance(module, { env: { memory } });
  const moduleExports = instance.exports as JitModuleExports;
  if (typeof moduleExports.run !== "function") {
    throw new TypeError("kernel JIT module does not export run(vmPtr)");
  }
  return { run: moduleExports.run.bind(moduleExports) };
}

export function hasJitExecutionAbi(exports: ExecutionKernelExports): exports is JitKernelExports {
  return exports.memory !== undefined
    && exports.thread_step_jit !== undefined
    && exports.thread_jit_module_ptr !== undefined
    && exports.thread_jit_module_len !== undefined
    && exports.thread_jit_install !== undefined
    && exports.thread_jit_compiled_id !== undefined
    && exports.thread_jit_complete !== undefined
    && exports.thread_jit_clear_pending !== undefined;
}

function copyPendingModule(
  exports: JitKernelExports,
  vmPtr: number,
): Uint8Array<ArrayBuffer> | null {
  const ptr = exports.thread_jit_module_ptr(vmPtr);
  const length = exports.thread_jit_module_len(vmPtr);
  const end = ptr + length;
  if (
    ptr === 0
    || length === 0
    || !Number.isSafeInteger(end)
    || end > exports.memory.buffer.byteLength
  ) {
    return null;
  }
  const bytes = new Uint8Array(length);
  bytes.set(new Uint8Array(exports.memory.buffer, ptr, length));
  return bytes;
}

function executionResult(
  engine: "interpreter" | "jit",
  status: number,
  returnedBudget: number,
  statusCodes: ExecutionBoundaryStatusCodes,
): ExecutionStepResult {
  return {
    engine,
    status,
    boundary: classifyExecutionBoundary(status, statusCodes),
    returnedBudget,
  };
}

export function createJitExecutionEngine(options: JitExecutionEngineOptions): ExecutionEngine {
  const { exports, statusCodes } = options;
  if (!hasJitExecutionAbi(exports)) {
    throw new TypeError("kernel JIT execution ABI is incomplete");
  }
  const moduleFactory = options.moduleFactory ?? defaultModuleFactory;
  const modules = new Map<number, CompiledJitModule>();
  let nextModuleId = 1;

  return {
    kind: "jit",
    step(request: ExecutionStepRequest): ExecutionStepResult {
      let returnedBudget = exports.thread_step_jit(request.vmPtr, request.budget);
      let status = exports.thread_status(request.vmPtr);
      if (status !== statusCodes.JIT_READY) {
        return executionResult("interpreter", status, returnedBudget, statusCodes);
      }

      let moduleId = exports.thread_jit_compiled_id(request.vmPtr);
      if (moduleId === 0) {
        const bytes = copyPendingModule(exports, request.vmPtr);
        if (bytes === null) {
          exports.thread_jit_clear_pending(request.vmPtr);
          status = exports.thread_status(request.vmPtr);
          return executionResult("interpreter", status, returnedBudget, statusCodes);
        }
        let compiled: CompiledJitModule;
        try {
          compiled = moduleFactory(bytes, exports.memory);
        } catch {
          exports.thread_jit_clear_pending(request.vmPtr);
          status = exports.thread_status(request.vmPtr);
          return executionResult("interpreter", status, returnedBudget, statusCodes);
        }
        moduleId = nextModuleId++;
        if (exports.thread_jit_install(request.vmPtr, moduleId) === 0) {
          status = exports.thread_status(request.vmPtr);
          return executionResult("interpreter", status, returnedBudget, statusCodes);
        }
        modules.set(moduleId, compiled);
      }

      const compiled = modules.get(moduleId);
      if (compiled === undefined) {
        throw new Error(`kernel referenced unknown JIT module id ${moduleId}`);
      }
      const executed = compiled.run(request.vmPtr);
      if (!Number.isInteger(executed) || executed < 0) {
        throw new RangeError(`kernel JIT module returned invalid instruction count ${executed}`);
      }
      status = exports.thread_jit_complete(request.vmPtr, executed);
      returnedBudget -= executed;
      return executionResult("jit", status, returnedBudget, statusCodes);
    },
  };
}
