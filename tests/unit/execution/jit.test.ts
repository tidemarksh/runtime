import { expect, test, vi } from "vite-plus/test";
import { createJitExecutionEngine } from "../../../src/execution/jit.js";
import type { ExecutionBoundaryStatusCodes } from "../../../src/execution/types.js";

const statusCodes = {
  OK: 0,
  JIT_READY: 10,
} as ExecutionBoundaryStatusCodes;

function createKernel(options: { install?: number } = {}) {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  new Uint8Array(memory.buffer, 64, 3).set([1, 2, 3]);
  let status = statusCodes.JIT_READY;
  let installedId = 0;
  return {
    memory,
    thread_step: vi.fn(),
    thread_step_jit: vi.fn(() => {
      status = statusCodes.JIT_READY;
      return 80;
    }),
    thread_status: vi.fn(() => status),
    thread_jit_module_ptr: vi.fn(() => 64),
    thread_jit_module_len: vi.fn(() => 3),
    thread_jit_install: vi.fn((_vmPtr: number, moduleId: number) => {
      if ((options.install ?? 1) === 0) {
        status = statusCodes.OK;
        return 0;
      }
      installedId = moduleId;
      status = statusCodes.OK;
      return 1;
    }),
    thread_jit_compiled_id: vi.fn(() => installedId),
    thread_jit_complete: vi.fn((_vmPtr: number, _executed: number) => status),
    thread_jit_clear_pending: vi.fn(() => {
      status = statusCodes.OK;
    }),
  };
}

test("JIT engine copies, compiles, installs, invokes, and completes a kernel region", () => {
  const kernel = createKernel();
  const run = vi.fn(() => 7);
  const moduleFactory = vi.fn(() => ({ run }));
  const engine = createJitExecutionEngine({
    exports: kernel,
    statusCodes,
    moduleFactory,
  });

  const first = engine.step({ vmPtr: 0x1234, budget: 100 });
  expect(moduleFactory).toHaveBeenCalledTimes(1);
  expect([...moduleFactory.mock.calls[0][0]]).toEqual([1, 2, 3]);
  expect(kernel.thread_jit_install).toHaveBeenCalledWith(0x1234, 1);
  expect(run).toHaveBeenCalledWith(0x1234);
  expect(kernel.thread_jit_complete).toHaveBeenCalledWith(0x1234, 7);
  expect(first).toEqual({
    engine: "jit",
    status: statusCodes.OK,
    boundary: "continue",
    returnedBudget: 73,
  });

  engine.step({ vmPtr: 0x1234, budget: 100 });
  expect(moduleFactory).toHaveBeenCalledTimes(1);
  expect(run).toHaveBeenCalledTimes(2);
});

test("JIT engine falls back when compilation or generation-checked install fails", () => {
  const compileFailureKernel = createKernel();
  const compileFailure = createJitExecutionEngine({
    exports: compileFailureKernel,
    statusCodes,
    moduleFactory() {
      throw new WebAssembly.CompileError("unsupported");
    },
  }).step({ vmPtr: 0x1234, budget: 100 });
  expect(compileFailureKernel.thread_jit_clear_pending).toHaveBeenCalledWith(0x1234);
  expect(compileFailure.engine).toBe("interpreter");
  expect(compileFailure.status).toBe(statusCodes.OK);

  const staleKernel = createKernel({ install: 0 });
  const run = vi.fn(() => 7);
  const stale = createJitExecutionEngine({
    exports: staleKernel,
    statusCodes,
    moduleFactory: () => ({ run }),
  }).step({ vmPtr: 0x1234, budget: 100 });
  expect(run).not.toHaveBeenCalled();
  expect(stale.engine).toBe("interpreter");
  expect(stale.status).toBe(statusCodes.OK);
});
