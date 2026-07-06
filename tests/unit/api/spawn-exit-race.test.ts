import { afterEach, expect, test, vi } from "vite-plus/test";

afterEach(() => {
  vi.doUnmock("../../../src/worker.js");
  vi.resetModules();
});

test("Runtime.spawn preserves exit notifications that fire before spawn resolves", async () => {
  vi.resetModules();
  vi.doMock("../../../src/worker.js", () => ({
    clearRuntimeFailureSnapshot: vi.fn(),
    createProcess: vi.fn(async (_wasmBytes, _elfData, options) => {
      const handle = {
        runtimeId: options.runtimeId,
        pid: options.pid,
      };
      options.onExit?.(0);
      return handle;
    }),
    debugRuntimeExecutionStats: vi.fn(() => ""),
    debugRuntimeLastFailureSnapshot: vi.fn(() => null),
    debugRuntimeProcessHandles: vi.fn(() => ""),
    debugRuntimeWorkerLogFocus: vi.fn(() => []),
    destroyProcessTree: vi.fn(),
    destroyRuntimeProcesses: vi.fn(),
    destroyWorkerPool: vi.fn(),
    flushRuntimeFsStateFromDirtyHandle: vi.fn(() => null),
    getRuntimeFsState: vi.fn(() => null),
    injectHttpRequest: vi.fn(),
    precompileModule: vi.fn(async () => ({})),
    publishRuntimeFsState: vi.fn(() => 0),
    replaceRuntimeFsSnapshot: vi.fn(),
    writeProcessStdin: vi.fn(),
  }));

  const { Runtime } = await import("../../../src/index.js");
  const runtime = new (Runtime as unknown as {
    new(options: { wasmBytes: Uint8Array }): InstanceType<typeof Runtime>;
  })({ wasmBytes: new Uint8Array([0]) });

  const proc = await runtime.spawn(new Uint8Array([1, 2, 3]));

  expect(proc.exited).toBe(true);
  await expect(proc.waitForExit()).resolves.toBe(0);
});

test("Runtime.spawn preserves process exit listener while resolving waiters", async () => {
  vi.resetModules();
  let notifyExit: ((code: number) => void) | undefined;
  vi.doMock("../../../src/worker.js", () => ({
    clearRuntimeFailureSnapshot: vi.fn(),
    createProcess: vi.fn(async (_wasmBytes, _elfData, options) => {
      notifyExit = options.onExit;
      return {
        runtimeId: options.runtimeId,
        pid: options.pid,
      };
    }),
    debugRuntimeExecutionStats: vi.fn(() => ""),
    debugRuntimeLastFailureSnapshot: vi.fn(() => null),
    debugRuntimeProcessHandles: vi.fn(() => ""),
    debugRuntimeWorkerLogFocus: vi.fn(() => []),
    destroyProcessTree: vi.fn(),
    destroyRuntimeProcesses: vi.fn(),
    destroyWorkerPool: vi.fn(),
    flushRuntimeFsStateFromDirtyHandle: vi.fn(() => null),
    getRuntimeFsState: vi.fn(() => null),
    injectHttpRequest: vi.fn(),
    precompileModule: vi.fn(async () => ({})),
    publishRuntimeFsState: vi.fn(() => 0),
    replaceRuntimeFsSnapshot: vi.fn(),
    writeProcessStdin: vi.fn(),
  }));

  const { Runtime } = await import("../../../src/index.js");
  const runtime = new (Runtime as unknown as {
    new(options: { wasmBytes: Uint8Array }): InstanceType<typeof Runtime>;
  })({ wasmBytes: new Uint8Array([0]) });

  const proc = await runtime.spawn(new Uint8Array([1, 2, 3]));
  const observedExitCodes: number[] = [];
  proc.onExit((code) => {
    observedExitCodes.push(code);
  });
  const firstWaiter = proc.waitForExit();
  const secondWaiter = proc.waitForExit();

  notifyExit?.(7);

  await expect(firstWaiter).resolves.toBe(7);
  await expect(secondWaiter).resolves.toBe(7);
  expect(observedExitCodes).toEqual([7]);
  expect(proc.exited).toBe(true);
  await expect(proc.waitForExit()).resolves.toBe(7);
});
