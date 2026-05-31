// Orchestrator tests in this file validate public facade delegation only.
import { expect, test, vi } from "vite-plus/test";
import { createWorkerOrchestratorFacade } from "../../../../src/worker/orchestrator/facade.js";
import type { WorkerOrchestratorFacadePorts } from "../../../../src/worker/orchestrator/types.js";

function makePorts(): WorkerOrchestratorFacadePorts {
  return {
    clearRuntimeFailureSnapshot: vi.fn(),
    clearRuntimeFsState: vi.fn(),
    createProcess: vi.fn(async () => ({ pid: 7 }) as never),
    debugRuntimeExecutionStats: vi.fn(() => []),
    debugRuntimeLastFailureSnapshot: vi.fn(() => null),
    debugRuntimeProcessHandles: vi.fn(() => "handles"),
    debugRuntimeWorkerLogFocus: vi.fn(() => []),
    destroyProcessTree: vi.fn(),
    destroyRuntimeProcesses: vi.fn(),
    flushRuntimeFsStateFromDirtyHandle: vi.fn(() => null),
    getRuntimeFsState: vi.fn(() => null),
    injectHttpRequest: vi.fn(async () => "response"),
    mirrorRuntimeFsEntry: vi.fn(),
    primeRuntimeFsState: vi.fn(() => 1),
    publishRuntimeFsState: vi.fn(() => 2),
    replaceRuntimeFsSnapshot: vi.fn(),
    writeProcessStdin: vi.fn(),
  };
}

test("worker orchestrator facade delegates public operations to composition ports", async () => {
  const ports = makePorts();
  const facade = createWorkerOrchestratorFacade(ports);
  const handle = { pid: 7 } as never;
  const entry = { path: "/file", data: new Uint8Array([1]) } as never;

  facade.clearRuntimeFailureSnapshot(1);
  facade.clearRuntimeFsState(2);
  await facade.createProcess(new Uint8Array([0]), new Uint8Array([1]), { pid: 9 });
  facade.debugRuntimeExecutionStats(3);
  facade.debugRuntimeLastFailureSnapshot(4);
  facade.debugRuntimeProcessHandles(5);
  facade.debugRuntimeWorkerLogFocus(6);
  facade.destroyProcessTree(7, 8, 9);
  facade.destroyRuntimeProcesses(10, 11);
  facade.flushRuntimeFsStateFromDirtyHandle(12);
  facade.getRuntimeFsState(13);
  await facade.injectHttpRequest(handle, 80, "GET / HTTP/1.1\r\n\r\n");
  facade.mirrorRuntimeFsEntry(14, entry);
  facade.primeRuntimeFsState(15, [entry]);
  facade.publishRuntimeFsState(16, [entry]);
  facade.replaceRuntimeFsSnapshot(17, [entry]);
  facade.writeProcessStdin(handle, new Uint8Array([2]));

  expect(ports.clearRuntimeFailureSnapshot).toHaveBeenCalledWith(1);
  expect(ports.clearRuntimeFsState).toHaveBeenCalledWith(2);
  expect(ports.createProcess).toHaveBeenCalledWith(new Uint8Array([0]), new Uint8Array([1]), { pid: 9 });
  expect(ports.debugRuntimeExecutionStats).toHaveBeenCalledWith(3);
  expect(ports.debugRuntimeLastFailureSnapshot).toHaveBeenCalledWith(4);
  expect(ports.debugRuntimeProcessHandles).toHaveBeenCalledWith(5);
  expect(ports.debugRuntimeWorkerLogFocus).toHaveBeenCalledWith(6);
  expect(ports.destroyProcessTree).toHaveBeenCalledWith(7, 8, 9);
  expect(ports.destroyRuntimeProcesses).toHaveBeenCalledWith(10, 11);
  expect(ports.flushRuntimeFsStateFromDirtyHandle).toHaveBeenCalledWith(12);
  expect(ports.getRuntimeFsState).toHaveBeenCalledWith(13);
  expect(ports.injectHttpRequest).toHaveBeenCalledWith(handle, 80, "GET / HTTP/1.1\r\n\r\n");
  expect(ports.mirrorRuntimeFsEntry).toHaveBeenCalledWith(14, entry);
  expect(ports.primeRuntimeFsState).toHaveBeenCalledWith(15, [entry]);
  expect(ports.publishRuntimeFsState).toHaveBeenCalledWith(16, [entry]);
  expect(ports.replaceRuntimeFsSnapshot).toHaveBeenCalledWith(17, [entry]);
  expect(ports.writeProcessStdin).toHaveBeenCalledWith(handle, new Uint8Array([2]));
});
