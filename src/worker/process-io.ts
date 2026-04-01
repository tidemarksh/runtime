import { readKernelRuntimeState } from "../abi.js";
import { publishKernelRuntimeState } from "../bridge/runtime-state.js";
import type { ProcessHandle } from "./types.js";

interface WorkerProcessIoDeps {
  drainProcessOutput(handle: ProcessHandle): boolean;
  failProcess(handle: ProcessHandle, error: Error): void;
  flushStdin(handle: ProcessHandle): boolean;
  isActiveHandle(handle: ProcessHandle): boolean;
  kickBlockedWorkers(handle: ProcessHandle): void;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  writeProcessStdin(handle: ProcessHandle, data: Uint8Array): void;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function createWorkerProcessIoHelpers(deps: WorkerProcessIoDeps) {
  function writeProcessStdin(handle: ProcessHandle, data: Uint8Array): void {
    deps.writeProcessStdin(handle, data);
    const changed = deps.flushStdin(handle);
    if (!changed) {
      return;
    }
    void deps.requestPipeStateSync(handle, true).catch((error) => {
      if (deps.isActiveHandle(handle)) {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      }
    });
  }

  async function injectHttpRequest(
    handle: ProcessHandle,
    port: number,
    request: string,
  ): Promise<string> {
    const requestBytes = encoder.encode(request);
    const scratch = handle.kernel.kernel_heap_ptr() + 0x20000;
    const chunkSize = 16 * 1024;
    const responseChunks: Uint8Array[] = [];

    new Uint8Array(handle.memory.buffer).set(requestBytes, scratch);
    const connId = handle.kernel.process_inject_http(
      handle.vmPtr,
      port,
      scratch,
      requestBytes.length,
    );
    if (connId < 0) {
      throw new Error(`no listening process on port ${port}`);
    }
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);

    const deadline = Date.now() + 10_000;
    try {
      for (;;) {
        deps.kickBlockedWorkers(handle);
        deps.drainProcessOutput(handle);

        const n = handle.kernel.process_read_http_response(
          handle.vmPtr,
          connId,
          scratch,
          chunkSize,
        );
        if (n < 0) {
          break;
        }
        if (n > 0) {
          responseChunks.push(new Uint8Array(handle.memory.buffer.slice(scratch, scratch + n)));
          continue;
        }
        if (Date.now() >= deadline) {
          throw new Error(`timed out waiting for HTTP response on port ${port}`);
        }
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
    } finally {
      handle.kernel.process_close_http_connection(handle.vmPtr, connId);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
    }

    const total = responseChunks.reduce((sum, chunk) => sum + chunk.length, 0);
    const merged = new Uint8Array(total);
    let offset = 0;
    for (const chunk of responseChunks) {
      merged.set(chunk, offset);
      offset += chunk.length;
    }
    return decoder.decode(merged);
  }

  return {
    injectHttpRequest,
    writeProcessStdin,
  };
}
