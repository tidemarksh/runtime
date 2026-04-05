import { readPipe, writePipe } from "../bridge/pipe.js";
import type { ProcessHandle, SharedHostStdio } from "./types.js";

export function writeProcessStdin(handle: ProcessHandle, data: Uint8Array): void {
  if (data.length === 0) {
    return;
  }
  handle.stdinQueue.push(new Uint8Array(data));
}

export function flushStdin(handle: ProcessHandle): boolean {
  if (handle.stdinPipe === 0) {
    return false;
  }

  let wrote = false;
  while (handle.stdinQueue.length > 0) {
    const chunk = handle.stdinQueue[0];
    if (!chunk) {
      handle.stdinQueue.shift();
      continue;
    }
    const result = writePipe(handle.memory, handle.kernelState.pipeBase, handle.stdinPipe, chunk);
    if (result.broken) {
      handle.stdinQueue.length = 0;
      return wrote;
    }
    if (result.written <= 0) {
      break;
    }
    wrote = true;
    if (result.written >= chunk.length) {
      handle.stdinQueue.shift();
      continue;
    }
    handle.stdinQueue[0] = chunk.slice(result.written);
    break;
  }

  return wrote;
}

export function drainPipeOutput(
  handle: ProcessHandle,
  pipeSlot: number,
): boolean {
  let drained = false;
  for (;;) {
    const result = readPipe(handle.memory, handle.kernelState.pipeBase, pipeSlot);
    if (result.data.length > 0) {
      drained = true;
      if (handle.onStdout) {
        handle.onStdout(result.data);
      } else {
        handle.outputBacklog.push(new Uint8Array(result.data));
      }
    }
    if (result.eof || result.data.length === 0) {
      return drained;
    }
  }
}

export function drainProcessOutput(handle: ProcessHandle): boolean {
  const stdoutDrained = handle.stdoutPipe !== 0
    && handle.stdoutPipe === handle.hostStdio.stdoutPipe
    ? drainPipeOutput(handle, handle.stdoutPipe)
    : false;
  const stderrDrained = handle.stderrPipe !== 0
    && handle.stderrPipe === handle.hostStdio.stderrPipe
    ? drainPipeOutput(handle, handle.stderrPipe)
    : false;
  return stdoutDrained || stderrDrained;
}

export function createInheritedStdoutSink(parent: ProcessHandle): (data: Uint8Array) => void {
  return (data: Uint8Array) => {
    if (parent.onStdout) {
      parent.onStdout(data);
      return;
    }
    parent.outputBacklog.push(new Uint8Array(data));
  };
}

export function createInheritedErrorSink(
  parent: ProcessHandle,
  childPid: number,
  getProcessByPid: (pid: number) => ProcessHandle | undefined,
  failProcess: (handle: ProcessHandle, error: Error) => void,
): (error: Error) => void {
  return (error: Error) => {
    if (parent.pid <= 0 || getProcessByPid(parent.pid) !== parent) {
      return;
    }
    failProcess(parent, new Error(`child pid=${childPid}: ${error.message}`));
  };
}

export function retainSharedHostStdio(shared: SharedHostStdio): SharedHostStdio {
  shared.refCount += 1;
  return shared;
}

export function releaseSharedHostStdioLocal(handle: ProcessHandle): void {
  if (handle.stdinPipe === 0 && handle.stdoutPipe === 0 && handle.stderrPipe === 0) {
    return;
  }
  const shared = handle.hostStdio;
  if (shared.refCount > 0) {
    shared.refCount -= 1;
  }
  handle.stdinPipe = 0;
  handle.stdoutPipe = 0;
  handle.stderrPipe = 0;
  if (shared.refCount !== 0) {
    return;
  }
  try {
    handle.kernel.process_release_host_stdio(handle.vmPtr);
  } catch {
    // Worker-error teardown can race with corrupted or already-released process state.
  }
}
