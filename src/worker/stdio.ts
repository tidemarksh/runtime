import { readPipe, writePipe } from "../bridge/pipe.js";
import { writeKernelScratch } from "./kernel-scratch.js";
import type { ProcessHandle, SharedHostStdio } from "./types.js";

function flushOutputBacklog(handle: ProcessHandle): void {
  const onStdout = handle.onStdout;
  if (!onStdout || handle.outputBacklog.length === 0) {
    return;
  }
  while (handle.outputBacklog.length > 0) {
    const chunk = handle.outputBacklog.shift();
    if (!chunk) {
      continue;
    }
    onStdout(chunk);
  }
}

function enqueueOutput(handle: ProcessHandle, data: Uint8Array): void {
  handle.outputBacklog.push(new Uint8Array(data));
  flushOutputBacklog(handle);
}

function outputTarget(handle: ProcessHandle): ProcessHandle {
  return handle.stdioOwner ?? handle;
}

function pipeStateSignature(handle: ProcessHandle): string {
  const pipeBase = handle.kernelState.pipeBase;
  const bytes = new DataView(handle.memory.buffer);
  const slots = [handle.hostStdio.stdoutPipe, handle.hostStdio.stderrPipe]
    .filter((slot, index, array) => slot > 0 && array.indexOf(slot) === index);
  return slots
    .map((pipeSlot) => {
      const base = pipeBase + pipeSlot * (32 + 4096);
      const head = bytes.getUint32(base, true);
      const tail = bytes.getUint32(base + 4, true);
      const readOpen = bytes.getInt32(base + 8, true);
      const writeOpen = bytes.getInt32(base + 12, true);
      return `${pipeSlot}:${head}:${tail}:${readOpen}:${writeOpen}`;
    })
    .join("|");
}

function readPipeCounters(handle: ProcessHandle, pipeSlot: number): {
  head: number;
  tail: number;
} {
  const base = handle.kernelState.pipeBase + pipeSlot * (32 + 4096);
  const bytes = new DataView(handle.memory.buffer);
  return {
    head: bytes.getUint32(base, true),
    tail: bytes.getUint32(base + 4, true),
  };
}

export function writeProcessStdin(handle: ProcessHandle, data: Uint8Array): void {
  if (data.length === 0) {
    return;
  }
  handle.stdinQueue.push(new Uint8Array(data));
}

export function flushStdin(handle: ProcessHandle): boolean {
  if (handle.stdinPipe === 0 && handle.ttyPtySlot === 0) {
    return false;
  }

  let wrote = false;
  while (handle.stdinQueue.length > 0) {
    const chunk = handle.stdinQueue[0];
    if (!chunk) {
      handle.stdinQueue.shift();
      continue;
    }
    if (handle.ttyPtySlot !== 0 && handle.kernel.process_host_write_stdin) {
      const srcPtr = writeKernelScratch(handle.kernel, chunk);
      const written = handle.kernel.process_host_write_stdin(handle.vmPtr, srcPtr, chunk.length);
      if (written < 0) {
        return wrote;
      }
      if (written === 0) {
        break;
      }
      wrote = true;
      if (written >= chunk.length) {
        handle.stdinQueue.shift();
        continue;
      }
      handle.stdinQueue[0] = chunk.slice(written);
      break;
    } else {
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
  }

  return wrote;
}

export function drainPipeOutput(
  source: ProcessHandle,
  target: ProcessHandle,
  pipeSlot: number,
): boolean {
  let drained = false;
  for (;;) {
    const result = readPipe(source.memory, source.kernelState.pipeBase, pipeSlot);
    if (result.data.length > 0) {
      drained = true;
      enqueueOutput(target, result.data);
    }
    if (result.eof || result.data.length === 0) {
      return drained;
    }
  }
}

export function drainProcessOutput(handle: ProcessHandle): boolean {
  if (handle.stdioOwner !== null) {
    return false;
  }
  const target = outputTarget(handle);
  const source = handle;
  const stdoutPipe = handle.hostStdio.stdoutPipe;
  const stderrPipe = handle.hostStdio.stderrPipe;
  const stdoutDrained = stdoutPipe !== 0
    ? drainPipeOutput(source, target, stdoutPipe)
    : false;
  const stderrDrained = stderrPipe !== 0 && stderrPipe !== stdoutPipe
    ? drainPipeOutput(source, target, stderrPipe)
    : false;
  return stdoutDrained || stderrDrained;
}

export function drainHiddenProcessOutput(handle: ProcessHandle): boolean {
  if (handle.stdioOwner === null) {
    return false;
  }
  const target = outputTarget(handle);
  const source = handle;
  const stdoutPipe = handle.stdoutPipe;
  const stderrPipe = handle.stderrPipe;
  const stdoutDrained = stdoutPipe !== 0
    ? drainPipeOutput(source, target, stdoutPipe)
    : false;
  const stderrDrained = stderrPipe !== 0 && stderrPipe !== stdoutPipe
    ? drainPipeOutput(source, target, stderrPipe)
    : false;
  return stdoutDrained || stderrDrained;
}

export async function settleVisibleProcessOutput(
  handle: ProcessHandle,
  requestPipeStateSync: (handle: ProcessHandle, wakeWaiters?: boolean) => Promise<void>,
  maxPasses = 4,
): Promise<boolean> {
  const visible = outputTarget(handle);
  let drainedAny = false;
  let stablePasses = 0;

  for (let pass = 0; pass < maxPasses; pass += 1) {
    if (handle.kernelWorkerRequest && handle.pid > 0) {
      await requestPipeStateSync(handle, false);
    }
    if (visible !== handle && visible.kernelWorkerRequest && visible.pid > 0) {
      await requestPipeStateSync(visible, false);
    }
    const before = pipeStateSignature(visible);
    const drained = drainProcessOutput(visible);
    const after = pipeStateSignature(visible);
    drainedAny ||= drained;
    if (!drained && before === after) {
      stablePasses += 1;
      if (stablePasses >= 2) {
        break;
      }
      continue;
    }
    stablePasses = 0;
  }

  return drainedAny;
}

export async function settleInheritedChildOutput(
  child: ProcessHandle,
  requestPipeStateSync: (handle: ProcessHandle, wakeWaiters?: boolean) => Promise<void>,
  maxPasses = 6,
): Promise<boolean> {
  const parent = child.stdioOwner;
  if (!parent) {
    return false;
  }
  const slots = [child.stdoutPipe, child.stderrPipe]
    .filter((slot, index, array) => slot > 0 && array.indexOf(slot) === index);
  let stablePasses = 0;

  for (let pass = 0; pass < maxPasses; pass += 1) {
    await requestPipeStateSync(child, false);
    const settled = slots.every((pipeSlot) => {
      const childCounters = readPipeCounters(child, pipeSlot);
      const parentCounters = readPipeCounters(parent, pipeSlot);
      return parentCounters.tail === childCounters.tail && parentCounters.head === parentCounters.tail;
    });
    if (settled) {
      stablePasses += 1;
      if (stablePasses >= 2) {
        break;
      }
      continue;
    }
    stablePasses = 0;
  }

  return true;
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
  const hadLocalHostStdio = handle.stdinPipe !== 0 || handle.stdoutPipe !== 0 || handle.stderrPipe !== 0;
  if (!hadLocalHostStdio) {
    return;
  }
  const shared = handle.hostStdio;
  if (shared.refCount > 0) {
    shared.refCount -= 1;
  }
  handle.stdinPipe = 0;
  handle.stdoutPipe = 0;
  handle.stderrPipe = 0;
  try {
    // Local teardown still has to clear this process's hidden stdio pipe slots
    // even if the visible host stdio attachment remains shared elsewhere.
    handle.kernel.process_release_host_stdio(handle.vmPtr);
  } catch {
    // Worker-error teardown can race with corrupted or already-released process state.
  }
}
