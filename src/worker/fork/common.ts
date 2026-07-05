import { createRuntimeStateBridge } from "../../bridge/runtime-state.js";
import { createInheritedErrorSink, retainSharedHostStdio } from "../stdio.js";
import type { KernelExports } from "../../abi.js";
import { copyGuestWriteWatchRegions } from "../guest-write-watch-state.js";
import { copyNetworkSocketState } from "../network-socket-state.js";
import type { ProcessHandle } from "../types.js";
import type { WorkerForkDeps } from "./types.js";
export { normalizeGuestMemoryPolicyLimitBytes } from "../memory-policy.js";

interface CreateForkChildHandleArgs {
  deps: WorkerForkDeps;
  parent: ProcessHandle;
  childPid: number;
  childPpid: number;
  vmPtr: number;
  worker: Worker;
  memory: WebAssembly.Memory;
  kernel: KernelExports;
  kernelState: ProcessHandle["kernelState"];
  guestMemoryPolicyLimitBytes: bigint;
  maxMemoryPages: number;
  debugCommand: string;
  workerDebugLabel: string;
}

export function createForkChildHandle({
  deps,
  parent,
  childPid,
  childPpid,
  vmPtr,
  worker,
  memory,
  kernel,
  kernelState,
  guestMemoryPolicyLimitBytes,
  maxMemoryPages,
  debugCommand,
  workerDebugLabel,
}: CreateForkChildHandleArgs): ProcessHandle {
  const childHandle: ProcessHandle = {
    runtimeId: parent.runtimeId,
    pid: childPid,
    parentPid: childPpid,
    vmPtr,
    guestMemoryPolicyLimitBytes,
    maxMemoryPages,
    forkMaxMemoryPages: parent.forkMaxMemoryPages,
    guestBase: parent.guestBase,
    worker,
    memory,
    wasmModule: parent.wasmModule,
    kernel,
    kernelState,
    stdinPipe: kernel.process_stdin_pipe(vmPtr),
    stdoutPipe: kernel.process_stdout_pipe(vmPtr),
    stderrPipe: kernel.process_stderr_pipe(vmPtr),
    ttyPtySlot: kernel.process_tty_pty_slot?.(vmPtr) ?? parent.ttyPtySlot,
    hostStdio: retainSharedHostStdio(parent.hostStdio),
    stdinClosed: parent.stdinClosed,
    stdioOwner: parent,
    kernelShare: parent.kernelShare,
    stdinFlushPromise: null,
    debugCommand,
    lastDebugMessage: undefined,
    runtimeBridge: createRuntimeStateBridge(),
    sabPageCache: parent.sabPageCache,
    kernelWorkerRequest: parent.kernelWorkerRequest,
    networkBridge: parent.networkBridge,
    workers: new Set<Worker>(),
    exitInProgress: false,
    onError: createInheritedErrorSink(
      parent,
      childPid,
      (pid) => deps.processHandles.get(pid),
      deps.failProcess,
    ),
    ...(parent.memoryOwnership ? { memoryOwnership: { ...parent.memoryOwnership } } : {}),
  };
  copyGuestWriteWatchRegions(childHandle, parent);
  copyNetworkSocketState(childHandle, parent);
  return childHandle;
}

export function detachInheritedHostStdioFromChild(child: ProcessHandle): void {
  if (child.stdioOwner === null) {
    return;
  }
  child.kernel.process_set_host_stdio(child.vmPtr, 0, 0, 0);
}
