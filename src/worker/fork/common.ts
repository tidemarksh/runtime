import { createRuntimeStateBridge } from "../../bridge/runtime-state.js";
import { createInheritedErrorSink, retainSharedHostStdio } from "../stdio.js";
import type { KernelExports } from "../../abi.js";
import { copyGuestWriteWatchRegions } from "../guest-write-watch-state.js";
import { copyProcessMemoryOwnership } from "../memory-ownership-state.js";
import { copyNetworkSocketState } from "../network-socket-state.js";
import {
  setProcessDebugCommand,
  setProcessLastDebugMessage,
} from "../process-debug-state.js";
import { setProcessOnError } from "../process-callback-state.js";
import {
  processHostStdio,
  processStdinClosed,
  processStdioOwner,
  processTtyPtySlot,
  setProcessStdioState,
} from "../process-stdio-state.js";
import { setProcessWorkers } from "../process-worker-state.js";
import {
  processKernelWorkerRequest,
  setProcessKernelWorkerRequest,
} from "../process-kernel-worker-request-state.js";
import {
  processKernelShareGroup,
  setProcessKernelShareGroup,
} from "../kernel-share.js";
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
    runtimeBridge: createRuntimeStateBridge(),
    sabPageCache: parent.sabPageCache,
    networkBridge: parent.networkBridge,
  };
  setProcessDebugCommand(childHandle, debugCommand);
  setProcessLastDebugMessage(childHandle, undefined);
  setProcessKernelShareGroup(childHandle, processKernelShareGroup(parent));
  setProcessKernelWorkerRequest(childHandle, processKernelWorkerRequest(parent));
  setProcessStdioState(childHandle, {
    stdinPipe: kernel.process_stdin_pipe(vmPtr),
    stdoutPipe: kernel.process_stdout_pipe(vmPtr),
    stderrPipe: kernel.process_stderr_pipe(vmPtr),
    ttyPtySlot: kernel.process_tty_pty_slot?.(vmPtr) ?? processTtyPtySlot(parent),
    hostStdio: retainSharedHostStdio(processHostStdio(parent)),
    stdinClosed: processStdinClosed(parent),
    stdioOwner: parent,
    inheritedParentOutput: true,
  });
  setProcessWorkers(childHandle, []);
  setProcessOnError(
    childHandle,
    createInheritedErrorSink(
      parent,
      childPid,
      (pid) => deps.processHandles.get(pid),
      deps.failProcess,
    ),
  );
  copyGuestWriteWatchRegions(childHandle, parent);
  copyProcessMemoryOwnership(childHandle, parent);
  copyNetworkSocketState(childHandle, parent);
  return childHandle;
}

export function detachInheritedHostStdioFromChild(child: ProcessHandle): void {
  if (processStdioOwner(child) === null) {
    return;
  }
  child.kernel.process_set_host_stdio(child.vmPtr, 0, 0, 0);
}
