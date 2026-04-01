import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  type KernelExports,
} from "../../abi.js";
import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import { createKernelEnv } from "../../wasm/env.js";
import { adoptKernelShare, discardCopiedSharedKernelObjects, retainForkSharedOfds } from "../kernel-share.js";
import { acquireWorker, ensureEvictionTimer } from "../pool.js";
import { createForkChildHandle } from "./common.js";
import type { WorkerForkDeps, ProcessHandle, ThreadWorkerState } from "./types.js";

export async function handlePlainFork(
  deps: WorkerForkDeps,
  handle: ProcessHandle,
  parentWorker: Worker,
  parentThread: ThreadWorkerState,
  childPid: number,
): Promise<void> {
  deps.appendRuntimeLifecycleEvent(handle.runtimeId, `plain-fork:start:${childPid}`);
  const cloneFlags = Number(handle.kernel.thread_clone_flags(parentThread.vmPtr));
  const childPpid = (cloneFlags & Number(handle.kernel.abi_clone_parent())) !== 0
    ? (handle.parentPid ?? handle.pid)
    : handle.pid;
  const lifecycle = handle.kernelWorkerRequest && handle.pid > 0
    ? await deps.canonicalizeChildLifecycleViaKernelWorker(handle, parentThread.vmPtr, childPid)
    : {
        childPid,
        nextPid: childPid + 1,
        pgid: handle.kernel.process_current_pgid(parentThread.vmPtr),
        sid: handle.kernel.process_current_sid(parentThread.vmPtr),
      };
  childPid = lifecycle.childPid;

  retainForkSharedOfds(handle);
  handle.kernelState = readKernelRuntimeState(handle.kernel);

  const parentMem = new Uint8Array(handle.memory.buffer);
  const childMemory = new WebAssembly.Memory({
    initial: Math.ceil(handle.memory.buffer.byteLength / 65536),
    maximum: deps.wasmMaxPages,
    shared: true,
  });
  new Uint8Array(childMemory.buffer).set(parentMem);

  const childInstance = await WebAssembly.instantiate(handle.wasmModule, {
    env: createKernelEnv(childMemory, { sabPageCache: handle.sabPageCache }),
  });
  const childKernel = childInstance.exports as unknown as KernelExports;
  attachKernelRuntimeState(childKernel, handle.kernelState);

  ensureEvictionTimer();
  const childWorker = acquireWorker();
  const childHandle = createForkChildHandle({
    deps,
    parent: handle,
    childPid,
    childPpid,
    vmPtr: handle.vmPtr,
    worker: childWorker,
    memory: childMemory,
    kernel: childKernel,
    kernelState: readKernelRuntimeState(childKernel),
    debugCommand: handle.debugCommand,
    workerDebugLabel: "spawn:plain-fork",
  });
  adoptKernelShare(handle, childHandle);
  discardCopiedSharedKernelObjects(childHandle);
  await deps.registerProcessWithKernelWorker(childHandle);
  await deps.syncForkSharedStateViaKernelWorker(handle, [childHandle]);
  await deps.initializeProcessLifecycleViaKernelWorker(
    childHandle,
    childHandle.vmPtr,
    childPpid,
    lifecycle.nextPid,
    lifecycle.pgid,
    lifecycle.sid,
    false,
  );
  deps.assertProcessStandardFdIntegrity(childHandle, "plain-fork-child-ready");
  publishKernelRuntimeState(childHandle.runtimeBridge, childHandle.kernelState);
  deps.processHandles.set(childPid, childHandle);
  deps.attachThreadWorker(childHandle, childWorker, childHandle.vmPtr);

  handle.kernel.thread_clear_spawn_handoff(parentThread.vmPtr);
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  deps.enqueueWorker(handle, parentWorker);
}
