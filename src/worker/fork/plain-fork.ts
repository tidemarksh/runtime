import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  type KernelExports,
} from "../../abi.js";
import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import { createKernelEnv } from "../../wasm/env.js";
import {
  configureKernelInstanceStack,
  MAIN_THREAD_KERNEL_STACK_SLOT,
} from "../../wasm/stack.js";
import {
  adoptKernelShare,
  discardCopiedSharedKernelObjects,
  restoreLocalOfdSlots,
  restoreLocalPipeSlots,
  retainForkSharedOfds,
  snapshotLocalPipeSlots,
} from "../kernel-share.js";
import { activeKernelShareOfdIds, collectProcessHeldOfds, snapshotKernelShareOfdSlots } from "../fork-shared.js";
import { copyLinearMemoryImage } from "../kernel-scratch.js";
import { acquireWorker, ensureEvictionTimer } from "../pool.js";
import { createForkChildHandle } from "./common.js";
import { detachInheritedHostStdioFromChild } from "./common.js";
import { computeGuestRamLimitBytes } from "./common.js";
import type { WorkerForkDeps, ProcessHandle, ThreadWorkerState } from "./types.js";

type ParentProcessSnapshot = {
  processPtr: number;
  processState: Uint8Array;
  pid: number;
  ppid: number;
};

const FORK_CHILD_EXECVE_HEADROOM_BYTES = 16 * 1024 * 1024;

type ExecPathPreflight = {
  path: string;
  ino: number;
  mode: number;
  size: number;
  dataCap: number;
  head: string;
};

function readExecPathPreflight(
  kernel: KernelExports,
  path: string,
): ExecPathPreflight {
  if (!kernel.memfs_resolve_path) {
    return {
      path,
      ino: 0,
      mode: 0,
      size: 0,
      dataCap: 0,
      head: "memfs-debug-unavailable",
    };
  }
  const bytes = new TextEncoder().encode(path);
  const ptr = kernel.kernel_scratch_base?.() ?? 0x1000;
  const memory = new Uint8Array(kernel.memory.buffer);
  memory.set(bytes, ptr);
  const ino = kernel.memfs_resolve_path(ptr, bytes.length);
  if (ino === 0) {
    return {
      path,
      ino,
      mode: 0,
      size: 0,
      dataCap: 0,
      head: "ino=0",
    };
  }
  const mode = kernel.memfs_inode_mode?.(ino) ?? 0;
  const size = Number(kernel.memfs_file_size?.(ino) ?? 0n);
  const maxInodes = kernel.kernel_memfs_first_child_bytes() / 4;
  const inodeBytes = kernel.kernel_memfs_inodes_bytes() / maxInodes;
  const memfsPtr = kernel.kernel_memfs_ptr();
  const memfsView = new DataView(kernel.memory.buffer, memfsPtr, 32);
  const inodesPtr = memfsView.getUint32(16, true);
  const inodeBase = inodesPtr + ino * inodeBytes;
  const dataCap = new DataView(kernel.memory.buffer).getUint32(inodeBase + 28, true);
  const headLen = Math.min(size, 16);
  let head = "unavailable";
  if (headLen > 0 && kernel.memfs_read_file_to) {
    const headPtr = ptr + 0x1000;
    const read = Number(kernel.memfs_read_file_to(ino, 0n, headPtr, headLen));
    if (read > 0) {
      head = Array.from(memory.subarray(headPtr, headPtr + read))
        .map((byte) => byte.toString(16).padStart(2, "0"))
        .join("");
    } else {
      head = `read=${read}`;
    }
  }
  return {
    path,
    ino,
    mode,
    size,
    dataCap,
    head,
  };
}

function formatExecPathPreflight(info: ExecPathPreflight): string {
  return `path=${JSON.stringify(info.path)}:ino=${info.ino}:mode=0o${info.mode.toString(8)}:size=${info.size}:dataCap=${info.dataCap}:head=${info.head}`;
}

function debugForkChildExecPaths(
  kernel: KernelExports,
  childPid: number,
  stage: string,
): string {
  return [
    `plain-fork:${stage}:${childPid}`,
    formatExecPathPreflight(readExecPathPreflight(kernel, "/usr/local/go/bin/go")),
    formatExecPathPreflight(readExecPathPreflight(kernel, "/usr/local/go/pkg/tool/linux_riscv64")),
    formatExecPathPreflight(readExecPathPreflight(kernel, "/usr/local/go/pkg/tool/linux_riscv64/compile")),
  ].join(":");
}

function assertForkChildExecPaths(
  kernel: KernelExports,
  childPid: number,
  stage: string,
): void {
  const goBinary = readExecPathPreflight(kernel, "/usr/local/go/bin/go");
  const compileTool = readExecPathPreflight(kernel, "/usr/local/go/pkg/tool/linux_riscv64/compile");
  const detail = [
    `plain-fork:${stage}:${childPid}`,
    formatExecPathPreflight(goBinary),
    formatExecPathPreflight(compileTool),
  ].join(":");
  if (goBinary.ino === 0) {
    throw new Error(`plain-fork missing go binary: ${detail}`);
  }
  if (compileTool.ino === 0) {
    throw new Error(`plain-fork missing go compile tool: ${detail}`);
  }
  if (goBinary.size > 4 && !goBinary.head.startsWith("7f454c46")) {
    throw new Error(`plain-fork invalid go ELF header: ${detail}`);
  }
  if (compileTool.size > 4 && !compileTool.head.startsWith("7f454c46")) {
    throw new Error(`plain-fork invalid compile ELF header: ${detail}`);
  }
}

function captureParentProcessSnapshot(
  handle: ProcessHandle,
  parentThread: ThreadWorkerState,
): ParentProcessSnapshot {
  const processPtr = parentThread.lastStepProcessPtr ?? handle.kernel.thread_process_ptr(parentThread.vmPtr);
  const processStateBytes = handle.kernel.process_state_bytes();
  const guestBytes = new Uint8Array(handle.memory.buffer);
  return {
    processPtr,
    processState: guestBytes.slice(
      handle.guestBase + processPtr,
      handle.guestBase + processPtr + processStateBytes,
    ),
    pid: handle.pid,
    ppid: handle.parentPid ?? 0,
  };
}

function restoreParentProcessSnapshot(
  handle: ProcessHandle,
  parentThread: ThreadWorkerState,
  snapshot: ParentProcessSnapshot,
): void {
  new Uint8Array(handle.memory.buffer).set(
    snapshot.processState,
    handle.guestBase + snapshot.processPtr,
  );
  handle.kernel.thread_clear_spawn_handoff(parentThread.vmPtr);
  handle.kernel.thread_restore_fork_parent?.(
    parentThread.vmPtr,
    snapshot.processPtr,
    snapshot.pid,
    snapshot.ppid,
  );
  const currentPid = handle.kernel.process_current_pid(parentThread.vmPtr);
  const currentProcessPtr = handle.kernel.thread_process_ptr(parentThread.vmPtr);
  if (currentPid !== snapshot.pid || currentProcessPtr !== snapshot.processPtr) {
    throw new Error(
      `plain-fork parent restore drift vm=0x${parentThread.vmPtr.toString(16)}`
        + ` expected_pid=${snapshot.pid} actual_pid=${currentPid}`
        + ` expected_process_ptr=0x${snapshot.processPtr.toString(16)}`
        + ` actual_process_ptr=0x${currentProcessPtr.toString(16)}`,
    );
  }
}

export async function handlePlainFork(
  deps: WorkerForkDeps,
  handle: ProcessHandle,
  parentWorker: Worker,
  parentThread: ThreadWorkerState,
  childPid: number,
): Promise<void> {
  handle.pendingChildSpawnRecords += 1;
  try {
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `plain-fork:start:${childPid}`);
    deps.appendWorkerDebugLog(
      handle,
      `plain-fork-parent:start:${childPid}:vm=0x${parentThread.vmPtr.toString(16)}:handle_vm=0x${handle.vmPtr.toString(16)}`,
    );
    if (handle.kernelWorkerRequest && handle.pid > 0) {
      await deps.syncProcessMemfsViaKernelWorker(handle);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `plain-fork:parent-memfs-synced:${handle.pid}`);
      await deps.requestPipeStateSync(handle, false);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `plain-fork:parent-pipe-synced:${handle.pid}`);
    }
    const parentProcessSnapshot = captureParentProcessSnapshot(handle, parentThread);
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
    const childMemoryBytes = Math.max(
      handle.memory.buffer.byteLength,
      handle.guestBase + handle.ramSize + FORK_CHILD_EXECVE_HEADROOM_BYTES,
    );
    const childMemory = new WebAssembly.Memory({
      initial: Math.ceil(childMemoryBytes / 65536),
      maximum: Math.max(handle.maxMemoryPages, handle.forkMaxMemoryPages),
      shared: true,
    });
    new Uint8Array(childMemory.buffer).set(parentMem);

    const childInstance = await WebAssembly.instantiate(handle.wasmModule, {
      env: createKernelEnv(childMemory, { sabPageCache: handle.sabPageCache }),
    });
    const childKernel = childInstance.exports as unknown as KernelExports;
    configureKernelInstanceStack(childKernel, MAIN_THREAD_KERNEL_STACK_SLOT);
    // Instantiating a kernel instance over imported child memory replays the
    // module's data segments. In browsers that can clobber copied guest text/data
    // below the probe's argv/envp strings, so restore the whole copied image here
    // and after kernel-worker registration before reattaching shared state.
    copyLinearMemoryImage(handle.memory, childMemory);
    attachKernelRuntimeState(childKernel, handle.kernelState);

    ensureEvictionTimer();
    const childHandle = createForkChildHandle({
      deps,
      parent: handle,
      childPid,
      childPpid,
      vmPtr: parentThread.vmPtr,
      worker: parentWorker,
      memory: childMemory,
      kernel: childKernel,
      kernelState: readKernelRuntimeState(childKernel),
      debugCommand: handle.debugCommand,
      workerDebugLabel: "spawn:plain-fork",
    });
    childHandle.onError = (error: Error) => {
      if (handle.pid <= 0 || deps.processHandles.get(handle.pid) !== handle) {
        return;
      }
      const childLogTail = childHandle.workerDebugLog.slice(-12).join(", ");
      deps.failProcess(handle, new Error(
        `child-debug:${childPid}:${childHandle.lastDebugMessage ?? "last-debug-unavailable"}`
        + ` worker_log=[${childLogTail}]`
        + ` cause=${error.message}`,
      ));
    };
    const childGuestRamLimit = computeGuestRamLimitBytes(
      childHandle.guestBase,
      childHandle.ramSize,
      childHandle.maxMemoryPages,
    );
    detachInheritedHostStdioFromChild(childHandle);
    adoptKernelShare(handle, childHandle);
    const childHeldOfdIds = collectProcessHeldOfds(childHandle);
    const sharedOfdIds = activeKernelShareOfdIds(handle);
    const rehydratedSharedOfdIds = sharedOfdIds.filter((ofdId) => !childHeldOfdIds.has(ofdId));
    const preservedOfdSlots = snapshotKernelShareOfdSlots(childHandle, childHeldOfdIds);
    const preservedPipeSlots = snapshotLocalPipeSlots(childHandle, childHeldOfdIds);
    discardCopiedSharedKernelObjects(childHandle, preservedOfdSlots);
    await deps.registerProcessWithKernelWorker(childHandle, {
      seedMemfsMode: "none",
      pipeSeedMode: "process",
    });
    // Kernel-worker registration also instantiates over the shared child memory,
    // so restore the copied parent image again before rebuilding canonical shared
    // state for the child.
    copyLinearMemoryImage(handle.memory, childMemory);
    attachKernelRuntimeState(childHandle.kernel, childHandle.kernelState);
    await deps.syncProcessMemfsViaKernelWorker(childHandle);
    assertForkChildExecPaths(childHandle.kernel, childPid, "restored");
    childHandle.lastDebugMessage = debugForkChildExecPaths(childHandle.kernel, childPid, "restored");
    deps.appendWorkerDebugLog(childHandle, childHandle.lastDebugMessage);
    deps.syncForkSharedStateLocally(handle, [childHandle], { includeMemfs: false });
    await deps.syncForkSharedStateViaKernelWorker(handle, [childHandle], { includeMemfs: false });
    restoreLocalOfdSlots(childHandle, preservedOfdSlots);
    restoreLocalPipeSlots(childHandle, preservedPipeSlots);
    await deps.rehydrateForkSharedStateViaKernelWorker([childHandle], rehydratedSharedOfdIds, {
      includeMemfs: false,
    });
    assertForkChildExecPaths(childHandle.kernel, childPid, "rehydrated");
    childHandle.lastDebugMessage = debugForkChildExecPaths(childHandle.kernel, childPid, "rehydrated");
    deps.appendWorkerDebugLog(childHandle, childHandle.lastDebugMessage);
    await deps.initializeProcessLifecycleViaKernelWorker(
      childHandle,
      childHandle.vmPtr,
      childPpid,
      lifecycle.nextPid,
      lifecycle.pgid,
      lifecycle.sid,
      false,
    );
    childHandle.kernel.thread_prepare_fork_child?.(childHandle.vmPtr, childPid);
    restoreLocalOfdSlots(childHandle, preservedOfdSlots);
    restoreLocalPipeSlots(childHandle, preservedPipeSlots);
    childHandle.kernel.thread_set_guest_ram_limit(childHandle.vmPtr, childGuestRamLimit);
    assertForkChildExecPaths(childHandle.kernel, childPid, "ready");
    childHandle.lastDebugMessage = debugForkChildExecPaths(childHandle.kernel, childPid, "ready");
    deps.appendWorkerDebugLog(childHandle, childHandle.lastDebugMessage);
    childHandle.kernelState = readKernelRuntimeState(childHandle.kernel);
    deps.assertProcessStandardFdIntegrity(childHandle, "plain-fork-child-ready");
    publishKernelRuntimeState(childHandle.runtimeBridge, childHandle.kernelState);
    deps.processHandles.set(childPid, childHandle);
    restoreParentProcessSnapshot(handle, parentThread, parentProcessSnapshot);
    deps.appendWorkerDebugLog(
      handle,
      `plain-fork-parent:spawn-child:${childPid}:vm=0x${childHandle.vmPtr.toString(16)}`,
    );
    deps.attachThreadWorker(childHandle, {
      worker: parentWorker,
      vmPtr: childHandle.vmPtr,
      transferFrom: handle,
    });
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    deps.kickBlockedWorkers(handle);
    const parentReplacementWorker = acquireWorker();
    deps.attachThreadWorker(handle, {
      worker: parentReplacementWorker,
      vmPtr: parentThread.vmPtr,
    });
    deps.appendWorkerDebugLog(handle, `plain-fork-parent:done:${childPid}`);
  } finally {
    if (handle.pendingChildSpawnRecords > 0) {
      handle.pendingChildSpawnRecords -= 1;
    }
    deps.kickBlockedWorkers(handle);
  }
}
