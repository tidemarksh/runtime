import { attachKernelRuntimeState, readKernelRuntimeState, type KernelExports, type KernelRuntimeState } from "../abi.js";

interface CanonicalProcessEntry {
  pid: number;
  ppid: number;
  pgid: number;
  sid: number;
  didExec: boolean;
  vmPtr: number;
}

interface KernelWorkerLifecycleDeps {
  processKernels: Map<number, KernelExports>;
  canonicalProcesses: Map<number, CanonicalProcessEntry>;
  getCanonicalNextPid(): number;
  setCanonicalNextPid(nextPid: number): void;
  requireProcessKernel(pid: number): KernelExports;
  formatProcessDebug(processExports: KernelExports, vmPtr: number): string;
}

export function createKernelWorkerLifecycle(deps: KernelWorkerLifecycleDeps) {
  function reserveCanonicalPid(provisionalChildPid: number): number {
    const childPid = Math.max(deps.getCanonicalNextPid(), provisionalChildPid);
    deps.setCanonicalNextPid(childPid + 1);
    return childPid;
  }

  function upsertCanonicalProcess(meta: {
    pid: number;
    ppid: number;
    pgid: number;
    sid: number;
    didExec: boolean;
    vmPtr?: number;
  }): void {
    const previous = deps.canonicalProcesses.get(meta.pid);
    deps.canonicalProcesses.set(meta.pid, {
      ...meta,
      vmPtr: meta.vmPtr ?? previous?.vmPtr ?? 0,
    });
    for (const processExports of deps.processKernels.values()) {
      processExports.process_upsert_process_entry(
        meta.pid,
        meta.ppid,
        meta.pgid,
        meta.sid,
        meta.didExec ? 1 : 0,
      );
    }
  }

  function dropCanonicalProcess(pid: number): void {
    deps.canonicalProcesses.delete(pid);
    for (const processExports of deps.processKernels.values()) {
      processExports.process_remove_process_entry(pid);
    }
  }

  function recordChildExit(
    pid: number,
    childPid: number,
    waitStatus: number,
    kernelState: KernelRuntimeState,
  ): KernelRuntimeState {
    const processExports = deps.requireProcessKernel(pid);
    attachKernelRuntimeState(processExports, kernelState);
    processExports.process_record_child_exit(childPid, waitStatus);
    return readKernelRuntimeState(processExports);
  }

  function resumeConnect(
    pid: number,
    vmPtr: number,
    result: bigint,
    kernelState: KernelRuntimeState,
  ): KernelRuntimeState {
    const processExports = deps.requireProcessKernel(pid);
    attachKernelRuntimeState(processExports, kernelState);
    processExports.thread_resume_connect_result(vmPtr, result);
    return readKernelRuntimeState(processExports);
  }

  function resumeWait4(
    pid: number,
    vmPtr: number,
    kernelState: KernelRuntimeState,
  ): KernelRuntimeState {
    const processExports = deps.requireProcessKernel(pid);
    attachKernelRuntimeState(processExports, kernelState);
    processExports.thread_resume_wait4(vmPtr);
    return readKernelRuntimeState(processExports);
  }

  function resumeBlockedSyscall(
    pid: number,
    vmPtr: number,
    kernelState: KernelRuntimeState,
  ): KernelRuntimeState {
    const processExports = deps.requireProcessKernel(pid);
    attachKernelRuntimeState(processExports, kernelState);
    try {
      processExports.thread_resume_blocked_syscall(vmPtr);
    } catch (error) {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${deps.formatProcessDebug(processExports, vmPtr)}`);
    }
    return readKernelRuntimeState(processExports);
  }

  function resumeVforkParent(
    pid: number,
    vmPtr: number,
    waitStatus: number,
    kernelState: KernelRuntimeState,
  ): { childPid: number; kernelState: KernelRuntimeState } {
    const processExports = deps.requireProcessKernel(pid);
    attachKernelRuntimeState(processExports, kernelState);
    const childPid = processExports.thread_vfork_child_pid();
    if (childPid > 0) {
      processExports.thread_resume_vfork_parent(vmPtr, childPid);
      if (processExports.process_record_child_exit_for_parent) {
        processExports.process_record_child_exit_for_parent(pid, childPid, waitStatus);
      } else {
        processExports.process_record_child_exit(childPid, waitStatus);
      }
    }
    return {
      childPid,
      kernelState: readKernelRuntimeState(processExports),
    };
  }

  function canonicalizeChildPid(
    pid: number,
    vmPtr: number,
    provisionalChildPid: number,
    kernelState: KernelRuntimeState,
  ): { childPid: number; nextPid: number; pgid: number; sid: number; kernelState: KernelRuntimeState } {
    const processExports = deps.requireProcessKernel(pid);
    attachKernelRuntimeState(processExports, kernelState);
    const childPid = reserveCanonicalPid(provisionalChildPid);
    const nextPid = deps.getCanonicalNextPid();
    processExports.process_rewrite_child_pid(vmPtr, provisionalChildPid, childPid, nextPid);
    upsertCanonicalProcess({
      pid: childPid,
      ppid: processExports.process_current_ppid(vmPtr),
      pgid: processExports.process_current_pgid(vmPtr),
      sid: processExports.process_current_sid(vmPtr),
      didExec: false,
      vmPtr,
    });
    return {
      childPid,
      nextPid,
      pgid: processExports.process_current_pgid(vmPtr),
      sid: processExports.process_current_sid(vmPtr),
      kernelState: readKernelRuntimeState(processExports),
    };
  }

  function initializeProcessLifecycle(
    pid: number,
    vmPtr: number,
    ppid: number,
    nextPid: number,
    pgid: number,
    sid: number,
    didExec: boolean,
    kernelState: KernelRuntimeState,
  ): KernelRuntimeState {
    const processExports = deps.requireProcessKernel(pid);
    attachKernelRuntimeState(processExports, kernelState);
    deps.setCanonicalNextPid(Math.max(deps.getCanonicalNextPid(), nextPid, pid + 1));
    processExports.process_reset_lifecycle(
      vmPtr,
      pid,
      ppid,
      deps.getCanonicalNextPid(),
      pgid,
      sid,
      didExec ? 1 : 0,
    );
    upsertCanonicalProcess({
      pid,
      ppid,
      pgid,
      sid,
      didExec,
      vmPtr,
    });
    return readKernelRuntimeState(processExports);
  }

  function resumeVforkExecveParent(
    pid: number,
    vmPtr: number,
    childPid: number,
    kernelState: KernelRuntimeState,
  ): KernelRuntimeState {
    const processExports = deps.requireProcessKernel(pid);
    attachKernelRuntimeState(processExports, kernelState);
    processExports.thread_clear_spawn_handoff(vmPtr);
    // The runtime resumes the suspended parent thread locally after the child
    // worker has been materialized. This kernel-worker copy must not clear the
    // vfork snapshot before the owner runtime restores the parent locally,
    // otherwise thread_restore_vfork_parent_without_cleanup() becomes a no-op.
    if (childPid > 0 && !deps.processKernels.has(childPid)) {
      dropCanonicalProcess(childPid);
    }
    return readKernelRuntimeState(processExports);
  }

  function syncProcessLifecycle(
    pid: number,
    vmPtr: number,
    kernelState: KernelRuntimeState,
  ): KernelRuntimeState {
    const processExports = deps.requireProcessKernel(pid);
    attachKernelRuntimeState(processExports, kernelState);
    upsertCanonicalProcess({
      pid,
      ppid: processExports.process_current_ppid(vmPtr),
      pgid: processExports.process_current_pgid(vmPtr),
      sid: processExports.process_current_sid(vmPtr),
      didExec: processExports.process_current_did_exec(vmPtr) !== 0,
      vmPtr,
    });
    return readKernelRuntimeState(processExports);
  }

  return {
    canonicalizeChildPid,
    initializeProcessLifecycle,
    recordChildExit,
    resumeBlockedSyscall,
    resumeConnect,
    resumeVforkExecveParent,
    resumeVforkParent,
    resumeWait4,
    syncProcessLifecycle,
  };
}
