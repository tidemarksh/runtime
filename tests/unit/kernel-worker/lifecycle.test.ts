// Kernel-worker tests in this file validate transitional runtime-owned bridge behavior.
import { expect, test } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../src/abi.js";
import { createKernelWorkerLifecycle } from "../../../src/kernel-worker/lifecycle.js";

type FakeKernelExports = KernelExports & {
  opLog: string[];
};

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memfsHeaderBytes: 0,
    memfsInodesBytes: 0,
    memfsFirstChildBytes: 0,
    memfsDirEntriesBytes: 0,
    memfsDataBytes: 0,
    memStatePtr: 0x240000,
    epollTablePtr: 0x250000,
    socketTablePtr: 0x260000,
    socketUsed: 0,
    pipeBase: 0x270000,
    pipeUsed: 0n,
    netBufBase: 0x280000,
    netBufUsed: 0n,
    sigSlotsPtr: 0xc00000,
    processPendingPtr: 0xc10000,
    processRuntimeStatePtr: 0xc20000,
    timeStatePtr: 0xc30000,
  };
}

function makeKernelExports(): FakeKernelExports {
  let runtimeState = makeKernelState();
  const opLog: string[] = [];
  const exports = {
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    kernel_heap_base: () => runtimeState.heapBase,
    kernel_heap_ptr: () => runtimeState.heapPtr,
    kernel_ofd_table_ptr: () => runtimeState.ofdTablePtr,
    kernel_memfs_ptr: () => runtimeState.memfsPtr,
    kernel_memfs_header_bytes: () => runtimeState.memfsHeaderBytes,
    kernel_memfs_inodes_bytes: () => runtimeState.memfsInodesBytes,
    kernel_memfs_first_child_bytes: () => runtimeState.memfsFirstChildBytes,
    kernel_memfs_dir_entries_bytes: () => runtimeState.memfsDirEntriesBytes,
    kernel_memfs_data_bytes: () => runtimeState.memfsDataBytes,
    kernel_mem_state_ptr: () => runtimeState.memStatePtr,
    kernel_epoll_table_ptr: () => runtimeState.epollTablePtr,
    kernel_socket_table_ptr: () => runtimeState.socketTablePtr,
    kernel_socket_used: () => runtimeState.socketUsed,
    kernel_pipe_base: () => runtimeState.pipeBase,
    kernel_pipe_used: () => runtimeState.pipeUsed,
    kernel_net_buf_base: () => runtimeState.netBufBase,
    kernel_net_buf_used: () => runtimeState.netBufUsed,
    kernel_sig_slots_ptr: () => runtimeState.sigSlotsPtr,
    kernel_process_pending_ptr: () => runtimeState.processPendingPtr,
    kernel_process_runtime_state_ptr: () => runtimeState.processRuntimeStatePtr,
    kernel_time_state_ptr: () => runtimeState.timeStatePtr,
    kernel_attach_runtime_state: (
      heapBase: number,
      heapPtr: number,
      ofdTablePtr: number,
      memfsPtr: number,
      memfsHeaderBytes: number,
      memfsInodesBytes: number,
      memfsFirstChildBytes: number,
      memfsDirEntriesBytes: number,
      memfsDataBytes: number,
      memStatePtr: number,
      epollTablePtr: number,
      socketTablePtr: number,
      socketUsed: number,
      pipeBase: number,
      pipeUsed: bigint,
      netBufBase: number,
      netBufUsed: bigint,
      sigSlotsPtr: number,
      processPendingPtr: number,
      processRuntimeStatePtr: number,
      timeStatePtr: number,
    ) => {
      runtimeState = {
        heapBase,
        heapPtr,
        ofdTablePtr,
        memfsPtr,
        memfsHeaderBytes,
        memfsInodesBytes,
        memfsFirstChildBytes,
        memfsDirEntriesBytes,
        memfsDataBytes,
        memStatePtr,
        epollTablePtr,
        socketTablePtr,
        socketUsed,
        pipeBase,
        pipeUsed,
        netBufBase,
        netBufUsed,
        sigSlotsPtr,
        processPendingPtr,
        processRuntimeStatePtr,
        timeStatePtr,
      };
    },
    process_rewrite_child_pid: (_vmPtr: number, oldPid: number, newPid: number, nextPid: number) => {
      opLog.push(`rewrite:${oldPid}->${newPid}@${nextPid}`);
    },
    process_upsert_process_entry: (pid: number, ppid: number, pgid: number, sid: number, didExec: number) => {
      opLog.push(`upsert:${pid}:${ppid}:${pgid}:${sid}:${didExec}`);
    },
    process_remove_process_entry: (pid: number) => {
      opLog.push(`remove:${pid}`);
    },
    process_current_ppid: () => 1,
    process_current_pgid: () => 33,
    process_current_sid: () => 44,
    thread_clear_spawn_handoff: (vmPtr: number) => {
      opLog.push(`clear-spawn:${vmPtr}`);
    },
    thread_clear_vfork_runtime_state: () => {
      opLog.push("clear-vfork");
    },
    thread_restore_vfork_parent_without_cleanup: (vmPtr: number, childPid: number) => {
      opLog.push(`restore-vfork:${vmPtr}:${childPid}`);
    },
    thread_restore_vfork_parent_proc_self_exe_snapshot: () => {
      opLog.push("restore-vfork-proc-self-exe");
    },
    thread_resume_vfork_parent: (vmPtr: number, childPid: number) => {
      opLog.push(`resume-vfork:${vmPtr}:${childPid}`);
    },
    opLog,
  };

  return exports as unknown as FakeKernelExports;
}

test("resumeVforkExecveParent drops canonical child state without clearing the owner-side vfork snapshot", () => {
  const parentKernel = makeKernelExports();
  const siblingKernel = makeKernelExports();
  const processKernels = new Map<number, KernelExports>([
    [1, parentKernel],
    [7, siblingKernel],
  ]);
  const canonicalProcesses = new Map<number, {
    pid: number;
    ppid: number;
    pgid: number;
    sid: number;
    didExec: boolean;
    vmPtr: number;
  }>([
    [1, { pid: 1, ppid: 0, pgid: 1, sid: 1, didExec: false, vmPtr: 0x1234 }],
  ]);
  let canonicalNextPid = 3;

  const lifecycle = createKernelWorkerLifecycle({
    processKernels,
    canonicalProcesses,
    getCanonicalNextPid: () => canonicalNextPid,
    setCanonicalNextPid: (nextPid) => {
      canonicalNextPid = nextPid;
    },
    requireProcessKernel: (pid) => {
      const exports = processKernels.get(pid);
      if (!exports) {
        throw new Error(`missing process kernel: ${pid}`);
      }
      return exports;
    },
    formatProcessDebug: () => "n/a",
  });

  const child = lifecycle.canonicalizeChildPid(1, 0x1234, 2, makeKernelState());
  expect(child.childPid).toBe(3);
  expect(canonicalProcesses.has(3)).toBe(true);

  parentKernel.opLog.length = 0;
  siblingKernel.opLog.length = 0;
  lifecycle.resumeVforkExecveParent(1, 0x1234, child.childPid, makeKernelState());

  expect(canonicalProcesses.has(3)).toBe(false);
  expect(parentKernel.opLog).toContain("clear-spawn:4660");
  expect(parentKernel.opLog).toContain("restore-vfork-proc-self-exe");
  expect(parentKernel.opLog).not.toContain("restore-vfork:4660:3");
  expect(parentKernel.opLog).not.toContain("clear-vfork");
  expect(parentKernel.opLog).toContain("remove:3");
  expect(siblingKernel.opLog).toContain("remove:3");
});
