import type { KernelExports } from "./abi.js";

export interface KernelSupervisorHandle {
  kernel: KernelExports;
}

type PipeSnapshotConstExport =
  | "supervisor_pipe_slot_snapshot_context_shared_state"
  | "supervisor_pipe_slot_snapshot_context_thread_step"
  | "supervisor_pipe_slot_snapshot_context_kernel_worker_apply"
  | "supervisor_pipe_slot_snapshot_context_kernel_worker_process"
  | "supervisor_pipe_slot_snapshot_context_kernel_worker_canonical"
  | "supervisor_pipe_slot_snapshot_action_skip"
  | "supervisor_pipe_slot_snapshot_action_apply_slot"
  | "supervisor_pipe_slot_snapshot_action_apply_control"
  | "supervisor_pipe_slot_snapshot_action_set_head_to_source"
  | "supervisor_pipe_slot_snapshot_action_set_tail_to_source"
  | "supervisor_pipe_slot_snapshot_action_set_head_zero"
  | "supervisor_pipe_slot_snapshot_action_copy_full"
  | "supervisor_pipe_slot_snapshot_action_copy_tail_delta"
  | "supervisor_pipe_slot_snapshot_action_sync_generation"
  | "supervisor_pipe_slot_snapshot_action_stop";

export interface PipeSlotSnapshotActionSet {
  sharedStateContext: number;
  threadStepContext: number;
  kernelWorkerApplyContext: number;
  kernelWorkerProcessContext: number;
  kernelWorkerCanonicalContext: number;
  skip: number;
  applySlot: number;
  applyControl: number;
  setHeadToSource: number;
  setTailToSource: number;
  setHeadZero: number;
  copyFull: number;
  copyTailDelta: number;
  syncGeneration: number;
  stop: number;
}

export interface PipeSlotSnapshotDecisionFacts {
  context: number;
  mode: number;
  fresh: boolean;
  sourceHead: number;
  sourceTail: number;
  sourceReadOpen: number;
  sourceWriteOpen: number;
  sourceGeneration: number;
  targetHead: number;
  targetTail: number;
  targetReadOpen: number;
  targetWriteOpen: number;
  targetGeneration: number;
}

function boolFact(value: boolean): number {
  return value ? 1 : 0;
}

function requirePipeSnapshotConst(
  kernel: KernelExports,
  exportName: PipeSnapshotConstExport,
): number {
  const read = kernel[exportName];
  if (!read) {
    throw new Error(`kernel supervisor ABI missing ${exportName}`);
  }
  return read.call(kernel);
}

export function pipeSlotSnapshotActionSetForKernel(kernel: KernelExports): PipeSlotSnapshotActionSet {
  return {
    sharedStateContext: requirePipeSnapshotConst(kernel, "supervisor_pipe_slot_snapshot_context_shared_state"),
    threadStepContext: requirePipeSnapshotConst(kernel, "supervisor_pipe_slot_snapshot_context_thread_step"),
    kernelWorkerApplyContext: requirePipeSnapshotConst(
      kernel,
      "supervisor_pipe_slot_snapshot_context_kernel_worker_apply",
    ),
    kernelWorkerProcessContext: requirePipeSnapshotConst(
      kernel,
      "supervisor_pipe_slot_snapshot_context_kernel_worker_process",
    ),
    kernelWorkerCanonicalContext: requirePipeSnapshotConst(
      kernel,
      "supervisor_pipe_slot_snapshot_context_kernel_worker_canonical",
    ),
    skip: requirePipeSnapshotConst(kernel, "supervisor_pipe_slot_snapshot_action_skip"),
    applySlot: requirePipeSnapshotConst(kernel, "supervisor_pipe_slot_snapshot_action_apply_slot"),
    applyControl: requirePipeSnapshotConst(kernel, "supervisor_pipe_slot_snapshot_action_apply_control"),
    setHeadToSource: requirePipeSnapshotConst(kernel, "supervisor_pipe_slot_snapshot_action_set_head_to_source"),
    setTailToSource: requirePipeSnapshotConst(kernel, "supervisor_pipe_slot_snapshot_action_set_tail_to_source"),
    setHeadZero: requirePipeSnapshotConst(kernel, "supervisor_pipe_slot_snapshot_action_set_head_zero"),
    copyFull: requirePipeSnapshotConst(kernel, "supervisor_pipe_slot_snapshot_action_copy_full"),
    copyTailDelta: requirePipeSnapshotConst(kernel, "supervisor_pipe_slot_snapshot_action_copy_tail_delta"),
    syncGeneration: requirePipeSnapshotConst(kernel, "supervisor_pipe_slot_snapshot_action_sync_generation"),
    stop: requirePipeSnapshotConst(kernel, "supervisor_pipe_slot_snapshot_action_stop"),
  };
}

export function pipeSlotSnapshotActionSet(handle: KernelSupervisorHandle): PipeSlotSnapshotActionSet {
  return pipeSlotSnapshotActionSetForKernel(handle.kernel);
}

export function pipeSlotSnapshotMergeActionForKernel(
  kernel: KernelExports,
  facts: PipeSlotSnapshotDecisionFacts,
): number {
  const decide = kernel.supervisor_pipe_slot_snapshot_merge_action;
  if (!decide) {
    throw new Error("kernel supervisor ABI missing supervisor_pipe_slot_snapshot_merge_action");
  }
  return decide.call(
    kernel,
    facts.context,
    facts.mode,
    boolFact(facts.fresh),
    facts.sourceHead,
    facts.sourceTail,
    facts.sourceReadOpen,
    facts.sourceWriteOpen,
    facts.sourceGeneration,
    facts.targetHead,
    facts.targetTail,
    facts.targetReadOpen,
    facts.targetWriteOpen,
    facts.targetGeneration,
  );
}

export function pipeSlotSnapshotMergeAction(
  handle: KernelSupervisorHandle,
  facts: PipeSlotSnapshotDecisionFacts,
): number {
  return pipeSlotSnapshotMergeActionForKernel(handle.kernel, facts);
}

export function pipeSlotSnapshotActionHas(action: number, flag: number): boolean {
  return (action & flag) !== 0;
}
