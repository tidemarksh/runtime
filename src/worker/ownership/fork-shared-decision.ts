import type { ProcessHandle } from "../types.js";

type KernelHandle = Pick<ProcessHandle, "kernel">;

function boolFact(value: boolean): number {
  return value ? 1 : 0;
}

function requireForkSharedConst(
  handle: KernelHandle,
  exportName:
    | "supervisor_fork_shared_sync_target_action_reject"
    | "supervisor_fork_shared_sync_target_action_defer"
    | "supervisor_fork_shared_sync_target_action_use"
    | "supervisor_fork_shared_ofd_action_include"
    | "supervisor_fork_shared_ofd_action_add_shared"
    | "supervisor_fork_shared_ofd_action_rehydrate",
): number {
  const read = handle.kernel[exportName];
  if (!read) {
    throw new Error(`kernel supervisor ABI missing ${exportName}`);
  }
  return read.call(handle.kernel);
}

export function forkSharedSyncTargetActionUse(handle: KernelHandle): number {
  return requireForkSharedConst(handle, "supervisor_fork_shared_sync_target_action_use");
}

export function forkSharedSyncTargetActionReject(handle: KernelHandle): number {
  return requireForkSharedConst(handle, "supervisor_fork_shared_sync_target_action_reject");
}

export function forkSharedOfdActionInclude(handle: KernelHandle): number {
  return requireForkSharedConst(handle, "supervisor_fork_shared_ofd_action_include");
}

export function forkSharedOfdActionAddShared(handle: KernelHandle): number {
  return requireForkSharedConst(handle, "supervisor_fork_shared_ofd_action_add_shared");
}

export function forkSharedOfdActionRehydrate(handle: KernelHandle): number {
  return requireForkSharedConst(handle, "supervisor_fork_shared_ofd_action_rehydrate");
}

export function forkSharedActionIncludes(handle: KernelHandle, action: number): boolean {
  return (action & forkSharedOfdActionInclude(handle)) !== 0;
}

export function forkSharedActionAddsShared(handle: KernelHandle, action: number): boolean {
  return (action & forkSharedOfdActionAddShared(handle)) !== 0;
}

export function forkSharedActionRehydrates(handle: KernelHandle, action: number): boolean {
  return (action & forkSharedOfdActionRehydrate(handle)) !== 0;
}

export function forkSharedSyncTargetAction(
  handle: KernelHandle,
  facts: {
    invalidPid: boolean;
    runningWorker: boolean;
    vforkReadyWorker: boolean;
    hasHostBlockedWorker: boolean;
    rpcPending: boolean;
    processExit: boolean;
    registeredProcessExit: boolean;
    registeredMismatch: boolean;
    incompatibleOfd: boolean;
    requireHostBlocked: boolean;
    includeMemfs: boolean;
    runtimeNetworkState: boolean;
  },
): number {
  const decide = handle.kernel.supervisor_fork_shared_sync_target_action;
  if (!decide) {
    throw new Error("kernel supervisor ABI missing supervisor_fork_shared_sync_target_action");
  }
  return decide.call(
    handle.kernel,
    boolFact(facts.invalidPid),
    boolFact(facts.runningWorker),
    boolFact(facts.vforkReadyWorker),
    boolFact(facts.hasHostBlockedWorker),
    boolFact(facts.rpcPending),
    boolFact(facts.processExit),
    boolFact(facts.registeredProcessExit),
    boolFact(facts.registeredMismatch),
    boolFact(facts.incompatibleOfd),
    boolFact(facts.requireHostBlocked),
    boolFact(facts.includeMemfs),
    boolFact(facts.runtimeNetworkState),
  );
}

export function forkSharedSyncableOfdAction(
  handle: KernelHandle,
  facts: {
    syncable: boolean;
    sourceConflicting: boolean;
    allowed: boolean;
    regularFile: boolean;
    sourcePublished: boolean;
    runtimeNetworkSocket: boolean;
  },
): number {
  const decide = handle.kernel.supervisor_fork_shared_syncable_ofd_action;
  if (!decide) {
    throw new Error("kernel supervisor ABI missing supervisor_fork_shared_syncable_ofd_action");
  }
  return decide.call(
    handle.kernel,
    boolFact(facts.syncable),
    boolFact(facts.sourceConflicting),
    boolFact(facts.allowed),
    boolFact(facts.regularFile),
    boolFact(facts.sourcePublished),
    boolFact(facts.runtimeNetworkSocket),
  );
}

export function forkSharedDeltaSourceOfdAction(
  handle: KernelHandle,
  facts: {
    allowed: boolean;
    sourcePublished: boolean;
    sourceHeld: boolean;
    pipePeer: boolean;
  },
): number {
  const decide = handle.kernel.supervisor_fork_shared_delta_source_ofd_action;
  if (!decide) {
    throw new Error("kernel supervisor ABI missing supervisor_fork_shared_delta_source_ofd_action");
  }
  return decide.call(
    handle.kernel,
    boolFact(facts.allowed),
    boolFact(facts.sourcePublished),
    boolFact(facts.sourceHeld),
    boolFact(facts.pipePeer),
  );
}

export function forkSharedTrackedSourceOfdAction(
  handle: KernelHandle,
  facts: {
    kernelShared: boolean;
    sourceFdEntry: boolean;
    syncable: boolean;
    typeNone: boolean;
    incompatiblePeer: boolean;
  },
): number {
  const decide = handle.kernel.supervisor_fork_shared_tracked_source_ofd_action;
  if (!decide) {
    throw new Error("kernel supervisor ABI missing supervisor_fork_shared_tracked_source_ofd_action");
  }
  return decide.call(
    handle.kernel,
    boolFact(facts.kernelShared),
    boolFact(facts.sourceFdEntry),
    boolFact(facts.syncable),
    boolFact(facts.typeNone),
    boolFact(facts.incompatiblePeer),
  );
}

export function forkSharedPipePeerOfdAction(
  handle: KernelHandle,
  facts: { stale: boolean; syncable: boolean },
): number {
  const decide = handle.kernel.supervisor_fork_shared_pipe_peer_ofd_action;
  if (!decide) {
    throw new Error("kernel supervisor ABI missing supervisor_fork_shared_pipe_peer_ofd_action");
  }
  return decide.call(handle.kernel, boolFact(facts.stale), boolFact(facts.syncable));
}

export function forkSharedRehydrateOfdAction(
  handle: KernelHandle,
  facts: { active: boolean; sourceHeld: boolean; allowed: boolean },
): number {
  const decide = handle.kernel.supervisor_fork_shared_rehydrate_ofd_action;
  if (!decide) {
    throw new Error("kernel supervisor ABI missing supervisor_fork_shared_rehydrate_ofd_action");
  }
  return decide.call(
    handle.kernel,
    boolFact(facts.active),
    boolFact(facts.sourceHeld),
    boolFact(facts.allowed),
  );
}
