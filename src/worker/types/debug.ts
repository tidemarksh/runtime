export interface WorkerMessageDebugStats {
  received: number;
  statusReceived: number;
  controlReceived: number;
  rerouted: number;
  inactiveHandleDrops: number;
  exitInProgressDrops: number;
  ownershipInvariantDrops: number;
  staleGenerationDrops: number;
  staleProcessStatusDrops: number;
  readyRehydrateAdopts: number;
  statusHandlerNoThreadDrops: number;
  ownerAsyncStatusReturns: number;
  statusHandled: number;
  statusClearedRunning: number;
  statusNotRunning: number;
  lastEvents: string[];
}

export function createWorkerMessageDebugStats(): WorkerMessageDebugStats {
  return {
    received: 0,
    statusReceived: 0,
    controlReceived: 0,
    rerouted: 0,
    inactiveHandleDrops: 0,
    exitInProgressDrops: 0,
    ownershipInvariantDrops: 0,
    staleGenerationDrops: 0,
    staleProcessStatusDrops: 0,
    readyRehydrateAdopts: 0,
    statusHandlerNoThreadDrops: 0,
    ownerAsyncStatusReturns: 0,
    statusHandled: 0,
    statusClearedRunning: 0,
    statusNotRunning: 0,
    lastEvents: [],
  };
}

export function recordWorkerMessageDebugEvent(
  stats: WorkerMessageDebugStats,
  key: Exclude<keyof WorkerMessageDebugStats, "lastEvents">,
  event?: string,
): void {
  stats[key] += 1;
  if (event === undefined) {
    return;
  }
  stats.lastEvents.push(event);
  if (stats.lastEvents.length > 32) {
    stats.lastEvents.splice(0, stats.lastEvents.length - 32);
  }
}

export interface TransferDebugStats {
  fsResumeCalls: number;
  fsKernelWorkerCalls: number;
  fsLocalCalls: number;
  fsReadCalls: number;
  fsReadBytes: number;
  fsWriteCalls: number;
  fsWriteBytes: number;
  packagePathWriteCalls: number;
  packagePathWriteBytes: number;
  memfsMutationOps: number;
  memfsFileDataOps: number;
  pageCachePublishCandidates: number;
  pageCachePublishCalls: number;
  forkSharedMemfsSyncCalls: number;
  networkPumpCalls: number;
  networkPumpChangedCalls: number;
  networkPumpElapsedMs: number;
  networkBridgeReadCalls: number;
  networkBridgeReadBytes: number;
  networkBridgeReadQueued: number;
  networkInboundInjectCalls: number;
  networkInboundInjectedBytes: number;
  networkInboundBlockedCalls: number;
  networkPendingInboundBytesMax: number;
  networkOutboundDrainCalls: number;
  networkOutboundDrainedBytes: number;
  networkGuestSocketReadCalls: number;
  networkGuestSocketReadBytes: number;
  pipeSyncRequests: number;
  pipeSyncKernelCalls: number;
  pipeSyncVisibleDrainCalls: number;
  socketSyncRequests: number;
  socketSyncKernelCalls: number;
}

export function createTransferDebugStats(): TransferDebugStats {
  return {
    fsResumeCalls: 0,
    fsKernelWorkerCalls: 0,
    fsLocalCalls: 0,
    fsReadCalls: 0,
    fsReadBytes: 0,
    fsWriteCalls: 0,
    fsWriteBytes: 0,
    packagePathWriteCalls: 0,
    packagePathWriteBytes: 0,
    memfsMutationOps: 0,
    memfsFileDataOps: 0,
    pageCachePublishCandidates: 0,
    pageCachePublishCalls: 0,
    forkSharedMemfsSyncCalls: 0,
    networkPumpCalls: 0,
    networkPumpChangedCalls: 0,
    networkPumpElapsedMs: 0,
    networkBridgeReadCalls: 0,
    networkBridgeReadBytes: 0,
    networkBridgeReadQueued: 0,
    networkInboundInjectCalls: 0,
    networkInboundInjectedBytes: 0,
    networkInboundBlockedCalls: 0,
    networkPendingInboundBytesMax: 0,
    networkOutboundDrainCalls: 0,
    networkOutboundDrainedBytes: 0,
    networkGuestSocketReadCalls: 0,
    networkGuestSocketReadBytes: 0,
    pipeSyncRequests: 0,
    pipeSyncKernelCalls: 0,
    pipeSyncVisibleDrainCalls: 0,
    socketSyncRequests: 0,
    socketSyncKernelCalls: 0,
  };
}
