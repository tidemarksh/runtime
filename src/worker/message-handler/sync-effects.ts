export interface WorkerMessageHandlerSyncEffects {
  statusOkCompletedSyncEffects: number;
  syncEffectPipeState: number;
  syncEffectSocketState: number;
  syncEffectEpollState: number;
  syncEffectEpollDependentOfdState: number;
  syncEffectProcessLifecycle: number;
  syncEffectSignalWaiters: number;
  syncEffectForkSharedOfdState: number;
  syncEffectProcessMemfs: number;
}

export const WORKER_MESSAGE_HANDLER_SYNC_EFFECTS: WorkerMessageHandlerSyncEffects = {
  statusOkCompletedSyncEffects: (1 << 0) | (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4),
  syncEffectPipeState: 1 << 0,
  syncEffectSocketState: 1 << 1,
  syncEffectEpollState: 1 << 2,
  syncEffectEpollDependentOfdState: 1 << 3,
  syncEffectProcessLifecycle: 1 << 4,
  syncEffectSignalWaiters: 1 << 5,
  syncEffectForkSharedOfdState: 1 << 6,
  syncEffectProcessMemfs: 1 << 7,
};

export function resolveWorkerMessageHandlerSyncEffects(
  overrides: Partial<WorkerMessageHandlerSyncEffects>,
): WorkerMessageHandlerSyncEffects {
  return {
    ...WORKER_MESSAGE_HANDLER_SYNC_EFFECTS,
    ...overrides,
  };
}
