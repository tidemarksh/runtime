export interface ThreadWorkerStatusOutcome {
  type: "status";
  status: number;
  waitInWorker?: boolean;
  syncEffects?: number;
}

export type ThreadWorkerBoundaryOutcome = ThreadWorkerStatusOutcome;
export type ThreadWorkerStatusEmitter = (
  status: number,
  waitInWorker?: boolean,
  syncEffects?: number,
) => void;
export type ThreadWorkerOutcomePublisher = (outcome: ThreadWorkerBoundaryOutcome) => void;

export function createThreadWorkerStatusOutcome(
  status: number,
  waitInWorker = false,
  syncEffects?: number,
): ThreadWorkerStatusOutcome {
  return {
    type: "status",
    status,
    waitInWorker: waitInWorker ? true : undefined,
    syncEffects,
  };
}

export function publishThreadWorkerStatusOutcome(
  publish: ThreadWorkerStatusEmitter,
  outcome: ThreadWorkerStatusOutcome,
): void {
  if (outcome.syncEffects !== undefined) {
    publish(outcome.status, outcome.waitInWorker === true, outcome.syncEffects);
    return;
  }
  if (outcome.waitInWorker === true) {
    publish(outcome.status, true);
    return;
  }
  publish(outcome.status);
}
