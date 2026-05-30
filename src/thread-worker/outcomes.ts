export interface ThreadWorkerStatusOutcome {
  type: "status";
  status: number;
  waitInWorker?: boolean;
  syncEffects?: number;
}

export type ThreadWorkerBoundaryOutcome = ThreadWorkerStatusOutcome;
export type ThreadWorkerStatusPublisher = (
  status: number,
  waitInWorker?: boolean,
  syncEffects?: number,
) => void;

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
  publish: ThreadWorkerStatusPublisher,
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
