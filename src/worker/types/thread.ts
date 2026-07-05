export interface ThreadWorkerState {
  vmPtr: number;
  ownerPid: number;
  generation: number;
  runningGeneration?: number | null;
  reinitCount: number;
  initialized: boolean;
}
