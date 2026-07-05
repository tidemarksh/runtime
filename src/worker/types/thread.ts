export interface ThreadWorkerState {
  vmPtr: number;
  ownerPid: number;
  generation: number;
  reinitCount: number;
  initialized: boolean;
}
