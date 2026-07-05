export interface ThreadWorkerState {
  vmPtr: number;
  ownerPid: number;
  generation: number;
  runningGeneration?: number | null;
  reinitCount: number;
  stepCount: number;
  totalStepBudget: number;
  totalStepWallMs: number;
  maxStepWallMs: number;
  lastStepDurationMs: number | null;
  statusCount: number;
  statusCounts: Record<string, number>;
  lastStatus: number | null;
  recentStatuses?: ThreadWorkerStatusSnapshot[];
  lastStepAtMs: number | null;
  lastStatusAtMs: number | null;
  lastStepProcessPtr: number | null;
  lastStepPc: bigint | null;
  lastStepSp: bigint | null;
  lastStepS11: bigint | null;
  hotStepPcs: Record<string, number>;
  recentSteps?: ThreadWorkerStepSnapshot[];
  lastStatusPc: bigint | null;
  lastStatusSp: bigint | null;
  lastStatusS11: bigint | null;
  execveProcessState?: Uint8Array;
  hotStatusPcs: Record<string, number>;
  initialized: boolean;
}

export interface ThreadWorkerStepSnapshot {
  pc: bigint;
  sp: bigint;
  s11: bigint;
  atMs: number;
}

export interface ThreadWorkerStatusSnapshot {
  status: number;
  pc: bigint;
  syscallNr: bigint | null;
  fdTraceSyscallNr: bigint | null;
  fdTraceResult: bigint | null;
  futexWaitAddr: bigint | null;
  futexBlockedAddr: bigint | null;
  atMs: number;
}
