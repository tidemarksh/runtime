import type { KernelRuntimeState } from "../../abi.js";
import type { WasmFeatureSupport } from "../../wasm/features.js";
import type {
  GuestMemoryWriteSnapshot,
  GuestWriteWatchRegion,
  KernelMemoryWriteSnapshot,
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPipeSlotSnapshot,
  KernelWorkerSocketStateSnapshot,
  ThreadWorkerMemoryPreserveRange,
} from "./snapshots.js";

export interface ThreadWorkerProcessIdentity {
  processPtr: number;
  pid: number;
  ppid: number;
  nextPid: number;
}

export interface ThreadWorkerExitedThreadPublication {
  vmPtr: number;
  tid: number;
}

export type ThreadWorkerInbound =
  | {
      type: "prepare";
      wasmModule: WebAssembly.Module;
      memory: WebAssembly.Memory;
      preserveBytes?: number;
      preserveMemoryRanges?: ThreadWorkerMemoryPreserveRange[];
      kernelStackSlot: number;
      sabPageCache: SharedArrayBuffer | null;
      runtimeNotify: SharedArrayBuffer;
      runtimeState: SharedArrayBuffer;
    }
  | {
      type: "init";
      wasmModule: WebAssembly.Module;
      memory: WebAssembly.Memory;
      forcePrepare?: boolean;
      vmPtr: number;
      guestBase: number;
      kernelStackSlot: number;
      generation: number;
      runtimeEpoch?: number;
      preserveMemoryBytes?: number;
      preserveMemoryRanges?: ThreadWorkerMemoryPreserveRange[];
      guestWriteWatchRegions?: GuestWriteWatchRegion[];
      processIdentity?: ThreadWorkerProcessIdentity;
      exitedThreads?: ThreadWorkerExitedThreadPublication[];
      kernelState: KernelRuntimeState;
      pipeSlots?: KernelWorkerPipeSlotSnapshot[];
      socketState?: KernelWorkerSocketStateSnapshot;
      fdEntries?: KernelWorkerFdEntrySnapshot[];
      ofdSlots?: KernelWorkerOfdSlotSnapshot[];
      guestMemoryWrites?: GuestMemoryWriteSnapshot[];
      sabPageCache: SharedArrayBuffer | null;
      runtimeNotify: SharedArrayBuffer;
      runtimeState: SharedArrayBuffer;
    }
  | {
      type: "step";
      stepSequence: number;
      budget: number;
      processIdentity?: ThreadWorkerProcessIdentity;
      exitedThreads?: ThreadWorkerExitedThreadPublication[];
      kernelState: KernelRuntimeState;
      pipeSlots?: KernelWorkerPipeSlotSnapshot[];
      socketState?: KernelWorkerSocketStateSnapshot;
      fdEntries?: KernelWorkerFdEntrySnapshot[];
      ofdSlots?: KernelWorkerOfdSlotSnapshot[];
      guestMemoryWrites?: GuestMemoryWriteSnapshot[];
      kernelMemoryWrites?: KernelMemoryWriteSnapshot[];
      resumeBlockedSyscall?: boolean;
      resumeBlockedSyscallTimeoutExpired?: boolean;
    };

export interface ThreadWorkerPreparedMessage {
  type: "prepared";
  wasmFeatures: WasmFeatureSupport;
}

export interface ThreadWorkerReadyMessage {
  type: "ready";
  generation: number;
  vmPtr: number;
  kernelState: KernelRuntimeState;
  wasmFeatures: WasmFeatureSupport;
  debug?: string;
}

export interface ThreadWorkerStatusMessage {
  type: "status";
  generation: number;
  stepSequence?: number;
  status: number;
  exitCode: number;
  termSignal: number;
  pc: bigint;
  ra?: bigint;
  sp?: bigint;
  s11?: bigint;
  syscallNr?: bigint;
  blockedFd?: number;
  blockedHasOfdPin?: boolean;
  blockedSyscallNr?: bigint;
  blockedSyscallArgs?: [bigint, bigint, bigint, bigint, bigint, bigint];
  futexWaitAddr?: bigint;
  futexBlockedAddr?: bigint;
  vmPtr: number;
  processPtr?: number;
  processPid?: number;
  kernelState: KernelRuntimeState;
  fdEntries?: KernelWorkerFdEntrySnapshot[];
  ofdSlots?: KernelWorkerOfdSlotSnapshot[];
  processState?: Uint8Array;
  pipeSlots?: KernelWorkerPipeSlotSnapshot[];
  socketState?: KernelWorkerSocketStateSnapshot;
  debug?: string;
  fdTraceDebug?: string;
  fdTraceSyscallNr?: bigint;
  fdTraceArg0OfdId?: number;
  fdTraceArg0OfdType?: number;
  fdTraceArg0OfdOffset?: bigint;
  fdTraceArg1?: bigint;
  fdTraceArg2OfdId?: number;
  fdTraceArg2OfdType?: number;
  fdTraceResult?: bigint;
  guestMemoryWrites?: GuestMemoryWriteSnapshot[];
  kernelMemoryWrites?: KernelMemoryWriteSnapshot[];
  syncEffects?: number;
  waitInWorker?: boolean;
}

export interface ThreadWorkerErrorMessage {
  type: "error";
  generation: number;
  vmPtr: number;
  message: string;
}

export type ThreadWorkerOutbound =
  | ThreadWorkerPreparedMessage
  | ThreadWorkerReadyMessage
  | ThreadWorkerStatusMessage
  | ThreadWorkerErrorMessage
  | { type: "exit"; code: number; vmPtr: number; generation: number };
