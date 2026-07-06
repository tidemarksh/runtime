import type { KernelRuntimeState } from "../../abi.js";

export interface GuestWriteWatchRegion {
  slot: number;
  start: number;
  end: number;
}

export interface KernelWorkerOfdSlotSnapshot {
  ofdId: number;
  slot: Uint8Array;
}

export interface KernelWorkerPipeSlotSnapshot {
  pipeSlot: number;
  slot: Uint8Array;
  mode?: number;
  fresh?: boolean;
}

export interface KernelWorkerNetBufferSnapshot {
  bufferIndex: number;
  bytes: Uint8Array;
}

export interface KernelWorkerSocketStateSnapshot {
  socketTable: Uint8Array;
  netBufPool?: Uint8Array;
  netBufSlots?: KernelWorkerNetBufferSnapshot[];
  socketUsed: number;
  netBufUsed: bigint;
}

export interface KernelWorkerFdEntrySnapshot {
  fd: number;
  ofdId: number;
  fdFlags: number;
  generation?: number;
}

export interface KernelWorkerPidStateSnapshot {
  pid: number;
  vmPtr: number;
  kernelState: KernelRuntimeState;
  fdEntries?: KernelWorkerFdEntrySnapshot[];
}

export interface KernelWorkerPidBytesSnapshot {
  pid: number;
  bytes: Uint8Array;
}

export interface GuestMemoryWriteSnapshot {
  guestPtr: number;
  bytes: Uint8Array;
}

export interface KernelMemoryWriteSnapshot {
  ptr: number;
  bytes: Uint8Array;
}

export interface ThreadWorkerMemoryPreserveRange {
  start: number;
  end: number;
}
