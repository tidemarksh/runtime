/**
 * Worker message contracts.
 * §6.4: blocking STATUS values stay explicit in the thread-worker channel.
 * Kernel-worker RPCs carry request ids so Runtime can pipeline FS operations safely.
 */

export type {
  GuestMemoryWriteSnapshot,
  GuestWriteWatchRegion,
  KernelMemoryWriteSnapshot,
  KernelWorkerFdEntrySnapshot,
  KernelWorkerNetBufferSnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerPidBytesSnapshot,
  KernelWorkerPidStateSnapshot,
  KernelWorkerPipeSlotSnapshot,
  KernelWorkerSocketStateSnapshot,
  ThreadWorkerMemoryPreserveRange,
} from "./snapshots.js";

export type {
  ThreadWorkerErrorMessage,
  ThreadWorkerInbound,
  ThreadWorkerOutbound,
  ThreadWorkerPreparedMessage,
  ThreadWorkerReadyMessage,
  ThreadWorkerStatusMessage,
} from "./thread-worker.js";

export type {
  KernelWorkerFileLayerEntry,
  KernelWorkerInbound,
  KernelWorkerOutbound,
  KernelWorkerProcessFsProgress,
  KernelWorkerRequest,
  KernelWorkerRequestBase,
  KernelWorkerResponse,
} from "./kernel-worker.js";
