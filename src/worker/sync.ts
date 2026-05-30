export { adoptKernelState, adoptKernelStateForHandle } from "./sync/kernel-state.js";
export type { SyncCause } from "./sync/effects.js";
export { snapshotProcessFdOfdStateForKernelWorker, syncEpollStateViaKernelWorker, syncOfdStateViaKernelWorker } from "./sync/fd-ofd.js";
export { blockedStatusUsesForkSharedStateSync, syncForkSharedStateLocally, syncForkSharedStateLocallyForWorkerRuntime, syncForkSharedStateViaKernelWorker, syncForkSharedStateForWorkerRuntime, rehydrateForkSharedStateViaKernelWorker, syncBlockedForkSharedStateViaKernelWorker, syncBlockedForkSharedStateForWorkerRuntime } from "./sync/fork-shared.js";
export { syncPipeStateViaKernelWorker } from "./sync/pipe.js";
export { applySocketStateFromKernelWorker, syncSocketStateViaKernelWorker } from "./sync/socket.js";
export { releaseHostStdioViaKernelWorker, releaseSharedHostStdioViaKernelWorker } from "./sync/stdio.js";
