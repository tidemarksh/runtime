export { adoptKernelState, adoptKernelStateForHandle } from "./sync/kernel-state.js";
export type { SyncCause } from "./sync/effects.js";
export { snapshotProcessFdOfdStateForKernelWorker, syncEpollStateViaKernelWorker, syncOfdStateViaKernelWorker } from "./sync/fd-ofd.js";
export { blockedStatusUsesForkSharedStateSync, syncForkSharedStateLocally, syncForkSharedStateViaKernelWorker, rehydrateForkSharedStateViaKernelWorker, syncBlockedForkSharedStateViaKernelWorker } from "./sync/fork-shared.js";
export { syncPipeStateViaKernelWorker } from "./sync/pipe.js";
export { applySocketStateFromKernelWorker, syncSocketStateViaKernelWorker } from "./sync/socket.js";
export { releaseHostStdioViaKernelWorker } from "./sync/stdio.js";
