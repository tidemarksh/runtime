export { adoptKernelState, adoptKernelStateForHandle } from "./kernel-state.js";
export type { SyncCause } from "./sync/effects.js";
export { snapshotProcessFdOfdStateForKernelWorker, syncEpollStateViaKernelWorker, syncOfdStateViaKernelWorker } from "./sync/fd-ofd.js";
export { blockedStatusUsesForkSharedStateSync, syncForkSharedStateLocally, syncForkSharedStateViaKernelWorker, rehydrateForkSharedStateViaKernelWorker, syncBlockedForkSharedStateViaKernelWorker } from "./sync/fork-shared.js";
export { syncPipeStateViaKernelWorker, syncPipeStateForWorkerRuntime } from "./sync/pipe.js";
export { applySocketStateFromKernelWorker, syncSocketStateViaKernelWorker, syncSocketStateForWorkerRuntime } from "./sync/socket.js";
export { releaseHostStdioViaKernelWorker, releaseSharedHostStdioViaKernelWorker } from "./sync/stdio.js";
export { syncProcessMemfsForWorkerRuntime, syncProcessMemfsViaKernelWorker } from "./sync/memfs.js";
