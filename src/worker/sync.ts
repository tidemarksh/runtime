export { adoptKernelState, adoptKernelStateForHandle } from "./kernel-state.js";
export type { SyncCause } from "./contracts/sync.js";
export { snapshotProcessFdOfdStateForKernelWorker, syncEpollStateViaKernelWorker, syncOfdStateViaKernelWorker } from "./host-boundary/fd-ofd-state-sync.js";
export { syncForkSharedStateViaKernelWorker, rehydrateForkSharedStateViaKernelWorker, syncBlockedForkSharedStateViaKernelWorker } from "./host-boundary/fork-shared-state-sync.js";
export { syncPipeStateViaKernelWorker } from "./host-boundary/pipe-state-sync.js";
export { applySocketStateFromKernelWorker, syncSocketStateViaKernelWorker } from "./host-boundary/socket-state-sync.js";
export { releaseHostStdioViaKernelWorker, releaseSharedHostStdioViaKernelWorker } from "./host-boundary/stdio-state-sync.js";
export { syncProcessMemfsViaKernelWorker } from "./host-boundary/memfs-state-sync.js";
