export { adoptKernelState, adoptKernelStateForHandle } from "./kernel-state.js";
export type { SyncCause } from "./contracts/sync.js";
export { snapshotProcessFdOfdStateForKernelWorker, syncEpollStateViaKernelWorker, syncOfdStateViaKernelWorker } from "./sync/fd-ofd.js";
export { syncForkSharedStateLocally, syncForkSharedStateViaKernelWorker, rehydrateForkSharedStateViaKernelWorker, syncBlockedForkSharedStateViaKernelWorker } from "./sync/fork-shared.js";
export { syncPipeStateViaKernelWorker } from "./sync/pipe.js";
export { applySocketStateFromKernelWorker, syncSocketStateViaKernelWorker } from "./sync/socket.js";
export { releaseHostStdioViaKernelWorker, releaseSharedHostStdioViaKernelWorker } from "./sync/stdio.js";
export { syncProcessMemfsViaKernelWorker } from "./sync/memfs.js";
