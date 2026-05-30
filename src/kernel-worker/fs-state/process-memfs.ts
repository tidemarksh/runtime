import { attachKernelRuntimeState, readKernelRuntimeState, type KernelExports, type KernelRuntimeState } from "../../abi.js";

interface KernelWorkerProcessMemfsSyncDeps {
  getCanonicalExports(): KernelExports;
  requireProcessKernel(pid: number): KernelExports;
  copyCanonicalMemfsToProcess(
    canonicalState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    options?: { includeData?: boolean },
  ): void;
  restorePublishedRegularFileDataFromPageCache(targetExports?: KernelExports): void;
}

export function createKernelWorkerProcessMemfsSync(deps: KernelWorkerProcessMemfsSyncDeps) {
  return function syncProcessMemfs(pid: number, kernelState: KernelRuntimeState): KernelRuntimeState {
    const canonicalExports = deps.getCanonicalExports();
    const processExports = deps.requireProcessKernel(pid);
    attachKernelRuntimeState(processExports, kernelState);
    const canonicalState = readKernelRuntimeState(canonicalExports);
    const processState = readKernelRuntimeState(processExports);
    deps.copyCanonicalMemfsToProcess(canonicalState, processExports, processState, {
      includeData: true,
    });
    deps.restorePublishedRegularFileDataFromPageCache(processExports);
    return readKernelRuntimeState(processExports);
  };
}
