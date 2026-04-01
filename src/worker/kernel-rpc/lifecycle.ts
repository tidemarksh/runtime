import { createWorkerKernelRpcProcessLifecycleHelpers } from "./lifecycle/process.js";
import { createWorkerKernelRpcResumeLifecycleHelpers } from "./lifecycle/resume.js";
import type { WorkerKernelRpcLifecycleDeps } from "./lifecycle/types.js";

export function createWorkerKernelRpcLifecycleHelpers(
  deps: WorkerKernelRpcLifecycleDeps,
) {
  const processLifecycleHelpers = createWorkerKernelRpcProcessLifecycleHelpers(deps);
  const resumeLifecycleHelpers = createWorkerKernelRpcResumeLifecycleHelpers({
    ...deps,
    canonicalizeChildLifecycleViaKernelWorker: (
      handle,
      vmPtr,
      provisionalChildPid,
    ) => processLifecycleHelpers.canonicalizeChildLifecycleViaKernelWorker(
      handle,
      vmPtr,
      provisionalChildPid,
    ),
  });

  return {
    ...processLifecycleHelpers,
    ...resumeLifecycleHelpers,
  };
}
