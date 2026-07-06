import { createWorkerKernelRpcSocketHelpers } from "../socket.js";
import { createWorkerKernelRpcVforkHelpers } from "../vfork.js";
import { createWorkerKernelRpcWait4Helpers } from "../wait4.js";
import type {
  CanonicalizedChildLifecycle,
  ProcessHandle,
} from "../../types.js";
import type { WorkerKernelRpcLifecycleDeps } from "./types.js";

interface WorkerKernelRpcResumeLifecycleDeps extends WorkerKernelRpcLifecycleDeps {
  canonicalizeChildLifecycleViaKernelWorker(
    handle: ProcessHandle,
    vmPtr: number,
    provisionalChildPid: number,
  ): Promise<CanonicalizedChildLifecycle>;
}

export function createWorkerKernelRpcResumeLifecycleHelpers(
  deps: WorkerKernelRpcResumeLifecycleDeps,
) {
  return {
    ...createWorkerKernelRpcSocketHelpers(deps),
    ...createWorkerKernelRpcVforkHelpers(deps),
    ...createWorkerKernelRpcWait4Helpers(deps),
  };
}
