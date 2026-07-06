import type { KernelExports } from "../abi.js";
import { createKernelWorkerEpollPrimitives } from "./primitives/epoll.js";
import { createKernelWorkerMemfsPrimitives } from "./primitives/memfs.js";
import { createKernelWorkerObjectStatePrimitives } from "./primitives/object-state.js";
import { createKernelWorkerOfdPrimitives } from "./primitives/ofd.js";
import { createKernelWorkerPipePrimitives } from "./primitives/pipe.js";

interface KernelWorkerPrimitivesDeps {
  getCanonicalExports(): KernelExports;
  processKernels: Map<number, KernelExports>;
  canonicalProcesses: Map<number, { ppid: number }>;
  getCanonicalProcessVmPtr(pid: number): number;
}

export function createKernelWorkerPrimitives(deps: KernelWorkerPrimitivesDeps) {
  const ofdPrimitives = createKernelWorkerOfdPrimitives(deps);
  const epollPrimitives = createKernelWorkerEpollPrimitives();
  const pipePrimitives = createKernelWorkerPipePrimitives(deps);
  const memfsPrimitives = createKernelWorkerMemfsPrimitives();
  const objectStatePrimitives = createKernelWorkerObjectStatePrimitives();

  return {
    ...ofdPrimitives,
    ...epollPrimitives,
    ...pipePrimitives,
    ...memfsPrimitives,
    ...objectStatePrimitives,
  };
}
