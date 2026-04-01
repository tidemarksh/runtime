import type { KernelExports } from "../abi.js";
import { createKernelWorkerMemfsPrimitives } from "./primitives/memfs.js";
import { createKernelWorkerObjectStatePrimitives } from "./primitives/object-state.js";
import { createKernelWorkerOfdPrimitives } from "./primitives/ofd.js";
import { createKernelWorkerPipePrimitives } from "./primitives/pipe.js";

interface KernelWorkerPrimitivesDeps {
  getCanonicalExports(): KernelExports;
  processKernels: Map<number, KernelExports>;
  getCanonicalProcessVmPtr(pid: number): number;
}

export function createKernelWorkerPrimitives(deps: KernelWorkerPrimitivesDeps) {
  const ofdPrimitives = createKernelWorkerOfdPrimitives(deps);
  const pipePrimitives = createKernelWorkerPipePrimitives(deps);
  const memfsPrimitives = createKernelWorkerMemfsPrimitives();
  const objectStatePrimitives = createKernelWorkerObjectStatePrimitives();

  return {
    ...ofdPrimitives,
    ...pipePrimitives,
    ...memfsPrimitives,
    ...objectStatePrimitives,
  };
}
