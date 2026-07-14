import { expect, test } from "vite-plus/test";
import type { KernelRuntimeState } from "../../../../src/abi.js";
import {
  addKernelShareMember,
  processKernelShareGroup,
  setProcessKernelShareGroup,
  transportProcessPendingSignals,
} from "../../../../src/worker/kernel-share.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

function makeHandle(pid: number): ProcessHandle {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const kernelState = {
    processPendingPtr: 0x100,
    processPendingBytes: 64,
  } as KernelRuntimeState;
  return { pid, memory, kernelState, kernel: {} } as ProcessHandle;
}

test("process-pending signal transport moves target bits without losing existing pending signals", () => {
  const source = makeHandle(1);
  const target = makeHandle(3);
  const group = processKernelShareGroup(source);
  setProcessKernelShareGroup(target, group);
  addKernelShareMember(source);
  addKernelShareMember(source, target);

  let sourcePending = 0x4000n;
  let targetPending = 0x20n;
  source.kernel.supervisor_signal_transport_take = (pid) => {
    expect(pid).toBe(target.pid);
    const pending = sourcePending;
    sourcePending = 0n;
    return pending;
  };
  target.kernel.supervisor_signal_transport_merge = (pid, pending) => {
    expect(pid).toBe(target.pid);
    targetPending |= pending;
    return 0;
  };

  expect(transportProcessPendingSignals(source)).toEqual([target]);
  expect(sourcePending).toBe(0n);
  expect(targetPending).toBe(0x4020n);
});
