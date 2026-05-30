import { attachKernelRuntimeState, readKernelRuntimeState } from "../../abi.js";
import type { KernelExports, KernelRuntimeState } from "../../abi.js";
import type { KernelWorkerSocketStateSnapshot } from "../../messages.js";

interface KernelWorkerSocketStateBridgeDeps {
  processKernels: Map<number, KernelExports>;
}

export function createKernelWorkerSocketStateBridge(deps: KernelWorkerSocketStateBridgeDeps) {
  function applySocketStateSnapshot(
    pid: number,
    kernelState: KernelRuntimeState,
    snapshot: KernelWorkerSocketStateSnapshot,
  ): KernelRuntimeState {
    const processExports = deps.processKernels.get(pid);
    if (!processExports) {
      return kernelState;
    }
    const state = readKernelRuntimeState(processExports);
    const memory = new Uint8Array(processExports.memory.buffer);
    const netBufPoolBytes = processExports.kernel_net_buf_pool_bytes();
    const maxBuffers = Math.max(1, Number(processExports.abi_max_sockets?.() ?? 32) * 2);
    const netBufSlotBytes = Math.floor(netBufPoolBytes / maxBuffers);
    memory.set(snapshot.socketTable, state.socketTablePtr);
    if (snapshot.netBufPool !== undefined) {
      memory.set(snapshot.netBufPool, state.netBufBase);
    } else if (netBufSlotBytes > 0) {
      for (const slot of snapshot.netBufSlots ?? []) {
        if (slot.bufferIndex < 0 || slot.bufferIndex >= maxBuffers || slot.bytes.length !== netBufSlotBytes) {
          continue;
        }
        memory.set(slot.bytes, state.netBufBase + slot.bufferIndex * netBufSlotBytes);
      }
    }
    attachKernelRuntimeState(processExports, {
      ...state,
      socketUsed: snapshot.socketUsed,
      netBufUsed: snapshot.netBufUsed,
    });
    return readKernelRuntimeState(processExports);
  }

  function snapshotSocketState(
    pid: number,
    kernelState: KernelRuntimeState,
  ): KernelWorkerSocketStateSnapshot | undefined {
    const processExports = deps.processKernels.get(pid);
    if (!processExports) {
      return undefined;
    }
    attachKernelRuntimeState(processExports, kernelState);
    const state = readKernelRuntimeState(processExports);
    const memory = new Uint8Array(processExports.memory.buffer);
    const netBufPoolBytes = processExports.kernel_net_buf_pool_bytes();
    const maxBuffers = Math.max(1, Number(processExports.abi_max_sockets?.() ?? 32) * 2);
    const netBufSlotBytes = Math.floor(netBufPoolBytes / maxBuffers);
    const netBufSlots: KernelWorkerSocketStateSnapshot["netBufSlots"] = [];
    if (netBufSlotBytes > 0) {
      for (let bufferIndex = 0; bufferIndex < maxBuffers; bufferIndex += 1) {
        if ((state.netBufUsed & (1n << BigInt(bufferIndex))) === 0n) {
          continue;
        }
        const offset = state.netBufBase + bufferIndex * netBufSlotBytes;
        netBufSlots.push({
          bufferIndex,
          bytes: memory.slice(offset, offset + netBufSlotBytes),
        });
      }
    }
    return {
      socketTable: memory.slice(
        state.socketTablePtr,
        state.socketTablePtr + processExports.kernel_socket_table_bytes(),
      ),
      netBufSlots,
      socketUsed: state.socketUsed,
      netBufUsed: state.netBufUsed,
    };
  }

  return {
    applySocketStateSnapshot,
    snapshotSocketState,
  };
}
