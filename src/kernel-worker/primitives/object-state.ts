import { attachKernelRuntimeState, type KernelExports, type KernelRuntimeState } from "../../abi.js";
import { clearMemoryRange, copyMemoryRange } from "../common.js";

export function createKernelWorkerObjectStatePrimitives() {
  function copyEpollState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
  ): void {
    const tableBytes = sourceExports.kernel_epoll_table_bytes();
    const instanceBytes = sourceExports.kernel_epoll_instance_bytes();
    const tableEntries = tableBytes >>> 2;
    const sourceTable = new Uint32Array(sourceExports.memory.buffer, sourceState.epollTablePtr, tableEntries);
    const targetTable = new Uint32Array(targetExports.memory.buffer, targetState.epollTablePtr, tableEntries);
    const targetPtrs = [...targetTable];
    for (let i = 0; i < tableEntries; i += 1) {
      const sourcePtr = sourceTable[i] ?? 0;
      const targetPtr = targetPtrs[i] ?? 0;
      if (sourcePtr === 0) {
        targetTable[i] = 0;
        if (targetPtr !== 0) {
          clearMemoryRange(targetExports.memory, targetPtr, instanceBytes);
        }
        continue;
      }
      const instancePtr = targetPtr !== 0 ? targetPtr : sourcePtr;
      targetTable[i] = instancePtr;
      copyMemoryRange(
        sourceExports.memory,
        sourcePtr,
        targetExports.memory,
        instancePtr,
        instanceBytes,
      );
    }
  }

  function copySocketState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
  ): void {
    attachKernelRuntimeState(targetExports, {
      ...targetState,
      socketUsed: sourceState.socketUsed,
      netBufUsed: sourceState.netBufUsed,
    });
    copyMemoryRange(
      sourceExports.memory,
      sourceState.socketTablePtr,
      targetExports.memory,
      targetState.socketTablePtr,
      sourceExports.kernel_socket_table_bytes(),
    );
    copyMemoryRange(
      sourceExports.memory,
      sourceState.netBufBase,
      targetExports.memory,
      targetState.netBufBase,
      sourceExports.kernel_net_buf_pool_bytes(),
    );
  }

  return {
    copyEpollState,
    copySocketState,
  };
}
