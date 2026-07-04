import {
  attachKernelRuntimeState,
  kernelSocketStateBytes,
  kernelSocketStatePtr,
  type KernelExports,
  type KernelRuntimeState,
} from "../../abi.js";
import { kernelNetBufferCount } from "../../kernel-socket-layout.js";
import {
  NET_BUFFER_CAPACITY_OFFSET,
  NET_BUFFER_DATA_OFFSET,
  NET_BUFFER_HEAD_OFFSET,
  NET_BUFFER_TAIL_OFFSET,
} from "../../layout/net-buffer.js";
import { clearMemoryRange, copyMemoryRange } from "../common.js";

export function createKernelWorkerObjectStatePrimitives() {
  const NET_BUF_OFF_HEAD = NET_BUFFER_HEAD_OFFSET;
  const NET_BUF_OFF_TAIL = NET_BUFFER_TAIL_OFFSET;
  const NET_BUF_OFF_CAPACITY = NET_BUFFER_CAPACITY_OFFSET;
  const NET_BUF_OFF_DATA = NET_BUFFER_DATA_OFFSET;

  function counterAhead(sourceValue: number, targetValue: number, capacity: number): boolean {
    if (sourceValue === targetValue) {
      return false;
    }
    if (!Number.isFinite(capacity) || capacity <= 0) {
      return sourceValue > targetValue;
    }
    return sourceValue > targetValue
      || (
        sourceValue < targetValue
        && targetValue > capacity / 2
        && sourceValue < capacity / 2
      );
  }

  function copySocketNetBufferState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
  ): void {
    const maxBuffers = kernelNetBufferCount(sourceExports);
    const poolBytes = sourceExports.kernel_net_buf_pool_bytes();
    const slotBytes = Math.floor(poolBytes / maxBuffers);
    const dataBytes = slotBytes - NET_BUF_OFF_DATA;
    if (slotBytes <= NET_BUF_OFF_DATA || dataBytes <= 0) {
      copyMemoryRange(
        sourceExports.memory,
        sourceState.netBufBase,
        targetExports.memory,
        targetState.netBufBase,
        poolBytes,
      );
      return;
    }
    const sourceView = new DataView(sourceExports.memory.buffer);
    const targetView = new DataView(targetExports.memory.buffer);
    for (let bufferIndex = 0; bufferIndex < maxBuffers; bufferIndex += 1) {
      const mask = 1n << BigInt(bufferIndex);
      const sourceOffset = sourceState.netBufBase + bufferIndex * slotBytes;
      const targetOffset = targetState.netBufBase + bufferIndex * slotBytes;
      if ((sourceState.netBufUsed & mask) === 0n) {
        if ((targetState.netBufUsed & mask) !== 0n) {
          clearMemoryRange(targetExports.memory, targetOffset, slotBytes);
        }
        continue;
      }
      if ((targetState.netBufUsed & mask) === 0n) {
        copyMemoryRange(
          sourceExports.memory,
          sourceOffset,
          targetExports.memory,
          targetOffset,
          slotBytes,
        );
        continue;
      }

      const sourceHead = sourceView.getUint32(sourceOffset + NET_BUF_OFF_HEAD, true);
      const sourceTail = sourceView.getUint32(sourceOffset + NET_BUF_OFF_TAIL, true);
      const targetHead = targetView.getUint32(targetOffset + NET_BUF_OFF_HEAD, true);
      const targetTail = targetView.getUint32(targetOffset + NET_BUF_OFF_TAIL, true);
      const mergedHead = counterAhead(sourceHead, targetHead, dataBytes) ? sourceHead : targetHead;
      const sourceTailAhead = counterAhead(sourceTail, targetTail, dataBytes);

      if (sourceTailAhead) {
        const copySpan = (start: number, end: number): void => {
          if (end <= start) {
            return;
          }
          copyMemoryRange(
            sourceExports.memory,
            sourceOffset + NET_BUF_OFF_DATA + start,
            targetExports.memory,
            targetOffset + NET_BUF_OFF_DATA + start,
            end - start,
          );
        };
        if (targetTail < sourceTail) {
          copySpan(targetTail, sourceTail);
        } else {
          copySpan(targetTail, dataBytes);
          copySpan(0, sourceTail);
        }
      }
      copyMemoryRange(
        sourceExports.memory,
        sourceOffset + NET_BUF_OFF_CAPACITY,
        targetExports.memory,
        targetOffset + NET_BUF_OFF_CAPACITY,
        NET_BUF_OFF_DATA - NET_BUF_OFF_CAPACITY,
      );
      targetView.setUint32(targetOffset + NET_BUF_OFF_HEAD, mergedHead, true);
      targetView.setUint32(
        targetOffset + NET_BUF_OFF_TAIL,
        sourceTailAhead ? sourceTail : targetTail,
        true,
      );
    }
  }

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
    const tableBytes = sourceExports.kernel_socket_table_bytes();
    const stateBytes = kernelSocketStateBytes(sourceExports, sourceState);
    const hasFullState = Boolean(
      sourceExports.kernel_socket_state_ptr
        || sourceExports.kernel_socket_state_bytes
        || (sourceState.socketStatePtr !== undefined && sourceState.socketStatePtr > 0 && sourceState.socketStatePtr !== sourceState.socketTablePtr)
        || stateBytes > tableBytes,
    );
    copyMemoryRange(
      sourceExports.memory,
      hasFullState ? kernelSocketStatePtr(sourceState) : sourceState.socketTablePtr,
      targetExports.memory,
      hasFullState ? kernelSocketStatePtr(targetState) : targetState.socketTablePtr,
      hasFullState ? stateBytes : tableBytes,
    );
    copySocketNetBufferState(sourceExports, sourceState, targetExports, targetState);
  }

  return {
    copyEpollState,
    copySocketState,
  };
}
