import {
  supervisorOfdFdType,
  supervisorOfdIno,
  type KernelExports,
  type KernelRuntimeState,
} from "../../abi.js";
import { copyMemoryRange } from "../common.js";
import { collectProcessHeldOfdRefcounts } from "./ofd.js";

export function createKernelWorkerEpollPrimitives() {
  function collectProcessHeldEpollOfdIds(processExports: KernelExports, vmPtr: number): number[] {
    const maxFds = processExports.abi_max_fds();
    const supervisorHeldEpollOfd = processExports.supervisor_process_epoll_held_ofd;
    if (supervisorHeldEpollOfd) {
      const epollOfdIds: number[] = [];
      for (let index = 0; index < maxFds; index += 1) {
        const ofdId = supervisorHeldEpollOfd.call(processExports, vmPtr, index);
        if (ofdId === 0) {
          break;
        }
        if (ofdId > 0 && ofdId < maxFds) {
          epollOfdIds.push(ofdId);
        }
      }
      return epollOfdIds;
    }
    const epollType = processExports.abi_fd_type_epoll();
    const heldRefcounts = collectProcessHeldOfdRefcounts(processExports, vmPtr);
    const epollOfdIds: number[] = [];

    for (let ofdId = 1; ofdId < maxFds; ofdId += 1) {
      if (heldRefcounts[ofdId] <= 0 || supervisorOfdFdType(processExports, ofdId) !== epollType) {
        continue;
      }
      epollOfdIds.push(ofdId);
    }

    return epollOfdIds;
  }

  function copySelectedEpollState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    epollOfdIds: Iterable<number>,
  ): void {
    const maxFds = sourceExports.abi_max_fds();
    const epollType = sourceExports.abi_fd_type_epoll();
    const tableBytes = sourceExports.kernel_epoll_table_bytes();
    const instanceBytes = sourceExports.kernel_epoll_instance_bytes();
    const tableEntries = tableBytes >>> 2;
    const sourceTable = new Uint32Array(sourceExports.memory.buffer, sourceState.epollTablePtr, tableEntries);
    const targetTable = new Uint32Array(targetExports.memory.buffer, targetState.epollTablePtr, tableEntries);
    const copiedSlots = new Set<number>();

    for (const ofdId of epollOfdIds) {
      if (
        ofdId <= 0
        || ofdId >= maxFds
        || supervisorOfdFdType(sourceExports, ofdId) !== epollType
      ) {
        continue;
      }

      const epollIdx = supervisorOfdIno(sourceExports, ofdId);
      if (epollIdx < 0 || epollIdx >= tableEntries || copiedSlots.has(epollIdx)) {
        continue;
      }
      copiedSlots.add(epollIdx);

      const sourcePtr = sourceTable[epollIdx] ?? 0;
      if (sourcePtr === 0) {
        continue;
      }

      const targetPtr = targetTable[epollIdx] ?? 0;
      const instancePtr = targetPtr !== 0 ? targetPtr : sourcePtr;
      targetTable[epollIdx] = instancePtr;
      copyMemoryRange(
        sourceExports.memory,
        sourcePtr,
        targetExports.memory,
        instancePtr,
        instanceBytes,
      );
    }
  }

  return {
    collectProcessHeldEpollOfdIds,
    copySelectedEpollState,
  };
}
