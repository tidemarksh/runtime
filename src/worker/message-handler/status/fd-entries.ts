import {
  applyThreadFdEntries,
  canonicalFdTableVmPtrForThread,
  supervisorProcessFdFlagsForFd,
  supervisorProcessOfdForFd,
} from "../../ownership/fd-table.js";
import { processWorkers } from "../../process-worker-state.js";
import { appendPendingAuthoritativeFdForNextStep } from "../../thread-step-payload.js";
import type { ProcessHandle, ThreadWorkerState } from "../../types.js";
import type { ThreadWorkerStatusMessage } from "../types.js";
import type { KernelWorkerFdEntrySnapshot } from "../../../messages.js";
import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_TYPE_OFFSET,
} from "../../../layout/ofd.js";
import { appendStatusSocketDebug } from "./debug.js";
import { publishRuntimeFdAuthorityEpoch } from "../../../bridge/runtime-state.js";

interface StatusFdState {
  fdFlags: number;
  generation?: number;
  ofdId: number;
  ofdType: number;
}

function postedOfdIdentityMatchesCurrent(
  handle: ProcessHandle,
  ofdId: number,
  postedOfdSlots: ReadonlyMap<number, Uint8Array> | undefined,
): boolean {
  if (ofdId <= 0) {
    return true;
  }
  const slot = postedOfdSlots?.get(ofdId);
  if (slot === undefined || slot.byteLength < OFD_INO_OFFSET + Uint32Array.BYTES_PER_ELEMENT) {
    return true;
  }
  const view = new DataView(slot.buffer, slot.byteOffset, slot.byteLength);
  const currentType = handle.kernel.kernel_ofd_fd_type(ofdId);
  if (currentType <= 0) {
    return true;
  }
  return currentType === view.getInt32(OFD_TYPE_OFFSET, true)
    && handle.kernel.kernel_ofd_host_fd(ofdId) === view.getInt32(OFD_HOST_FD_OFFSET, true)
    && handle.kernel.kernel_ofd_ino(ofdId) === view.getUint32(OFD_INO_OFFSET, true);
}

function readStatusFdState(handle: ProcessHandle, vmPtr: number, fd: number): StatusFdState {
  const ofdId = supervisorProcessOfdForFd(handle, vmPtr, fd);
  return {
    fdFlags: supervisorProcessFdFlagsForFd(handle, vmPtr, fd),
    generation: handle.kernel.process_fd_generation?.(vmPtr, fd),
    ofdId,
    ofdType: ofdId > 0 ? handle.kernel.kernel_ofd_fd_type(ofdId) : 0,
  };
}

export function applyStatusFdEntries(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  authoritativeFds?: ReadonlySet<number>,
  postedFdEntries?: ReadonlyMap<number, KernelWorkerFdEntrySnapshot>,
  postedOfdSlots?: ReadonlyMap<number, Uint8Array>,
): number[] {
  const canonicalVmPtr = canonicalFdTableVmPtrForThread(handle, msg.vmPtr);
  const fdEntries = authoritativeFds === undefined
    ? msg.fdEntries
    : msg.fdEntries?.filter((entry) => {
      if (!authoritativeFds.has(entry.fd)) {
        return false;
      }
      const posted = postedFdEntries?.get(entry.fd);
      if (posted === undefined) {
        return true;
      }
      const canonical = readStatusFdState(handle, canonicalVmPtr, entry.fd);
      const postedIdentityMatches = postedOfdIdentityMatchesCurrent(
        handle,
        posted.ofdId,
        postedOfdSlots,
      );
      const canonicalMatchesPosted = canonical.ofdId === posted.ofdId
        && canonical.fdFlags === posted.fdFlags
        && (posted.generation === undefined || canonical.generation === posted.generation)
        && postedIdentityMatches;
      const canonicalMatchesReported = canonical.ofdId === entry.ofdId
        && canonical.fdFlags === entry.fdFlags;
      return canonicalMatchesPosted || canonicalMatchesReported;
    });
  const candidateFds = [...new Set((fdEntries ?? [])
    .map((entry) => entry.fd)
    .filter((fd) => fd >= 0 && fd < handle.kernel.abi_max_fds()))];
  const before = new Map(candidateFds.map((fd) => [
    fd,
    readStatusFdState(handle, canonicalVmPtr, fd),
  ]));
  const options = {
    appendStatusSocketDebug,
    // Status fd snapshots come from a thread worker kernel instance; compare
    // descriptor state first, and only use generations to reject closed-fd
    // resurrection from stale non-creating syscall snapshots.
    ignoreSnapshotGeneration: true,
    preserveClosedDescriptorsFromStaleSnapshots: authoritativeFds === undefined,
    preserveLiveDescriptorsFromStaleCloseSnapshots: authoritativeFds === undefined,
    preserveLiveEpollDependentDescriptors: authoritativeFds === undefined,
    preserveLiveRegularFileDescriptors: authoritativeFds === undefined,
    syscallNr: msg.syscallNr,
  };
  applyThreadFdEntries(handle, msg.vmPtr, fdEntries, options);
  if (canonicalVmPtr !== msg.vmPtr) {
    applyThreadFdEntries(handle, canonicalVmPtr, fdEntries, options);
  }
  const changedFds = authoritativeFds !== undefined
    ? candidateFds
    : candidateFds.filter((fd) => {
        const previous = before.get(fd);
        const current = readStatusFdState(handle, canonicalVmPtr, fd);
        return previous !== undefined
          && (previous.ofdId !== current.ofdId || previous.fdFlags !== current.fdFlags);
      });
  for (const fd of changedFds) {
    publishRuntimeFdAuthorityEpoch(handle.runtimeBridge, fd);
  }
  return changedFds;
}

export function markStatusFdChangesForNextStep(
  handle: ProcessHandle,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
  fds: readonly number[],
): void {
  if (fds.length === 0) {
    return;
  }
  for (const worker of processWorkers(handle)) {
    const thread = threadWorkerStates.get(worker);
    if (!thread) {
      continue;
    }
    for (const fd of fds) {
      appendPendingAuthoritativeFdForNextStep(thread, fd);
    }
  }
}
