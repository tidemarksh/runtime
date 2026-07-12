import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_TYPE_OFFSET,
} from "../../../layout/ofd.js";
import { readRuntimeFdAuthorityEpoch } from "../../../bridge/runtime-state.js";
import type { KernelWorkerFdEntrySnapshot } from "../../../messages.js";
import {
  applyThreadEventfdTraceOffset,
  applyThreadOfdSlots,
} from "../../ownership/ofd-slot.js";
import {
  canonicalFdTableVmPtrForThread,
  supervisorProcessOfdForFd,
} from "../../ownership/fd-table.js";
import type { ProcessHandle } from "../../types.js";
import type { ThreadWorkerStatusMessage } from "../types.js";
import { appendStatusSocketDebug } from "./debug.js";

interface RegularFileOfdIdentity {
  hostFd: number;
  ino: number;
  type: number;
}

function readRegularFileOfdIdentity(
  handle: ProcessHandle,
  ofdId: number,
): RegularFileOfdIdentity {
  return {
    hostFd: handle.kernel.kernel_ofd_host_fd(ofdId),
    ino: handle.kernel.kernel_ofd_ino(ofdId),
    type: handle.kernel.kernel_ofd_fd_type(ofdId),
  };
}

function regularFileIdentityChanged(
  before: RegularFileOfdIdentity,
  after: RegularFileOfdIdentity,
): boolean {
  return before.type !== after.type
    || before.hostFd !== after.hostFd
    || before.ino !== after.ino;
}

function readOfdIdentityFromSlot(slot: Uint8Array): RegularFileOfdIdentity | undefined {
  if (slot.byteLength < OFD_INO_OFFSET + Uint32Array.BYTES_PER_ELEMENT) {
    return undefined;
  }
  const view = new DataView(slot.buffer, slot.byteOffset, slot.byteLength);
  return {
    hostFd: view.getInt32(OFD_HOST_FD_OFFSET, true),
    ino: view.getUint32(OFD_INO_OFFSET, true),
    type: view.getInt32(OFD_TYPE_OFFSET, true),
  };
}

function staleSnapshotWouldReplacePostedIdentity(
  handle: ProcessHandle,
  snapshot: { ofdId: number; slot: Uint8Array },
  postedSlot: Uint8Array | undefined,
): boolean {
  const fileType = handle.kernel.abi_fd_type_file?.();
  const posted = postedSlot === undefined ? undefined : readOfdIdentityFromSlot(postedSlot);
  const candidate = readOfdIdentityFromSlot(snapshot.slot);
  if (
    fileType === undefined
    || posted === undefined
    || candidate === undefined
  ) {
    return false;
  }
  const current = readRegularFileOfdIdentity(handle, snapshot.ofdId);
  if (posted.type !== fileType && candidate.type !== fileType && current.type !== fileType) {
    return false;
  }
  return regularFileIdentityChanged(posted, current)
    && regularFileIdentityChanged(candidate, current);
}

function statusCanReplaceRegularFileIdentity(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
): boolean {
  const openat = handle.kernel.abi_sys_openat?.();
  return openat !== undefined
    && msg.syscallNr !== undefined
    && BigInt(openat) === msg.syscallNr;
}

function regeneratedRegularFileOfdIds(
  msg: ThreadWorkerStatusMessage,
  postedFdEntries: ReadonlyMap<number, KernelWorkerFdEntrySnapshot> | undefined,
): ReadonlySet<number> {
  if (postedFdEntries === undefined) {
    return new Set();
  }
  return new Set((msg.fdEntries ?? [])
    .filter((entry) => {
      const posted = postedFdEntries.get(entry.fd);
      return entry.ofdId > 0
        && entry.generation !== undefined
        && posted?.generation !== undefined
        && entry.generation !== posted.generation;
    })
    .map((entry) => entry.ofdId));
}

function staleFdAuthorityWouldReplaceCurrentIdentity(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  snapshot: { ofdId: number; slot: Uint8Array },
  postedFdEntries: ReadonlyMap<number, KernelWorkerFdEntrySnapshot> | undefined,
): boolean {
  const candidate = readOfdIdentityFromSlot(snapshot.slot);
  if (candidate === undefined || postedFdEntries === undefined) {
    return false;
  }
  const canonicalVmPtr = canonicalFdTableVmPtrForThread(handle, msg.vmPtr);
  const current = readRegularFileOfdIdentity(handle, snapshot.ofdId);
  const fileType = handle.kernel.abi_fd_type_file?.();
  if (fileType === undefined || (candidate.type !== fileType && current.type !== fileType)) {
    return false;
  }
  if (!regularFileIdentityChanged(candidate, current)) {
    return false;
  }
  const references = (msg.fdEntries ?? []).filter((entry) => (
    entry.ofdId === snapshot.ofdId
  ));
  if (references.length === 0) {
    return false;
  }
  return !references.some((entry) => (
    supervisorProcessOfdForFd(handle, canonicalVmPtr, entry.fd) === snapshot.ofdId
    && (() => {
      const posted = postedFdEntries.get(entry.fd);
      return posted?.authorityEpoch === undefined
        || readRuntimeFdAuthorityEpoch(handle.runtimeBridge, entry.fd) <= posted.authorityEpoch;
    })()
  ));
}

function selectAuthoritativeOfdSlots(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  authoritativeOfdIds: ReadonlySet<number> | undefined,
  postedFdEntries: ReadonlyMap<number, KernelWorkerFdEntrySnapshot> | undefined,
  postedOfdSlots: ReadonlyMap<number, Uint8Array> | undefined,
): ThreadWorkerStatusMessage {
  if (authoritativeOfdIds === undefined || !msg.ofdSlots) {
    return msg;
  }
  const ofdSlots = msg.ofdSlots.filter((snapshot) => (
    authoritativeOfdIds.has(snapshot.ofdId)
    && !staleFdAuthorityWouldReplaceCurrentIdentity(handle, msg, snapshot, postedFdEntries)
    && !staleSnapshotWouldReplacePostedIdentity(
      handle,
      snapshot,
      postedOfdSlots?.get(snapshot.ofdId),
    )
  ));
  return { ...msg, ofdSlots };
}

export function applyStatusEventfdTraceOffset(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
): boolean {
  return applyThreadEventfdTraceOffset(handle, msg);
}

export function applyStatusOfdSlots(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  options: {
    trustRegularFileOfdOffsets?: boolean;
    authoritativeOfdIds?: ReadonlySet<number>;
    postedFdEntries?: ReadonlyMap<number, KernelWorkerFdEntrySnapshot>;
    postedOfdSlots?: ReadonlyMap<number, Uint8Array>;
  } = {},
): number[] {
  const regeneratedOfdIds = regeneratedRegularFileOfdIds(
    msg,
    options.postedFdEntries,
  );
  const authoritativeOfdIds = options.authoritativeOfdIds === undefined
    ? undefined
    : new Set([...options.authoritativeOfdIds, ...regeneratedOfdIds]);
  const statusMessage = selectAuthoritativeOfdSlots(
    handle,
    msg,
    authoritativeOfdIds,
    options.postedFdEntries,
    options.postedOfdSlots,
  );
  const fileType = handle.kernel.abi_fd_type_file?.();
  const candidates = fileType === undefined
    ? []
    : (statusMessage.fdEntries ?? [])
      .filter((entry) => entry.fd >= 0 && entry.ofdId > 0 && entry.ofdId < handle.kernel.abi_max_fds())
      .map((entry) => ({
        fd: entry.fd,
        ofdId: entry.ofdId,
        before: readRegularFileOfdIdentity(handle, entry.ofdId),
      }));
  applyThreadOfdSlots(
    handle,
    {
      ...msg,
      ...statusMessage,
      trustRegularFileOfdOffsets: options.trustRegularFileOfdOffsets,
      trustRegularFileOffsetOfdIds: regeneratedOfdIds,
      allowRegularFileIdentityReplacement: authoritativeOfdIds !== undefined
        || statusCanReplaceRegularFileIdentity(handle, msg),
    },
    appendStatusSocketDebug,
  );
  if (fileType === undefined) {
    return [];
  }
  return [...new Set(candidates
    .filter(({ before, ofdId }) => {
      const after = readRegularFileOfdIdentity(handle, ofdId);
      return (before.type === fileType || after.type === fileType)
        && regularFileIdentityChanged(before, after);
    })
    .map(({ fd }) => fd))];
}
