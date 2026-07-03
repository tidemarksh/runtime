import type { ProcessHandle } from "../types.js";
import {
  collectProcessHeldOfds,
  trackForkSharedOfds,
} from "./ofd-table.js";
import {
  hasPipePeerInOtherKernelShareMember,
  isProcessHostStdioOfd,
  isProcessHostStdioPipeOfd,
} from "./pipe-control.js";

export interface LatestFdSyscallTrace {
  nr: bigint;
  arg0OfdId: number;
  arg0OfdType: number;
  arg1: bigint | null;
  arg2OfdId: number;
  arg2OfdType: number;
  result: bigint | null;
}

type SupervisorNumberExport = () => bigint | number;

function requireSupervisorNumberExport(
  handle: ProcessHandle,
  exportName: "supervisor_fd_syscall_nr_current" | "supervisor_fd_trace_arg_unknown",
): SupervisorNumberExport {
  const nextValue = handle.kernel[exportName];
  if (typeof nextValue !== "function") {
    throw new Error(`kernel supervisor ABI missing ${exportName}`);
  }
  return nextValue.bind(handle.kernel);
}

function supervisorNumber(value: bigint | number): bigint {
  return typeof value === "bigint" ? value : BigInt(value);
}

function supervisorSyscallNr(handle: ProcessHandle, syscallNr: bigint | null): bigint {
  if (syscallNr !== null) {
    return syscallNr;
  }
  return supervisorNumber(requireSupervisorNumberExport(handle, "supervisor_fd_syscall_nr_current")());
}

function supervisorTraceArg(handle: ProcessHandle, value: bigint | null): bigint {
  if (value !== null) {
    return value;
  }
  return supervisorNumber(requireSupervisorNumberExport(handle, "supervisor_fd_trace_arg_unknown")());
}

function requireSupervisorFdPolicyExport<
  Name extends
    | "supervisor_fd_syscall_may_change_fork_shared_ofd_topology"
    | "supervisor_fd_explicit_fork_shared_effect_may_require_sync"
    | "supervisor_fd_syscall_only_regular_file_offset_sync_when_peer_shared",
>(
  handle: ProcessHandle,
  exportName: Name,
): NonNullable<ProcessHandle["kernel"][Name]> {
  const policy = handle.kernel[exportName];
  if (typeof policy !== "function") {
    throw new Error(`kernel supervisor ABI missing ${exportName}`);
  }
  return policy.bind(handle.kernel) as NonNullable<ProcessHandle["kernel"][Name]>;
}

function requireSupervisorTraceExport<
  Name extends
    | "supervisor_fd_trace_fork_shared_topology_action"
    | "supervisor_fd_trace_data_plane_io_uses_socket_or_pipe"
    | "supervisor_fd_trace_regular_file_write_mutated_memfs",
>(
  handle: ProcessHandle,
  exportName: Name,
): NonNullable<ProcessHandle["kernel"][Name]> {
  const classify = handle.kernel[exportName];
  if (typeof classify !== "function") {
    throw new Error(`kernel supervisor ABI missing ${exportName}`);
  }
  return classify.bind(handle.kernel) as NonNullable<ProcessHandle["kernel"][Name]>;
}

function requireSupervisorTraceActionCode(
  handle: ProcessHandle,
  exportName:
    | "supervisor_fd_trace_topology_action_rules_out"
    | "supervisor_fd_trace_topology_action_needs_peer_check",
): number {
  const code = handle.kernel[exportName];
  if (typeof code !== "function") {
    throw new Error(`kernel supervisor ABI missing ${exportName}`);
  }
  return code.call(handle.kernel);
}

function forkSharedOfdIdentityKey(handle: ProcessHandle, ofdId: number): string | null {
  if (ofdId <= 0 || ofdId >= handle.kernel.abi_max_fds()) {
    return null;
  }
  const type = handle.kernel.kernel_ofd_fd_type(ofdId);
  if (type <= 0) {
    return null;
  }
  const ino = handle.kernel.kernel_ofd_ino(ofdId);
  const hostFd = handle.kernel.kernel_ofd_host_fd(ofdId);
  if (type === handle.kernel.abi_fd_type_pipe()) {
    const endpoint = hostFd >= 0 ? "read" : "write";
    return `${type}:${ino}:${endpoint}`;
  }
  if (type === handle.kernel.abi_fd_type_socket?.()) {
    return `${type}:${ino}:${hostFd}`;
  }
  return `${type}:${ino}`;
}

export function sourceForkSharedOfdPeerState(
  handle: ProcessHandle,
  ofdId: number,
): { compatiblePeer: boolean; incompatiblePeer: boolean } {
  const sourceKey = forkSharedOfdIdentityKey(handle, ofdId);
  if (sourceKey === null) {
    return { compatiblePeer: false, incompatiblePeer: true };
  }
  let compatiblePeer = false;
  let incompatiblePeer = false;
  for (const member of handle.kernelShare.members) {
    if (member === handle || !collectProcessHeldOfds(member).has(ofdId)) {
      continue;
    }
    const peerKey = forkSharedOfdIdentityKey(member, ofdId);
    if (peerKey === sourceKey) {
      compatiblePeer = true;
    } else {
      incompatiblePeer = true;
    }
  }
  return { compatiblePeer, incompatiblePeer };
}

export function sourceOfdCanPublishForkSharedState(handle: ProcessHandle, ofdId: number): boolean {
  if (!handle.kernelShare.sharedOfdIds.has(ofdId)) {
    return false;
  }
  const { compatiblePeer, incompatiblePeer } = sourceForkSharedOfdPeerState(handle, ofdId);
  if (incompatiblePeer) {
    return false;
  }
  const fdType = handle.kernel.kernel_ofd_fd_type(ofdId);
  const fileType = handle.kernel.abi_fd_type_file?.();
  if (fileType !== undefined && fdType === fileType) {
    return compatiblePeer;
  }
  if (fdType === handle.kernel.abi_fd_type_pipe()) {
    return compatiblePeer || hasPipePeerInOtherKernelShareMember(handle, ofdId);
  }
  return true;
}

export function targetHasIncompatibleForkSharedOfd(
  source: ProcessHandle,
  target: ProcessHandle,
  ofdIds: Iterable<number>,
): boolean {
  if (target.vmPtr === 0) {
    return false;
  }
  const trackedOfdIds = new Set(ofdIds);
  if (trackedOfdIds.size === 0) {
    return false;
  }
  const maxFds = target.kernel.abi_max_fds();
  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = target.kernel.process_fd_ofd_id(target.vmPtr, fd);
    if (
      ofdId <= 0
      || ofdId >= maxFds
      || !trackedOfdIds.has(ofdId)
      || !source.kernelShare.sharedOfdIds.has(ofdId)
    ) {
      continue;
    }
    const sourceKey = forkSharedOfdIdentityKey(source, ofdId);
    const targetKey = forkSharedOfdIdentityKey(target, ofdId);
    if (sourceKey !== null && targetKey !== null && sourceKey !== targetKey) {
      return true;
    }
  }
  return false;
}

export function targetHoldsSyncedRegularFileOfd(target: ProcessHandle, ofdIds: ReadonlySet<number>): boolean {
  if (target.vmPtr === 0 || ofdIds.size === 0) {
    return false;
  }
  const fileType = target.kernel.abi_fd_type_file();
  for (const ofdId of collectProcessHeldOfds(target)) {
    if (
      ofdIds.has(ofdId)
      && target.kernel.kernel_ofd_fd_type(ofdId) === fileType
    ) {
      return true;
    }
  }
  return false;
}

export function sourceHoldsSharedRegularFileOfd(handle: ProcessHandle, vmPtr: number): boolean {
  if (vmPtr === 0) {
    return false;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const fileType = (handle.kernel as { abi_fd_type_file?: () => number }).abi_fd_type_file?.();
  if (fileType === undefined) {
    return false;
  }
  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (
      ofdId > 0
      && ofdId < maxFds
      && handle.kernelShare.sharedOfdIds.has(ofdId)
      && handle.kernel.kernel_ofd_fd_type(ofdId) === fileType
    ) {
      return true;
    }
  }
  return false;
}

export function syscallMayChangeForkSharedOfdTopology(
  handle: ProcessHandle,
  vmPtr: number,
  syscallNr: bigint | null,
): boolean {
  const policy = requireSupervisorFdPolicyExport(
    handle,
    "supervisor_fd_syscall_may_change_fork_shared_ofd_topology",
  );
  return policy(vmPtr, supervisorSyscallNr(handle, syscallNr)) !== 0;
}

export function explicitForkSharedEffectMayRequireSync(
  handle: ProcessHandle,
  vmPtr: number,
  syscallNr: bigint | null,
): boolean {
  const policy = requireSupervisorFdPolicyExport(
    handle,
    "supervisor_fd_explicit_fork_shared_effect_may_require_sync",
  );
  return policy(vmPtr, supervisorSyscallNr(handle, syscallNr)) !== 0;
}

export function latestFdTraceRulesOutForkSharedTopologyChange(
  handle: ProcessHandle,
  vmPtr: number,
  latestFdTrace: LatestFdSyscallTrace | null,
): boolean {
  const trace = latestFdTrace;
  if (trace === null) {
    return false;
  }
  const classify = requireSupervisorTraceExport(
    handle,
    "supervisor_fd_trace_fork_shared_topology_action",
  );
  const action = classify(
    vmPtr,
    trace.nr,
    supervisorTraceArg(handle, trace.arg1),
    trace.result ?? 0n,
    trace.result === null ? 0 : 1,
  );
  if (action === requireSupervisorTraceActionCode(handle, "supervisor_fd_trace_topology_action_rules_out")) {
    return true;
  }
  if (action === requireSupervisorTraceActionCode(handle, "supervisor_fd_trace_topology_action_needs_peer_check")) {
    return !fdTraceTouchesForkSharedPeer(handle, trace);
  }
  return false;
}

function fdTraceTouchesForkSharedPeer(
  handle: ProcessHandle,
  trace: LatestFdSyscallTrace,
): boolean {
  const touchedOfdIds = fdTraceTouchedOfdIds(handle, trace);
  if (touchedOfdIds.length === 0) {
    return true;
  }
  for (const member of handle.kernelShare.members) {
    if (member === handle || member.vmPtr === 0) {
      continue;
    }
    const maxFds = member.kernel.abi_max_fds();
    for (let fd = 0; fd < maxFds; fd += 1) {
      const memberOfdId = member.kernel.process_fd_ofd_id(member.vmPtr, fd);
      if (touchedOfdIds.includes(memberOfdId)) {
        return true;
      }
    }
  }
  return false;
}

function fdTraceTouchedOfdIds(handle: ProcessHandle, trace: LatestFdSyscallTrace): number[] {
  return [...new Set([trace.arg0OfdId, trace.arg2OfdId])]
    .filter((ofdId) => ofdId > 0 && ofdId < handle.kernel.abi_max_fds());
}

export function syscallOnlyNeedsRegularFileOffsetSyncWhenPeerShared(
  handle: ProcessHandle,
  vmPtr: number,
  syscallNr: bigint | null,
): boolean {
  const policy = requireSupervisorFdPolicyExport(
    handle,
    "supervisor_fd_syscall_only_regular_file_offset_sync_when_peer_shared",
  );
  return policy(vmPtr, supervisorSyscallNr(handle, syscallNr)) !== 0;
}

export function fdTraceDataPlaneIoUsesSocketOrPipe(
  handle: ProcessHandle,
  trace: LatestFdSyscallTrace | null | undefined,
): boolean {
  if (trace == null) {
    return false;
  }
  const classify = requireSupervisorTraceExport(
    handle,
    "supervisor_fd_trace_data_plane_io_uses_socket_or_pipe",
  );
  return classify(
    trace.nr,
    trace.arg0OfdId,
    trace.arg0OfdType,
    trace.result ?? 0n,
    trace.result === null ? 0 : 1,
  ) !== 0;
}

export function regularFileWriteMutatedMemfs(
  handle: ProcessHandle,
  trace: LatestFdSyscallTrace | null | undefined,
): boolean {
  if (trace == null) {
    return false;
  }
  const classify = requireSupervisorTraceExport(
    handle,
    "supervisor_fd_trace_regular_file_write_mutated_memfs",
  );
  return classify(
    trace.nr,
    trace.arg0OfdType,
    trace.result ?? 0n,
    trace.result === null ? 0 : 1,
  ) !== 0;
}

export function sourceSharesRegularFileOfdWithPeer(handle: ProcessHandle, vmPtr: number): boolean {
  if (vmPtr === 0) {
    return false;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const fileType = (handle.kernel as { abi_fd_type_file?: () => number }).abi_fd_type_file?.();
  if (fileType === undefined) {
    return false;
  }
  const sourceRegularOfds = new Set<number>();
  const sourceRegularInoByOfd = new Map<number, number>();
  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (
      ofdId > 0
      && ofdId < maxFds
      && handle.kernelShare.sharedOfdIds.has(ofdId)
      && handle.kernel.kernel_ofd_fd_type(ofdId) === fileType
    ) {
      sourceRegularOfds.add(ofdId);
      sourceRegularInoByOfd.set(ofdId, handle.kernel.kernel_ofd_ino(ofdId));
    }
  }
  if (sourceRegularOfds.size === 0) {
    return false;
  }
  for (const peer of handle.kernelShare.members) {
    if (peer === handle || peer.vmPtr === 0) {
      continue;
    }
    for (let fd = 0; fd < maxFds; fd += 1) {
      const ofdId = peer.kernel.process_fd_ofd_id(peer.vmPtr, fd);
      if (
        sourceRegularOfds.has(ofdId)
        && peer.kernel.kernel_ofd_fd_type(ofdId) === fileType
        && peer.kernel.kernel_ofd_ino(ofdId) === sourceRegularInoByOfd.get(ofdId)
      ) {
        return true;
      }
    }
  }
  return false;
}

export function collectTrackableForkSharedOfds(handle: ProcessHandle): Set<number> {
  const stdioOfdIds = new Set<number>();
  const stdioFdOfdIds = new Set<number>();
  for (const [fd, stdioPipeSlot] of [
    [0, handle.stdinPipe],
    [1, handle.stdoutPipe],
    [2, handle.stderrPipe],
  ] as const) {
    const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
    if (ofdId !== 0) {
      stdioFdOfdIds.add(ofdId);
    }
    if (
      ofdId !== 0
      && stdioPipeSlot > 0
      && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_pipe()
      && handle.kernel.kernel_ofd_ino(ofdId) === stdioPipeSlot
    ) {
      stdioOfdIds.add(ofdId);
    }
  }
  const tracked = new Set<number>();
  for (const ofdId of handle.kernelShare.sharedOfdIds) {
    if (
      ofdId > 0
      && ofdId < handle.kernel.abi_max_fds()
    ) {
      tracked.add(ofdId);
    }
  }
  for (const ofdId of collectProcessHeldOfds(handle)) {
    if (stdioOfdIds.has(ofdId)) {
      continue;
    }
    if (
      !handle.kernelShare.sharedOfdIds.has(ofdId)
      && !stdioFdOfdIds.has(ofdId)
      && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_file()
    ) {
      continue;
    }
    tracked.add(ofdId);
  }
  return tracked;
}

export function collectTracedForkSharedOfdIds(
  handle: ProcessHandle,
  sourceVmPtr: number,
): number[] {
  const len = handle.kernel.debug_fd_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return [];
  }
  const traced = new Set<number>();
  const sampleToPid = (sample: bigint | number | undefined): number => {
    if (sample === undefined) {
      return 0;
    }
    return Number(BigInt.asIntN(32, BigInt(sample) & 0xffffffffn));
  };
  for (let index = 0; index < len; index += 1) {
    const traceVmPtr = handle.kernel.debug_fd_syscall_trace_vm_ptr?.(index) ?? 0;
    if (traceVmPtr !== 0 && sourceVmPtr !== 0 && traceVmPtr !== sourceVmPtr) {
      continue;
    }
    const tracePid = sampleToPid(handle.kernel.debug_fd_syscall_trace_pid_sample0?.(index));
    if (tracePid > 0 && tracePid !== handle.pid) {
      continue;
    }
    const arg0OfdId = handle.kernel.debug_fd_syscall_trace_arg0_ofd_id?.(index) ?? 0;
    const arg2OfdId = handle.kernel.debug_fd_syscall_trace_arg2_ofd_id?.(index) ?? 0;
    if (arg0OfdId > 0) {
      traced.add(arg0OfdId);
    }
    if (arg2OfdId > 0) {
      traced.add(arg2OfdId);
    }
  }
  return [...traced];
}

export function trackHeldForkSharedOfds(handle: ProcessHandle, vmPtr: number): void {
  trackForkSharedOfds(handle, vmPtr, false);
  for (const ofdId of collectProcessHeldOfds(handle)) {
    if (isProcessHostStdioOfd(handle, vmPtr, ofdId) || isProcessHostStdioPipeOfd(handle, ofdId)) {
      continue;
    }
    if (
      !handle.kernelShare.sharedOfdIds.has(ofdId)
      && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_file()
    ) {
      continue;
    }
    handle.kernelShare.sharedOfdIds.add(ofdId);
  }
}

export function isRegularFileOfd(handle: ProcessHandle, ofdId: number): boolean {
  return ofdId > 0
    && ofdId < handle.kernel.abi_max_fds()
    && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_file();
}
