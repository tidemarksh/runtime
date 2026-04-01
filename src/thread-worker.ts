/**
 * Thread-worker: executes a single guest thread on a shared process memory.
 */

import type { KernelExports, KernelRuntimeState, StatusCodes } from "./abi.js";
import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  readStatusCodes,
} from "./abi.js";
import type { ThreadWorkerInbound, ThreadWorkerOutbound } from "./messages.js";
import {
  readPublishedKernelRuntimeState,
  type RuntimeStateBridge,
  waitForRuntimeBridgeWake,
} from "./runtime-state-bridge.js";
import { detectWasmFeatures, type WasmFeatureSupport } from "./wasm-features.js";
import { createKernelEnv } from "./wasm-env.js";
import { bindWorkerMessageHandler, workerPostMessage } from "./worker-scope.js";

let exports: KernelExports;
let statusCodes: StatusCodes;
let vmPtr = 0;
let guestBase = 0;
let wasmFeatures: WasmFeatureSupport;
let runtimeBridge: RuntimeStateBridge | null = null;
const THREAD_STEP_BUDGET = 20_000;
const SYNC_EFFECT_PIPE_STATE = 1 << 0;
const SYNC_EFFECT_SOCKET_STATE = 1 << 1;
const SYNC_EFFECT_EPOLL_STATE = 1 << 2;
const SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE = 1 << 3;
const SYNC_EFFECT_PROCESS_LIFECYCLE = 1 << 4;
const SYNC_EFFECT_SIGNAL_WAITERS = 1 << 5;
const SYNC_EFFECT_FORK_SHARED_OFD_STATE = 1 << 6;
const EINTR_ERRNO = 4;
const FUTEX_WAIT_SLICE_MS = 10;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

interface SyscallNumbers {
  read: bigint | null;
  write: bigint | null;
  lseek: bigint | null;
  execve: bigint | null;
  close: bigint | null;
  pipe2: bigint | null;
  epollCtl: bigint | null;
  dup: bigint | null;
  dup3: bigint | null;
  fcntl: bigint | null;
  readv: bigint | null;
  writev: bigint | null;
  socket: bigint | null;
  bind: bigint | null;
  listen: bigint | null;
  accept4: bigint | null;
  eventfd2: bigint | null;
  setpgid: bigint | null;
  setsid: bigint | null;
  kill: bigint | null;
  tkill: bigint | null;
  tgkill: bigint | null;
}

let syscallNumbers: SyscallNumbers | null = null;

function readOptionalU64(value: bigint | number | undefined): bigint | null {
  if (value === undefined) {
    return null;
  }
  return typeof value === "bigint" ? value : BigInt(value);
}

function clearSyscallTrace(): void {
  exports.debug_clear_syscall_trace?.();
}

function formatU64(value: bigint): string {
  return BigInt.asUintN(64, value).toString(16);
}

function readOptionalTraceU64(value: bigint | number | undefined): bigint {
  if (value === undefined) {
    return 0n;
  }
  return typeof value === "bigint" ? value : BigInt(value);
}

function matchesSyscall(syscallNr: bigint, candidate: bigint | null): boolean {
  return candidate !== null && syscallNr === candidate;
}

function readCompletedSyncEffects(status: number): number {
  const aggregatedEffects = exports.debug_completed_sync_effects?.();
  if (aggregatedEffects !== undefined && aggregatedEffects !== 0) {
    return aggregatedEffects;
  }

  if (status !== statusCodes.OK || syscallNumbers === null) {
    return 0;
  }

  const syscallNr = exports.debug_syscall_trace_nr?.();
  if (syscallNr === undefined || syscallNr === 0n) {
    return 0;
  }

  if (
    matchesSyscall(syscallNr, syscallNumbers.read)
    || matchesSyscall(syscallNr, syscallNumbers.write)
    || matchesSyscall(syscallNr, syscallNumbers.lseek)
    || matchesSyscall(syscallNr, syscallNumbers.readv)
    || matchesSyscall(syscallNr, syscallNumbers.writev)
    || matchesSyscall(syscallNr, syscallNumbers.close)
    || matchesSyscall(syscallNr, syscallNumbers.dup)
    || matchesSyscall(syscallNr, syscallNumbers.dup3)
    || matchesSyscall(syscallNr, syscallNumbers.fcntl)
  ) {
    return SYNC_EFFECT_PIPE_STATE
      | SYNC_EFFECT_SOCKET_STATE
      | SYNC_EFFECT_EPOLL_STATE
      | SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE
      | SYNC_EFFECT_FORK_SHARED_OFD_STATE;
  }

  if (matchesSyscall(syscallNr, syscallNumbers.pipe2)) {
    return SYNC_EFFECT_PIPE_STATE | SYNC_EFFECT_EPOLL_STATE | SYNC_EFFECT_FORK_SHARED_OFD_STATE;
  }

  if (
    matchesSyscall(syscallNr, syscallNumbers.socket)
    || matchesSyscall(syscallNr, syscallNumbers.bind)
    || matchesSyscall(syscallNr, syscallNumbers.listen)
    || matchesSyscall(syscallNr, syscallNumbers.accept4)
  ) {
    return SYNC_EFFECT_SOCKET_STATE | SYNC_EFFECT_EPOLL_STATE | SYNC_EFFECT_FORK_SHARED_OFD_STATE;
  }

  if (matchesSyscall(syscallNr, syscallNumbers.eventfd2)) {
    return SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE
      | SYNC_EFFECT_EPOLL_STATE
      | SYNC_EFFECT_FORK_SHARED_OFD_STATE;
  }

  if (matchesSyscall(syscallNr, syscallNumbers.epollCtl)) {
    return SYNC_EFFECT_EPOLL_STATE;
  }

  if (
    matchesSyscall(syscallNr, syscallNumbers.execve)
    || matchesSyscall(syscallNr, syscallNumbers.setpgid)
    || matchesSyscall(syscallNr, syscallNumbers.setsid)
  ) {
    return SYNC_EFFECT_PROCESS_LIFECYCLE;
  }

  if (
    matchesSyscall(syscallNr, syscallNumbers.kill)
    || matchesSyscall(syscallNr, syscallNumbers.tkill)
    || matchesSyscall(syscallNr, syscallNumbers.tgkill)
  ) {
    return SYNC_EFFECT_SIGNAL_WAITERS;
  }

  return 0;
}

async function init(
  wasmModule: WebAssembly.Module,
  memory: WebAssembly.Memory,
  threadVmPtr: number,
  threadGuestBase: number,
  kernelState: KernelRuntimeState,
  sabPageCache: SharedArrayBuffer | null,
  _notifySab: SharedArrayBuffer,
  _stateSab: SharedArrayBuffer,
): Promise<void> {
  const instance = await WebAssembly.instantiate(wasmModule, {
    env: createKernelEnv(memory, { sabPageCache }),
  });
  exports = instance.exports as unknown as KernelExports;
  attachKernelRuntimeState(exports, kernelState);
  statusCodes = readStatusCodes(exports);
  vmPtr = threadVmPtr;
  guestBase = threadGuestBase;
  wasmFeatures = detectWasmFeatures();
  syscallNumbers = {
    read: readOptionalU64(exports.abi_sys_read?.()),
    write: readOptionalU64(exports.abi_sys_write?.()),
    lseek: readOptionalU64(exports.abi_sys_lseek?.()),
    execve: readOptionalU64(exports.abi_sys_execve?.()),
    close: readOptionalU64(exports.abi_sys_close?.()),
    pipe2: readOptionalU64(exports.abi_sys_pipe2?.()),
    epollCtl: readOptionalU64(exports.abi_sys_epoll_ctl?.()),
    dup: readOptionalU64(exports.abi_sys_dup?.()),
    dup3: readOptionalU64(exports.abi_sys_dup3?.()),
    fcntl: readOptionalU64(exports.abi_sys_fcntl?.()),
    readv: readOptionalU64(exports.abi_sys_readv?.()),
    writev: readOptionalU64(exports.abi_sys_writev?.()),
    socket: readOptionalU64(exports.abi_sys_socket?.()),
    bind: readOptionalU64(exports.abi_sys_bind?.()),
    listen: readOptionalU64(exports.abi_sys_listen?.()),
    accept4: readOptionalU64(exports.abi_sys_accept4?.()),
    eventfd2: readOptionalU64(exports.abi_sys_eventfd2?.()),
    setpgid: readOptionalU64(exports.abi_sys_setpgid?.()),
    setsid: readOptionalU64(exports.abi_sys_setsid?.()),
    kill: readOptionalU64(exports.abi_sys_kill?.()),
    tkill: readOptionalU64(exports.abi_sys_tkill?.()),
    tgkill: readOptionalU64(exports.abi_sys_tgkill?.()),
  };
  runtimeBridge = {
    notifySab: _notifySab,
    stateSab: _stateSab,
  };
}

function reattachPublishedKernelRuntimeState(): void {
  if (runtimeBridge === null) {
    return;
  }
  attachKernelRuntimeState(exports, readPublishedKernelRuntimeState(runtimeBridge));
}

function postReady(): void {
  const msg: ThreadWorkerOutbound = {
    type: "ready",
    vmPtr,
    kernelState: readKernelRuntimeState(exports),
    wasmFeatures,
  };
  workerPostMessage(msg);
}

function formatWriteTraceBytes(): string | null {
  const ptr = exports.debug_write_trace_ptr?.() ?? 0;
  const len = exports.debug_write_trace_len?.() ?? 0;
  if (ptr === 0 || len === 0) {
    return null;
  }
  const bytes = new Uint8Array(exports.memory.buffer, ptr, len);
  let out = "";
  for (const byte of bytes) {
    if (byte >= 0x20 && byte <= 0x7e) {
      out += String.fromCharCode(byte);
    } else if (byte === 0x0a) {
      out += "\\n";
    } else if (byte === 0x0d) {
      out += "\\r";
    } else if (byte === 0x09) {
      out += "\\t";
    } else {
      out += `\\x${byte.toString(16).padStart(2, "0")}`;
    }
  }
  return out;
}

function collectDebugParts(): string[] {
  if (typeof exports === "undefined") {
    return [];
  }

  const debugParts: string[] = [];
  debugParts.push(`tid=${exports.thread_tid(vmPtr)}`);
  debugParts.push(`gp=0x${formatU64(exports.thread_gp(vmPtr))}`);
  debugParts.push(`tp=0x${formatU64(exports.thread_tp(vmPtr))}`);
  debugParts.push(`g=0x${formatU64(exports.thread_g(vmPtr))}`);
  debugParts.push(`tls=0x${formatU64(exports.thread_tls_base(vmPtr))}`);
  debugParts.push(`sig_mask=0x${formatU64(exports.thread_sig_mask(vmPtr))}`);
  debugParts.push(`sig_pending=0x${formatU64(exports.thread_sig_pending(vmPtr))}`);
  debugParts.push(`sig_code=${exports.thread_sig_info_code(vmPtr)}`);
  debugParts.push(`fault_pc=0x${formatU64(exports.thread_fault_pc(vmPtr))}`);
  debugParts.push(`fault_addr=0x${formatU64(exports.thread_fault_addr(vmPtr))}`);
  debugParts.push(`pc=0x${exports.thread_pc(vmPtr).toString(16)}`);
  debugParts.push(`ra=0x${exports.thread_ra(vmPtr).toString(16)}`);
  debugParts.push(`sp=0x${exports.thread_sp(vmPtr).toString(16)}`);
  debugParts.push(`stk0=0x${exports.thread_stack_word(vmPtr, 0).toString(16)}`);
  debugParts.push(`stk1=0x${exports.thread_stack_word(vmPtr, 1).toString(16)}`);
  debugParts.push(`stk4=0x${exports.thread_stack_word(vmPtr, 4).toString(16)}`);
  debugParts.push(`stk8=0x${exports.thread_stack_word(vmPtr, 8).toString(16)}`);
  debugParts.push(`stk10=0x${exports.thread_stack_word(vmPtr, 10).toString(16)}`);
  debugParts.push(`stk14=0x${exports.thread_stack_word(vmPtr, 14).toString(16)}`);
  debugParts.push(`stk18=0x${exports.thread_stack_word(vmPtr, 18).toString(16)}`);
  debugParts.push(`stk19=0x${exports.thread_stack_word(vmPtr, 19).toString(16)}`);
  debugParts.push(`a0=${exports.thread_a0(vmPtr).toString()}`);
  debugParts.push(`a1=${exports.thread_a1(vmPtr).toString()}`);
  const syscallNr = exports.debug_syscall_trace_nr?.();
  if (syscallNr !== undefined && syscallNr !== 0n) {
    debugParts.push(`syscall=${syscallNr.toString()}`);
    debugParts.push(`status=${exports.debug_syscall_trace_status?.() ?? "n/a"}`);
    debugParts.push(`result=${exports.debug_syscall_trace_result?.().toString() ?? "n/a"}`);
  }
  const writeNr = exports.debug_write_trace_last_nr?.();
  if (writeNr !== undefined && writeNr !== 0n) {
    const writeFd = Number(exports.debug_write_trace_last_fd?.() ?? 0n);
    const writeOfdId = exports.debug_write_trace_last_ofd_id?.() ?? 0;
    const writeOfdType = exports.debug_write_trace_last_ofd_type?.() ?? "n/a";
    debugParts.push(`write_nr=${writeNr.toString()}`);
    debugParts.push(`write_fd=${writeFd.toString()}`);
    debugParts.push(`write_buf=0x${exports.debug_write_trace_last_buf_addr?.().toString(16) ?? "0"}`);
    debugParts.push(`write_count=${exports.debug_write_trace_last_count?.().toString() ?? "n/a"}`);
    debugParts.push(`write_result=${exports.debug_write_trace_last_result?.().toString() ?? "n/a"}`);
    debugParts.push(`write_ofd_id=${writeOfdId}`);
    debugParts.push(`write_ofd_type=${writeOfdType}`);
    if (writeOfdId > 0) {
      const writeOfdIno = exports.debug_write_trace_last_ofd_ino?.() ?? exports.kernel_ofd_ino(writeOfdId);
      debugParts.push(`write_ofd_ino=${writeOfdIno}`);
      debugParts.push(`write_ofd_host_fd=${exports.kernel_ofd_host_fd(writeOfdId)}`);
      debugParts.push(`write_ofd_flags=${exports.kernel_ofd_status_flags(writeOfdId)}`);
      if (writeOfdType === 6) {
        debugParts.push(`write_pipe_read_open=${exports.debug_write_trace_last_pipe_read_open?.() ?? "n/a"}`);
        debugParts.push(`write_pipe_write_open=${exports.debug_write_trace_last_pipe_write_open?.() ?? "n/a"}`);
      }
    }
    const writeBytes = formatWriteTraceBytes();
    if (writeBytes !== null) {
      debugParts.push(`write_data=${JSON.stringify(writeBytes)}`);
    }
  }
  const lastPipeCloseSlot = exports.debug_last_pipe_close_slot?.() ?? 0;
  if (lastPipeCloseSlot !== 0) {
    debugParts.push(`last_pipe_close_slot=${lastPipeCloseSlot}`);
    debugParts.push(`last_pipe_close_end=${exports.debug_last_pipe_close_is_read_end?.() === 1 ? "read" : "write"}`);
    debugParts.push(`last_pipe_close_ofd=${exports.debug_last_pipe_close_ofd_id?.() ?? "n/a"}`);
    debugParts.push(`last_pipe_close_ro=${exports.debug_last_pipe_close_read_open?.() ?? "n/a"}`);
    debugParts.push(`last_pipe_close_wo=${exports.debug_last_pipe_close_write_open?.() ?? "n/a"}`);
  }
  const memoryNr = exports.debug_memory_trace_nr?.();
  if (memoryNr !== undefined && memoryNr !== 0n) {
    debugParts.push(`mem_nr=${memoryNr.toString()}`);
    debugParts.push(`mem_a0=${exports.debug_memory_trace_arg0?.().toString() ?? "n/a"}`);
    debugParts.push(`mem_a1=${exports.debug_memory_trace_arg1?.().toString() ?? "n/a"}`);
    debugParts.push(`mem_a2=${exports.debug_memory_trace_arg2?.().toString() ?? "n/a"}`);
    debugParts.push(`mem_a3=${exports.debug_memory_trace_arg3?.().toString() ?? "n/a"}`);
    debugParts.push(`mem_result=${exports.debug_memory_trace_result?.().toString() ?? "n/a"}`);
  }
  const guestMemAddr = exports.debug_guest_mem_last_addr?.();
  if (guestMemAddr !== undefined && guestMemAddr !== 0n) {
    debugParts.push(`guest_mem_addr=0x${formatU64(guestMemAddr)}`);
    debugParts.push(`guest_mem_len=${exports.debug_guest_mem_last_len?.() ?? "n/a"}`);
    debugParts.push(`guest_mem_kind=${exports.debug_guest_mem_last_kind?.() ?? "n/a"}`);
  }
  const signalDeliverySig = exports.debug_signal_delivery_sig?.();
  if (signalDeliverySig !== undefined && signalDeliverySig !== 0) {
    debugParts.push(`sig_deliver=${signalDeliverySig}`);
    debugParts.push(`sig_frame=0x${formatU64(exports.debug_signal_delivery_frame_base?.() ?? 0n)}`);
    debugParts.push(`sig_pc=0x${formatU64(exports.debug_signal_delivery_pc?.() ?? 0n)}`);
    debugParts.push(`sig_ra=0x${formatU64(exports.debug_signal_delivery_ra?.() ?? 0n)}`);
    debugParts.push(`sig_sp=0x${formatU64(exports.debug_signal_delivery_sp?.() ?? 0n)}`);
    debugParts.push(`sig_g=0x${formatU64(exports.debug_signal_delivery_g?.() ?? 0n)}`);
  }
  const sigreturnPc = exports.debug_sigreturn_pc?.();
  if (sigreturnPc !== undefined && sigreturnPc !== 0n) {
    debugParts.push(`sigret_frame=0x${formatU64(exports.debug_sigreturn_frame_base?.() ?? 0n)}`);
    debugParts.push(`sigret_pc=0x${formatU64(sigreturnPc)}`);
    debugParts.push(`sigret_ra=0x${formatU64(exports.debug_sigreturn_ra?.() ?? 0n)}`);
    debugParts.push(`sigret_sp=0x${formatU64(exports.debug_sigreturn_sp?.() ?? 0n)}`);
    debugParts.push(`sigret_g=0x${formatU64(exports.debug_sigreturn_g?.() ?? 0n)}`);
  }
  const cloneSpawnTid = exports.debug_clone_spawn_tid?.();
  if (cloneSpawnTid !== undefined && cloneSpawnTid !== 0) {
    debugParts.push(`clone_tid=${cloneSpawnTid}`);
    debugParts.push(`clone_pc=0x${formatU64(exports.debug_clone_spawn_pc?.() ?? 0n)}`);
    debugParts.push(`clone_ra=0x${formatU64(exports.debug_clone_spawn_ra?.() ?? 0n)}`);
    debugParts.push(`clone_sp=0x${formatU64(exports.debug_clone_spawn_sp?.() ?? 0n)}`);
    debugParts.push(`clone_g=0x${formatU64(exports.debug_clone_spawn_g?.() ?? 0n)}`);
  }
  const trampolineTid = exports.debug_signal_trampoline_hit_tid?.();
  if (trampolineTid !== undefined && trampolineTid !== 0) {
    debugParts.push(`sigtramp_tid=${trampolineTid}`);
    debugParts.push(`sigtramp_pc=0x${formatU64(exports.debug_signal_trampoline_hit_pc?.() ?? 0n)}`);
  }
  const sigreturnCount = exports.debug_sigreturn_count?.();
  if (sigreturnCount !== undefined && sigreturnCount !== 0) {
    debugParts.push(`sigreturn_count=${sigreturnCount}`);
  }
  const panicLine = exports.debug_panic_line?.() ?? 0;
  if (panicLine !== 0) {
    debugParts.push(`panic_line=${panicLine}`);
    debugParts.push(`panic_col=${exports.debug_panic_column?.() ?? 0}`);
    const panicFilePtr = exports.debug_panic_file_ptr?.() ?? 0;
    const panicFileLen = exports.debug_panic_file_len?.() ?? 0;
    if (panicFilePtr !== 0 && panicFileLen !== 0) {
      const panicFileBytes = new Uint8Array(exports.memory.buffer, panicFilePtr, panicFileLen);
      debugParts.push(`panic_file=${JSON.stringify(decoder.decode(panicFileBytes))}`);
    }
  }
  const fdTraceLen = exports.debug_fd_syscall_trace_len?.() ?? 0;
  if (fdTraceLen > 0) {
    const traces: string[] = [];
    for (let index = 0; index < fdTraceLen; index += 1) {
      const nr = readOptionalTraceU64(exports.debug_fd_syscall_trace_nr?.(index));
      if (nr === 0n) {
        continue;
      }
      const arg0 = readOptionalTraceU64(exports.debug_fd_syscall_trace_arg0?.(index));
      const arg1 = readOptionalTraceU64(exports.debug_fd_syscall_trace_arg1?.(index));
      const arg2 = readOptionalTraceU64(exports.debug_fd_syscall_trace_arg2?.(index));
      const arg3 = readOptionalTraceU64(exports.debug_fd_syscall_trace_arg3?.(index));
      const result = readOptionalTraceU64(exports.debug_fd_syscall_trace_result?.(index));
      const status = exports.debug_fd_syscall_trace_status?.(index) ?? 0;
      traces.push(
        `${nr.toString()}(${arg0.toString()},${arg1.toString()},${arg2.toString()},${arg3.toString()})=`
        + `${BigInt.asIntN(64, result).toString()}/${status}`,
      );
    }
    if (traces.length > 0) {
      debugParts.push(`fd_syscalls=[${traces.join(",")}]`);
    }
  }
  const zeroRegPc = exports.debug_zero_reg_pc?.();
  if (zeroRegPc !== undefined && zeroRegPc !== 0n) {
    debugParts.push(`zero_reg_pc=0x${formatU64(zeroRegPc)}`);
    debugParts.push(`zero_reg_raw=0x${(exports.debug_zero_reg_raw?.() ?? 0).toString(16)}`);
    debugParts.push(`zero_reg=${exports.debug_zero_reg_index?.() ?? "n/a"}`);
    debugParts.push(`zero_reg_prev=0x${formatU64(exports.debug_zero_reg_prev_value?.() ?? 0n)}`);
    debugParts.push(`zero_reg_sp=0x${formatU64(exports.debug_zero_reg_sp?.() ?? 0n)}`);
    debugParts.push(`zero_reg_g=0x${formatU64(exports.debug_zero_reg_g?.() ?? 0n)}`);
  }
  const gWritePc = exports.debug_g_write_pc?.();
  if (gWritePc !== undefined && gWritePc !== 0n) {
    debugParts.push(`g_write_pc=0x${formatU64(gWritePc)}`);
    debugParts.push(`g_write_raw=0x${(exports.debug_g_write_raw?.() ?? 0).toString(16)}`);
    debugParts.push(`g_write_src=${exports.debug_g_write_source?.() ?? "n/a"}`);
    debugParts.push(`g_write_prev=0x${formatU64(exports.debug_g_write_prev_value?.() ?? 0n)}`);
    debugParts.push(`g_write=0x${formatU64(exports.debug_g_write_value?.() ?? 0n)}`);
  }
  const spWritePc = exports.debug_sp_write_pc?.();
  if (spWritePc !== undefined && spWritePc !== 0n) {
    debugParts.push(`sp_write_pc=0x${formatU64(spWritePc)}`);
    debugParts.push(`sp_write_raw=0x${(exports.debug_sp_write_raw?.() ?? 0).toString(16)}`);
    debugParts.push(`sp_write_src=${exports.debug_sp_write_source?.() ?? "n/a"}`);
    debugParts.push(`sp_write_prev=0x${formatU64(exports.debug_sp_write_prev_value?.() ?? 0n)}`);
    debugParts.push(`sp_write=0x${formatU64(exports.debug_sp_write_value?.() ?? 0n)}`);
  }
  const jalrPc = exports.debug_jalr_pc?.();
  if (jalrPc !== undefined && jalrPc !== 0n) {
    debugParts.push(`jalr_pc=0x${formatU64(jalrPc)}`);
    debugParts.push(`jalr_rs1=${exports.debug_jalr_rs1?.() ?? "n/a"}`);
    debugParts.push(`jalr_rd=${exports.debug_jalr_rd?.() ?? "n/a"}`);
    debugParts.push(`jalr_rs1v=0x${formatU64(exports.debug_jalr_rs1_value?.() ?? 0n)}`);
    debugParts.push(`jalr_target=0x${formatU64(exports.debug_jalr_target?.() ?? 0n)}`);
  }
  const execveStage = exports.debug_execve_stage?.();
  if (execveStage !== undefined && execveStage !== 0) {
    debugParts.push(`execve_stage=${execveStage}`);
    debugParts.push(`execve_v0=${exports.debug_execve_value0?.().toString() ?? "n/a"}`);
    debugParts.push(`execve_v1=${exports.debug_execve_value1?.().toString() ?? "n/a"}`);
  }
  try {
    const pathBytes = encoder.encode("/proc/self/exe");
    const scratchBase = exports.kernel_scratch_base?.();
    if (scratchBase === undefined) {
      throw new Error("kernel_scratch_base unavailable");
    }
    const pathPtr = scratchBase;
    const bufPtr = scratchBase + 0x200;
    new Uint8Array(exports.memory.buffer, pathPtr, pathBytes.length).set(pathBytes);
    const ino = exports.memfs_resolve_path(pathPtr, pathBytes.length);
    if (ino === 0 || (exports.memfs_inode_mode(ino) & 0o170000) !== 0o120000) {
      debugParts.push("proc_self_exe=missing");
    } else {
      const len = Number(exports.memfs_read_link(ino, bufPtr, 1024));
      if (len <= 0) {
        debugParts.push("proc_self_exe=empty");
      } else {
        const target = new Uint8Array(exports.memory.buffer.slice(bufPtr, bufPtr + len));
        debugParts.push(`proc_self_exe=${JSON.stringify(decoder.decode(target))}`);
      }
    }
  } catch {
    debugParts.push("proc_self_exe=unavailable");
  }

  return debugParts;
}

function postStatus(status: number, waitInWorker = false): void {
  const includeDebug = status !== statusCodes.FS;
  const msg: ThreadWorkerOutbound = {
    type: "status",
    status,
    exitCode: exports.thread_exit_code(vmPtr),
    termSignal: exports.thread_term_signal(vmPtr),
    pc: exports.thread_pc(vmPtr),
    vmPtr,
    kernelState: readKernelRuntimeState(exports),
    syncEffects: readCompletedSyncEffects(status),
    debug: includeDebug ? (() => {
      const parts = collectDebugParts();
      return parts.length === 0 ? undefined : parts.join(" ");
    })() : undefined,
  };
  if (waitInWorker) {
    msg.waitInWorker = true;
  }
  workerPostMessage(msg);
}

function formatWorkerError(error: unknown): string {
  const base = error instanceof Error ? (error.stack ?? error.message) : String(error);
  const debugParts = collectDebugParts();
  return debugParts.length === 0 ? base : `${base}\n${debugParts.join(" ")}`;
}

function postError(error: unknown): void {
  const msg: ThreadWorkerOutbound = {
    type: "error",
    vmPtr,
    message: formatWorkerError(error),
  };
  workerPostMessage(msg);
}

function step(budget: number): void {
  for (;;) {
    clearSyscallTrace();
    exports.thread_step(vmPtr, budget);
    const status = exports.thread_status(vmPtr);

    if (status === statusCodes.FUTEX_WAIT) {
      if (handleFutexWait(status)) {
        budget = THREAD_STEP_BUDGET;
        continue;
      }
      return;
    }

    if (isRuntimeBridgeBlockedStatus(status)) {
      handleRuntimeBridgeBlockedStatus(status);
      return;
    }

    postStatus(status);
    return;
  }
}

function isRuntimeBridgeBlockedStatus(status: number): boolean {
  // §6.4: EPOLL_WAIT can stay in the thread-worker because another worker can
  // wake it through the runtime bridge. STATUS_SLEEP must stay owner-side so
  // its JS timer can fire while this worker is idle.
  return status === statusCodes.EPOLL_WAIT;
}

function checkPendingSignalsAtBoundary(): { status: number; delivered: boolean } {
  const beforePc = exports.thread_pc(vmPtr);
  const beforeSp = exports.thread_sp(vmPtr);
  clearSyscallTrace();
  exports.thread_step(vmPtr, 0);
  const afterPc = exports.thread_pc(vmPtr);
  const afterSp = exports.thread_sp(vmPtr);
  const status = exports.thread_status(vmPtr);
  return {
    status,
    delivered: status === statusCodes.OK
      && (afterPc !== beforePc || afterSp !== beforeSp),
  };
}

function handleRuntimeBridgeBlockedStatus(status: number): void {
  if (runtimeBridge === null) {
    postStatus(status);
    return;
  }

  let blockedStatus = status;
  for (;;) {
    postStatus(blockedStatus, true);

    const timeoutMs = Number(exports.thread_epoll_wait_timeout(vmPtr));
    const waitResult = timeoutMs < 0
      ? waitForRuntimeBridgeWake(runtimeBridge)
      : waitForRuntimeBridgeWake(runtimeBridge, Math.max(1, timeoutMs));

    reattachPublishedKernelRuntimeState();
    const boundary = checkPendingSignalsAtBoundary();
    if (boundary.delivered) {
      if (blockedStatus === statusCodes.EPOLL_WAIT) {
        const interrupted = exports.thread_interrupt_epoll_wait?.(vmPtr, -EINTR_ERRNO) ?? 0;
        if (interrupted === 0) {
          blockedStatus = exports.thread_status(vmPtr);
          if (!isRuntimeBridgeBlockedStatus(blockedStatus)) {
            postStatus(blockedStatus);
            return;
          }
          continue;
        }
      }
      postStatus(statusCodes.OK);
      return;
    }
    if (boundary.status !== statusCodes.OK) {
      blockedStatus = boundary.status;
      if (blockedStatus === statusCodes.FUTEX_WAIT) {
        if (handleFutexWait(blockedStatus)) {
          step(THREAD_STEP_BUDGET);
        }
        return;
      }
      if (!isRuntimeBridgeBlockedStatus(blockedStatus)) {
        postStatus(blockedStatus);
        return;
      }
      continue;
    }
    if (blockedStatus === statusCodes.EPOLL_WAIT && waitResult === "timed-out") {
      clearSyscallTrace();
      exports.thread_resume_epoll_wait_timeout(vmPtr);
    } else {
      clearSyscallTrace();
      exports.thread_resume_blocked_syscall(vmPtr);
    }

    blockedStatus = exports.thread_status(vmPtr);
    if (blockedStatus === statusCodes.FUTEX_WAIT) {
      if (handleFutexWait(blockedStatus)) {
        step(THREAD_STEP_BUDGET);
      }
      return;
    }
    if (!isRuntimeBridgeBlockedStatus(blockedStatus)) {
      postStatus(blockedStatus);
      return;
    }
  }
}

function handleFutexWait(status: number): boolean {
  const addr = guestBase + Number(exports.thread_futex_wait_addr(vmPtr));
  const i32 = new Int32Array(exports.memory.buffer);
  const wordIndex = addr >>> 2;
  const expected = exports.thread_futex_wait_expected(vmPtr);

  if (exports.thread_record_blocked_addr(vmPtr) === 0) {
    exports.thread_resume_futex_wait(vmPtr);
    return true;
  }

  postStatus(status, true);

  const timeoutPtr = Number(exports.thread_futex_wait_timeout(vmPtr));
  const timeoutMs = readTimespecMillis(timeoutPtr);
  const deadlineMs = timeoutMs === undefined ? undefined : Date.now() + timeoutMs;

  for (;;) {
    const waitMs = deadlineMs === undefined
      ? FUTEX_WAIT_SLICE_MS
      : Math.max(0, Math.min(FUTEX_WAIT_SLICE_MS, deadlineMs - Date.now()));
    const result = Atomics.wait(i32, wordIndex, expected, waitMs);

    reattachPublishedKernelRuntimeState();
    const stillBlockedAddr = exports.thread_record_blocked_addr(vmPtr);
    const hasPendingSignal = exports.thread_sig_pending(vmPtr) !== 0n;
    if (stillBlockedAddr !== 0 && hasPendingSignal) {
      clearSyscallTrace();
      exports.thread_resume_futex_wait_result(vmPtr, BigInt(-(exports.abi_eintr?.() ?? EINTR_ERRNO)));
      break;
    }
    if (stillBlockedAddr === 0) {
      clearSyscallTrace();
      exports.thread_resume_futex_wait(vmPtr);
      break;
    }
    if (result === "not-equal") {
      clearSyscallTrace();
      exports.thread_resume_futex_wait_result(vmPtr, BigInt(-exports.abi_eagain()));
      break;
    }
    if (deadlineMs !== undefined && Date.now() >= deadlineMs) {
      clearSyscallTrace();
      exports.thread_resume_futex_wait_result(vmPtr, BigInt(-110));
      break;
    }
  }

  postStatus(exports.thread_status(vmPtr));
  return false;
}

function readTimespecMillis(timespecPtr: number): number | undefined {
  if (timespecPtr === 0) {
    return undefined;
  }
  const view = new DataView(exports.memory.buffer);
  const absolute = guestBase + timespecPtr;
  const sec = Number(view.getBigUint64(absolute, true));
  const nsec = Number(view.getBigUint64(absolute + 8, true));
  return Math.max(0, sec * 1000 + Math.floor(nsec / 1_000_000));
}

await bindWorkerMessageHandler<ThreadWorkerInbound>(async (event) => {
  const msg = event.data;

  switch (msg.type) {
    case "init":
      try {
        await init(
          msg.wasmModule,
          msg.memory,
          msg.vmPtr,
          msg.guestBase,
          msg.kernelState,
          msg.sabPageCache,
          msg.runtimeNotify,
          msg.runtimeState,
        );
        postReady();
      } catch (error) {
        postError(error);
      }
      break;
    case "step":
      try {
        attachKernelRuntimeState(exports, msg.kernelState);
        step(msg.budget);
      } catch (error) {
        postError(error);
      }
      break;
  }
});
