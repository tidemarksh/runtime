import type { KernelExports } from "../abi.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function formatU64(value: bigint): string {
  return BigInt.asUintN(64, value).toString(16);
}

function readOptionalTraceU64(value: bigint | number | undefined): bigint {
  if (value === undefined) {
    return 0n;
  }
  return typeof value === "bigint" ? value : BigInt(value);
}

function debugScratchBase(exports: KernelExports): number {
  return (exports.kernel_scratch_base?.() ?? (exports.kernel_heap_ptr() + 0x10000));
}

function readOptionalThreadU64(
  reader: ((vmPtr: number) => bigint) | undefined,
  vmPtr: number,
): bigint {
  return reader?.(vmPtr) ?? 0n;
}

function readOptionalThreadI32(
  reader: ((vmPtr: number) => number) | undefined,
  vmPtr: number,
): number {
  return reader?.(vmPtr) ?? 0;
}

function readOptionalThreadReg(
  exports: KernelExports,
  vmPtr: number,
  regIndex: number,
): bigint {
  return exports.thread_reg?.(vmPtr, regIndex) ?? 0n;
}

function formatWriteTraceBytes(exports: KernelExports): string | null {
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

function formatTrackedEpollState(exports: KernelExports, vmPtr: number): string | null {
  const tablePtr = exports.kernel_epoll_table_ptr?.() ?? 0;
  const tableBytes = exports.kernel_epoll_table_bytes?.() ?? 0;
  const instanceBytes = exports.kernel_epoll_instance_bytes?.() ?? 0;
  if (tablePtr === 0 || tableBytes === 0 || instanceBytes < 4) {
    return null;
  }

  const table = new Uint32Array(exports.memory.buffer, tablePtr, tableBytes >>> 2);
  const view = new DataView(exports.memory.buffer);
  const entries: string[] = [];
  for (let fd = 0; fd < Math.min(exports.abi_max_fds(), 32); fd += 1) {
    const ofdId = exports.process_fd_ofd_id(vmPtr, fd);
    if (ofdId === 0 || exports.kernel_ofd_fd_type(ofdId) !== exports.abi_fd_type_epoll()) {
      continue;
    }
    const ino = exports.kernel_ofd_ino(ofdId);
    const ptr = ino >= 0 && ino < table.length ? table[ino] ?? 0 : 0;
    const count = ptr !== 0 ? view.getUint32(ptr + instanceBytes - 4, true) : 0;
    entries.push(`fd${fd}:${ofdId}/${ino}->0x${ptr.toString(16)}#${count}`);
  }

  return entries.length > 0 ? entries.join(",") : null;
}

function formatProcessThreads(exports: KernelExports, vmPtr: number): string | null {
  const threadCount = exports.thread_process_thread_count(vmPtr);
  if (threadCount <= 0) {
    return null;
  }
  const entries: string[] = [];
  const limit = Math.min(exports.abi_max_threads(), Math.max(threadCount, 8));
  for (let slot = 0; slot < limit; slot += 1) {
    const tid = exports.process_thread_tid(vmPtr, slot);
    const state = exports.process_thread_state(vmPtr, slot);
    const threadVmPtr = exports.process_thread_vm_ptr(vmPtr, slot);
    if (tid === 0 && state === 0 && threadVmPtr === 0) {
      continue;
    }
    entries.push(`${slot}:${tid}/${state}/0x${threadVmPtr.toString(16)}`);
  }
  return entries.length > 0 ? entries.join(",") : null;
}

function formatProcessFds(exports: KernelExports, vmPtr: number): string | null {
  const entries: string[] = [];
  const maxFds = Math.min(exports.abi_max_fds(), 32);
  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = exports.process_fd_ofd_id(vmPtr, fd);
    const fdFlags = exports.process_fd_flags(vmPtr, fd);
    if (ofdId === 0 && fdFlags === 0) {
      continue;
    }
    const fdType = ofdId === 0 ? 0 : exports.kernel_ofd_fd_type(ofdId);
    const ino = ofdId === 0 ? 0 : exports.kernel_ofd_ino(ofdId);
    entries.push(`${fd}:${ofdId}/${fdType}/${ino}/${fdFlags}`);
  }
  return entries.length > 0 ? entries.join(",") : null;
}

export function clearSyscallTrace(exports: KernelExports): void {
  exports.debug_clear_syscall_trace?.();
}

function readGuestInsn(exports: KernelExports, guestBase: number, pc: bigint): string | null {
  const addr = Number(pc);
  if (!Number.isFinite(addr) || addr < 0) {
    return null;
  }
  const absolute = guestBase + addr;
  if (absolute < 0 || absolute + 4 > exports.memory.buffer.byteLength) {
    return null;
  }
  const view = new DataView(exports.memory.buffer);
  return `0x${view.getUint32(absolute, true).toString(16)}`;
}

export function collectDebugParts(exports: KernelExports, vmPtr: number, guestBase: number): string[] {
  const debugParts: string[] = [];
  debugParts.push(`process=0x${exports.thread_process_ptr(vmPtr).toString(16)}`);
  debugParts.push(`process_threads=${exports.thread_process_thread_count(vmPtr)}`);
  debugParts.push(`tid=${readOptionalThreadI32(exports.thread_tid, vmPtr)}`);
  debugParts.push(`gp=0x${formatU64(readOptionalThreadU64(exports.thread_gp, vmPtr))}`);
  debugParts.push(`tp=0x${formatU64(readOptionalThreadU64(exports.thread_tp, vmPtr))}`);
  debugParts.push(`x27=0x${formatU64(readOptionalThreadU64(exports.thread_x27, vmPtr))}`);
  debugParts.push(`tls=0x${formatU64(readOptionalThreadU64(exports.thread_tls_base, vmPtr))}`);
  debugParts.push(`sig_mask=0x${formatU64(readOptionalThreadU64(exports.thread_sig_mask, vmPtr))}`);
  debugParts.push(`sig_pending=0x${formatU64(exports.thread_sig_pending(vmPtr))}`);
  debugParts.push(`sig_code=${readOptionalThreadI32(exports.thread_sig_info_code, vmPtr)}`);
  debugParts.push(`blocked_fd=${exports.thread_blocked_fd?.(vmPtr) ?? 0}`);
  debugParts.push(`blocked_ofd_id=${exports.thread_blocked_ofd_id?.(vmPtr) ?? 0}`);
  debugParts.push(`epoll_timeout_ms=${exports.thread_epoll_wait_timeout?.(vmPtr)?.toString() ?? "n/a"}`);
  debugParts.push(`fault_pc=0x${formatU64(readOptionalThreadU64(exports.thread_fault_pc, vmPtr))}`);
  debugParts.push(`fault_addr=0x${formatU64(readOptionalThreadU64(exports.thread_fault_addr, vmPtr))}`);
  debugParts.push(`pc=0x${exports.thread_pc(vmPtr).toString(16)}`);
  debugParts.push(`ra=0x${readOptionalThreadU64(exports.thread_ra, vmPtr).toString(16)}`);
  debugParts.push(`sp=0x${exports.thread_sp(vmPtr).toString(16)}`);
  debugParts.push(`stk0=0x${readOptionalThreadU64((ptr) => exports.thread_stack_word?.(ptr, 0) ?? 0n, vmPtr).toString(16)}`);
  debugParts.push(`stk1=0x${readOptionalThreadU64((ptr) => exports.thread_stack_word?.(ptr, 1) ?? 0n, vmPtr).toString(16)}`);
  debugParts.push(`stk4=0x${readOptionalThreadU64((ptr) => exports.thread_stack_word?.(ptr, 4) ?? 0n, vmPtr).toString(16)}`);
  debugParts.push(`stk8=0x${readOptionalThreadU64((ptr) => exports.thread_stack_word?.(ptr, 8) ?? 0n, vmPtr).toString(16)}`);
  debugParts.push(`stk10=0x${readOptionalThreadU64((ptr) => exports.thread_stack_word?.(ptr, 10) ?? 0n, vmPtr).toString(16)}`);
  debugParts.push(`stk14=0x${readOptionalThreadU64((ptr) => exports.thread_stack_word?.(ptr, 14) ?? 0n, vmPtr).toString(16)}`);
  debugParts.push(`stk18=0x${readOptionalThreadU64((ptr) => exports.thread_stack_word?.(ptr, 18) ?? 0n, vmPtr).toString(16)}`);
  debugParts.push(`stk19=0x${readOptionalThreadU64((ptr) => exports.thread_stack_word?.(ptr, 19) ?? 0n, vmPtr).toString(16)}`);
  debugParts.push(`a0=${exports.thread_a0(vmPtr).toString()}`);
  debugParts.push(`a1=${exports.thread_a1(vmPtr).toString()}`);
  debugParts.push(`a2=0x${formatU64(readOptionalThreadReg(exports, vmPtr, 12))}`);
  debugParts.push(`a3=0x${formatU64(readOptionalThreadReg(exports, vmPtr, 13))}`);
  debugParts.push(`a4=0x${formatU64(readOptionalThreadReg(exports, vmPtr, 14))}`);
  debugParts.push(`a5=0x${formatU64(readOptionalThreadReg(exports, vmPtr, 15))}`);
  debugParts.push(`a6=0x${formatU64(readOptionalThreadReg(exports, vmPtr, 16))}`);
  debugParts.push(`a7=0x${formatU64(readOptionalThreadReg(exports, vmPtr, 17))}`);
  debugParts.push(`t3=0x${formatU64(readOptionalThreadReg(exports, vmPtr, 28))}`);
  debugParts.push(`t5=0x${formatU64(readOptionalThreadReg(exports, vmPtr, 30))}`);
  const tpWriteTid = exports.debug_tp_write_tid?.() ?? 0;
  if (tpWriteTid !== 0) {
    debugParts.push(`tp_write_tid=${tpWriteTid}`);
  }
  const cloneFlags = exports.thread_clone_flags(vmPtr);
  if (cloneFlags !== 0n) {
    debugParts.push(`clone_flags=0x${formatU64(cloneFlags)}`);
    debugParts.push(`clone_child_stack=0x${formatU64(readOptionalThreadU64(exports.thread_clone_child_stack, vmPtr))}`);
    debugParts.push(`clone_tls=0x${formatU64(readOptionalThreadU64(exports.thread_clone_tls, vmPtr))}`);
    debugParts.push(`clone_child_tidptr=0x${formatU64(readOptionalThreadU64(exports.thread_clone_child_tidptr, vmPtr))}`);
  }
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
    const writeBytes = formatWriteTraceBytes(exports);
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
  const memBrkBase = exports.debug_memory_brk_base?.();
  if (memBrkBase !== undefined) {
    debugParts.push(`heap_base=0x${exports.kernel_heap_base?.().toString(16) ?? "0"}`);
    debugParts.push(`heap_ptr=0x${exports.kernel_heap_ptr?.().toString(16) ?? "0"}`);
    debugParts.push(`mem_state_ptr=0x${exports.kernel_mem_state_ptr?.().toString(16) ?? "0"}`);
    debugParts.push(`mem_brk_base=0x${memBrkBase.toString(16)}`);
    debugParts.push(`mem_brk_current=0x${exports.debug_memory_brk_current?.().toString(16) ?? "n/a"}`);
    debugParts.push(`mem_mmap_next=0x${exports.debug_memory_mmap_next?.().toString(16) ?? "n/a"}`);
    const vmaLen = exports.debug_memory_vma_len?.() ?? 0;
    debugParts.push(`mem_vmas=${vmaLen}`);
    for (let i = 0; i < Math.min(vmaLen, 4); i += 1) {
      debugParts.push(
        `mem_vma${i}=0x${exports.debug_memory_vma_start?.(i).toString(16) ?? "0"}-0x${exports.debug_memory_vma_end?.(i).toString(16) ?? "0"}` +
        `:${exports.debug_memory_vma_prot?.(i) ?? "n/a"}/${exports.debug_memory_vma_flags?.(i) ?? "n/a"}`,
      );
    }
  }
  const guestMemAddr = exports.debug_guest_mem_last_addr?.();
  if (guestMemAddr !== undefined && guestMemAddr !== 0n) {
    debugParts.push(`guest_mem_addr=0x${formatU64(guestMemAddr)}`);
    debugParts.push(`guest_mem_len=${exports.debug_guest_mem_last_len?.() ?? "n/a"}`);
    debugParts.push(`guest_mem_kind=${exports.debug_guest_mem_last_kind?.() ?? "n/a"}`);
  }
  const epollWaitReason = exports.debug_epoll_wait_reason?.() ?? 0;
  if (epollWaitReason !== 0) {
    debugParts.push(`epoll_wait_fd=${exports.debug_epoll_wait_epfd?.().toString() ?? "n/a"}`);
    debugParts.push(`epoll_wait_ofd=${exports.debug_epoll_wait_ofd_id?.() ?? "n/a"}`);
    debugParts.push(`epoll_wait_type=${exports.debug_epoll_wait_ofd_type?.() ?? "n/a"}`);
    debugParts.push(`epoll_wait_idx=${exports.debug_epoll_wait_idx?.() ?? "n/a"}`);
    debugParts.push(`epoll_wait_ptr=0x${(exports.debug_epoll_wait_ptr?.() ?? 0).toString(16)}`);
    debugParts.push(`epoll_wait_count=${exports.debug_epoll_wait_count?.() ?? "n/a"}`);
    debugParts.push(`epoll_wait_result=${exports.debug_epoll_wait_result?.().toString() ?? "n/a"}`);
    debugParts.push(`epoll_wait_reason=${epollWaitReason}`);
  }
  const signalDeliverySig = exports.debug_signal_delivery_sig?.();
  if (signalDeliverySig !== undefined && signalDeliverySig !== 0) {
    debugParts.push(`sig_deliver=${signalDeliverySig}`);
    debugParts.push(`sig_frame=0x${formatU64(exports.debug_signal_delivery_frame_base?.() ?? 0n)}`);
    debugParts.push(`sig_pc=0x${formatU64(exports.debug_signal_delivery_pc?.() ?? 0n)}`);
    debugParts.push(`sig_ra=0x${formatU64(exports.debug_signal_delivery_ra?.() ?? 0n)}`);
    debugParts.push(`sig_sp=0x${formatU64(exports.debug_signal_delivery_sp?.() ?? 0n)}`);
    debugParts.push(`sig_x27=0x${formatU64(exports.debug_signal_delivery_x27?.() ?? 0n)}`);
  }
  const sigreturnPc = exports.debug_sigreturn_pc?.();
  if (sigreturnPc !== undefined && sigreturnPc !== 0n) {
    debugParts.push(`sigret_frame=0x${formatU64(exports.debug_sigreturn_frame_base?.() ?? 0n)}`);
    debugParts.push(`sigret_pc=0x${formatU64(sigreturnPc)}`);
    debugParts.push(`sigret_ra=0x${formatU64(exports.debug_sigreturn_ra?.() ?? 0n)}`);
    debugParts.push(`sigret_sp=0x${formatU64(exports.debug_sigreturn_sp?.() ?? 0n)}`);
    debugParts.push(`sigret_x27=0x${formatU64(exports.debug_sigreturn_x27?.() ?? 0n)}`);
  }
  const cloneSpawnTid = exports.debug_clone_spawn_tid?.();
  if (cloneSpawnTid !== undefined && cloneSpawnTid !== 0) {
    debugParts.push(`clone_tid=${cloneSpawnTid}`);
    debugParts.push(`clone_pc=0x${formatU64(exports.debug_clone_spawn_pc?.() ?? 0n)}`);
    debugParts.push(`clone_ra=0x${formatU64(exports.debug_clone_spawn_ra?.() ?? 0n)}`);
    debugParts.push(`clone_sp=0x${formatU64(exports.debug_clone_spawn_sp?.() ?? 0n)}`);
    debugParts.push(`clone_x27=0x${formatU64(exports.debug_clone_spawn_x27?.() ?? 0n)}`);
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
      const arg0OfdId = exports.debug_fd_syscall_trace_arg0_ofd_id?.(index) ?? 0;
      const arg0OfdType = exports.debug_fd_syscall_trace_arg0_ofd_type?.(index) ?? 0;
      const arg1 = readOptionalTraceU64(exports.debug_fd_syscall_trace_arg1?.(index));
      const arg2 = readOptionalTraceU64(exports.debug_fd_syscall_trace_arg2?.(index));
      const arg2OfdId = exports.debug_fd_syscall_trace_arg2_ofd_id?.(index) ?? 0;
      const arg2OfdType = exports.debug_fd_syscall_trace_arg2_ofd_type?.(index) ?? 0;
      const arg3 = readOptionalTraceU64(exports.debug_fd_syscall_trace_arg3?.(index));
      const result = readOptionalTraceU64(exports.debug_fd_syscall_trace_result?.(index));
      const status = exports.debug_fd_syscall_trace_status?.(index) ?? 0;
      const traceVmPtr = exports.debug_fd_syscall_trace_vm_ptr?.(index) ?? 0;
      traces.push(
        `0x${traceVmPtr.toString(16)}:${nr.toString()}(${arg0.toString()}[${arg0OfdId}/${arg0OfdType}],${arg1.toString()},${arg2.toString()}[${arg2OfdId}/${arg2OfdType}],${arg3.toString()})=`
        + `${BigInt.asIntN(64, result).toString()}/${status}`,
      );
    }
    if (traces.length > 0) {
      debugParts.push(`fd_syscalls=[${traces.join(",")}]`);
    }
  }
  const pathTraceLen = exports.debug_path_syscall_trace_len?.() ?? 0;
  if (pathTraceLen > 0) {
    const traces: string[] = [];
    for (let index = 0; index < pathTraceLen; index += 1) {
      const nr = readOptionalTraceU64(exports.debug_path_syscall_trace_nr?.(index));
      if (nr === 0n) {
        continue;
      }
      const pathPtr = exports.debug_path_syscall_trace_path_ptr?.(index) ?? 0;
      const pathLen = exports.debug_path_syscall_trace_path_len?.(index) ?? 0;
      const path = pathPtr !== 0 && pathLen > 0
        ? decoder.decode(new Uint8Array(exports.memory.buffer, pathPtr, pathLen))
        : "";
      const traceVmPtr = exports.debug_path_syscall_trace_vm_ptr?.(index) ?? 0;
      const dirfd = readOptionalTraceU64(exports.debug_path_syscall_trace_dirfd?.(index));
      const result = readOptionalTraceU64(exports.debug_path_syscall_trace_result?.(index));
      const status = exports.debug_path_syscall_trace_status?.(index) ?? 0;
      traces.push(
        `0x${traceVmPtr.toString(16)}:${nr.toString()}(${BigInt.asIntN(64, dirfd).toString()},${JSON.stringify(path)})=`
        + `${BigInt.asIntN(64, result).toString()}/${status}`,
      );
    }
    if (traces.length > 0) {
      debugParts.push(`path_syscalls=[${traces.join(",")}]`);
    }
  }
  const threadCountTraceLen = exports.debug_thread_count_trace_len?.() ?? 0;
  if (threadCountTraceLen > 0) {
    const traces: string[] = [];
    for (let index = 0; index < threadCountTraceLen; index += 1) {
      const kind = exports.debug_thread_count_trace_kind?.(index) ?? 0;
      if (kind === 0) {
        continue;
      }
      const traceVmPtr = exports.debug_thread_count_trace_vm_ptr?.(index) ?? 0;
      const traceTid = exports.debug_thread_count_trace_tid?.(index) ?? 0;
      const traceProcessPtr = exports.debug_thread_count_trace_process_ptr?.(index) ?? 0;
      const before = exports.debug_thread_count_trace_before?.(index) ?? 0;
      const after = exports.debug_thread_count_trace_after?.(index) ?? 0;
      traces.push(
        `${kind}:${traceTid}@0x${traceVmPtr.toString(16)}/0x${traceProcessPtr.toString(16)}:${before}->${after}`,
      );
    }
    if (traces.length > 0) {
      debugParts.push(`thread_counts=[${traces.join(",")}]`);
    }
  }
  const trackedEpollState = formatTrackedEpollState(exports, vmPtr);
  if (trackedEpollState !== null) {
    debugParts.push(`epoll_state=[${trackedEpollState}]`);
  }
  const processThreads = formatProcessThreads(exports, vmPtr);
  if (processThreads !== null) {
    debugParts.push(`process_slots=[${processThreads}]`);
  }
  const processFds = formatProcessFds(exports, vmPtr);
  if (processFds !== null) {
    debugParts.push(`process_fds=[${processFds}]`);
  }
  const zeroRegPc = exports.debug_zero_reg_pc?.();
  if (zeroRegPc !== undefined && zeroRegPc !== 0n) {
    debugParts.push(`zero_reg_pc=0x${formatU64(zeroRegPc)}`);
    debugParts.push(`zero_reg_raw=0x${(exports.debug_zero_reg_raw?.() ?? 0).toString(16)}`);
    debugParts.push(`zero_reg=${exports.debug_zero_reg_index?.() ?? "n/a"}`);
    debugParts.push(`zero_reg_prev=0x${formatU64(exports.debug_zero_reg_prev_value?.() ?? 0n)}`);
    debugParts.push(`zero_reg_sp=0x${formatU64(exports.debug_zero_reg_sp?.() ?? 0n)}`);
    debugParts.push(`zero_reg_x27=0x${formatU64(exports.debug_zero_reg_x27?.() ?? 0n)}`);
  }
  const x27WritePc = exports.debug_x27_write_pc?.();
  if (x27WritePc !== undefined && x27WritePc !== 0n) {
    debugParts.push(`x27_write_pc=0x${formatU64(x27WritePc)}`);
    debugParts.push(`x27_write_raw=0x${(exports.debug_x27_write_raw?.() ?? 0).toString(16)}`);
    debugParts.push(`x27_write_src=${exports.debug_x27_write_source?.() ?? "n/a"}`);
    debugParts.push(
      `x27_write_prev=0x${formatU64(exports.debug_x27_write_prev_value?.() ?? 0n)}`,
    );
    debugParts.push(`x27_write=0x${formatU64(exports.debug_x27_write_value?.() ?? 0n)}`);
  }
  const tpWritePc = exports.debug_tp_write_pc?.();
  if (tpWritePc !== undefined && tpWritePc !== 0n) {
    debugParts.push(`tp_write_pc=0x${formatU64(tpWritePc)}`);
    debugParts.push(`tp_write_raw=0x${(exports.debug_tp_write_raw?.() ?? 0).toString(16)}`);
    const tpWriteInsn = readGuestInsn(exports, guestBase, tpWritePc);
    if (tpWriteInsn !== null) {
      debugParts.push(`tp_write_insn=${tpWriteInsn}`);
    }
    debugParts.push(`tp_write_src=${exports.debug_tp_write_source?.() ?? "n/a"}`);
    debugParts.push(`tp_write_prev=0x${formatU64(exports.debug_tp_write_prev_value?.() ?? 0n)}`);
    debugParts.push(`tp_write=0x${formatU64(exports.debug_tp_write_value?.() ?? 0n)}`);
    debugParts.push(`tp_write_rs1=${exports.debug_tp_write_rs1?.() ?? "n/a"}`);
    debugParts.push(`tp_write_rs1v=0x${formatU64(exports.debug_tp_write_rs1_value?.() ?? 0n)}`);
  }
  const spWritePc = exports.debug_sp_write_pc?.();
  if (spWritePc !== undefined && spWritePc !== 0n) {
    debugParts.push(`sp_write_pc=0x${formatU64(spWritePc)}`);
    debugParts.push(`sp_write_raw=0x${(exports.debug_sp_write_raw?.() ?? 0).toString(16)}`);
    const spWriteInsn = readGuestInsn(exports, guestBase, spWritePc);
    if (spWriteInsn !== null) {
      debugParts.push(`sp_write_insn=${spWriteInsn}`);
    }
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
    const scratchBase = debugScratchBase(exports);
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

export function formatWorkerError(
  exports: KernelExports,
  vmPtr: number,
  guestBase: number,
  error: unknown,
): string {
  const base = error instanceof Error ? (error.stack ?? error.message) : String(error);
  const debugParts = collectDebugParts(exports, vmPtr, guestBase);
  return debugParts.length === 0 ? base : `${base}\n${debugParts.join(" ")}`;
}
