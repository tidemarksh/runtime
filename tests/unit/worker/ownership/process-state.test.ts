import { expect, test, vi } from "vite-plus/test";
import {
  restoreProcessIdentity,
  restoreProcessState,
  snapshotProcessIdentity,
  snapshotProcessState,
} from "../../../../src/worker/ownership/process-state.js";
import type { KernelExports } from "../../../../src/abi.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

function makeHandle(state: {
  processPtr: number;
  pid: number;
  ppid: number;
  nextPid: number;
}) {
  const restore = vi.fn();
  const handle = {
    pid: state.pid,
    kernel: {
      thread_process_ptr: () => state.processPtr,
      process_current_pid: () => state.pid,
      process_current_ppid: () => state.ppid,
      process_next_pid: () => state.nextPid,
      process_restore_current_identity: restore,
    },
  } as unknown as ProcessHandle;
  return { handle, restore };
}

test("restoreProcessIdentity repairs identity drift within the same process", () => {
  const state = { processPtr: 0x2000, pid: 1, ppid: 0, nextPid: 4 };
  const { handle, restore } = makeHandle(state);
  const snapshot = snapshotProcessIdentity(handle, 0x1234);

  state.pid = 77;
  restoreProcessIdentity(handle, 0x1234, snapshot);

  expect(restore).toHaveBeenCalledWith(0x1234, 1, 0, 4);
});

test("restoreProcessIdentity does not apply a snapshot across a process pointer change", () => {
  const state = { processPtr: 0x2000, pid: 1, ppid: 0, nextPid: 4 };
  const { handle, restore } = makeHandle(state);
  const snapshot = snapshotProcessIdentity(handle, 0x1234);

  state.processPtr = 0x3000;
  state.pid = 4;
  restoreProcessIdentity(handle, 0x1234, snapshot);

  expect(restore).not.toHaveBeenCalled();
});

test("restoreProcessIdentity does not replay a foreign vfork identity into the handle process", () => {
  const state = { processPtr: 0x2000, pid: 3, ppid: 1, nextPid: 7 };
  const { handle, restore } = makeHandle(state);
  handle.pid = 1;

  restoreProcessIdentity(handle, 0x1234, {
    processPtr: 0x2000,
    pid: 3,
    ppid: 1,
    nextPid: 7,
  });

  expect(restore).not.toHaveBeenCalled();
});

test("snapshotProcessState rejects a transient foreign vfork process", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const kernel = {
    process_current_pid: () => 3,
    thread_process_ptr: () => 0x100,
    process_state_bytes: () => 64,
  } as unknown as KernelExports;
  const handle = {
    guestBase: 0,
    kernel,
    memory,
    pid: 1,
    vmPtr: 0x1234,
  } as unknown as ProcessHandle;

  expect(snapshotProcessState(handle)).toBeNull();
});

test("restoreProcessState preserves fd generations with the current fd table", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const offset = 0x100;
  const maxFds = 2;
  const bytes = new Uint8Array(64);
  new DataView(bytes.buffer).setUint32(maxFds * 8, 3, true);
  const view = new DataView(memory.buffer);
  view.setUint32(offset, 7, true);
  view.setUint32(offset + maxFds * 8, 9, true);
  const zero = () => 0;
  const kernel = {
    abi_max_fds: () => maxFds,
    kernel_heap_base: zero,
    kernel_heap_ptr: zero,
    kernel_ofd_table_ptr: zero,
    kernel_memfs_ptr: zero,
    kernel_mem_state_ptr: zero,
    kernel_epoll_table_ptr: zero,
    kernel_socket_table_ptr: zero,
    kernel_socket_used: zero,
    kernel_pipe_base: zero,
    kernel_pipe_used: () => 0n,
    kernel_net_buf_base: zero,
    kernel_net_buf_used: () => 0n,
    kernel_sig_slots_ptr: zero,
    kernel_process_pending_ptr: zero,
    kernel_process_runtime_state_ptr: zero,
    kernel_time_state_ptr: zero,
    kernel_attach_runtime_state: vi.fn(),
  } as unknown as KernelExports;
  const handle = {
    memory,
    kernel,
    vmPtr: 0x1234,
  } as unknown as ProcessHandle;
  Object.assign(kernel, { thread_process_ptr: () => offset });

  restoreProcessState(handle, { processPtr: offset, offset, bytes }, { preserveCurrentFdTable: true });

  expect(view.getUint32(offset, true)).toBe(7);
  expect(view.getUint32(offset + maxFds * 8, true)).toBe(9);
});

test("restoreProcessState preserves live synchronization and thread registry state", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const offset = 0x100;
  const maxFds = 2;
  const fdStateBytes = maxFds * 12;
  const runtimeSyncBytes = 4 + (64 * 4 * 2);
  const metadataBytes = 9 * 4;
  const threadStateOffset = fdStateBytes + runtimeSyncBytes + metadataBytes;
  const bytes = new Uint8Array(threadStateOffset + 32).fill(0x11);
  const target = new Uint8Array(memory.buffer, offset, bytes.length);
  target.fill(0x22);
  target.fill(0xaa, fdStateBytes, fdStateBytes + runtimeSyncBytes);
  target.fill(0xbb, threadStateOffset);
  const zero = () => 0;
  const kernel = {
    abi_max_fds: () => maxFds,
    thread_process_ptr: () => offset,
    kernel_heap_base: zero,
    kernel_heap_ptr: zero,
    kernel_ofd_table_ptr: zero,
    kernel_memfs_ptr: zero,
    kernel_mem_state_ptr: zero,
    kernel_epoll_table_ptr: zero,
    kernel_socket_table_ptr: zero,
    kernel_socket_used: zero,
    kernel_pipe_base: zero,
    kernel_pipe_used: () => 0n,
    kernel_net_buf_base: zero,
    kernel_net_buf_used: () => 0n,
    kernel_sig_slots_ptr: zero,
    kernel_process_pending_ptr: zero,
    kernel_process_runtime_state_ptr: zero,
    kernel_time_state_ptr: zero,
    kernel_attach_runtime_state: vi.fn(),
  } as unknown as KernelExports;
  const handle = { memory, kernel, vmPtr: 0x1234 } as unknown as ProcessHandle;

  restoreProcessState(handle, { processPtr: offset, offset, bytes });

  expect([...target.slice(fdStateBytes, fdStateBytes + runtimeSyncBytes)])
    .toEqual(new Array(runtimeSyncBytes).fill(0xaa));
  expect([...target.slice(fdStateBytes + runtimeSyncBytes, threadStateOffset)])
    .toEqual(new Array(metadataBytes).fill(0x11));
  expect([...target.slice(threadStateOffset)])
    .toEqual(new Array(32).fill(0xbb));
});

test("restoreProcessState rejects a snapshot after process pointer changes", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const offset = 0x100;
  const current = new Uint8Array(memory.buffer, offset, 64).fill(0xaa);
  const bytes = new Uint8Array(64).fill(0x11);
  const kernel = {
    thread_process_ptr: () => 0x3000,
  } as unknown as KernelExports;
  const handle = { memory, kernel, vmPtr: 0x1234 } as unknown as ProcessHandle;

  restoreProcessState(handle, { processPtr: 0x2000, offset, bytes });

  expect([...current]).toEqual(new Array(64).fill(0xaa));
});
