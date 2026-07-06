import { expect, test, vi } from "vite-plus/test";
import type { KernelExports } from "../../../src/abi.js";
import type { ProcessHandle } from "../../../src/worker/types.js";
import {
  decodeSupervisorFdControlRecord,
  decodeSupervisorFsControlRecord,
  decodeSupervisorSignalControlRecord,
  encodeSupervisorShadowThreadEvent,
  formatSupervisorFdControlRecord,
  formatSupervisorFsControlRecord,
  formatSupervisorSignalControlRecord,
  hasSupervisorShadowProtocol,
  pollSupervisorFdControlDiagnostic,
  pollSupervisorFsControlDiagnostic,
  pollSupervisorSignalControlDiagnostic,
  readSupervisorFdControlRecord,
  readSupervisorFsControlRecord,
  readSupervisorSignalControlRecord,
  submitSupervisorShadowStatusEvent,
  submitSupervisorShadowThreadEvent,
  supervisorThreadEventKindForStatus,
  type SupervisorShadowThreadEvent,
} from "../../../src/worker/supervisor-shadow.js";

const SUPERVISOR_SHADOW_THREAD_EVENT_MAGIC = 0x5645_5354;
const SUPERVISOR_SHADOW_THREAD_EVENT_RECORD_BYTES = 80;
const SUPERVISOR_SHADOW_THREAD_EVENT_VERSION = 1;
const SUPERVISOR_THREAD_EVENT_KIND_BLOCKED = 1;
const SUPERVISOR_THREAD_EVENT_KIND_READY = 2;
const SUPERVISOR_THREAD_EVENT_KIND_FAULTED = 3;
const SUPERVISOR_FD_CONTROL_RECORD_MAGIC = 0x4446_5354;
const SUPERVISOR_FD_CONTROL_RECORD_BYTES = 96;
const SUPERVISOR_FD_CONTROL_KIND_PIPE_CREATED = 1;
const SUPERVISOR_FD_CONTROL_KIND_FD_CLOSED = 2;
const SUPERVISOR_FD_CONTROL_KIND_FD_DUPLICATED = 3;
const SUPERVISOR_FD_CONTROL_KIND_FD_FLAGS_UPDATED = 4;
const SUPERVISOR_FD_CONTROL_KIND_EPOLL_CREATED = 5;
const SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_ADD = 6;
const SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_DEL = 7;
const SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_MOD = 8;
const SUPERVISOR_FD_CONTROL_KIND_SOCKET_CREATED = 9;
const SUPERVISOR_FD_CONTROL_KIND_SOCKETPAIR_CREATED = 10;
const SUPERVISOR_FD_CONTROL_KIND_SOCKET_BOUND = 11;
const SUPERVISOR_FD_CONTROL_KIND_SOCKET_LISTENING = 12;
const SUPERVISOR_FD_CONTROL_KIND_SOCKET_CONNECT_STARTED = 13;
const SUPERVISOR_FD_CONTROL_KIND_SOCKET_CONNECTED = 14;
const SUPERVISOR_FD_CONTROL_KIND_SOCKET_CONNECT_FAILED = 15;
const SUPERVISOR_FD_CONTROL_KIND_SOCKET_ACCEPTED = 16;
const SUPERVISOR_FS_CONTROL_RECORD_MAGIC = 0x5346_5354;
const SUPERVISOR_FS_CONTROL_RECORD_BYTES = 112;
const SUPERVISOR_FS_CONTROL_KIND_PAGE_CACHE_MISS = 1;
const SUPERVISOR_FS_CONTROL_KIND_PAGE_CACHE_FILL = 2;
const SUPERVISOR_FS_CONTROL_KIND_DIRTY_PAGE_PUBLISHED = 3;
const SUPERVISOR_FS_CONTROL_KIND_DIRTY_PAGE_FLUSHED = 4;
const SUPERVISOR_SIGNAL_CONTROL_RECORD_MAGIC = 0x4753_5354;
const SUPERVISOR_SIGNAL_CONTROL_RECORD_BYTES = 120;
const SUPERVISOR_SIGNAL_CONTROL_KIND_BOUNDARY_CHECK = 1;
const SUPERVISOR_SIGNAL_CONTROL_KIND_SIGNAL_DELIVERED = 2;
const SUPERVISOR_SIGNAL_CONTROL_KIND_BLOCKED_WAIT_INTERRUPTED = 3;

function makeSupervisorShadowKernelExports(
  overrides: Partial<KernelExports> = {},
): KernelExports {
  return {
    memory: new WebAssembly.Memory({ initial: 1 }),
    supervisor_abi_version: () => 1,
    supervisor_thread_event_record_bytes: () => SUPERVISOR_SHADOW_THREAD_EVENT_RECORD_BYTES,
    supervisor_thread_event_record_magic: () => SUPERVISOR_SHADOW_THREAD_EVENT_MAGIC,
    supervisor_thread_event_kind_blocked: () => SUPERVISOR_THREAD_EVENT_KIND_BLOCKED,
    supervisor_thread_event_kind_ready: () => SUPERVISOR_THREAD_EVENT_KIND_READY,
    supervisor_thread_event_kind_faulted: () => SUPERVISOR_THREAD_EVENT_KIND_FAULTED,
    supervisor_fd_control_record_bytes: () => SUPERVISOR_FD_CONTROL_RECORD_BYTES,
    supervisor_fd_control_record_magic: () => SUPERVISOR_FD_CONTROL_RECORD_MAGIC,
    supervisor_fd_control_kind_pipe_created: () => SUPERVISOR_FD_CONTROL_KIND_PIPE_CREATED,
    supervisor_fd_control_kind_fd_closed: () => SUPERVISOR_FD_CONTROL_KIND_FD_CLOSED,
    supervisor_fd_control_kind_fd_duplicated: () => SUPERVISOR_FD_CONTROL_KIND_FD_DUPLICATED,
    supervisor_fd_control_kind_fd_flags_updated: () => SUPERVISOR_FD_CONTROL_KIND_FD_FLAGS_UPDATED,
    supervisor_fd_control_kind_epoll_created: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CREATED,
    supervisor_fd_control_kind_epoll_ctl_add: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_ADD,
    supervisor_fd_control_kind_epoll_ctl_del: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_DEL,
    supervisor_fd_control_kind_epoll_ctl_mod: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_MOD,
    supervisor_fd_control_kind_socket_created: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_CREATED,
    supervisor_fd_control_kind_socketpair_created: () => SUPERVISOR_FD_CONTROL_KIND_SOCKETPAIR_CREATED,
    supervisor_fd_control_kind_socket_bound: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_BOUND,
    supervisor_fd_control_kind_socket_listening: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_LISTENING,
    supervisor_fd_control_kind_socket_connect_started: () =>
      SUPERVISOR_FD_CONTROL_KIND_SOCKET_CONNECT_STARTED,
    supervisor_fd_control_kind_socket_connected: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_CONNECTED,
    supervisor_fd_control_kind_socket_connect_failed: () =>
      SUPERVISOR_FD_CONTROL_KIND_SOCKET_CONNECT_FAILED,
    supervisor_fd_control_kind_socket_accepted: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_ACCEPTED,
    supervisor_fs_control_record_bytes: () => SUPERVISOR_FS_CONTROL_RECORD_BYTES,
    supervisor_fs_control_record_magic: () => SUPERVISOR_FS_CONTROL_RECORD_MAGIC,
    supervisor_fs_control_kind_page_cache_miss: () => SUPERVISOR_FS_CONTROL_KIND_PAGE_CACHE_MISS,
    supervisor_fs_control_kind_page_cache_fill: () => SUPERVISOR_FS_CONTROL_KIND_PAGE_CACHE_FILL,
    supervisor_fs_control_kind_dirty_page_published: () =>
      SUPERVISOR_FS_CONTROL_KIND_DIRTY_PAGE_PUBLISHED,
    supervisor_fs_control_kind_dirty_page_flushed: () =>
      SUPERVISOR_FS_CONTROL_KIND_DIRTY_PAGE_FLUSHED,
    supervisor_signal_control_record_bytes: () => SUPERVISOR_SIGNAL_CONTROL_RECORD_BYTES,
    supervisor_signal_control_record_magic: () => SUPERVISOR_SIGNAL_CONTROL_RECORD_MAGIC,
    supervisor_signal_control_kind_boundary_check: () =>
      SUPERVISOR_SIGNAL_CONTROL_KIND_BOUNDARY_CHECK,
    supervisor_signal_control_kind_signal_delivered: () =>
      SUPERVISOR_SIGNAL_CONTROL_KIND_SIGNAL_DELIVERED,
    supervisor_signal_control_kind_blocked_wait_interrupted: () =>
      SUPERVISOR_SIGNAL_CONTROL_KIND_BLOCKED_WAIT_INTERRUPTED,
    ...overrides,
  } as KernelExports;
}

function makeEvent(): SupervisorShadowThreadEvent {
  return {
    eventKind: SUPERVISOR_THREAD_EVENT_KIND_BLOCKED,
    sequence: 3n,
    vmPtr: 0x2000n,
    pid: 7,
    tid: 11,
    status: 20,
    syncEffects: 4,
    blockedReason: 2,
    pc: 0x1234n,
    sp: 0x5678n,
  };
}

function writeFdControlRecord(
  view: DataView,
  kind = SUPERVISOR_FD_CONTROL_KIND_PIPE_CREATED,
  seq = 7n,
): void {
  view.setUint32(0, SUPERVISOR_FD_CONTROL_RECORD_MAGIC, true);
  view.setUint32(4, 1, true);
  view.setUint32(8, SUPERVISOR_FD_CONTROL_RECORD_BYTES, true);
  view.setUint32(12, kind, true);
  view.setBigUint64(16, seq, true);
  view.setUint32(24, 0x1234, true);
  view.setUint32(28, 0x2000, true);
  view.setUint32(32, 9, true);
  view.setUint32(36, 3, true);
  view.setUint32(40, 4, true);
  view.setUint32(44, 11, true);
  view.setUint32(48, 12, true);
  view.setUint32(52, 2, true);
  view.setUint32(56, 5, true);
  view.setUint32(60, 6, true);
  view.setBigInt64(64, 0n, true);
  view.setBigUint64(72, 0x80800n, true);
  view.setBigUint64(80, 1n, true);
  view.setBigUint64(88, 1n, true);
}

function writeFsControlRecord(
  view: DataView,
  kind = SUPERVISOR_FS_CONTROL_KIND_PAGE_CACHE_MISS,
  seq = 9n,
): void {
  view.setUint32(0, SUPERVISOR_FS_CONTROL_RECORD_MAGIC, true);
  view.setUint32(4, 1, true);
  view.setUint32(8, SUPERVISOR_FS_CONTROL_RECORD_BYTES, true);
  view.setUint32(12, kind, true);
  view.setBigUint64(16, seq, true);
  view.setUint32(24, 0x1234, true);
  view.setUint32(28, 0x2000, true);
  view.setUint32(32, 9, true);
  view.setUint32(36, 3, true);
  view.setUint32(40, 55, true);
  view.setUint32(44, 1, true);
  view.setBigUint64(48, 77n, true);
  view.setBigUint64(56, 2n, true);
  view.setBigUint64(64, 8192n, true);
  view.setBigUint64(72, 4096n, true);
  view.setBigInt64(80, -2n, true);
  view.setBigUint64(88, 0x1n, true);
  view.setBigUint64(96, 16384n, true);
  view.setBigUint64(104, 0n, true);
}

function writeSignalControlRecord(
  view: DataView,
  kind = SUPERVISOR_SIGNAL_CONTROL_KIND_SIGNAL_DELIVERED,
  seq = 11n,
): void {
  view.setUint32(0, SUPERVISOR_SIGNAL_CONTROL_RECORD_MAGIC, true);
  view.setUint32(4, 1, true);
  view.setUint32(8, SUPERVISOR_SIGNAL_CONTROL_RECORD_BYTES, true);
  view.setUint32(12, kind, true);
  view.setBigUint64(16, seq, true);
  view.setUint32(24, 0x1234, true);
  view.setUint32(28, 0x2000, true);
  view.setUint32(32, 9, true);
  view.setUint32(36, 3, true);
  view.setUint32(40, 10, true);
  view.setUint32(44, 0, true);
  view.setInt32(48, -6, true);
  view.setUint32(52, 1, true);
  view.setBigUint64(56, 0x200n, true);
  view.setBigUint64(64, 0x400n, true);
  view.setBigUint64(72, 0x800n, true);
  view.setBigUint64(80, 0x200n, true);
  view.setBigUint64(88, 0x10n, true);
  view.setBigUint64(96, 0x91_000n, true);
  view.setBigUint64(104, 0x91_000n, true);
  view.setBigUint64(112, 0x500n, true);
}

test("hasSupervisorShadowProtocol treats supervisor exports as optional", () => {
  const kernelExports = makeSupervisorShadowKernelExports({
    memory: new WebAssembly.Memory({ initial: 1 }),
  });

  expect(hasSupervisorShadowProtocol(kernelExports)).toBe(false);
});

test("encodeSupervisorShadowThreadEvent writes a deterministic little-endian record", () => {
  const kernelExports = makeSupervisorShadowKernelExports();
  const first = new Uint8Array(SUPERVISOR_SHADOW_THREAD_EVENT_RECORD_BYTES);
  const second = new Uint8Array(SUPERVISOR_SHADOW_THREAD_EVENT_RECORD_BYTES);
  encodeSupervisorShadowThreadEvent(kernelExports, new DataView(first.buffer), 0, makeEvent());
  encodeSupervisorShadowThreadEvent(kernelExports, new DataView(second.buffer), 0, makeEvent());

  expect([...first]).toEqual([...second]);

  const view = new DataView(first.buffer);
  expect(view.getUint32(0, true)).toBe(SUPERVISOR_SHADOW_THREAD_EVENT_MAGIC);
  expect(view.getUint32(4, true)).toBe(SUPERVISOR_SHADOW_THREAD_EVENT_VERSION);
  expect(view.getUint32(8, true)).toBe(SUPERVISOR_SHADOW_THREAD_EVENT_RECORD_BYTES);
  expect(view.getUint32(12, true)).toBe(SUPERVISOR_THREAD_EVENT_KIND_BLOCKED);
  expect(view.getBigUint64(16, true)).toBe(3n);
  expect(view.getBigUint64(24, true)).toBe(0x2000n);
  expect(view.getUint32(32, true)).toBe(7);
  expect(view.getUint32(36, true)).toBe(11);
  expect(view.getUint32(40, true)).toBe(20);
  expect(view.getUint32(44, true)).toBe(4);
  expect(view.getUint32(48, true)).toBe(2);
  expect(view.getBigUint64(56, true)).toBe(0x1234n);
  expect(view.getBigUint64(64, true)).toBe(0x5678n);
  expect(view.getBigUint64(72, true)).toBe(0n);
});

test("submitSupervisorShadowThreadEvent writes the record and calls the kernel export", () => {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const submissions: Array<[number, number]> = [];
  const kernelExports = makeSupervisorShadowKernelExports({
    memory,
    supervisor_abi_version: () => 1,
    supervisor_submit_thread_event: (ptr: number, len: number) => {
      submissions.push([ptr, len]);
      return 0;
    },
  });

  const result = submitSupervisorShadowThreadEvent(kernelExports, 32, makeEvent());

  expect(result).toBe(0);
  expect(submissions).toEqual([[32, SUPERVISOR_SHADOW_THREAD_EVENT_RECORD_BYTES]]);
  const view = new DataView(memory.buffer);
  expect(view.getUint32(32, true)).toBe(SUPERVISOR_SHADOW_THREAD_EVENT_MAGIC);
  expect(view.getUint32(44, true)).toBe(SUPERVISOR_THREAD_EVENT_KIND_BLOCKED);
  expect(view.getUint32(72, true)).toBe(20);
});

test("supervisorThreadEventKindForStatus maps worker statuses to thread event families", () => {
  const kernelExports = makeSupervisorShadowKernelExports({
    memory: new WebAssembly.Memory({ initial: 1 }),
    abi_status_ok: () => 0,
    abi_status_fault: () => 3,
    abi_status_jit_ready: () => 27,
    supervisor_thread_event_kind_ready: () => SUPERVISOR_THREAD_EVENT_KIND_READY,
    supervisor_thread_event_kind_faulted: () => SUPERVISOR_THREAD_EVENT_KIND_FAULTED,
    supervisor_thread_event_kind_blocked: () => SUPERVISOR_THREAD_EVENT_KIND_BLOCKED,
  });

  expect(supervisorThreadEventKindForStatus(kernelExports, 0)).toBe(SUPERVISOR_THREAD_EVENT_KIND_READY);
  expect(supervisorThreadEventKindForStatus(kernelExports, 27)).toBe(SUPERVISOR_THREAD_EVENT_KIND_READY);
  expect(supervisorThreadEventKindForStatus(kernelExports, 3)).toBe(SUPERVISOR_THREAD_EVENT_KIND_FAULTED);
  expect(supervisorThreadEventKindForStatus(kernelExports, 20)).toBe(SUPERVISOR_THREAD_EVENT_KIND_BLOCKED);
});

test("submitSupervisorShadowStatusEvent uses scratch memory and keeps status deterministic", () => {
  const memory = new WebAssembly.Memory({ initial: 8 });
  const submissions: Array<[number, number]> = [];
  const kernelExports = makeSupervisorShadowKernelExports({
    memory,
    kernel_heap_ptr: () => 0,
    abi_status_ok: () => 0,
    abi_status_fault: () => 3,
    abi_status_jit_ready: () => 27,
    supervisor_abi_version: () => 1,
    supervisor_thread_event_kind_ready: () => SUPERVISOR_THREAD_EVENT_KIND_READY,
    supervisor_submit_thread_event: (ptr: number, len: number) => {
      submissions.push([ptr, len]);
      return 0;
    },
  });
  const handle = {
    kernel: kernelExports,
    memory,
    pid: 42,
  } as ProcessHandle;

  const result = submitSupervisorShadowStatusEvent(handle, {
    sequence: 9n,
    vmPtr: 0x7000,
    pid: 42,
    tid: 5,
    status: 0,
    syncEffects: 6,
    pc: 0x1000n,
    sp: 0x2000n,
  });

  expect(result).toBe(0);
  expect(submissions).toHaveLength(1);
  const [ptr, len] = submissions[0]!;
  expect(len).toBe(SUPERVISOR_SHADOW_THREAD_EVENT_RECORD_BYTES);
  const view = new DataView(memory.buffer, ptr, len);
  expect(view.getUint32(12, true)).toBe(SUPERVISOR_THREAD_EVENT_KIND_READY);
  expect(view.getBigUint64(16, true)).toBe(9n);
  expect(view.getBigUint64(24, true)).toBe(0x7000n);
  expect(view.getUint32(32, true)).toBe(42);
  expect(view.getUint32(36, true)).toBe(5);
  expect(view.getUint32(40, true)).toBe(0);
  expect(view.getUint32(44, true)).toBe(6);
});

test("decodeSupervisorFdControlRecord reads the supervisor fd control layout", () => {
  const bytes = new Uint8Array(SUPERVISOR_FD_CONTROL_RECORD_BYTES);
  writeFdControlRecord(new DataView(bytes.buffer));
  const kernelExports = makeSupervisorShadowKernelExports({
    memory: new WebAssembly.Memory({ initial: 1 }),
    supervisor_abi_version: () => 1,
    supervisor_fd_control_kind_pipe_created: () => SUPERVISOR_FD_CONTROL_KIND_PIPE_CREATED,
    supervisor_fd_control_kind_fd_closed: () => SUPERVISOR_FD_CONTROL_KIND_FD_CLOSED,
    supervisor_fd_control_kind_fd_duplicated: () => SUPERVISOR_FD_CONTROL_KIND_FD_DUPLICATED,
    supervisor_fd_control_kind_fd_flags_updated: () => SUPERVISOR_FD_CONTROL_KIND_FD_FLAGS_UPDATED,
    supervisor_fd_control_kind_epoll_created: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CREATED,
    supervisor_fd_control_kind_epoll_ctl_add: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_ADD,
    supervisor_fd_control_kind_epoll_ctl_del: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_DEL,
    supervisor_fd_control_kind_epoll_ctl_mod: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_MOD,
    supervisor_fd_control_kind_socket_created: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_CREATED,
    supervisor_fd_control_kind_socketpair_created: () => SUPERVISOR_FD_CONTROL_KIND_SOCKETPAIR_CREATED,
    supervisor_fd_control_kind_socket_bound: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_BOUND,
    supervisor_fd_control_kind_socket_listening: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_LISTENING,
    supervisor_fd_control_kind_socket_connect_started: () =>
      SUPERVISOR_FD_CONTROL_KIND_SOCKET_CONNECT_STARTED,
    supervisor_fd_control_kind_socket_connected: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_CONNECTED,
    supervisor_fd_control_kind_socket_connect_failed: () =>
      SUPERVISOR_FD_CONTROL_KIND_SOCKET_CONNECT_FAILED,
    supervisor_fd_control_kind_socket_accepted: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_ACCEPTED,
  });

  const record = decodeSupervisorFdControlRecord(kernelExports, new DataView(bytes.buffer));

  expect(record).toEqual({
    kind: SUPERVISOR_FD_CONTROL_KIND_PIPE_CREATED,
    seq: 7n,
    vmPtr: 0x1234,
    processPtr: 0x2000,
    pid: 9,
    fd: 3,
    fd2: 4,
    ofdId: 11,
    ofd2Id: 12,
    fdType: 2,
    resourceSlot: 5,
    resourceGeneration: 6,
    result: 0n,
    flags: 0x80800n,
    detail0: 1n,
    detail1: 1n,
  });
  expect(formatSupervisorFdControlRecord(record)).toBe(
    "supervisor-fd-control:kind=1:seq=7:vm=0x1234:process=0x2000:pid=9"
      + ":fd=3:fd2=4:ofd=11:ofd2=12:type=2:slot=5:generation=6"
      + ":result=0:flags=0x80800:detail0=1:detail1=1",
  );
});

test("decodeSupervisorFdControlRecord accepts fd lifecycle control kinds", () => {
  const kernelExports = makeSupervisorShadowKernelExports({
    memory: new WebAssembly.Memory({ initial: 1 }),
    supervisor_abi_version: () => 1,
    supervisor_fd_control_kind_pipe_created: () => SUPERVISOR_FD_CONTROL_KIND_PIPE_CREATED,
    supervisor_fd_control_kind_fd_closed: () => SUPERVISOR_FD_CONTROL_KIND_FD_CLOSED,
    supervisor_fd_control_kind_fd_duplicated: () => SUPERVISOR_FD_CONTROL_KIND_FD_DUPLICATED,
    supervisor_fd_control_kind_fd_flags_updated: () => SUPERVISOR_FD_CONTROL_KIND_FD_FLAGS_UPDATED,
  });

  for (const kind of [
    SUPERVISOR_FD_CONTROL_KIND_FD_DUPLICATED,
    SUPERVISOR_FD_CONTROL_KIND_FD_FLAGS_UPDATED,
  ]) {
    const bytes = new Uint8Array(SUPERVISOR_FD_CONTROL_RECORD_BYTES);
    writeFdControlRecord(new DataView(bytes.buffer), kind);

    const record = decodeSupervisorFdControlRecord(kernelExports, new DataView(bytes.buffer));

    expect(record.kind).toBe(kind);
  }
});

test("decodeSupervisorFdControlRecord accepts epoll control kinds", () => {
  const kernelExports = makeSupervisorShadowKernelExports({
    memory: new WebAssembly.Memory({ initial: 1 }),
    supervisor_abi_version: () => 1,
    supervisor_fd_control_kind_pipe_created: () => SUPERVISOR_FD_CONTROL_KIND_PIPE_CREATED,
    supervisor_fd_control_kind_fd_closed: () => SUPERVISOR_FD_CONTROL_KIND_FD_CLOSED,
    supervisor_fd_control_kind_fd_duplicated: () => SUPERVISOR_FD_CONTROL_KIND_FD_DUPLICATED,
    supervisor_fd_control_kind_fd_flags_updated: () => SUPERVISOR_FD_CONTROL_KIND_FD_FLAGS_UPDATED,
    supervisor_fd_control_kind_epoll_created: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CREATED,
    supervisor_fd_control_kind_epoll_ctl_add: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_ADD,
    supervisor_fd_control_kind_epoll_ctl_del: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_DEL,
    supervisor_fd_control_kind_epoll_ctl_mod: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_MOD,
  });

  for (const kind of [
    SUPERVISOR_FD_CONTROL_KIND_EPOLL_CREATED,
    SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_ADD,
    SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_DEL,
    SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_MOD,
  ]) {
    const bytes = new Uint8Array(SUPERVISOR_FD_CONTROL_RECORD_BYTES);
    writeFdControlRecord(new DataView(bytes.buffer), kind);

    const record = decodeSupervisorFdControlRecord(kernelExports, new DataView(bytes.buffer));

    expect(record.kind).toBe(kind);
  }
});

test("decodeSupervisorFdControlRecord accepts socket creation control kinds", () => {
  const kernelExports = makeSupervisorShadowKernelExports({
    memory: new WebAssembly.Memory({ initial: 1 }),
    supervisor_abi_version: () => 1,
    supervisor_fd_control_kind_pipe_created: () => SUPERVISOR_FD_CONTROL_KIND_PIPE_CREATED,
    supervisor_fd_control_kind_fd_closed: () => SUPERVISOR_FD_CONTROL_KIND_FD_CLOSED,
    supervisor_fd_control_kind_fd_duplicated: () => SUPERVISOR_FD_CONTROL_KIND_FD_DUPLICATED,
    supervisor_fd_control_kind_fd_flags_updated: () => SUPERVISOR_FD_CONTROL_KIND_FD_FLAGS_UPDATED,
    supervisor_fd_control_kind_epoll_created: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CREATED,
    supervisor_fd_control_kind_epoll_ctl_add: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_ADD,
    supervisor_fd_control_kind_epoll_ctl_del: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_DEL,
    supervisor_fd_control_kind_epoll_ctl_mod: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_MOD,
    supervisor_fd_control_kind_socket_created: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_CREATED,
    supervisor_fd_control_kind_socketpair_created: () => SUPERVISOR_FD_CONTROL_KIND_SOCKETPAIR_CREATED,
  });

  for (const kind of [
    SUPERVISOR_FD_CONTROL_KIND_SOCKET_CREATED,
    SUPERVISOR_FD_CONTROL_KIND_SOCKETPAIR_CREATED,
  ]) {
    const bytes = new Uint8Array(SUPERVISOR_FD_CONTROL_RECORD_BYTES);
    writeFdControlRecord(new DataView(bytes.buffer), kind);

    const record = decodeSupervisorFdControlRecord(kernelExports, new DataView(bytes.buffer));

    expect(record.kind).toBe(kind);
  }
});

test("decodeSupervisorFdControlRecord accepts socket lifecycle control kinds", () => {
  const kernelExports = makeSupervisorShadowKernelExports({
    memory: new WebAssembly.Memory({ initial: 1 }),
    supervisor_abi_version: () => 1,
    supervisor_fd_control_kind_pipe_created: () => SUPERVISOR_FD_CONTROL_KIND_PIPE_CREATED,
    supervisor_fd_control_kind_fd_closed: () => SUPERVISOR_FD_CONTROL_KIND_FD_CLOSED,
    supervisor_fd_control_kind_fd_duplicated: () => SUPERVISOR_FD_CONTROL_KIND_FD_DUPLICATED,
    supervisor_fd_control_kind_fd_flags_updated: () => SUPERVISOR_FD_CONTROL_KIND_FD_FLAGS_UPDATED,
    supervisor_fd_control_kind_epoll_created: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CREATED,
    supervisor_fd_control_kind_epoll_ctl_add: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_ADD,
    supervisor_fd_control_kind_epoll_ctl_del: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_DEL,
    supervisor_fd_control_kind_epoll_ctl_mod: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_MOD,
    supervisor_fd_control_kind_socket_created: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_CREATED,
    supervisor_fd_control_kind_socketpair_created: () => SUPERVISOR_FD_CONTROL_KIND_SOCKETPAIR_CREATED,
    supervisor_fd_control_kind_socket_bound: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_BOUND,
    supervisor_fd_control_kind_socket_listening: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_LISTENING,
    supervisor_fd_control_kind_socket_connect_started: () =>
      SUPERVISOR_FD_CONTROL_KIND_SOCKET_CONNECT_STARTED,
    supervisor_fd_control_kind_socket_connected: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_CONNECTED,
    supervisor_fd_control_kind_socket_connect_failed: () =>
      SUPERVISOR_FD_CONTROL_KIND_SOCKET_CONNECT_FAILED,
    supervisor_fd_control_kind_socket_accepted: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_ACCEPTED,
  });

  for (const kind of [
    SUPERVISOR_FD_CONTROL_KIND_SOCKET_BOUND,
    SUPERVISOR_FD_CONTROL_KIND_SOCKET_LISTENING,
    SUPERVISOR_FD_CONTROL_KIND_SOCKET_CONNECT_STARTED,
    SUPERVISOR_FD_CONTROL_KIND_SOCKET_CONNECTED,
    SUPERVISOR_FD_CONTROL_KIND_SOCKET_CONNECT_FAILED,
    SUPERVISOR_FD_CONTROL_KIND_SOCKET_ACCEPTED,
  ]) {
    const bytes = new Uint8Array(SUPERVISOR_FD_CONTROL_RECORD_BYTES);
    writeFdControlRecord(new DataView(bytes.buffer), kind);

    const record = decodeSupervisorFdControlRecord(kernelExports, new DataView(bytes.buffer));

    expect(record.kind).toBe(kind);
  }
});

test("readSupervisorFdControlRecord polls the kernel export", () => {
  const memory = new WebAssembly.Memory({ initial: 8 });
  const kernelExports = makeSupervisorShadowKernelExports({
    memory,
    kernel_heap_ptr: () => 0,
    supervisor_abi_version: () => 1,
    supervisor_fd_control_record_bytes: () => SUPERVISOR_FD_CONTROL_RECORD_BYTES,
    supervisor_fd_control_kind_pipe_created: () => SUPERVISOR_FD_CONTROL_KIND_PIPE_CREATED,
    supervisor_fd_control_kind_fd_closed: () => SUPERVISOR_FD_CONTROL_KIND_FD_CLOSED,
    supervisor_fd_control_kind_fd_duplicated: () => SUPERVISOR_FD_CONTROL_KIND_FD_DUPLICATED,
    supervisor_fd_control_kind_fd_flags_updated: () => SUPERVISOR_FD_CONTROL_KIND_FD_FLAGS_UPDATED,
    supervisor_fd_control_kind_epoll_created: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CREATED,
    supervisor_fd_control_kind_epoll_ctl_add: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_ADD,
    supervisor_fd_control_kind_epoll_ctl_del: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_DEL,
    supervisor_fd_control_kind_epoll_ctl_mod: () => SUPERVISOR_FD_CONTROL_KIND_EPOLL_CTL_MOD,
    supervisor_fd_control_kind_socket_created: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_CREATED,
    supervisor_fd_control_kind_socketpair_created: () => SUPERVISOR_FD_CONTROL_KIND_SOCKETPAIR_CREATED,
    supervisor_fd_control_kind_socket_bound: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_BOUND,
    supervisor_fd_control_kind_socket_listening: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_LISTENING,
    supervisor_fd_control_kind_socket_connect_started: () =>
      SUPERVISOR_FD_CONTROL_KIND_SOCKET_CONNECT_STARTED,
    supervisor_fd_control_kind_socket_connected: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_CONNECTED,
    supervisor_fd_control_kind_socket_connect_failed: () =>
      SUPERVISOR_FD_CONTROL_KIND_SOCKET_CONNECT_FAILED,
    supervisor_fd_control_kind_socket_accepted: () => SUPERVISOR_FD_CONTROL_KIND_SOCKET_ACCEPTED,
    supervisor_fd_control_record: vi.fn((outPtr: number, outLen: number) => {
      expect(outLen).toBe(SUPERVISOR_FD_CONTROL_RECORD_BYTES);
      const view = new DataView(memory.buffer, outPtr, outLen);
      writeFdControlRecord(view, SUPERVISOR_FD_CONTROL_KIND_FD_CLOSED);
      view.setBigUint64(80, 1n, true);
      view.setBigUint64(88, 0n, true);
      return SUPERVISOR_FD_CONTROL_RECORD_BYTES;
    }),
  });
  const handle = {
    kernel: kernelExports,
    memory,
  } as ProcessHandle;

  const record = readSupervisorFdControlRecord(handle);

  expect(kernelExports.supervisor_fd_control_record).toHaveBeenCalledWith(
    expect.any(Number),
    SUPERVISOR_FD_CONTROL_RECORD_BYTES,
  );
  expect(record?.kind).toBe(SUPERVISOR_FD_CONTROL_KIND_FD_CLOSED);
  expect(record?.detail0).toBe(1n);
  expect(record?.detail1).toBe(0n);
});

test("pollSupervisorFdControlDiagnostic formats new records once", () => {
  const memory = new WebAssembly.Memory({ initial: 8 });
  let count = 0n;
  let seq = 7n;
  const kernelExports = makeSupervisorShadowKernelExports({
    memory,
    kernel_heap_ptr: () => 0,
    supervisor_abi_version: () => 1,
    supervisor_fd_control_record_bytes: () => SUPERVISOR_FD_CONTROL_RECORD_BYTES,
    supervisor_fd_control_kind_pipe_created: () => SUPERVISOR_FD_CONTROL_KIND_PIPE_CREATED,
    supervisor_shadow_fd_control_count: () => count,
    supervisor_fd_control_record: vi.fn((outPtr: number, outLen: number) => {
      expect(outLen).toBe(SUPERVISOR_FD_CONTROL_RECORD_BYTES);
      writeFdControlRecord(
        new DataView(memory.buffer, outPtr, outLen),
        SUPERVISOR_FD_CONTROL_KIND_PIPE_CREATED,
        seq,
      );
      return SUPERVISOR_FD_CONTROL_RECORD_BYTES;
    }),
  });
  const handle = {
    kernel: kernelExports,
    memory,
  } as ProcessHandle;

  expect(pollSupervisorFdControlDiagnostic(handle)).toBeUndefined();

  count = 1n;
  expect(pollSupervisorFdControlDiagnostic(handle)).toContain("supervisor-fd-control:kind=1:seq=7");
  expect(pollSupervisorFdControlDiagnostic(handle)).toBeUndefined();

  count = 2n;
  seq = 8n;
  expect(pollSupervisorFdControlDiagnostic(handle)).toContain("supervisor-fd-control:kind=1:seq=8");
});

test("decodeSupervisorFsControlRecord reads the supervisor fs control layout", () => {
  const bytes = new Uint8Array(SUPERVISOR_FS_CONTROL_RECORD_BYTES);
  writeFsControlRecord(new DataView(bytes.buffer));
  const kernelExports = makeSupervisorShadowKernelExports({
    memory: new WebAssembly.Memory({ initial: 1 }),
    supervisor_abi_version: () => 1,
    supervisor_fs_control_kind_page_cache_miss: () => SUPERVISOR_FS_CONTROL_KIND_PAGE_CACHE_MISS,
    supervisor_fs_control_kind_page_cache_fill: () => SUPERVISOR_FS_CONTROL_KIND_PAGE_CACHE_FILL,
    supervisor_fs_control_kind_dirty_page_published: () =>
      SUPERVISOR_FS_CONTROL_KIND_DIRTY_PAGE_PUBLISHED,
    supervisor_fs_control_kind_dirty_page_flushed: () =>
      SUPERVISOR_FS_CONTROL_KIND_DIRTY_PAGE_FLUSHED,
  });

  const record = decodeSupervisorFsControlRecord(kernelExports, new DataView(bytes.buffer));

  expect(record).toEqual({
    kind: SUPERVISOR_FS_CONTROL_KIND_PAGE_CACHE_MISS,
    seq: 9n,
    vmPtr: 0x1234,
    processPtr: 0x2000,
    pid: 9,
    fd: 3,
    ino: 55,
    opcode: 1,
    fileObjectId: 77n,
    pageIndex: 2n,
    offset: 8192n,
    length: 4096n,
    result: -2n,
    flags: 0x1n,
    detail0: 16384n,
    detail1: 0n,
  });
  expect(formatSupervisorFsControlRecord(record)).toBe(
    "supervisor-fs-control:kind=1:seq=9:vm=0x1234:process=0x2000:pid=9"
      + ":fd=3:ino=55:opcode=1:fileObject=77:page=2:offset=8192:length=4096"
      + ":result=-2:flags=0x1:detail0=16384:detail1=0",
  );
});

test("decodeSupervisorFsControlRecord accepts fs control kinds", () => {
  const kernelExports = makeSupervisorShadowKernelExports({
    memory: new WebAssembly.Memory({ initial: 1 }),
    supervisor_abi_version: () => 1,
    supervisor_fs_control_kind_page_cache_miss: () => SUPERVISOR_FS_CONTROL_KIND_PAGE_CACHE_MISS,
    supervisor_fs_control_kind_page_cache_fill: () => SUPERVISOR_FS_CONTROL_KIND_PAGE_CACHE_FILL,
    supervisor_fs_control_kind_dirty_page_published: () =>
      SUPERVISOR_FS_CONTROL_KIND_DIRTY_PAGE_PUBLISHED,
    supervisor_fs_control_kind_dirty_page_flushed: () =>
      SUPERVISOR_FS_CONTROL_KIND_DIRTY_PAGE_FLUSHED,
  });

  for (const kind of [
    SUPERVISOR_FS_CONTROL_KIND_PAGE_CACHE_MISS,
    SUPERVISOR_FS_CONTROL_KIND_PAGE_CACHE_FILL,
    SUPERVISOR_FS_CONTROL_KIND_DIRTY_PAGE_PUBLISHED,
    SUPERVISOR_FS_CONTROL_KIND_DIRTY_PAGE_FLUSHED,
  ]) {
    const bytes = new Uint8Array(SUPERVISOR_FS_CONTROL_RECORD_BYTES);
    writeFsControlRecord(new DataView(bytes.buffer), kind);

    const record = decodeSupervisorFsControlRecord(kernelExports, new DataView(bytes.buffer));

    expect(record.kind).toBe(kind);
  }
});

test("readSupervisorFsControlRecord polls the kernel export", () => {
  const memory = new WebAssembly.Memory({ initial: 8 });
  const kernelExports = makeSupervisorShadowKernelExports({
    memory,
    kernel_heap_ptr: () => 0,
    supervisor_abi_version: () => 1,
    supervisor_fs_control_record_bytes: () => SUPERVISOR_FS_CONTROL_RECORD_BYTES,
    supervisor_fs_control_kind_page_cache_miss: () => SUPERVISOR_FS_CONTROL_KIND_PAGE_CACHE_MISS,
    supervisor_fs_control_kind_page_cache_fill: () => SUPERVISOR_FS_CONTROL_KIND_PAGE_CACHE_FILL,
    supervisor_fs_control_kind_dirty_page_published: () =>
      SUPERVISOR_FS_CONTROL_KIND_DIRTY_PAGE_PUBLISHED,
    supervisor_fs_control_kind_dirty_page_flushed: () =>
      SUPERVISOR_FS_CONTROL_KIND_DIRTY_PAGE_FLUSHED,
    supervisor_fs_control_record: vi.fn((outPtr: number, outLen: number) => {
      expect(outLen).toBe(SUPERVISOR_FS_CONTROL_RECORD_BYTES);
      writeFsControlRecord(new DataView(memory.buffer, outPtr, outLen));
      return SUPERVISOR_FS_CONTROL_RECORD_BYTES;
    }),
  });
  const handle = {
    kernel: kernelExports,
    memory,
  } as ProcessHandle;

  const record = readSupervisorFsControlRecord(handle);

  expect(kernelExports.supervisor_fs_control_record).toHaveBeenCalledWith(
    expect.any(Number),
    SUPERVISOR_FS_CONTROL_RECORD_BYTES,
  );
  expect(record?.kind).toBe(SUPERVISOR_FS_CONTROL_KIND_PAGE_CACHE_MISS);
  expect(record?.ino).toBe(55);
  expect(record?.offset).toBe(8192n);
});

test("pollSupervisorFsControlDiagnostic formats new records once", () => {
  const memory = new WebAssembly.Memory({ initial: 8 });
  let count = 0n;
  let seq = 9n;
  const kernelExports = makeSupervisorShadowKernelExports({
    memory,
    kernel_heap_ptr: () => 0,
    supervisor_abi_version: () => 1,
    supervisor_fs_control_record_bytes: () => SUPERVISOR_FS_CONTROL_RECORD_BYTES,
    supervisor_fs_control_kind_page_cache_miss: () => SUPERVISOR_FS_CONTROL_KIND_PAGE_CACHE_MISS,
    supervisor_shadow_fs_control_count: () => count,
    supervisor_fs_control_record: vi.fn((outPtr: number, outLen: number) => {
      expect(outLen).toBe(SUPERVISOR_FS_CONTROL_RECORD_BYTES);
      writeFsControlRecord(
        new DataView(memory.buffer, outPtr, outLen),
        SUPERVISOR_FS_CONTROL_KIND_PAGE_CACHE_MISS,
        seq,
      );
      return SUPERVISOR_FS_CONTROL_RECORD_BYTES;
    }),
  });
  const handle = {
    kernel: kernelExports,
    memory,
  } as ProcessHandle;

  expect(pollSupervisorFsControlDiagnostic(handle)).toBeUndefined();

  count = 1n;
  expect(pollSupervisorFsControlDiagnostic(handle)).toContain("supervisor-fs-control:kind=1:seq=9");
  expect(pollSupervisorFsControlDiagnostic(handle)).toBeUndefined();

  count = 2n;
  seq = 10n;
  expect(pollSupervisorFsControlDiagnostic(handle)).toContain("supervisor-fs-control:kind=1:seq=10");
});

test("decodeSupervisorSignalControlRecord reads the supervisor signal control layout", () => {
  const bytes = new Uint8Array(SUPERVISOR_SIGNAL_CONTROL_RECORD_BYTES);
  writeSignalControlRecord(new DataView(bytes.buffer));
  const kernelExports = makeSupervisorShadowKernelExports({
    memory: new WebAssembly.Memory({ initial: 1 }),
    supervisor_abi_version: () => 1,
    supervisor_signal_control_kind_boundary_check: () =>
      SUPERVISOR_SIGNAL_CONTROL_KIND_BOUNDARY_CHECK,
    supervisor_signal_control_kind_signal_delivered: () =>
      SUPERVISOR_SIGNAL_CONTROL_KIND_SIGNAL_DELIVERED,
    supervisor_signal_control_kind_blocked_wait_interrupted: () =>
      SUPERVISOR_SIGNAL_CONTROL_KIND_BLOCKED_WAIT_INTERRUPTED,
  });

  const record = decodeSupervisorSignalControlRecord(kernelExports, new DataView(bytes.buffer));

  expect(record).toEqual({
    kind: SUPERVISOR_SIGNAL_CONTROL_KIND_SIGNAL_DELIVERED,
    seq: 11n,
    vmPtr: 0x1234,
    processPtr: 0x2000,
    pid: 9,
    tid: 3,
    sig: 10,
    status: 0,
    sigInfoCode: -6,
    boundary: 1,
    threadPending: 0x200n,
    processPending: 0x400n,
    recordPending: 0x800n,
    deliverable: 0x200n,
    mask: 0x10n,
    pc: 0x91_000n,
    detail0: 0x91_000n,
    detail1: 0x500n,
  });
  expect(formatSupervisorSignalControlRecord(record)).toBe(
    "supervisor-signal-control:kind=2:seq=11:vm=0x1234:process=0x2000"
      + ":pid=9:tid=3:sig=10:status=0:code=-6:boundary=1"
      + ":threadPending=0x200:processPending=0x400:recordPending=0x800"
      + ":deliverable=0x200:mask=0x10:pc=0x91000:detail0=593920:detail1=1280",
  );
});

test("decodeSupervisorSignalControlRecord accepts signal control kinds", () => {
  const kernelExports = makeSupervisorShadowKernelExports({
    memory: new WebAssembly.Memory({ initial: 1 }),
    supervisor_abi_version: () => 1,
    supervisor_signal_control_kind_boundary_check: () =>
      SUPERVISOR_SIGNAL_CONTROL_KIND_BOUNDARY_CHECK,
    supervisor_signal_control_kind_signal_delivered: () =>
      SUPERVISOR_SIGNAL_CONTROL_KIND_SIGNAL_DELIVERED,
    supervisor_signal_control_kind_blocked_wait_interrupted: () =>
      SUPERVISOR_SIGNAL_CONTROL_KIND_BLOCKED_WAIT_INTERRUPTED,
  });

  for (const kind of [
    SUPERVISOR_SIGNAL_CONTROL_KIND_BOUNDARY_CHECK,
    SUPERVISOR_SIGNAL_CONTROL_KIND_SIGNAL_DELIVERED,
    SUPERVISOR_SIGNAL_CONTROL_KIND_BLOCKED_WAIT_INTERRUPTED,
  ]) {
    const bytes = new Uint8Array(SUPERVISOR_SIGNAL_CONTROL_RECORD_BYTES);
    writeSignalControlRecord(new DataView(bytes.buffer), kind);

    const record = decodeSupervisorSignalControlRecord(kernelExports, new DataView(bytes.buffer));

    expect(record.kind).toBe(kind);
  }
});

test("readSupervisorSignalControlRecord polls the kernel export", () => {
  const memory = new WebAssembly.Memory({ initial: 8 });
  const kernelExports = makeSupervisorShadowKernelExports({
    memory,
    kernel_heap_ptr: () => 0,
    supervisor_abi_version: () => 1,
    supervisor_signal_control_record_bytes: () => SUPERVISOR_SIGNAL_CONTROL_RECORD_BYTES,
    supervisor_signal_control_kind_signal_delivered: () =>
      SUPERVISOR_SIGNAL_CONTROL_KIND_SIGNAL_DELIVERED,
    supervisor_signal_control_record: vi.fn((outPtr: number, outLen: number) => {
      expect(outLen).toBe(SUPERVISOR_SIGNAL_CONTROL_RECORD_BYTES);
      writeSignalControlRecord(new DataView(memory.buffer, outPtr, outLen));
      return SUPERVISOR_SIGNAL_CONTROL_RECORD_BYTES;
    }),
  });
  const handle = {
    kernel: kernelExports,
    memory,
  } as ProcessHandle;

  const record = readSupervisorSignalControlRecord(handle);

  expect(kernelExports.supervisor_signal_control_record).toHaveBeenCalledWith(
    expect.any(Number),
    SUPERVISOR_SIGNAL_CONTROL_RECORD_BYTES,
  );
  expect(record?.kind).toBe(SUPERVISOR_SIGNAL_CONTROL_KIND_SIGNAL_DELIVERED);
  expect(record?.sig).toBe(10);
});

test("pollSupervisorSignalControlDiagnostic formats new records once", () => {
  const memory = new WebAssembly.Memory({ initial: 8 });
  let count = 0n;
  let seq = 11n;
  const kernelExports = makeSupervisorShadowKernelExports({
    memory,
    kernel_heap_ptr: () => 0,
    supervisor_abi_version: () => 1,
    supervisor_signal_control_record_bytes: () => SUPERVISOR_SIGNAL_CONTROL_RECORD_BYTES,
    supervisor_signal_control_kind_signal_delivered: () =>
      SUPERVISOR_SIGNAL_CONTROL_KIND_SIGNAL_DELIVERED,
    supervisor_shadow_signal_control_count: () => count,
    supervisor_signal_control_record: vi.fn((outPtr: number, outLen: number) => {
      expect(outLen).toBe(SUPERVISOR_SIGNAL_CONTROL_RECORD_BYTES);
      writeSignalControlRecord(
        new DataView(memory.buffer, outPtr, outLen),
        SUPERVISOR_SIGNAL_CONTROL_KIND_SIGNAL_DELIVERED,
        seq,
      );
      return SUPERVISOR_SIGNAL_CONTROL_RECORD_BYTES;
    }),
  });
  const handle = {
    kernel: kernelExports,
    memory,
  } as ProcessHandle;

  expect(pollSupervisorSignalControlDiagnostic(handle)).toBeUndefined();

  count = 1n;
  expect(pollSupervisorSignalControlDiagnostic(handle)).toContain(
    "supervisor-signal-control:kind=2:seq=11",
  );
  expect(pollSupervisorSignalControlDiagnostic(handle)).toBeUndefined();

  count = 2n;
  seq = 12n;
  expect(pollSupervisorSignalControlDiagnostic(handle)).toContain(
    "supervisor-signal-control:kind=2:seq=12",
  );
});
