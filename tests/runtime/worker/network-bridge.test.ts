import { expect, test, vi } from "vite-plus/test";
import { createWorkerProcessIoHelpers } from "../../../src/worker/process-io.js";
import type { ProcessHandle } from "../../../src/worker/types.js";

function createDeps() {
  return {
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    drainProcessOutput: vi.fn(() => false),
    failProcess: vi.fn(),
    flushStdin: vi.fn(() => false),
    getActiveHandle: vi.fn((handle: ProcessHandle) => handle),
    isActiveHandle: vi.fn(() => true),
    kickBlockedWorkers: vi.fn(),
    pumpProcess: vi.fn(),
    requestPipeStateSync: vi.fn(async () => {}),
    requestSocketStateSync: vi.fn(async () => {}),
    writeProcessStdin: vi.fn(),
  };
}

test("resolveNetworkConnect reports IPv4 connect endpoint to a generic runtime bridge", async () => {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  const view = new Uint8Array(memory.buffer);
  const bridge = {
    connect: vi.fn(async () => undefined),
  };
  const handle = {
    pid: 42,
    memory,
    networkBridge: bridge,
    networkConnections: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    kernel: {
      kernel_heap_ptr: () => 0x100,
      abi_econnrefused: () => 111,
      thread_connect_fd: () => 7,
      thread_connect_peer_addr: (_vmPtr: number, dstPtr: number) => {
        view.set([
          2, 0,
          0x01, 0xbb,
          93, 184, 216, 34,
        ], dstPtr);
        return 8;
      },
    },
  } as unknown as ProcessHandle;
  const helpers = createWorkerProcessIoHelpers(createDeps());

  const result = await helpers.resolveNetworkConnect(handle, 0x2000);

  expect(result).toBe(0n);
  expect(bridge.connect).toHaveBeenCalledWith({
    pid: 42,
    fd: 7,
    endpoint: {
      family: "inet",
      address: "93.184.216.34",
      port: 443,
    },
  });
});

test("resolveNetworkConnect tracks pending fd while bridge connect is in flight", async () => {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  const view = new Uint8Array(memory.buffer);
  let finishConnect: ((value: unknown) => void) | null = null;
  const handle = {
    pid: 42,
    memory,
    networkConnections: new Map(),
    networkPendingConnectFds: new Set<number>(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    networkBridge: {
      connect: vi.fn(() => new Promise((resolve) => {
        finishConnect = resolve;
      })),
    },
    kernel: {
      kernel_heap_ptr: () => 0x100,
      abi_econnrefused: () => 111,
      thread_connect_fd: () => 7,
      thread_connect_peer_addr: (_vmPtr: number, dstPtr: number) => {
        view.set([2, 0, 0, 80, 127, 0, 0, 1], dstPtr);
        return 8;
      },
    },
  } as unknown as ProcessHandle;
  const helpers = createWorkerProcessIoHelpers(createDeps());

  const pending = helpers.resolveNetworkConnect(handle, 0x2000);
  await vi.waitFor(() => {
    expect(handle.networkPendingConnectFds.has(7)).toBe(true);
  });

  finishConnect?.(undefined);
  await expect(pending).resolves.toBe(0n);
  expect(handle.networkPendingConnectFds.has(7)).toBe(false);
});

test("commitNetworkConnect adopts a successful bridge connection after the local status advances", async () => {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  const view = new Uint8Array(memory.buffer);
  const connection = {
    close: vi.fn(async () => {}),
    read: vi.fn(() => undefined),
    write: vi.fn(),
  };
  const handle = {
    pid: 42,
    runtimeId: 1,
    vmPtr: 0x2000,
    memory,
    networkBridge: {
      connect: vi.fn(async () => ({ connection })),
    },
    networkConnections: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set([3]),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map([[3, 4096]]),
    networkRecvTailWatermarks: new Map([[3, 8192]]),
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(8),
      stateSab: new SharedArrayBuffer(176),
    },
    kernelState: {},
    kernel: {
      kernel_heap_base: () => 0,
      kernel_heap_ptr: () => 0x100,
      kernel_ofd_table_ptr: () => 0,
      kernel_memfs_ptr: () => 0,
      kernel_memfs_header_bytes: () => 0,
      kernel_memfs_inodes_bytes: () => 0,
      kernel_memfs_first_child_bytes: () => 0,
      kernel_memfs_dir_entries_bytes: () => 0,
      kernel_memfs_data_bytes: () => 0,
      kernel_mem_state_ptr: () => 0,
      kernel_epoll_table_ptr: () => 0,
      kernel_socket_table_ptr: () => 0,
      kernel_socket_used: () => 0,
      kernel_pipe_base: () => 0,
      kernel_pipe_used: () => 0n,
      kernel_net_buf_base: () => 0,
      kernel_net_buf_used: () => 0n,
      kernel_sig_slots_ptr: () => 0,
      kernel_process_pending_ptr: () => 0,
      kernel_process_runtime_state_ptr: () => 0,
      kernel_time_state_ptr: () => 0,
      abi_econnrefused: () => 111,
      abi_einprogress: () => -115,
      abi_status_ok: () => 0,
      thread_connect_fd: () => 3,
      thread_connect_peer_addr: (_vmPtr: number, dstPtr: number) => {
        view.set([2, 0, 0x1f, 0x90, 127, 0, 0, 1], dstPtr);
        return 8;
      },
      thread_a0: () => 99n,
      thread_status: () => 30,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 3 ? 19 : 0,
      thread_socket_idx_for_fd: vi.fn(() => 0),
      thread_socket_state: vi.fn(() => 3),
      thread_socket_recv_buf_idx: vi.fn(() => 0),
      thread_socket_recv_available: vi.fn(() => 0),
      thread_socket_peer_idx: vi.fn(() => 1),
      thread_socket_external_peer: vi.fn(() => 1),
      thread_socket_mark_external_peer: vi.fn(() => 0),
    },
  } as unknown as ProcessHandle;
  const helpers = createWorkerProcessIoHelpers(createDeps());

  await expect(helpers.resolveNetworkConnect(handle, 0x2000)).resolves.toBe(0n);
  await helpers.commitNetworkConnect(handle, 0x2000, 0n);

  expect(handle.kernel.thread_socket_mark_external_peer).toHaveBeenCalledWith(0x2000, 3);
  expect(handle.networkConnections.get(3)).toBe(connection);
  expect(handle.networkPendingConnectFds.has(3)).toBe(false);
  expect(handle.networkClosedFds.has(3)).toBe(false);
  expect(handle.networkRecvHeadWatermarks.has(3)).toBe(false);
  expect(handle.networkRecvTailWatermarks.has(3)).toBe(false);
  expect(connection.close).not.toHaveBeenCalled();
});

test("resolveNetworkConnect normalizes bridge errno to negative Linux result", async () => {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  const view = new Uint8Array(memory.buffer);
  const handle = {
    pid: 42,
    memory,
    networkConnections: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    networkBridge: {
      connect: vi.fn(async () => ({ errno: 13 })),
    },
    kernel: {
      kernel_heap_ptr: () => 0x100,
      abi_econnrefused: () => 111,
      thread_connect_fd: () => 7,
      thread_connect_peer_addr: (_vmPtr: number, dstPtr: number) => {
        view.set([2, 0, 0, 80, 127, 0, 0, 1], dstPtr);
        return 8;
      },
    },
  } as unknown as ProcessHandle;
  const helpers = createWorkerProcessIoHelpers(createDeps());

  await expect(helpers.resolveNetworkConnect(handle, 0x2000)).resolves.toBe(-13n);
});

test("pumpNetworkConnections drains guest outbound bytes and injects inbound bytes", async () => {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  const view = new Uint8Array(memory.buffer);
  const written: string[] = [];
  let inboundRead = false;
  const handle = {
    pid: 42,
    vmPtr: 0x2000,
    memory,
    networkConnections: new Map([
      [7, {
        write: vi.fn((data: Uint8Array) => {
          written.push(new TextDecoder().decode(data));
        }),
        read: vi.fn(() => {
          if (inboundRead) {
            return undefined;
          }
          inboundRead = true;
          return new TextEncoder().encode("HTTP/1.1 200 OK\r\n\r\nok");
        }),
      }],
    ]),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    kernel: {
      kernel_heap_ptr: () => 0x100,
      abi_max_fds: () => 16,
      abi_fd_type_socket: () => 8,
      abi_enotconn: () => -107,
      abi_enotsock: () => -88,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 7 ? 7 : 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 7 ? 8 : 0,
      thread_socket_mark_external_peer: vi.fn(() => 0),
      thread_socket_drain_outbound: vi.fn((_vmPtr: number, _fd: number, dstPtr: number) => {
        if (written.length > 0) {
          return 0;
        }
        const bytes = new TextEncoder().encode("GET / HTTP/1.1\r\n\r\n");
        view.set(bytes, dstPtr);
        return bytes.length;
      }),
      thread_socket_inject_inbound: vi.fn((_vmPtr: number, _fd: number, srcPtr: number, srcLen: number) => srcLen),
    },
  } as unknown as ProcessHandle;
  const helpers = createWorkerProcessIoHelpers(createDeps());

  await expect(helpers.pumpNetworkConnections(handle)).resolves.toBe(true);

  expect(written).toEqual(["GET / HTTP/1.1\r\n\r\n"]);
  expect(handle.kernel.thread_socket_inject_inbound).toHaveBeenCalledWith(0x2000, 7, 0x34100, 21);
});

test("pumpNetworkConnections requests and injects large generic inbound chunks", async () => {
  const memory = new WebAssembly.Memory({ initial: 8, maximum: 8, shared: true });
  const payload = new Uint8Array(96 * 1024);
  payload.fill(0x61);
  let inboundRead = false;
  const readMaxBytes: number[] = [];
  const handle = {
    pid: 42,
    vmPtr: 0x2000,
    memory,
    networkConnections: new Map([
      [7, {
        write: vi.fn(),
        read: vi.fn((maxBytes: number) => {
          readMaxBytes.push(maxBytes);
          if (inboundRead) {
            return undefined;
          }
          inboundRead = true;
          return payload;
        }),
      }],
    ]),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    networkClosedSocketSlots: new Map(),
    kernel: {
      kernel_heap_ptr: () => 0x100,
      abi_max_fds: () => 16,
      abi_fd_type_socket: () => 8,
      abi_enotconn: () => -107,
      abi_enotsock: () => -88,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 7 ? 7 : 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 7 ? 8 : 0,
      thread_socket_mark_external_peer: vi.fn(() => 0),
      thread_socket_drain_outbound: vi.fn(() => 0),
      thread_socket_inject_inbound: vi.fn((_vmPtr: number, _fd: number, _srcPtr: number, srcLen: number) => srcLen),
    },
  } as unknown as ProcessHandle;
  const helpers = createWorkerProcessIoHelpers(createDeps());

  await expect(helpers.pumpNetworkConnections(handle)).resolves.toBe(true);

  expect(readMaxBytes[0]).toBe(128 * 1024);
  expect(handle.kernel.thread_socket_inject_inbound).toHaveBeenCalledWith(0x2000, 7, 0x34100, payload.length);
});

test("pumpNetworkConnections preserves pending large inbound bytes across partial socket writes", async () => {
  const memory = new WebAssembly.Memory({ initial: 16, maximum: 16, shared: true });
  const view = new Uint8Array(memory.buffer);
  const payload = new Uint8Array(384 * 1024);
  for (let i = 0; i < payload.length; i += 1) {
    payload[i] = i % 251;
  }
  const readMaxBytes: number[] = [];
  let readOffset = 0;
  let injectedOffset = 0;
  const maxSocketWrite = 48 * 1024;
  const handle = {
    pid: 42,
    vmPtr: 0x2000,
    memory,
    networkConnections: new Map([
      [7, {
        write: vi.fn(),
        read: vi.fn((maxBytes: number) => {
          readMaxBytes.push(maxBytes);
          if (readOffset >= payload.length) {
            return undefined;
          }
          const end = Math.min(payload.length, readOffset + maxBytes);
          const chunk = payload.subarray(readOffset, end);
          readOffset = end;
          return chunk;
        }),
      }],
    ]),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    networkClosedSocketSlots: new Map(),
    kernel: {
      kernel_heap_ptr: () => 0x100,
      abi_max_fds: () => 16,
      abi_fd_type_socket: () => 8,
      abi_enotconn: () => -107,
      abi_enotsock: () => -88,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 7 ? 7 : 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 7 ? 8 : 0,
      thread_socket_mark_external_peer: vi.fn(() => 0),
      thread_socket_drain_outbound: vi.fn(() => 0),
      thread_socket_inject_inbound: vi.fn((_vmPtr: number, _fd: number, srcPtr: number, srcLen: number) => {
        const written = Math.min(srcLen, maxSocketWrite);
        expect(view.slice(srcPtr, srcPtr + written)).toEqual(payload.slice(injectedOffset, injectedOffset + written));
        injectedOffset += written;
        return written;
      }),
    },
  } as unknown as ProcessHandle;
  const helpers = createWorkerProcessIoHelpers(createDeps());

  while (injectedOffset < payload.length) {
    await expect(helpers.pumpNetworkConnections(handle)).resolves.toBe(true);
  }

  expect(readMaxBytes.every((value) => value === 128 * 1024)).toBe(true);
  expect(readOffset).toBe(payload.length);
  expect(injectedOffset).toBe(payload.length);
});

test("async inbound read resumes pump with the source thread vm", async () => {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  let resolveRead: ((data: Uint8Array) => void) | null = null;
  let readStarted = false;
  const deps = createDeps();
  const handle = {
    pid: 42,
    vmPtr: 0x2000,
    memory,
    networkConnections: new Map([
      [7, {
        write: vi.fn(),
        read: vi.fn(() => {
          if (readStarted) {
            return undefined;
          }
          readStarted = true;
          return new Promise<Uint8Array>((resolve) => {
            resolveRead = resolve;
          });
        }),
      }],
    ]),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    kernel: {
      kernel_heap_ptr: () => 0x100,
      abi_max_fds: () => 16,
      abi_fd_type_socket: () => 8,
      abi_enotconn: () => -107,
      abi_enotsock: () => -88,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 7 ? 7 : 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 7 ? 8 : 0,
      thread_socket_mark_external_peer: vi.fn(() => 0),
      thread_socket_drain_outbound: vi.fn(() => 0),
      thread_socket_inject_inbound: vi.fn((_vmPtr: number, _fd: number, _srcPtr: number, srcLen: number) => srcLen),
    },
  } as unknown as ProcessHandle;
  const helpers = createWorkerProcessIoHelpers(deps);

  await expect(helpers.pumpNetworkConnections(handle, 0x3000)).resolves.toBe(false);

  resolveRead?.(new TextEncoder().encode("HTTP/1.1 200 OK\r\n\r\nok"));
  await vi.waitFor(() => {
    expect(handle.kernel.thread_socket_inject_inbound).toHaveBeenCalledWith(0x3000, 7, 0x34100, 21);
  });
  expect(deps.requestSocketStateSync).toHaveBeenCalledWith(handle, true);
  expect(deps.kickBlockedWorkers).toHaveBeenCalledWith(handle);
  expect(deps.pumpProcess).toHaveBeenCalledWith(handle);
});

test("async inbound read adopts the current process handle before injecting", async () => {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  let resolveRead: ((data: Uint8Array) => void) | null = null;
  const connection = {
    write: vi.fn(),
    read: vi.fn(() => new Promise<Uint8Array>((resolve) => {
      resolveRead = resolve;
    })),
  };
  const kernel = {
    kernel_heap_ptr: () => 0x100,
    abi_max_fds: () => 16,
    abi_fd_type_socket: () => 8,
    abi_enotconn: () => -107,
    abi_enotsock: () => -88,
    process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 7 ? 7 : 0,
    kernel_ofd_fd_type: (ofdId: number) => ofdId === 7 ? 8 : 0,
    thread_socket_mark_external_peer: vi.fn(() => 0),
    thread_socket_drain_outbound: vi.fn(() => 0),
    thread_socket_inject_inbound: vi.fn((_vmPtr: number, _fd: number, _srcPtr: number, srcLen: number) => srcLen),
  };
  const oldHandle = {
    pid: 42,
    vmPtr: 0x2000,
    memory,
    networkConnections: new Map([[7, connection]]),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    kernel,
  } as unknown as ProcessHandle;
  const activeHandle = {
    ...oldHandle,
    networkConnections: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set([7]),
    networkRecvHeadWatermarks: new Map([[7, 4096]]),
    networkRecvTailWatermarks: new Map([[7, 8192]]),
    kernel: {
      ...kernel,
      thread_socket_inject_inbound: vi.fn((_vmPtr: number, _fd: number, _srcPtr: number, srcLen: number) => srcLen),
    },
  } as unknown as ProcessHandle;
  const deps = createDeps();
  deps.isActiveHandle = vi.fn((handle: ProcessHandle) => handle === activeHandle);
  deps.getActiveHandle = vi.fn(() => activeHandle);
  const helpers = createWorkerProcessIoHelpers(deps);

  await expect(helpers.pumpNetworkConnections(oldHandle, 0x3000)).resolves.toBe(false);

  resolveRead?.(new TextEncoder().encode("HTTP/1.1 200 OK\r\n\r\nok"));
  await vi.waitFor(() => {
    expect(activeHandle.kernel.thread_socket_inject_inbound).toHaveBeenCalledWith(0x3000, 7, 0x34100, 21);
  });

  expect(activeHandle.networkConnections.get(7)).toBe(connection);
  expect(activeHandle.networkClosedFds.has(7)).toBe(false);
  expect(activeHandle.networkRecvHeadWatermarks.has(7)).toBe(false);
  expect(activeHandle.networkRecvTailWatermarks.has(7)).toBe(false);
  expect(deps.requestSocketStateSync).toHaveBeenCalledWith(activeHandle, true);
  expect(deps.kickBlockedWorkers).toHaveBeenCalledWith(activeHandle);
  expect(deps.pumpProcess).toHaveBeenCalledWith(activeHandle);
});

test("pending async empty read does not drop the next inbound response", async () => {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  const view = new Uint8Array(memory.buffer);
  let resolveEmptyRead: ((data: undefined) => void) | null = null;
  let readCalls = 0;
  let responseQueued = false;
  const deps = createDeps();
  const handle = {
    pid: 42,
    vmPtr: 0x2000,
    memory,
    networkConnections: new Map([
      [7, {
        write: vi.fn(),
        read: vi.fn(() => {
          readCalls += 1;
          if (readCalls === 1) {
            return new Promise<undefined>((resolve) => {
              resolveEmptyRead = resolve;
            });
          }
          if (responseQueued) {
            return undefined;
          }
          responseQueued = true;
          return Promise.resolve(new TextEncoder().encode("HTTP/1.1 200 OK\r\n\r\nok"));
        }),
      }],
    ]),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    kernel: {
      kernel_heap_ptr: () => 0x100,
      abi_max_fds: () => 16,
      abi_fd_type_socket: () => 8,
      abi_enotconn: () => -107,
      abi_enotsock: () => -88,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 7 ? 7 : 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 7 ? 8 : 0,
      thread_socket_mark_external_peer: vi.fn(() => 0),
      thread_socket_drain_outbound: vi.fn((_vmPtr: number, _fd: number, dstPtr: number) => {
        const bytes = new TextEncoder().encode("GET / HTTP/1.1\r\n\r\n");
        view.set(bytes, dstPtr);
        return bytes.length;
      }),
      thread_socket_inject_inbound: vi.fn((_vmPtr: number, _fd: number, _srcPtr: number, srcLen: number) => srcLen),
    },
  } as unknown as ProcessHandle;
  const helpers = createWorkerProcessIoHelpers(deps);

  await expect(helpers.pumpNetworkConnections(handle, 0x3000)).resolves.toBe(true);
  expect(readCalls).toBe(1);

  resolveEmptyRead?.(undefined);
  await Promise.resolve();
  await expect(helpers.pumpNetworkConnections(handle, 0x3000)).resolves.toBe(true);

  await vi.waitFor(() => {
    expect(handle.kernel.thread_socket_inject_inbound).toHaveBeenCalledWith(0x3000, 7, 0x34100, 21);
  });
});

test("post-write pump starts a response read after an async empty pre-read clears", async () => {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  const view = new Uint8Array(memory.buffer);
  let readCalls = 0;
  let wroteRequest = false;
  const deps = createDeps();
  const handle = {
    pid: 42,
    vmPtr: 0x2000,
    memory,
    networkConnections: new Map([
      [7, {
        write: vi.fn(() => {
          wroteRequest = true;
        }),
        read: vi.fn(() => {
          readCalls += 1;
          if (!wroteRequest) {
            return Promise.resolve(undefined);
          }
          return Promise.resolve(new TextEncoder().encode("HTTP/1.1 200 OK\r\n\r\nok"));
        }),
      }],
    ]),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    kernel: {
      kernel_heap_ptr: () => 0x100,
      abi_max_fds: () => 16,
      abi_fd_type_socket: () => 8,
      abi_enotconn: () => -107,
      abi_enotsock: () => -88,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 7 ? 7 : 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 7 ? 8 : 0,
      thread_socket_mark_external_peer: vi.fn(() => 0),
      thread_socket_drain_outbound: vi.fn((_vmPtr: number, _fd: number, dstPtr: number) => {
        if (wroteRequest) {
          return 0;
        }
        const bytes = new TextEncoder().encode("GET / HTTP/1.1\r\n\r\n");
        view.set(bytes, dstPtr);
        return bytes.length;
      }),
      thread_socket_inject_inbound: vi.fn((_vmPtr: number, _fd: number, _srcPtr: number, srcLen: number) => srcLen),
    },
  } as unknown as ProcessHandle;
  const helpers = createWorkerProcessIoHelpers(deps);

  await expect(helpers.pumpNetworkConnections(handle, 0x3000)).resolves.toBe(true);

  await vi.waitFor(() => {
    expect(readCalls).toBeGreaterThanOrEqual(2);
    expect(handle.kernel.thread_socket_inject_inbound).toHaveBeenCalledWith(0x3000, 7, 0x34100, 21);
  });
});

test("buffered EOF waits for guest recv drain before force-closing the socket", async () => {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  let readCalls = 0;
  let recvAvailable = 0;
  const connection = {
    close: vi.fn(async () => {}),
    write: vi.fn(),
    read: vi.fn(() => {
      readCalls += 1;
      if (readCalls === 1) {
        return new TextEncoder().encode("HTTP/1.1 200 OK\r\n\r\nok");
      }
      return null;
    }),
  };
  const handle = {
    pid: 42,
    vmPtr: 0x3000,
    memory,
    networkConnections: new Map([[7, connection]]),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    kernel: {
      kernel_heap_ptr: () => 0x100,
      abi_max_fds: () => 16,
      abi_fd_type_socket: () => 8,
      abi_enotconn: () => -107,
      abi_enotsock: () => -88,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 7 ? 7 : 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 7 ? 8 : 0,
      thread_socket_idx_for_fd: vi.fn(() => 0),
      thread_socket_state: vi.fn(() => 3),
      thread_socket_recv_buf_idx: vi.fn(() => 0),
      thread_socket_recv_available: vi.fn(() => recvAvailable),
      thread_socket_peer_idx: vi.fn(() => 1),
      thread_socket_mark_external_peer: vi.fn(() => 0),
      thread_socket_force_shutdown: vi.fn(),
      thread_socket_drain_outbound: vi.fn(() => 0),
      thread_socket_inject_inbound: vi.fn((_vmPtr: number, _fd: number, _srcPtr: number, srcLen: number) => {
        recvAvailable += srcLen;
        return srcLen;
      }),
    },
  } as unknown as ProcessHandle;
  const helpers = createWorkerProcessIoHelpers(createDeps());

  await expect(helpers.pumpNetworkConnections(handle, 0x3000)).resolves.toBe(true);

  expect(connection.read).toHaveBeenCalledTimes(2);
  expect(handle.kernel.thread_socket_force_shutdown).not.toHaveBeenCalled();
  expect(handle.networkClosedFds.has(7)).toBe(false);
  expect(handle.networkConnections.get(7)).toBe(connection);
  expect(connection.close).not.toHaveBeenCalled();
});

test("bridge EOF is deferred immediately after inbound injection even when recv availability is stale", async () => {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  let readCalls = 0;
  const connection = {
    close: vi.fn(async () => {}),
    write: vi.fn(),
    read: vi.fn(() => {
      readCalls += 1;
      if (readCalls === 1) {
        return new TextEncoder().encode("HTTP/1.1 200 OK\r\n\r\nok");
      }
      return null;
    }),
  };
  const handle = {
    pid: 42,
    vmPtr: 0x3000,
    memory,
    networkConnections: new Map([[7, connection]]),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    kernel: {
      kernel_heap_ptr: () => 0x100,
      abi_max_fds: () => 16,
      abi_fd_type_socket: () => 8,
      abi_enotconn: () => -107,
      abi_enotsock: () => -88,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 7 ? 7 : 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 7 ? 8 : 0,
      thread_socket_idx_for_fd: vi.fn(() => 0),
      thread_socket_state: vi.fn(() => 3),
      thread_socket_recv_buf_idx: vi.fn(() => 0),
      thread_socket_recv_available: vi.fn(() => 0),
      thread_socket_peer_idx: vi.fn(() => 1),
      thread_socket_mark_external_peer: vi.fn(() => 0),
      thread_socket_close_external_peer: vi.fn(),
      thread_socket_drain_outbound: vi.fn(() => 0),
      thread_socket_inject_inbound: vi.fn((_vmPtr: number, _fd: number, _srcPtr: number, srcLen: number) => srcLen),
    },
  } as unknown as ProcessHandle;
  const helpers = createWorkerProcessIoHelpers(createDeps());

  await expect(helpers.pumpNetworkConnections(handle, 0x3000)).resolves.toBe(true);

  expect(connection.read).toHaveBeenCalledTimes(2);
  expect(handle.kernel.thread_socket_close_external_peer).not.toHaveBeenCalled();
  expect(connection.close).not.toHaveBeenCalled();
  expect(handle.networkConnections.get(7)).toBe(connection);
});

test("bridge EOF marks peer shutdown but preserves the guest socket until fd close", async () => {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  const connection = {
    close: vi.fn(async () => {}),
    write: vi.fn(),
    read: vi.fn(() => null),
  };
  const handle = {
    pid: 42,
    vmPtr: 0x3000,
    memory,
    networkConnections: new Map([[7, connection]]),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map([[7, 0]]),
    networkRecvTailWatermarks: new Map([[7, 4096]]),
    kernel: {
      kernel_heap_ptr: () => 0x100,
      abi_max_fds: () => 16,
      abi_fd_type_socket: () => 8,
      abi_enotconn: () => -107,
      abi_enotsock: () => -88,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 7 ? 7 : 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 7 ? 8 : 0,
      thread_socket_idx_for_fd: vi.fn(() => 0),
      thread_socket_state: vi.fn(() => 3),
      thread_socket_recv_buf_idx: vi.fn(() => 0),
      thread_socket_recv_available: vi.fn(() => 0),
      thread_socket_peer_idx: vi.fn(() => 1),
      thread_socket_mark_external_peer: vi.fn(() => 0),
      thread_socket_close_external_peer: vi.fn(),
      thread_socket_drain_outbound: vi.fn(() => 0),
      thread_socket_inject_inbound: vi.fn(),
    },
  } as unknown as ProcessHandle;
  const helpers = createWorkerProcessIoHelpers(createDeps());

  await expect(helpers.pumpNetworkConnections(handle, 0x3000)).resolves.toBe(true);

  expect(handle.kernel.thread_socket_close_external_peer).toHaveBeenCalledWith(0x3000, 7);
  expect(connection.close).toHaveBeenCalledTimes(1);
  expect(handle.networkConnections.get(7)).toBe(connection);
  expect(handle.networkRecvHeadWatermarks.has(7)).toBe(false);
  expect(handle.networkRecvTailWatermarks.has(7)).toBe(false);
});

test("partial inbound is drained before bridge EOF closes the socket", async () => {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  let readCalls = 0;
  let recvAvailable = 0;
  let injectLimit = 7;
  const response = new TextEncoder().encode("HTTP/1.1 200 OK\r\nContent-Length: 12\r\n\r\nhello world!");
  const connection = {
    close: vi.fn(async () => {}),
    write: vi.fn(),
    read: vi.fn(() => {
      readCalls += 1;
      return readCalls === 1 ? response : null;
    }),
  };
  const handle = {
    pid: 42,
    vmPtr: 0x3000,
    memory,
    networkConnections: new Map([[7, connection]]),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    kernel: {
      kernel_heap_ptr: () => 0x100,
      abi_max_fds: () => 16,
      abi_fd_type_socket: () => 8,
      abi_enotconn: () => -107,
      abi_enotsock: () => -88,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 7 ? 7 : 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 7 ? 8 : 0,
      thread_socket_idx_for_fd: vi.fn(() => 0),
      thread_socket_state: vi.fn(() => 3),
      thread_socket_recv_buf_idx: vi.fn(() => 0),
      thread_socket_recv_available: vi.fn(() => recvAvailable),
      thread_socket_peer_idx: vi.fn(() => 1),
      thread_socket_mark_external_peer: vi.fn(() => 0),
      thread_socket_close_external_peer: vi.fn(),
      thread_socket_drain_outbound: vi.fn(() => 0),
      thread_socket_inject_inbound: vi.fn((_vmPtr: number, _fd: number, _srcPtr: number, srcLen: number) => {
        const written = Math.min(injectLimit, srcLen);
        recvAvailable += written;
        return written;
      }),
    },
  } as unknown as ProcessHandle;
  const helpers = createWorkerProcessIoHelpers(createDeps());

  await expect(helpers.pumpNetworkConnections(handle, 0x3000)).resolves.toBe(true);
  expect(handle.kernel.thread_socket_inject_inbound).toHaveBeenLastCalledWith(0x3000, 7, 0x34100, response.length);
  expect(connection.close).not.toHaveBeenCalled();
  expect(handle.networkConnections.get(7)).toBe(connection);

  recvAvailable = 0;
  injectLimit = response.length;
  await expect(helpers.pumpNetworkConnections(handle, 0x3000)).resolves.toBe(true);
  expect(connection.read).toHaveBeenCalledTimes(2);
  expect(connection.close).not.toHaveBeenCalled();
  expect(handle.networkConnections.get(7)).toBe(connection);

  recvAvailable = 0;
  for (let attempt = 0; attempt < 3; attempt += 1) {
    await helpers.pumpNetworkConnections(handle, 0x3000);
  }
  expect(connection.close).toHaveBeenCalledTimes(1);
  expect(handle.networkConnections.get(7)).toBe(connection);
});

test("stale network OFD rekeys when fork-shared sync renumbers the same socket fd", async () => {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  const view = new Uint8Array(memory.buffer);
  let currentOfd = 19;
  const connection = {
    close: vi.fn(async () => {}),
    read: vi.fn(() => undefined),
    write: vi.fn(),
  };
  const handle = {
    pid: 42,
    runtimeId: 1,
    vmPtr: 0x2000,
    memory,
    networkBridge: {
      connect: vi.fn(async () => ({ connection })),
    },
    networkConnections: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(8),
      stateSab: new SharedArrayBuffer(176),
    },
    kernelState: {},
    kernel: {
      kernel_heap_base: () => 0,
      kernel_heap_ptr: () => 0x100,
      kernel_ofd_table_ptr: () => 0,
      kernel_memfs_ptr: () => 0,
      kernel_memfs_header_bytes: () => 0,
      kernel_memfs_inodes_bytes: () => 0,
      kernel_memfs_first_child_bytes: () => 0,
      kernel_memfs_dir_entries_bytes: () => 0,
      kernel_memfs_data_bytes: () => 0,
      kernel_mem_state_ptr: () => 0,
      kernel_epoll_table_ptr: () => 0,
      kernel_socket_table_ptr: () => 0,
      kernel_socket_used: () => 0,
      kernel_pipe_base: () => 0,
      kernel_pipe_used: () => 0n,
      kernel_net_buf_base: () => 0,
      kernel_net_buf_used: () => 0n,
      kernel_sig_slots_ptr: () => 0,
      kernel_process_pending_ptr: () => 0,
      kernel_process_runtime_state_ptr: () => 0,
      kernel_time_state_ptr: () => 0,
      abi_econnrefused: () => 111,
      abi_einprogress: () => -115,
      abi_status_ok: () => 0,
      abi_max_fds: () => 8,
      abi_fd_type_socket: () => 8,
      abi_enotconn: () => -107,
      abi_enotsock: () => -88,
      thread_connect_fd: () => 3,
      thread_connect_peer_addr: (_vmPtr: number, dstPtr: number) => {
        view.set([2, 0, 0x1f, 0x90, 127, 0, 0, 1], dstPtr);
        return 8;
      },
      thread_a0: () => 0n,
      thread_status: () => 0,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 3 ? currentOfd : 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === currentOfd ? 8 : 0,
      thread_socket_idx_for_fd: vi.fn(() => 0),
      thread_socket_state: vi.fn(() => 3),
      thread_socket_recv_buf_idx: vi.fn(() => 0),
      thread_socket_recv_available: vi.fn(() => 0),
      thread_socket_peer_idx: vi.fn(() => -1),
      thread_socket_mark_external_peer: vi.fn(() => 0),
      thread_socket_force_shutdown: vi.fn(),
      thread_socket_drain_outbound: vi.fn(() => 0),
      thread_socket_inject_inbound: vi.fn(),
    },
  } as unknown as ProcessHandle;
  const helpers = createWorkerProcessIoHelpers(createDeps());

  handle.networkRecvHeadWatermarks.set(3, 4096);
  handle.networkRecvTailWatermarks.set(3, 188416);
  await expect(helpers.resolveNetworkConnect(handle, 0x2000)).resolves.toBe(0n);
  await helpers.commitNetworkConnect(handle, 0x2000);
  expect(handle.networkConnections.get(3)).toBe(connection);
  expect(handle.networkRecvHeadWatermarks.has(3)).toBe(false);
  expect(handle.networkRecvTailWatermarks.has(3)).toBe(false);

  currentOfd = 20;
  await expect(helpers.pumpNetworkConnections(handle, 0x2000)).resolves.toBe(true);

  expect(connection.close).not.toHaveBeenCalled();
  expect(handle.kernel.thread_socket_force_shutdown).not.toHaveBeenCalled();
  expect(handle.networkClosedFds.has(3)).toBe(false);
  expect(handle.networkConnections.get(3)).toBe(connection);
});

test("stale network OFD cleanup does not force-close a fd with a pending reconnect", async () => {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  const view = new Uint8Array(memory.buffer);
  let currentOfd = 19;
  let connectCount = 0;
  const oldConnection = {
    close: vi.fn(async () => {}),
    read: vi.fn(() => undefined),
    write: vi.fn(),
  };
  const nextConnection = {
    close: vi.fn(async () => {}),
    read: vi.fn(() => undefined),
    write: vi.fn(),
  };
  const handle = {
    pid: 42,
    runtimeId: 1,
    vmPtr: 0x2000,
    memory,
    networkBridge: {
      connect: vi.fn(async () => ({
        connection: connectCount++ === 0 ? oldConnection : nextConnection,
      })),
    },
    networkConnections: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(8),
      stateSab: new SharedArrayBuffer(176),
    },
    kernelState: {},
    kernel: {
      kernel_heap_base: () => 0,
      kernel_heap_ptr: () => 0x100,
      kernel_ofd_table_ptr: () => 0,
      kernel_memfs_ptr: () => 0,
      kernel_memfs_header_bytes: () => 0,
      kernel_memfs_inodes_bytes: () => 0,
      kernel_memfs_first_child_bytes: () => 0,
      kernel_memfs_dir_entries_bytes: () => 0,
      kernel_memfs_data_bytes: () => 0,
      kernel_mem_state_ptr: () => 0,
      kernel_epoll_table_ptr: () => 0,
      kernel_socket_table_ptr: () => 0,
      kernel_socket_used: () => 0,
      kernel_pipe_base: () => 0,
      kernel_pipe_used: () => 0n,
      kernel_net_buf_base: () => 0,
      kernel_net_buf_used: () => 0n,
      kernel_sig_slots_ptr: () => 0,
      kernel_process_pending_ptr: () => 0,
      kernel_process_runtime_state_ptr: () => 0,
      kernel_time_state_ptr: () => 0,
      abi_econnrefused: () => 111,
      abi_einprogress: () => -115,
      abi_status_ok: () => 0,
      abi_max_fds: () => 8,
      abi_fd_type_socket: () => 8,
      abi_enotconn: () => -107,
      abi_enotsock: () => -88,
      thread_connect_fd: () => 3,
      thread_connect_peer_addr: (_vmPtr: number, dstPtr: number) => {
        view.set([2, 0, 0x1f, 0x90, 127, 0, 0, 1], dstPtr);
        return 8;
      },
      thread_a0: () => 0n,
      thread_status: () => 0,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 3 ? currentOfd : 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === currentOfd ? 8 : 0,
      thread_socket_idx_for_fd: vi.fn(() => 0),
      thread_socket_state: vi.fn(() => 3),
      thread_socket_recv_buf_idx: vi.fn(() => 0),
      thread_socket_recv_available: vi.fn(() => 0),
      thread_socket_peer_idx: vi.fn(() => -1),
      thread_socket_mark_external_peer: vi.fn(() => 0),
      thread_socket_force_shutdown: vi.fn(),
      thread_socket_drain_outbound: vi.fn(() => 0),
      thread_socket_inject_inbound: vi.fn(),
    },
  } as unknown as ProcessHandle;
  const helpers = createWorkerProcessIoHelpers(createDeps());

  await expect(helpers.resolveNetworkConnect(handle, 0x2000)).resolves.toBe(0n);
  await helpers.commitNetworkConnect(handle, 0x2000);
  expect(handle.networkConnections.get(3)).toBe(oldConnection);

  currentOfd = 20;
  handle.networkRecvHeadWatermarks.set(3, 4096);
  handle.networkRecvTailWatermarks.set(3, 188416);
  await expect(helpers.resolveNetworkConnect(handle, 0x2000)).resolves.toBe(0n);
  await expect(helpers.pumpNetworkConnections(handle, 0x2000)).resolves.toBe(true);

  expect(oldConnection.close).toHaveBeenCalledTimes(1);
  expect(handle.kernel.thread_socket_force_shutdown).not.toHaveBeenCalled();
  expect(handle.networkClosedFds.has(3)).toBe(false);
  expect(handle.networkConnections.has(3)).toBe(false);
  expect(handle.networkRecvHeadWatermarks.has(3)).toBe(false);
  expect(handle.networkRecvTailWatermarks.has(3)).toBe(false);
});

test("stale network OFD cleanup closes bridge immediately when the fd disappeared", async () => {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  const view = new Uint8Array(memory.buffer);
  let currentOfd = 19;
  const connection = {
    close: vi.fn(async () => {}),
    read: vi.fn(() => undefined),
    write: vi.fn(),
  };
  const handle = {
    pid: 42,
    runtimeId: 1,
    vmPtr: 0x2000,
    memory,
    networkBridge: {
      connect: vi.fn(async () => ({ connection })),
    },
    networkConnections: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(8),
      stateSab: new SharedArrayBuffer(176),
    },
    kernelState: {},
    kernel: {
      kernel_heap_base: () => 0,
      kernel_heap_ptr: () => 0x100,
      kernel_ofd_table_ptr: () => 0,
      kernel_memfs_ptr: () => 0,
      kernel_memfs_header_bytes: () => 0,
      kernel_memfs_inodes_bytes: () => 0,
      kernel_memfs_first_child_bytes: () => 0,
      kernel_memfs_dir_entries_bytes: () => 0,
      kernel_memfs_data_bytes: () => 0,
      kernel_mem_state_ptr: () => 0,
      kernel_epoll_table_ptr: () => 0,
      kernel_socket_table_ptr: () => 0,
      kernel_socket_used: () => 0,
      kernel_pipe_base: () => 0,
      kernel_pipe_used: () => 0n,
      kernel_net_buf_base: () => 0,
      kernel_net_buf_used: () => 0n,
      kernel_sig_slots_ptr: () => 0,
      kernel_process_pending_ptr: () => 0,
      kernel_process_runtime_state_ptr: () => 0,
      kernel_time_state_ptr: () => 0,
      abi_econnrefused: () => 111,
      abi_einprogress: () => -115,
      abi_status_ok: () => 0,
      abi_max_fds: () => 8,
      abi_fd_type_socket: () => 8,
      abi_enotconn: () => -107,
      abi_enotsock: () => -88,
      thread_connect_fd: () => 3,
      thread_connect_peer_addr: (_vmPtr: number, dstPtr: number) => {
        view.set([2, 0, 0x1f, 0x90, 127, 0, 0, 1], dstPtr);
        return 8;
      },
      thread_a0: () => 0n,
      thread_status: () => 0,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 3 ? currentOfd : 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 19 ? 8 : 0,
      thread_socket_idx_for_fd: vi.fn(() => -1),
      thread_socket_state: vi.fn(() => -1),
      thread_socket_recv_buf_idx: vi.fn(() => -1),
      thread_socket_recv_available: vi.fn(() => 0),
      thread_socket_peer_idx: vi.fn(() => -1),
      thread_socket_mark_external_peer: vi.fn(() => 0),
      thread_socket_force_shutdown: vi.fn(),
      thread_socket_drain_outbound: vi.fn(() => -88),
      thread_socket_inject_inbound: vi.fn(),
    },
  } as unknown as ProcessHandle;
  const helpers = createWorkerProcessIoHelpers(createDeps());

  handle.networkRecvHeadWatermarks.set(3, 4096);
  handle.networkRecvTailWatermarks.set(3, 188416);
  await expect(helpers.resolveNetworkConnect(handle, 0x2000)).resolves.toBe(0n);
  await helpers.commitNetworkConnect(handle, 0x2000);
  expect(handle.networkConnections.get(3)).toBe(connection);

  currentOfd = 0;
  await expect(helpers.pumpNetworkConnections(handle, 0x2000)).resolves.toBe(true);

  expect(connection.close).toHaveBeenCalledTimes(1);
  expect(handle.kernel.thread_socket_force_shutdown).not.toHaveBeenCalled();
  expect(handle.networkClosedFds.has(3)).toBe(false);
  expect(handle.networkConnections.has(3)).toBe(false);
  expect(handle.networkRecvHeadWatermarks.has(3)).toBe(false);
  expect(handle.networkRecvTailWatermarks.has(3)).toBe(false);
});

test("not-connected drain during pending reconnect closes only the stale bridge", async () => {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  const view = new Uint8Array(memory.buffer);
  let connectCount = 0;
  const oldConnection = {
    close: vi.fn(async () => {}),
    read: vi.fn(() => undefined),
    write: vi.fn(),
  };
  const nextConnection = {
    close: vi.fn(async () => {}),
    read: vi.fn(() => undefined),
    write: vi.fn(),
  };
  const handle = {
    pid: 42,
    runtimeId: 1,
    vmPtr: 0x2000,
    memory,
    networkBridge: {
      connect: vi.fn(async () => ({
        connection: connectCount++ === 0 ? oldConnection : nextConnection,
      })),
    },
    networkConnections: new Map(),
    networkPendingConnectFds: new Set(),
    networkClosedFds: new Set(),
    networkClosedSocketSlots: new Map(),
    networkRecvHeadWatermarks: new Map(),
    networkRecvTailWatermarks: new Map(),
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(8),
      stateSab: new SharedArrayBuffer(176),
    },
    kernelState: {},
    kernel: {
      kernel_heap_base: () => 0,
      kernel_heap_ptr: () => 0x100,
      kernel_ofd_table_ptr: () => 0,
      kernel_memfs_ptr: () => 0,
      kernel_memfs_header_bytes: () => 0,
      kernel_memfs_inodes_bytes: () => 0,
      kernel_memfs_first_child_bytes: () => 0,
      kernel_memfs_dir_entries_bytes: () => 0,
      kernel_memfs_data_bytes: () => 0,
      kernel_mem_state_ptr: () => 0,
      kernel_epoll_table_ptr: () => 0,
      kernel_socket_table_ptr: () => 0,
      kernel_socket_used: () => 0,
      kernel_pipe_base: () => 0,
      kernel_pipe_used: () => 0n,
      kernel_net_buf_base: () => 0,
      kernel_net_buf_used: () => 0n,
      kernel_sig_slots_ptr: () => 0,
      kernel_process_pending_ptr: () => 0,
      kernel_process_runtime_state_ptr: () => 0,
      kernel_time_state_ptr: () => 0,
      abi_econnrefused: () => 111,
      abi_einprogress: () => -115,
      abi_status_ok: () => 0,
      abi_max_fds: () => 8,
      abi_fd_type_socket: () => 8,
      abi_enotconn: () => -107,
      abi_enotsock: () => -88,
      thread_connect_fd: () => 3,
      thread_connect_peer_addr: (_vmPtr: number, dstPtr: number) => {
        view.set([2, 0, 0x1f, 0x90, 127, 0, 0, 1], dstPtr);
        return 8;
      },
      thread_a0: () => 0n,
      thread_status: () => 0,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 3 ? 19 : 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 19 ? 8 : 0,
      thread_socket_idx_for_fd: vi.fn(() => 0),
      thread_socket_state: vi.fn(() => 2),
      thread_socket_recv_buf_idx: vi.fn(() => -1),
      thread_socket_recv_available: vi.fn(() => 0),
      thread_socket_peer_idx: vi.fn(() => -1),
      thread_socket_mark_external_peer: vi.fn(() => -107),
      thread_socket_force_shutdown: vi.fn(),
      thread_socket_drain_outbound: vi.fn(() => -107),
      thread_socket_inject_inbound: vi.fn(),
    },
  } as unknown as ProcessHandle;
  const helpers = createWorkerProcessIoHelpers(createDeps());

  await expect(helpers.resolveNetworkConnect(handle, 0x2000)).resolves.toBe(0n);
  await helpers.commitNetworkConnect(handle, 0x2000);
  expect(handle.networkConnections.get(3)).toBe(oldConnection);

  await expect(helpers.resolveNetworkConnect(handle, 0x2000)).resolves.toBe(0n);
  await expect(helpers.pumpNetworkConnections(handle, 0x2000)).resolves.toBe(true);

  expect(oldConnection.close).toHaveBeenCalledTimes(1);
  expect(handle.kernel.thread_socket_force_shutdown).not.toHaveBeenCalled();
  expect(handle.networkClosedFds.has(3)).toBe(false);
  expect(handle.networkConnections.has(3)).toBe(false);
});
