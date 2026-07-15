import { expect, test, vi } from "vite-plus/test";
import {
  deriveNetBufUsedFromSocketSlots,
  preserveNetworkRecvBuffers,
  preserveNetworkRecvForKernelMemoryWrite,
  pruneStatusNetworkConnectionsFromFdEntries,
  reconcileNetworkRecvBufferConsumers,
  restoreNetworkRecvBufferProducers,
} from "../../../../src/worker/message-handler/status/network-recv.js";
import {
  networkClosedFds,
  networkClosedSocketSlots,
  networkConnectionOfds,
  networkConnections,
  networkRecvHeadWatermarks,
  networkRecvPayloadWatermarks,
  networkRecvTailWatermarks,
} from "../../../../src/worker/network-socket-state.js";
import { runtimeNetworkFds } from "../../../../src/worker/ownership/socket-control.js";
import { makeHandle, makeKernel, makeStatusMessage, seedNetworkSocketState } from "./helpers.js";

test("status network recv derives used net buffers from live socket slots", () => {
  const handle = makeHandle({
    kernel: makeKernel({
      abi_max_sockets: () => 2,
      thread_socket_recv_buf_idx: (socketIdx: number) => socketIdx === 0 ? 1 : 3,
    }),
  });

  expect(deriveNetBufUsedFromSocketSlots(handle as never, 0b11)).toBe(0b1010n);
});

test("status network recv retains proven consumer progress while restoring host payload", () => {
  let readBytes = 0;
  const handle = makeHandle({
    kernel: makeKernel({
      abi_max_sockets: () => 1,
      kernel_net_buf_pool_bytes: () => 64,
      thread_socket_idx_for_fd: (_vmPtr: number, fd: number) => fd === 4 ? 0 : -1,
      thread_socket_peer_idx: () => -1,
      thread_socket_recv_buf_idx: () => 0,
      thread_socket_state: () => 3,
      thread_socket_external_peer: () => 1,
      thread_socket_read_bytes: () => BigInt(readBytes),
      thread_socket_recv_available: () => 0,
    }),
  });
  seedNetworkSocketState(handle, {
    connections: [[4, {}]],
    recvPayloadWatermarks: [[4, {
      recvBufIdx: 0,
      capacity: 16,
      head: 0,
      tail: 10,
      readBytes: 0,
      availableBytes: 10,
      deliveredBytes: 10,
      data: Uint8Array.from({ length: 16 }, (_, index) => index + 1),
    }]],
  });
  const memory = new Uint8Array(handle.memory.buffer);
  const view = new DataView(handle.memory.buffer);
  view.setUint32(0x4000, 0, true);
  view.setUint32(0x4004, 10, true);
  memory.set(Uint8Array.from({ length: 16 }, (_, index) => index + 1), 0x4010);
  const preserved = preserveNetworkRecvBuffers(handle as never, 0x1234, 64);

  view.setUint32(0x4000, 10, true);
  view.setUint32(0x4004, 10, true);
  readBytes = 10;
  reconcileNetworkRecvBufferConsumers(handle as never, 0x1234, preserved);
  restoreNetworkRecvBufferProducers(memory, 0x4000, preserved);

  expect(view.getUint32(0x4000, true)).toBe(10);
  expect(view.getUint32(0x4004, true)).toBe(10);
  expect(networkRecvPayloadWatermarks(handle as never).get(4)).toEqual(expect.objectContaining({
    head: 10,
    readBytes: 10,
    availableBytes: 0,
  }));
});

test("status network recv lists runtime network fds", () => {
  const handle = makeHandle();
  seedNetworkSocketState(handle, {
    connections: [[3, {}]],
    pendingConnectFds: [4],
    closedFds: [5],
    socketSlotWatermarks: [[6, { offset: 0, data: new Uint8Array(1) }]],
    recvPayloadWatermarks: [[7, {
      capacity: 8,
      data: new Uint8Array(8),
      head: 0,
      tail: 1,
    }]],
  });

  expect(runtimeNetworkFds(handle as never)).toEqual(new Set([3, 4, 5, 6, 7]));
});

test("status network recv closes stale runtime network bridges when supervisor reports no active fd", async () => {
  const fdMap = new Map<number, number>([[4, 11]]);
  const close = vi.fn(async () => {});
  const appendWorkerDebugLog = vi.fn();
  const supervisorSocketBridgeActiveFd = vi.fn(() => -2);
  const handle = makeHandle({
    kernel: makeKernel({
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fdMap.get(fd) ?? 0,
      kernel_ofd_fd_type: () => 8,
      supervisor_socket_bridge_active_fd: supervisorSocketBridgeActiveFd,
    }),
  });
  seedNetworkSocketState(handle, {
    connections: [[4, { close }]],
    connectionOfds: [[4, 11]],
    closedFds: [4],
  });
  fdMap.delete(4);

  pruneStatusNetworkConnectionsFromFdEntries(
    handle as never,
    makeStatusMessage({
      fdEntries: [{ fd: 4, ofdId: 0, fdFlags: 0 }],
    }) as never,
    {
      appendWorkerDebugLog,
      failProcess: (_handle, error) => { throw error; },
    },
  );
  await Promise.resolve();

  expect((networkConnections(handle) as Map<number, unknown>).has(4)).toBe(false);
  expect(close).toHaveBeenCalled();
  expect(supervisorSocketBridgeActiveFd).toHaveBeenCalledWith(0x1234, 4, 11, 0);
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "net-status-close:7:fd=4:ofd=11->0",
  );
});

test("status network recv drops closed socket sidecars when an fd is reused", async () => {
  const close = vi.fn(async () => {});
  let generation = 8;
  const handle = makeHandle({
    kernel: makeKernel({
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 4 ? 11 : 0,
      process_fd_generation: (_vmPtr: number, fd: number) => fd === 4 ? generation : 0,
      supervisor_process_socket_ofd_for_fd: (_vmPtr: number, fd: number) => fd === 4 ? 11 : 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 11 ? 8 : 0,
      supervisor_socket_bridge_active_fd: () => -2,
    }),
  });
  seedNetworkSocketState(handle, {
    connections: [[4, { close }]],
    connectionOfds: [[4, 11]],
    closedFds: [4],
    closedSocketSlots: [[4, { offset: 0, data: new Uint8Array(32), ofdId: 11, generation: 7 }]],
    recvHeadWatermarks: [[4, 7]],
    recvTailWatermarks: [[4, 9]],
  });
  generation = 8;

  pruneStatusNetworkConnectionsFromFdEntries(
    handle as never,
    makeStatusMessage({
      fdEntries: [{ fd: 4, ofdId: 12, fdFlags: 0 }],
    }) as never,
    {
      appendWorkerDebugLog: vi.fn(),
      failProcess: (_handle, error) => { throw error; },
    },
  );
  await Promise.resolve();

  expect(networkConnections(handle as never).has(4)).toBe(false);
  expect(networkConnectionOfds(handle as never).has(4)).toBe(false);
  expect(networkClosedFds(handle as never).has(4)).toBe(false);
  expect(networkClosedSocketSlots(handle as never).has(4)).toBe(false);
  expect(networkRecvHeadWatermarks(handle as never).has(4)).toBe(false);
  expect(networkRecvTailWatermarks(handle as never).has(4)).toBe(false);
  expect(close).toHaveBeenCalledTimes(1);
});

test("status network recv does not preserve an old recv buffer for a newer fd generation", () => {
  const handle = makeHandle({
    kernel: makeKernel({
      kernel_socket_used: () => 1,
      kernel_net_buf_used: () => 1n,
      process_fd_generation: (_vmPtr: number, fd: number) => fd === 4 ? 7 : 0,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 4 ? 11 : 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 11 ? 8 : 0,
      thread_socket_idx_for_fd: (_vmPtr: number, fd: number) => fd === 4 ? 0 : -1,
      thread_socket_peer_idx: () => -1,
      thread_socket_recv_buf_idx: (socketIdx: number) => socketIdx === 0 ? 0 : -1,
    }),
  });
  handle.kernelState.socketUsed = 1;
  handle.kernelState.netBufUsed = 1n;
  seedNetworkSocketState(handle, {
    connections: [[4, {}]],
    connectionOfds: [[4, 11]],
  });
  const memory = new Uint8Array(handle.memory.buffer);
  const view = new DataView(handle.memory.buffer);
  view.setUint32(0x4000, 0, true);
  view.setUint32(0x4004, 16, true);
  memory[0x4010] = 0xaa;

  const restore = preserveNetworkRecvForKernelMemoryWrite(
    handle as never,
    makeStatusMessage({
      fdEntries: [{ fd: 4, ofdId: 12, fdFlags: 0, generation: 8 }],
    }) as never,
    0x4000,
    64,
  );

  expect(restore).toBeNull();
});

test("status network recv rekeys stale runtime network bridges from supervisor active fd", () => {
  const fdMap = new Map<number, number>([
    [4, 12],
    [6, 11],
  ]);
  const connection = { close: vi.fn(async () => {}) };
  const appendWorkerDebugLog = vi.fn();
  const supervisorSocketBridgeActiveFd = vi.fn(() => 6);
  const handle = makeHandle({
    kernel: makeKernel({
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fdMap.get(fd) ?? 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 11 || ofdId === 12 ? 8 : 0,
      supervisor_socket_bridge_active_fd: supervisorSocketBridgeActiveFd,
    }),
  });
  seedNetworkSocketState(handle, {
    connections: [[4, connection]],
    connectionOfds: [[4, 11]],
    recvHeadWatermarks: [[4, 4096]],
    recvTailWatermarks: [[4, 8192]],
  });

  pruneStatusNetworkConnectionsFromFdEntries(
    handle as never,
    makeStatusMessage({
      fdEntries: [
        { fd: 4, ofdId: 12, fdFlags: 0 },
        { fd: 6, ofdId: 11, fdFlags: 0 },
      ],
    }) as never,
    {
      appendWorkerDebugLog,
      failProcess: (_handle, error) => { throw error; },
    },
  );

  expect((networkConnections(handle) as Map<number, unknown>).has(4)).toBe(false);
  expect((networkConnections(handle) as Map<number, unknown>).get(6)).toBe(connection);
  expect(networkConnectionOfds(handle as never).has(4)).toBe(false);
  expect(networkConnectionOfds(handle as never).get(6)).toBe(11);
  expect(networkRecvHeadWatermarks(handle as never).has(4)).toBe(false);
  expect(networkRecvHeadWatermarks(handle as never).get(6)).toBe(4096);
  expect(networkRecvTailWatermarks(handle as never).has(4)).toBe(false);
  expect(networkRecvTailWatermarks(handle as never).get(6)).toBe(8192);
  expect(connection.close).not.toHaveBeenCalled();
  expect(supervisorSocketBridgeActiveFd).toHaveBeenCalledWith(0x1234, 4, 11, 0);
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "net-status-rekey:7:fd=4->6:ofd=11",
  );
});

test("status network recv keeps runtime network bridges from supervisor active fd", () => {
  const fdMap = new Map<number, number>([[4, 11]]);
  const connection = { close: vi.fn(async () => {}) };
  const appendWorkerDebugLog = vi.fn();
  const supervisorSocketBridgeActiveFd = vi.fn(() => 4);
  const handle = makeHandle({
    kernel: makeKernel({
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fdMap.get(fd) ?? 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 11 ? 8 : 0,
      supervisor_socket_bridge_active_fd: supervisorSocketBridgeActiveFd,
    }),
  });
  seedNetworkSocketState(handle, {
    connections: [[4, connection]],
    connectionOfds: [[4, 11]],
  });

  pruneStatusNetworkConnectionsFromFdEntries(
    handle as never,
    makeStatusMessage({
      fdEntries: [{ fd: 4, ofdId: 0, fdFlags: 0 }],
    }) as never,
    {
      appendWorkerDebugLog,
      failProcess: (_handle, error) => { throw error; },
    },
  );

  expect((networkConnections(handle) as Map<number, unknown>).get(4)).toBe(connection);
  expect(networkConnectionOfds(handle as never).get(4)).toBe(11);
  expect(connection.close).not.toHaveBeenCalled();
  expect(supervisorSocketBridgeActiveFd).toHaveBeenCalledWith(0x1234, 4, 11, 0);
  expect(appendWorkerDebugLog).not.toHaveBeenCalledWith(
    handle,
    expect.stringContaining("net-status-close"),
  );
});
