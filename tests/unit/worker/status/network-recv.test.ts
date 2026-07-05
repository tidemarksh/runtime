import { expect, test, vi } from "vite-plus/test";
import {
  deriveNetBufUsedFromSocketSlots,
  pruneStatusNetworkConnectionsFromFdEntries,
} from "../../../../src/worker/message-handler/status/network-recv.js";
import {
  networkConnectionOfds,
  networkConnections,
  networkRecvHeadWatermarks,
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

test("status network recv lists runtime network fds", () => {
  const handle = makeHandle({
    networkConnections: new Map([[3, {}]]),
  });
  seedNetworkSocketState(handle, {
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
    networkConnections: new Map([[4, { close }]]),
    kernel: makeKernel({
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fdMap.get(fd) ?? 0,
      kernel_ofd_fd_type: () => 8,
      supervisor_socket_bridge_active_fd: supervisorSocketBridgeActiveFd,
    }),
  });
  seedNetworkSocketState(handle, {
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

test("status network recv rekeys stale runtime network bridges from supervisor active fd", () => {
  const fdMap = new Map<number, number>([
    [4, 12],
    [6, 11],
  ]);
  const connection = { close: vi.fn(async () => {}) };
  const appendWorkerDebugLog = vi.fn();
  const supervisorSocketBridgeActiveFd = vi.fn(() => 6);
  const handle = makeHandle({
    networkConnections: new Map([[4, connection]]),
    kernel: makeKernel({
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fdMap.get(fd) ?? 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 11 || ofdId === 12 ? 8 : 0,
      supervisor_socket_bridge_active_fd: supervisorSocketBridgeActiveFd,
    }),
  });
  seedNetworkSocketState(handle, {
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
    networkConnections: new Map([[4, connection]]),
    kernel: makeKernel({
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fdMap.get(fd) ?? 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 11 ? 8 : 0,
      supervisor_socket_bridge_active_fd: supervisorSocketBridgeActiveFd,
    }),
  });
  seedNetworkSocketState(handle, {
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
