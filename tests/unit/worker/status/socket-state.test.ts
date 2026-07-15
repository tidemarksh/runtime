import { expect, test, vi } from "vite-plus/test";
import { applyThreadSocketState } from "../../../../src/worker/message-handler/status/socket-state.js";
import { makeHandle, makeKernel, makeKernelState, makeStatusMessage } from "./helpers.js";

test("status socket state applies table and net-buffer snapshots through the socket owner", () => {
  const attachRuntimeState = vi.fn();
  const socketTable = new Uint8Array(32);
  const netBufPool = new Uint8Array(64);
  socketTable[0] = 0xaa;
  netBufPool[16] = 0xbb;
  const handle = makeHandle({
    kernel: makeKernel({
      kernel_attach_runtime_state: attachRuntimeState,
      kernel_socket_table_bytes: () => socketTable.length,
      kernel_net_buf_pool_bytes: () => netBufPool.length,
      abi_max_sockets: () => 1,
    }),
    kernelState: makeKernelState({
      socketTablePtr: 0x2000,
      netBufBase: 0x3000,
    }),
  });

  applyThreadSocketState(handle as never, makeStatusMessage({
    socketState: {
      socketTable,
      netBufPool,
      socketUsed: 1,
      netBufUsed: 1n,
    },
  }) as never);

  const memory = new Uint8Array((handle.memory as WebAssembly.Memory).buffer);
  expect(memory[0x2000]).toBe(0xaa);
  expect(memory[0x3010]).toBe(0xbb);
  expect((handle.kernelState as { socketUsed: number }).socketUsed).toBe(1);
  expect((handle.kernelState as { netBufUsed: bigint }).netBufUsed).toBe(1n);
  expect(attachRuntimeState).toHaveBeenCalled();
});

test("status socket state applies complete Kernel state while retaining table compatibility", () => {
  const attachRuntimeState = vi.fn();
  const socketTableBytes = 32;
  const socketStateBytes = 64;
  const socketState = Uint8Array.from({ length: socketStateBytes }, (_, index) => 0x20 + index);
  const handle = makeHandle({
    kernel: makeKernel({
      kernel_attach_runtime_state: attachRuntimeState,
      kernel_socket_table_bytes: () => socketTableBytes,
      kernel_socket_state_ptr: () => 0x2000,
      kernel_socket_state_bytes: () => socketStateBytes,
      kernel_net_buf_pool_bytes: () => 64,
      abi_max_sockets: () => 1,
    }),
    kernelState: makeKernelState({
      socketTablePtr: 0x2000,
      socketStatePtr: 0x2000,
      socketStateBytes,
      netBufBase: 0x3000,
    }),
  });

  applyThreadSocketState(handle as never, makeStatusMessage({
    socketState: {
      socketTable: new Uint8Array(socketTableBytes),
      socketState,
      netBufPool: new Uint8Array(64),
      socketUsed: 1,
      netBufUsed: 0n,
    },
  }) as never);

  const memory = new Uint8Array((handle.memory as WebAssembly.Memory).buffer);
  expect([...memory.slice(0x2000, 0x2040)]).toEqual([...socketState]);
  expect(attachRuntimeState).toHaveBeenCalled();
});

test("status socket state rejects mismatched snapshot sizes", () => {
  const attachRuntimeState = vi.fn();
  const handle = makeHandle({
    kernel: makeKernel({
      kernel_attach_runtime_state: attachRuntimeState,
      kernel_socket_table_bytes: () => 32,
      kernel_net_buf_pool_bytes: () => 64,
      abi_max_sockets: () => 1,
    }),
  });

  applyThreadSocketState(handle as never, makeStatusMessage({
    socketState: {
      socketTable: new Uint8Array(31),
      netBufPool: new Uint8Array(64),
      socketUsed: 1,
      netBufUsed: 1n,
    },
  }) as never);

  expect((handle.kernelState as { socketUsed: number }).socketUsed).toBe(0);
  expect(attachRuntimeState).not.toHaveBeenCalled();
});

test("status socket state releases slots absent from the authoritative Kernel snapshot", () => {
  const handle = makeHandle({
    kernel: makeKernel({
      kernel_socket_table_bytes: () => 32,
      kernel_net_buf_pool_bytes: () => 64,
      abi_max_sockets: () => 1,
    }),
    kernelState: makeKernelState({
      socketUsed: 1,
      netBufUsed: 1n,
      socketTablePtr: 0x2000,
      netBufBase: 0x3000,
    }),
  });

  applyThreadSocketState(handle as never, makeStatusMessage({
    socketState: {
      socketTable: new Uint8Array(32),
      netBufPool: new Uint8Array(64),
      socketUsed: 0,
      netBufUsed: 0n,
    },
  }) as never);

  expect((handle.kernelState as { socketUsed: number }).socketUsed).toBe(0);
  expect((handle.kernelState as { netBufUsed: bigint }).netBufUsed).toBe(0n);
});
