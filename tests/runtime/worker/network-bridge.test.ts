import { expect, test, vi } from "vite-plus/test";
import { createWorkerProcessIoHelpers } from "../../../src/worker/process-io.js";
import type { ProcessHandle } from "../../../src/worker/types.js";

function createDeps() {
  return {
    drainProcessOutput: vi.fn(() => false),
    failProcess: vi.fn(),
    flushStdin: vi.fn(() => false),
    isActiveHandle: vi.fn(() => true),
    kickBlockedWorkers: vi.fn(),
    requestPipeStateSync: vi.fn(async () => {}),
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

test("resolveNetworkConnect normalizes bridge errno to negative Linux result", async () => {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  const view = new Uint8Array(memory.buffer);
  const handle = {
    pid: 42,
    memory,
    networkConnections: new Map(),
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
  const handle = {
    pid: 42,
    vmPtr: 0x2000,
    memory,
    networkConnections: new Map([
      [7, {
        write: vi.fn((data: Uint8Array) => {
          written.push(new TextDecoder().decode(data));
        }),
        read: vi.fn(() => new TextEncoder().encode("HTTP/1.1 200 OK\r\n\r\nok")),
      }],
    ]),
    kernel: {
      kernel_heap_ptr: () => 0x100,
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
  expect(handle.kernel.thread_socket_inject_inbound).toHaveBeenCalledWith(0x2000, 7, 0x24100, 21);
});
