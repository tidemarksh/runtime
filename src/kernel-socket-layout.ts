import type { KernelExports } from "./abi.js";

type SocketCountExports = Pick<KernelExports, "abi_max_sockets">;
type NetBufferExports = SocketCountExports & Pick<KernelExports, "kernel_net_buf_pool_bytes">;
type SocketTableExports = SocketCountExports & Pick<KernelExports, "kernel_socket_table_bytes">;

export function kernelSocketCount(exports: SocketCountExports): number | undefined {
  const value = exports.abi_max_sockets?.();
  return Number.isSafeInteger(value) && value !== undefined && value > 0 ? value : undefined;
}

export function requireKernelSocketCount(exports: SocketCountExports): number {
  const value = kernelSocketCount(exports);
  if (value === undefined) {
    throw new Error("kernel supervisor ABI missing or invalid abi_max_sockets");
  }
  return value;
}

export function kernelNetBufferCount(exports: SocketCountExports): number {
  return requireKernelSocketCount(exports) * 2;
}

export function kernelNetBufferCountOrUndefined(exports: SocketCountExports): number | undefined {
  const count = kernelSocketCount(exports);
  return count === undefined ? undefined : count * 2;
}

export function kernelNetBufferSlotBytes(
  exports: NetBufferExports,
  netBufPoolBytes = exports.kernel_net_buf_pool_bytes(),
): number {
  const count = kernelNetBufferCount(exports);
  const bytes = count > 0 ? Math.floor(netBufPoolBytes / count) : 0;
  return Number.isInteger(bytes) && bytes > 0 ? bytes : 0;
}

export function kernelSocketSlotBytes(
  exports: SocketTableExports,
  socketTableBytes = exports.kernel_socket_table_bytes(),
): number {
  const count = requireKernelSocketCount(exports);
  const bytes = count > 0 ? socketTableBytes / count : 0;
  return Number.isInteger(bytes) && bytes > 0 ? bytes : 0;
}
