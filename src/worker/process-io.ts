import { readKernelRuntimeState } from "../abi.js";
import { publishKernelRuntimeState } from "../bridge/runtime-state.js";
import type { RuntimeNetworkConnection, RuntimeNetworkEndpoint } from "../network.js";
import type { ProcessHandle } from "./types.js";

interface WorkerProcessIoDeps {
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  drainProcessOutput(handle: ProcessHandle): boolean;
  failProcess(handle: ProcessHandle, error: Error): void;
  flushStdin(handle: ProcessHandle): boolean;
  isActiveHandle(handle: ProcessHandle): boolean;
  kickBlockedWorkers(handle: ProcessHandle): void;
  requestPipeStateSync(handle: ProcessHandle, wakeWaiters?: boolean): Promise<void>;
  writeProcessStdin(handle: ProcessHandle, data: Uint8Array): void;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const NETWORK_FD_TRACE_LOG_ENABLED = ((globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.TIDEMARK_WORKER_DEBUG_FD_TRACE ?? "") === "1";

function logNetworkDebug(_event: string): void {
  // Runtime diagnostics are kept in the per-process worker debug buffer.
}

function formatNetworkFdTrace(handle: ProcessHandle): string {
  if (!NETWORK_FD_TRACE_LOG_ENABLED) {
    return "";
  }
  const len = handle.kernel.debug_fd_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return "";
  }
  const sampleToPid = (sample: bigint | number | undefined): number => {
    if (sample === undefined) {
      return 0;
    }
    return Number(BigInt.asIntN(32, BigInt(sample) & 0xffffffffn));
  };
  const entries: string[] = [];
  for (let index = 0; index < len; index += 1) {
    const nr = Number(handle.kernel.debug_fd_syscall_trace_nr?.(index) ?? -1n);
    if (![57, 63, 64, 66, 72, 73, 198, 203, 206, 207, 208, 209, 211, 212].includes(nr)) {
      continue;
    }
    const arg1 = Number(handle.kernel.debug_fd_syscall_trace_arg1?.(index) ?? 0n);
    const arg2 = Number(handle.kernel.debug_fd_syscall_trace_arg2?.(index) ?? 0n);
    let text = "";
    const absoluteArg1 = handle.guestBase + arg1;
    if (
      nr === 64
      && arg1 > 0
      && arg2 > 0
      && arg2 <= 512
      && absoluteArg1 + arg2 <= handle.memory.buffer.byteLength
    ) {
      text = `:text=${JSON.stringify(decoder.decode(new Uint8Array(handle.memory.buffer, absoluteArg1, arg2)))}`;
    }
    entries.push([
      `${index}:${nr}`,
      `pid=${sampleToPid(handle.kernel.debug_fd_syscall_trace_pid_sample0?.(index))}`,
      `a0=${handle.kernel.debug_fd_syscall_trace_arg0?.(index)?.toString() ?? "n/a"}`,
      `a0ofd=${handle.kernel.debug_fd_syscall_trace_arg0_ofd_id?.(index) ?? "n/a"}`,
      `a0ty=${handle.kernel.debug_fd_syscall_trace_arg0_ofd_type?.(index) ?? "n/a"}`,
      `a1=${arg1 || handle.kernel.debug_fd_syscall_trace_arg1?.(index)?.toString() || "n/a"}`,
      `a2=${arg2 || handle.kernel.debug_fd_syscall_trace_arg2?.(index)?.toString() || "n/a"}`,
      `a2ofd=${handle.kernel.debug_fd_syscall_trace_arg2_ofd_id?.(index) ?? "n/a"}`,
      `a2ty=${handle.kernel.debug_fd_syscall_trace_arg2_ofd_type?.(index) ?? "n/a"}`,
      `ret=${handle.kernel.debug_fd_syscall_trace_result?.(index)?.toString() ?? "n/a"}`,
      `st=${handle.kernel.debug_fd_syscall_trace_status?.(index) ?? "n/a"}`,
    ].join(":") + text);
  }
  return entries.slice(-96).join(",");
}

function isPromiseLike<T>(value: T | PromiseLike<T>): value is PromiseLike<T> {
  return value !== null && value !== undefined && typeof (value as { then?: unknown }).then === "function";
}

export function createWorkerProcessIoHelpers(deps: WorkerProcessIoDeps) {
  const pendingNetworkInbound = new WeakMap<RuntimeNetworkConnection, Uint8Array<ArrayBuffer> | null>();
  const pendingNetworkReads = new WeakMap<RuntimeNetworkConnection, Promise<void>>();
  const networkDrainMisses = new WeakMap<RuntimeNetworkConnection, number>();
  const networkConnectionOfds = new WeakMap<ProcessHandle, Map<number, number>>();
  const pendingNetworkConnects = new WeakMap<ProcessHandle, Map<number, {
    fd: number;
    connection: RuntimeNetworkConnection;
  }>>();

  function queueNetworkReadPromise(
    handle: ProcessHandle,
    connection: RuntimeNetworkConnection,
    read: PromiseLike<Uint8Array | null | undefined>,
  ): void {
    if (pendingNetworkReads.has(connection)) {
      return;
    }
    const pending = Promise.resolve(read)
      .then((value) => {
        if (value !== undefined) {
          pendingNetworkInbound.set(connection, value === null ? null : new Uint8Array(value));
          deps.kickBlockedWorkers(handle);
        }
      })
      .catch((error) => {
        if (deps.isActiveHandle(handle)) {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        }
      })
      .finally(() => {
        pendingNetworkReads.delete(connection);
      });
    pendingNetworkReads.set(connection, pending);
  }

  function readConnectionNow(
    handle: ProcessHandle,
    connection: RuntimeNetworkConnection,
    maxBytes: number,
  ): Uint8Array | null | undefined {
    const result = connection.read(maxBytes);
    if (!isPromiseLike(result)) {
      return result;
    }
    queueNetworkReadPromise(handle, connection, result);
    return undefined;
  }

  function scheduleQueuedStdinFlush(handle: ProcessHandle): void {
    if (handle.stdinFlushPromise !== null) {
      return;
    }
    handle.stdinFlushPromise = (async () => {
      const pendingPipeSync = handle.pipeSyncPromise;
      if (pendingPipeSync !== null) {
        await pendingPipeSync;
      }
      if (!deps.isActiveHandle(handle)) {
        return;
      }
      const changed = deps.flushStdin(handle);
      if (!changed) {
        return;
      }
      await deps.requestPipeStateSync(handle, true);
    })().catch((error) => {
      if (deps.isActiveHandle(handle)) {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      }
    }).finally(() => {
      handle.stdinFlushPromise = null;
      if (deps.isActiveHandle(handle) && handle.stdinQueue.length > 0) {
        scheduleQueuedStdinFlush(handle);
      }
    });
  }

  function writeProcessStdin(handle: ProcessHandle, data: Uint8Array): void {
    deps.writeProcessStdin(handle, data);
    if (handle.pipeSyncPromise !== null) {
      scheduleQueuedStdinFlush(handle);
      return;
    }
    const changed = deps.flushStdin(handle);
    if (!changed) {
      return;
    }
    void deps.requestPipeStateSync(handle, true).catch((error) => {
      if (deps.isActiveHandle(handle)) {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      }
    });
  }

  async function injectHttpRequest(
    handle: ProcessHandle,
    port: number,
    request: string,
  ): Promise<string> {
    const requestBytes = encoder.encode(request);
    const scratch = handle.kernel.kernel_heap_ptr() + 0x20000;
    const chunkSize = 64 * 1024;
    const responseChunks: Uint8Array[] = [];

    new Uint8Array(handle.memory.buffer).set(requestBytes, scratch);
    const connId = handle.kernel.process_inject_http(
      handle.vmPtr,
      port,
      scratch,
      requestBytes.length,
    );
    if (connId < 0) {
      throw new Error(`no listening process on port ${port}`);
    }
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);

    const deadline = Date.now() + 10_000;
    try {
      for (;;) {
        deps.kickBlockedWorkers(handle);
        deps.drainProcessOutput(handle);

        const n = handle.kernel.process_read_http_response(
          handle.vmPtr,
          connId,
          scratch,
          chunkSize,
        );
        if (n < 0) {
          break;
        }
        if (n > 0) {
          responseChunks.push(new Uint8Array(handle.memory.buffer.slice(scratch, scratch + n)));
          continue;
        }
        if (Date.now() >= deadline) {
          throw new Error(`timed out waiting for HTTP response on port ${port}`);
        }
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
    } finally {
      handle.kernel.process_close_http_connection(handle.vmPtr, connId);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
    }

    const total = responseChunks.reduce((sum, chunk) => sum + chunk.length, 0);
    const merged = new Uint8Array(total);
    let offset = 0;
    for (const chunk of responseChunks) {
      merged.set(chunk, offset);
      offset += chunk.length;
    }
    return decoder.decode(merged);
  }

  function readConnectEndpoint(handle: ProcessHandle, vmPtr: number): {
    fd: number;
    endpoint: RuntimeNetworkEndpoint;
  } | null {
    const readPeerAddr = handle.kernel.thread_connect_peer_addr;
    const readConnectFd = handle.kernel.thread_connect_fd;
    if (!readPeerAddr || !readConnectFd) {
      return null;
    }
    const scratch = handle.kernel.kernel_heap_ptr() + 0x20000;
    const addrLen = readPeerAddr(vmPtr, scratch, 28);
    if (addrLen < 8) {
      return null;
    }
    const bytes = new Uint8Array(handle.memory.buffer, scratch, addrLen);
    const family = bytes[0] | (bytes[1] << 8);
    const port = (bytes[2] << 8) | bytes[3];
    if (family === 2 && addrLen >= 8) {
      return {
        fd: readConnectFd(vmPtr),
        endpoint: {
          family: "inet",
          address: `${bytes[4]}.${bytes[5]}.${bytes[6]}.${bytes[7]}`,
          port,
        },
      };
    }
    if (family === 10 && addrLen >= 24) {
      const groups: string[] = [];
      for (let offset = 8; offset < 24; offset += 2) {
        groups.push(((bytes[offset] << 8) | bytes[offset + 1]).toString(16));
      }
      return {
        fd: readConnectFd(vmPtr),
        endpoint: {
          family: "inet6",
          address: groups.join(":"),
          port,
        },
      };
    }
    return null;
  }

  async function resolveNetworkConnect(handle: ProcessHandle, vmPtr: number): Promise<bigint> {
    if (!handle.networkBridge) {
      return BigInt(-handle.kernel.abi_econnrefused());
    }
    const connect = readConnectEndpoint(handle, vmPtr);
    if (!connect) {
      return BigInt(-handle.kernel.abi_econnrefused());
    }
    try {
      const result = await handle.networkBridge.connect({
        pid: handle.pid,
        fd: connect.fd,
        endpoint: connect.endpoint,
      });
      const errno = result?.errno;
      if (errno === undefined || errno === 0) {
        if (result?.connection) {
          let pending = pendingNetworkConnects.get(handle);
          if (!pending) {
            pending = new Map();
            pendingNetworkConnects.set(handle, pending);
          }
          pending.set(vmPtr, {
            fd: connect.fd,
            connection: result.connection,
          });
        }
        deps.appendWorkerDebugLog(
          handle,
          `net-connect:${handle.pid}:fd=${connect.fd}:${connect.endpoint.family}:${connect.endpoint.address}:${connect.endpoint.port}:ok`,
        );
        logNetworkDebug(`connect pid=${handle.pid} fd=${connect.fd} ${connect.endpoint.family} ${connect.endpoint.address}:${connect.endpoint.port} ok connection=${result?.connection ? 1 : 0}`);
        return 0n;
      }
      deps.appendWorkerDebugLog(
        handle,
        `net-connect:${handle.pid}:fd=${connect.fd}:${connect.endpoint.family}:${connect.endpoint.address}:${connect.endpoint.port}:errno=${errno}`,
      );
      logNetworkDebug(`connect pid=${handle.pid} fd=${connect.fd} ${connect.endpoint.family} ${connect.endpoint.address}:${connect.endpoint.port} errno=${errno}`);
      return BigInt(errno < 0 ? errno : -errno);
    } catch {
      deps.appendWorkerDebugLog(
        handle,
        `net-connect:${handle.pid}:fd=${connect.fd}:${connect.endpoint.family}:${connect.endpoint.address}:${connect.endpoint.port}:throw`,
      );
      logNetworkDebug(`connect pid=${handle.pid} fd=${connect.fd} ${connect.endpoint.family} ${connect.endpoint.address}:${connect.endpoint.port} throw`);
      return BigInt(-handle.kernel.abi_econnrefused());
    }
  }

  async function commitNetworkConnect(handle: ProcessHandle, vmPtr: number): Promise<void> {
    const pending = pendingNetworkConnects.get(handle);
    const connect = pending?.get(vmPtr);
    if (!pending || !connect) {
      return;
    }
    pending.delete(vmPtr);
    if (pending.size === 0) {
      pendingNetworkConnects.delete(handle);
    }

    const result = BigInt.asIntN(64, handle.kernel.thread_a0(vmPtr));
    const einprogress = BigInt(handle.kernel.abi_einprogress?.() ?? -115);
    if (
      (result === 0n || result === einprogress)
      && handle.kernel.thread_status(vmPtr) === handle.kernel.abi_status_ok()
    ) {
      const existing = handle.networkConnections.get(connect.fd);
      if (existing && existing !== connect.connection) {
        await existing.close?.();
        pendingNetworkInbound.delete(existing);
        pendingNetworkReads.delete(existing);
      }
      handle.networkConnections.set(connect.fd, connect.connection);
      let ofds = networkConnectionOfds.get(handle);
      if (!ofds) {
        ofds = new Map();
        networkConnectionOfds.set(handle, ofds);
      }
      ofds.set(connect.fd, handle.kernel.process_fd_ofd_id(vmPtr, connect.fd));
      handle.kernelState = readKernelRuntimeState(handle.kernel);
      publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
      const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, connect.fd);
      const peerIdx = socketIdx !== undefined && socketIdx >= 0
        ? handle.kernel.thread_socket_peer_idx?.(socketIdx)
        : undefined;
      const socketState = socketIdx !== undefined && socketIdx >= 0
        ? handle.kernel.thread_socket_state?.(socketIdx)
        : undefined;
      const externalPeer = socketIdx !== undefined && socketIdx >= 0
        ? handle.kernel.thread_socket_external_peer?.(socketIdx)
        : undefined;
      logNetworkDebug(`connect-commit pid=${handle.pid} fd=${connect.fd} result=${result.toString()} status=${handle.kernel.thread_status(vmPtr)} sock=${socketIdx ?? "n/a"} peer=${peerIdx ?? "n/a"} state=${socketState ?? "n/a"} external=${externalPeer ?? "n/a"}`);
      return;
    }
    logNetworkDebug(`connect-drop pid=${handle.pid} fd=${connect.fd} result=${result.toString()} status=${handle.kernel.thread_status(vmPtr)}`);
    if (handle.networkConnections.get(connect.fd) === connect.connection) {
      handle.networkConnections.delete(connect.fd);
    }
    await connect.connection.close?.();
  }

  async function pumpNetworkConnections(handle: ProcessHandle, vmPtr = handle.vmPtr): Promise<boolean> {
    const drainOutbound = handle.kernel.thread_socket_drain_outbound;
    const injectInbound = handle.kernel.thread_socket_inject_inbound;
    const closeExternalPeer = handle.kernel.thread_socket_close_external_peer;
    const markExternalPeer = handle.kernel.thread_socket_mark_external_peer;
    if (!drainOutbound || !injectInbound || handle.networkConnections.size === 0) {
      return false;
    }
    const scratch = handle.kernel.kernel_heap_ptr() + 0x24000;
    const chunkSize = 16 * 1024;
    const memory = new Uint8Array(handle.memory.buffer);
    let changed = false;
    const closeNetworkConnection = async (
      fd: number,
      connection: RuntimeNetworkConnection,
      reason: string,
    ): Promise<void> => {
      closeExternalPeer?.(vmPtr, fd);
      pendingNetworkInbound.delete(connection);
      pendingNetworkReads.delete(connection);
      networkDrainMisses.delete(connection);
      networkConnectionOfds.get(handle)?.delete(fd);
      handle.networkConnections.delete(fd);
      await connection.close?.();
      deps.appendWorkerDebugLog(handle, `net-close:${handle.pid}:fd=${fd}:${reason}`);
      logNetworkDebug(`close pid=${handle.pid} fd=${fd} ${reason}`);
      const fdTrace = formatNetworkFdTrace(handle);
      if (fdTrace) {
        logNetworkDebug(`fd-trace pid=${handle.pid} ${fdTrace}`);
      }
      changed = true;
    };
    const enotsock = handle.kernel.abi_enotsock?.() ?? -88;
    const enotconn = handle.kernel.abi_enotconn?.() ?? -107;

    const socketDrainCandidates = (connectedFd: number): number[] => {
      if (handle.networkConnections.size !== 1) {
        return [connectedFd];
      }
      const candidates = [connectedFd];
      const maxFds = handle.kernel.abi_max_fds();
      for (let fd = 0; fd < maxFds; fd += 1) {
        if (fd === connectedFd) {
          continue;
        }
        const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
        if (
          ofdId > 0
          && handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_socket()
        ) {
          candidates.push(fd);
        }
      }
      return candidates;
    };

    const hasPendingInbound = (connection: RuntimeNetworkConnection): boolean =>
      pendingNetworkInbound.has(connection) || pendingNetworkReads.has(connection);

    const pumpInbound = async (
      fd: number,
      connection: RuntimeNetworkConnection,
    ): Promise<"closed" | "idle" | "blocked" | "changed"> => {
      for (;;) {
        const inbound = pendingNetworkInbound.has(connection)
          ? pendingNetworkInbound.get(connection) ?? null
          : readConnectionNow(handle, connection, chunkSize);
        if (inbound === null) {
          await closeNetworkConnection(fd, connection, "eof");
          return "closed";
        }
        if (inbound === undefined || inbound.length === 0) {
          return "idle";
        }
        const owned = new Uint8Array(inbound);
        memory.set(owned, scratch);
        markExternalPeer?.(vmPtr, fd);
        let written = injectInbound(vmPtr, fd, scratch, owned.length);
        if (written === enotconn && markExternalPeer?.(vmPtr, fd) === 0) {
          written = injectInbound(vmPtr, fd, scratch, owned.length);
        }
        if (written > 0) {
          networkDrainMisses.delete(connection);
          deps.appendWorkerDebugLog(handle, `net-inject:${handle.pid}:fd=${fd}:n=${written}`);
          logNetworkDebug(`inject pid=${handle.pid} fd=${fd} n=${written}`);
          changed = true;
        }
        if (written > 0 && written < owned.length) {
          pendingNetworkInbound.set(connection, owned.slice(written));
          logNetworkDebug(`inject-partial pid=${handle.pid} fd=${fd} written=${written} pending=${owned.length - written}`);
          return "blocked";
        }
        if (written === enotsock || written === enotconn) {
          deps.appendWorkerDebugLog(handle, `net-inject:${handle.pid}:fd=${fd}:closed=${written}:pending=${owned.length}`);
          logNetworkDebug(`inject-closed pid=${handle.pid} fd=${fd} written=${written} pending=${owned.length}`);
          await closeNetworkConnection(fd, connection, written === enotsock ? "inject-not-socket" : "inject-not-connected");
          return "closed";
        }
        if (written <= 0) {
          deps.appendWorkerDebugLog(handle, `net-inject:${handle.pid}:fd=${fd}:blocked=${written}:pending=${owned.length}`);
          logNetworkDebug(`inject-blocked pid=${handle.pid} fd=${fd} written=${written} pending=${owned.length}`);
          pendingNetworkInbound.set(connection, owned);
          return "blocked";
        }
        pendingNetworkInbound.delete(connection);
      }
    };

    for (const [fd, connection] of [...handle.networkConnections]) {
      const expectedOfd = networkConnectionOfds.get(handle)?.get(fd);
      const currentOfd = handle.kernel.process_fd_ofd_id(vmPtr, fd);
      if (expectedOfd !== undefined && currentOfd !== expectedOfd) {
        await closeNetworkConnection(fd, connection, `stale-ofd:${expectedOfd}->${currentOfd}`);
        continue;
      }
      const initialInbound = await pumpInbound(fd, connection);
      if (initialInbound === "closed") {
        continue;
      }
      if (initialInbound === "changed") {
        continue;
      }
      for (const drainFd of socketDrainCandidates(fd)) {
        for (;;) {
          const available = handle.kernel.thread_socket_outbound_available?.(vmPtr, drainFd);
          const socketIdx = handle.kernel.thread_socket_idx_for_fd?.(vmPtr, drainFd);
          const peerIdx = socketIdx !== undefined && socketIdx >= 0
            ? handle.kernel.thread_socket_peer_idx?.(socketIdx)
            : undefined;
          const socketState = socketIdx !== undefined && socketIdx >= 0
            ? handle.kernel.thread_socket_state?.(socketIdx)
            : undefined;
          const externalPeer = socketIdx !== undefined && socketIdx >= 0
            ? handle.kernel.thread_socket_external_peer?.(socketIdx)
            : undefined;
          const socketRecvBuf = socketIdx !== undefined && socketIdx >= 0
            ? handle.kernel.thread_socket_recv_buf_idx?.(socketIdx)
            : undefined;
          const socketRecvAvail = socketIdx !== undefined && socketIdx >= 0
            ? handle.kernel.thread_socket_recv_available?.(socketIdx)
            : undefined;
          const peerRecvBuf = peerIdx !== undefined && peerIdx >= 0
            ? handle.kernel.thread_socket_recv_buf_idx?.(peerIdx)
            : undefined;
          const peerRecvAvail = peerIdx !== undefined && peerIdx >= 0
            ? handle.kernel.thread_socket_recv_available?.(peerIdx)
            : undefined;
          if (drainFd === fd) {
            const markResult = markExternalPeer?.(vmPtr, fd);
            const repairedExternalPeer = markResult === 0
              && (
                (socketState !== undefined && socketState !== 3)
                || (externalPeer !== undefined && externalPeer !== 1)
              );
            if (repairedExternalPeer) {
              changed = true;
              logNetworkDebug(`mark-repair pid=${handle.pid} fd=${fd} state=${socketState ?? "n/a"} external=${externalPeer ?? "n/a"}`);
            } else if (markResult !== undefined && markResult !== 0) {
              const state = readKernelRuntimeState(handle.kernel);
              logNetworkDebug(`mark pid=${handle.pid} fd=${fd} result=${markResult} state=${socketState ?? "n/a"} external=${externalPeer ?? "n/a"} socketUsed=0x${state.socketUsed.toString(16)} netBufUsed=0x${state.netBufUsed.toString(16)}`);
            }
          }
          let n = drainOutbound(vmPtr, drainFd, scratch, chunkSize);
          if (drainFd === fd && n === enotconn && markExternalPeer?.(vmPtr, fd) === 0) {
            n = drainOutbound(vmPtr, drainFd, scratch, chunkSize);
          }
          if (n <= 0) {
            deps.appendWorkerDebugLog(handle, `net-drain:${handle.pid}:fd=${drainFd}:n=${n}:avail=${available ?? "n/a"}:sock=${socketIdx ?? "n/a"}:peer=${peerIdx ?? "n/a"}`);
            logNetworkDebug(`drain pid=${handle.pid} fd=${drainFd} n=${n} avail=${available ?? "n/a"} sock=${socketIdx ?? "n/a"} peer=${peerIdx ?? "n/a"} state=${socketState ?? "n/a"} external=${externalPeer ?? "n/a"} sockRecv=${socketRecvBuf ?? "n/a"}:${socketRecvAvail ?? "n/a"} peerRecv=${peerRecvBuf ?? "n/a"}:${peerRecvAvail ?? "n/a"}`);
            const fdTrace = drainFd === fd ? formatNetworkFdTrace(handle) : "";
            if (fdTrace) {
              logNetworkDebug(`fd-trace pid=${handle.pid} ${fdTrace}`);
            }
            if (drainFd === fd && (n === enotsock || n === enotconn) && !hasPendingInbound(connection)) {
              if (n === enotconn) {
                const misses = (networkDrainMisses.get(connection) ?? 0) + 1;
                networkDrainMisses.set(connection, misses);
                if (misses <= 4) {
                  logNetworkDebug(`drain-defer pid=${handle.pid} fd=${fd} misses=${misses}`);
                  break;
                }
              }
              await closeNetworkConnection(fd, connection, n === enotsock ? "not-socket" : "not-connected");
            }
            break;
          }
          const outbound = new Uint8Array(handle.memory.buffer.slice(scratch, scratch + n));
          await connection.write(outbound);
          networkDrainMisses.delete(connection);
          deps.appendWorkerDebugLog(handle, `net-drain:${handle.pid}:fd=${drainFd}:n=${n}:write`);
          logNetworkDebug(`drain pid=${handle.pid} fd=${drainFd} n=${n} write`);
          changed = true;
          break;
        }
      }
      await pumpInbound(fd, connection);
    }
    return changed;
  }

  return {
    commitNetworkConnect,
    injectHttpRequest,
    pumpNetworkConnections,
    resolveNetworkConnect,
    writeProcessStdin,
  };
}
