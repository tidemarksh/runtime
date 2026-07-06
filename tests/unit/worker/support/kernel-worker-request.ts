import type {
  KernelWorkerOutbound,
  KernelWorkerRequest,
} from "../../../../src/messages.js";

export function createEchoKernelWorkerRequest(options: {
  getVforkChildPid?: () => number;
} = {}): (msg: KernelWorkerRequest) => Promise<KernelWorkerOutbound> {
  return async (msg): Promise<KernelWorkerOutbound> => {
    switch (msg.type) {
      case "register-process":
        return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
      case "unregister-process":
        return { type: "process-unregistered", requestId: 0 };
      case "initialize-process-lifecycle":
        return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
      case "sync-process-lifecycle":
        return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
      case "canonicalize-child-pid":
        return {
          type: "child-pid-canonicalized",
          requestId: 0,
          childPid: msg.provisionalChildPid,
          ppid: msg.pid,
          nextPid: Math.max(3, msg.provisionalChildPid + 1, msg.pid + 1),
          pgid: msg.pid,
          sid: msg.pid,
          kernelState: msg.kernelState,
        };
      case "record-child-exit":
        return { type: "child-exit-recorded", requestId: 0, kernelState: msg.kernelState };
      case "resume-wait4":
        return { type: "wait4-resumed", requestId: 0, kernelState: msg.kernelState };
      case "resume-vfork-parent":
        return {
          type: "vfork-parent-resumed",
          requestId: 0,
          childPid: options.getVforkChildPid?.() ?? msg.pid + 1,
          kernelState: msg.kernelState,
        };
      case "resume-vfork-execve-parent":
        return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: msg.kernelState };
      case "rollback-vfork-execve-handoff":
        return {
          type: "vfork-execve-handoff-rolled-back",
          requestId: 0,
          waitStatus: 127 << 8,
          kernelState: msg.kernelState,
        };
      case "sync-process-memfs":
        return { type: "process-memfs-synced", requestId: 0, kernelState: msg.kernelState };
      case "sync-pipe-state":
        return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
      case "sync-epoll-state":
        return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
      case "sync-ofd-state":
        return { type: "ofd-state-synced", requestId: 0, kernelState: msg.kernelState };
      case "sync-socket-state":
        return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
      case "sync-fork-shared-state":
        return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
      case "rehydrate-fork-shared-state":
        return { type: "fork-shared-state-rehydrated", requestId: 0 };
      case "release-host-stdio":
        return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
      case "resume-connect":
        return { type: "connect-resumed", requestId: 0, kernelState: msg.kernelState };
      case "process-fs":
        return {
          type: "fs-processed",
          requestId: 0,
          kernelState: msg.kernelState,
          result: 0n,
          memfsMutated: false,
          ofdStateChanged: false,
        };
      case "snapshot-fs":
        return { type: "snapshot-fs-result", requestId: 0, entries: [] };
      default:
        throw new Error(`unexpected kernel-worker request: ${msg.type}`);
    }
  };
}
