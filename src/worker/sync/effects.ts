export type SyncCause =
  | { kind: "syscall"; nr: bigint; name?: string }
  | { kind: "fork-child-ready"; childPid: number }
  | { kind: "child-exit"; childPid: number; status: number }
  | { kind: "blocked-rpc"; status: number };

export function describeSyncCause(cause: SyncCause): string {
  switch (cause.kind) {
    case "syscall":
      return cause.name === undefined
        ? `syscall:${cause.nr.toString()}`
        : `syscall:${cause.nr.toString()}:${cause.name}`;
    case "fork-child-ready":
      return `fork-child-ready:${cause.childPid}`;
    case "child-exit":
      return `child-exit:${cause.childPid}:${cause.status}`;
    case "blocked-rpc":
      return `blocked-rpc:${cause.status}`;
  }
}
