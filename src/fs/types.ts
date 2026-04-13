export type FsEntryKind = "file" | "dir" | "symlink";

export interface FsSnapshotEntry {
  kind: FsEntryKind;
  path: string;
  ino?: number;
  data?: Uint8Array;
  target?: Uint8Array;
  size?: number;
  mode?: number;
}

export interface RuntimeStat {
  ino: number;
  mode: number;
  nlink: number;
  uid: number;
  gid: number;
  size: number;
}
