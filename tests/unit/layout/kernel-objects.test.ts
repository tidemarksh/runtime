import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  FD_ENTRY_BYTES,
  FD_ENTRY_OFD_ID_OFFSET,
} from "../../../src/layout/fd.js";
import {
  MEMFS_HEADER_BYTES_FALLBACK,
  MEMFS_HEADER_OFF_DATA_CAP,
  MEMFS_HEADER_OFF_DATA_PTR,
  MEMFS_HEADER_OFF_DATA_USED,
  MEMFS_HEADER_OFF_DIR_ENTRIES_PTR,
  MEMFS_HEADER_OFF_DIR_ENTRY_COUNT,
  MEMFS_HEADER_OFF_FIRST_CHILD_PTR,
  MEMFS_HEADER_OFF_INODES_PTR,
  MEMFS_HEADER_OFF_INODE_COUNT,
  MEMFS_INODE_OFF_DATA_CAP,
  MEMFS_INODE_OFF_DATA_OFFSET,
  MEMFS_INODE_OFF_MODE,
  MEMFS_INODE_OFF_SIZE,
} from "../../../src/layout/memfs.js";
import {
  NET_BUFFER_CAPACITY_OFFSET,
  NET_BUFFER_DATA_OFFSET,
  NET_BUFFER_HEAD_OFFSET,
  NET_BUFFER_HEADER_BYTES,
  NET_BUFFER_TAIL_OFFSET,
} from "../../../src/layout/net-buffer.js";
import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_OFFSET_OFFSET,
  OFD_REFCOUNT_OFFSET,
  OFD_STATUS_FLAGS_OFFSET,
  OFD_TYPE_OFFSET,
} from "../../../src/layout/ofd.js";
import {
  PIPE_BUFFER_BYTES,
  PIPE_DATA_OFFSET,
  PIPE_GENERATION_OFFSET,
  PIPE_HEAD_OFFSET,
  PIPE_HEADER_BYTES,
  PIPE_READ_OPEN_OFFSET,
  PIPE_SLOT_BYTES,
  PIPE_TAIL_OFFSET,
  PIPE_WRITE_OPEN_OFFSET,
} from "../../../src/layout/pipe.js";
import { POLLFD_BYTES } from "../../../src/layout/poll.js";
import {
  SOCKET_SHUT_RD_WR_MASK,
  SOCKET_SLOT_EXTERNAL_PEER_OFFSET,
  SOCKET_SLOT_RECV_BUF_IDX_OFFSET,
  SOCKET_SLOT_SHUTDOWN_MASK_OFFSET,
  SOCKET_SLOT_STATE_OFFSET,
  SOCKET_STATE_CONNECTED,
  SOCKET_STATE_SHUTDOWN,
} from "../../../src/layout/socket.js";

const KERNEL_TYPES_SOURCE = "../kernel/kernel/src/types.rs";
const KERNEL_FD_TABLE_SOURCE = "../kernel/kernel/src/fs/fd_table.rs";
const KERNEL_MEMFS_SOURCE = "../kernel/kernel/src/fs/memfs.rs";
const KERNEL_PIPE_SOURCE = "../kernel/kernel/src/syscall/pipe.rs";
const KERNEL_NET_BUFFER_SOURCE = "../kernel/kernel/src/net/ring_buffer.rs";
const KERNEL_POLL_SOURCE = "../kernel/kernel/src/syscall/poll.rs";
const KERNEL_SOCKET_STATE_SOURCE = "../kernel/kernel/src/syscall/socket/state.rs";

type FieldLayout = { offset: number; size: number };

const TYPE_LAYOUTS = new Map<string, FieldLayout>([
  ["u8", { offset: 1, size: 1 }],
  ["u16", { offset: 2, size: 2 }],
  ["u32", { offset: 4, size: 4 }],
  ["i32", { offset: 4, size: 4 }],
  ["u64", { offset: 8, size: 8 }],
  ["i64", { offset: 8, size: 8 }],
]);

function alignUp(value: number, align: number): number {
  return Math.ceil(value / align) * align;
}

function readKernelConst(
  sourcePath: string,
  name: string,
  replacements: Record<string, number> = {},
): number {
  const source = readFileSync(sourcePath, "utf8");
  const match = new RegExp(`(?:pub\\(super\\)\\s+|pub\\s+)?const ${name}: [^=]+ = ([^;]+);`).exec(source);
  if (!match) {
    throw new Error(`kernel const not found: ${name}`);
  }
  let expression = match[1].replace(/\s+as\s+(?:u32|u64|usize|i32|i64)/g, "");
  for (const [constName, value] of Object.entries(replacements)) {
    expression = expression.replace(new RegExp(`\\b${constName}\\b`, "g"), String(value));
  }
  expression = expression
    .replace(/\s+as\s+(?:u32|u64|usize|i32|i64)/g, "")
    .replace(/_/g, "");
  if (!/^[\d\s+*()|<]+$/.test(expression)) {
    throw new Error(`unsupported kernel const expression: ${name} = ${match[1]}`);
  }
  return Function(`"use strict"; return (${expression});`)() as number;
}

function structBody(sourcePath: string, structName: string): string {
  const source = readFileSync(sourcePath, "utf8");
  const match = new RegExp(`struct ${structName} \\{([\\s\\S]*?)\\n\\}`, "m").exec(source);
  if (!match) {
    throw new Error(`kernel struct not found: ${structName}`);
  }
  return match[1];
}

function fieldTypeLayout(type: string, constants: Record<string, number>): { size: number; align: number } {
  if (type.startsWith("*mut ")) {
    return { size: 4, align: 4 };
  }

  const array = /^\[(.+); ([^\]]+)\]$/.exec(type);
  if (array) {
    const inner = fieldTypeLayout(array[1].trim(), constants);
    const lenExpr = array[2].trim();
    const len = constants[lenExpr] ?? Number(lenExpr);
    if (!Number.isInteger(len)) {
      throw new Error(`unsupported array length: ${type}`);
    }
    return { size: inner.size * len, align: inner.align };
  }

  const primitive = TYPE_LAYOUTS.get(type);
  if (!primitive) {
    throw new Error(`unsupported field type: ${type}`);
  }
  return { size: primitive.size, align: primitive.offset };
}

function readStructLayout(
  sourcePath: string,
  structName: string,
  constants: Record<string, number> = {},
): { fields: Record<string, number>; bytes: number } {
  const fields: Record<string, number> = {};
  let offset = 0;
  let structAlign = 1;
  const lines = structBody(sourcePath, structName).split("\n");
  for (const line of lines) {
    const match = /^\s*(?:pub(?:\([^)]*\))?\s+)?([A-Za-z_][A-Za-z0-9_]*):\s*([^,]+),/.exec(line);
    if (!match) {
      continue;
    }
    const [, name, rawType] = match;
    const { size, align } = fieldTypeLayout(rawType.trim(), constants);
    offset = alignUp(offset, align);
    fields[name] = offset;
    offset += size;
    structAlign = Math.max(structAlign, align);
  }
  return { fields, bytes: alignUp(offset, structAlign) };
}

describe("runtime/kernel object layout", () => {
  it("matches pipe slot constants from kernel source", () => {
    const pipeBufSize = readKernelConst(KERNEL_PIPE_SOURCE, "PIPE_BUF_SIZE");
    expect(PIPE_BUFFER_BYTES).toBe(pipeBufSize);
    expect(PIPE_SLOT_BYTES).toBe(readKernelConst(KERNEL_PIPE_SOURCE, "PIPE_SLOT_SIZE", {
      PIPE_BUF_SIZE: pipeBufSize,
    }));
    expect(PIPE_HEAD_OFFSET).toBe(readKernelConst(KERNEL_PIPE_SOURCE, "OFF_HEAD"));
    expect(PIPE_TAIL_OFFSET).toBe(readKernelConst(KERNEL_PIPE_SOURCE, "OFF_TAIL"));
    expect(PIPE_READ_OPEN_OFFSET).toBe(readKernelConst(KERNEL_PIPE_SOURCE, "OFF_READ_OPEN"));
    expect(PIPE_WRITE_OPEN_OFFSET).toBe(readKernelConst(KERNEL_PIPE_SOURCE, "OFF_WRITE_OPEN"));
    expect(PIPE_GENERATION_OFFSET).toBe(readKernelConst(KERNEL_PIPE_SOURCE, "OFF_GENERATION"));
    expect(PIPE_DATA_OFFSET).toBe(readKernelConst(KERNEL_PIPE_SOURCE, "OFF_DATA"));
    expect(PIPE_HEADER_BYTES).toBe(PIPE_DATA_OFFSET);
  });

  it("matches net-buffer and poll constants from kernel source", () => {
    expect(NET_BUFFER_HEADER_BYTES).toBe(readKernelConst(KERNEL_NET_BUFFER_SOURCE, "OFF_DATA"));
    expect(NET_BUFFER_HEAD_OFFSET).toBe(readKernelConst(KERNEL_NET_BUFFER_SOURCE, "OFF_HEAD"));
    expect(NET_BUFFER_TAIL_OFFSET).toBe(readKernelConst(KERNEL_NET_BUFFER_SOURCE, "OFF_TAIL"));
    expect(NET_BUFFER_CAPACITY_OFFSET).toBe(readKernelConst(KERNEL_NET_BUFFER_SOURCE, "OFF_CAP"));
    expect(NET_BUFFER_DATA_OFFSET).toBe(readKernelConst(KERNEL_NET_BUFFER_SOURCE, "OFF_DATA"));
    expect(POLLFD_BYTES).toBe(readKernelConst(KERNEL_POLL_SOURCE, "POLLFD_SIZE"));
  });

  it("matches fd and ofd repr(C) layout", () => {
    const fdEntry = readStructLayout(KERNEL_TYPES_SOURCE, "FdEntry");
    expect(FD_ENTRY_BYTES).toBe(fdEntry.bytes);
    expect(FD_ENTRY_OFD_ID_OFFSET).toBe(fdEntry.fields.ofd_id);

    const ofd = readStructLayout(KERNEL_TYPES_SOURCE, "OpenFileDesc");
    expect(OFD_TYPE_OFFSET).toBe(ofd.fields.fd_type);
    expect(OFD_HOST_FD_OFFSET).toBe(ofd.fields.host_fd);
    expect(OFD_STATUS_FLAGS_OFFSET).toBe(ofd.fields.status_flags);
    expect(OFD_REFCOUNT_OFFSET).toBe(ofd.fields.refcount);
    expect(OFD_OFFSET_OFFSET).toBe(ofd.fields.offset);
    expect(OFD_INO_OFFSET).toBe(ofd.fields.ino);

    const fdTableDoc = readFileSync(KERNEL_FD_TABLE_SOURCE, "utf8");
    expect(fdTableDoc).toContain("struct OpenFileDesc {");
  });

  it("matches memfs repr(C) header and inode layout", () => {
    const header = readStructLayout(KERNEL_MEMFS_SOURCE, "MemFsHeader");
    expect(MEMFS_HEADER_BYTES_FALLBACK).toBe(header.bytes);
    expect(MEMFS_HEADER_OFF_INODE_COUNT).toBe(header.fields.inode_count);
    expect(MEMFS_HEADER_OFF_DIR_ENTRY_COUNT).toBe(header.fields.dir_entry_count);
    expect(MEMFS_HEADER_OFF_DATA_USED).toBe(header.fields.data_used);
    expect(MEMFS_HEADER_OFF_DATA_CAP).toBe(header.fields.data_cap);
    expect(MEMFS_HEADER_OFF_INODES_PTR).toBe(header.fields.inodes);
    expect(MEMFS_HEADER_OFF_FIRST_CHILD_PTR).toBe(header.fields.first_child);
    expect(MEMFS_HEADER_OFF_DIR_ENTRIES_PTR).toBe(header.fields.dir_entries);
    expect(MEMFS_HEADER_OFF_DATA_PTR).toBe(header.fields.data);

    const inode = readStructLayout(KERNEL_MEMFS_SOURCE, "Inode");
    expect(MEMFS_INODE_OFF_MODE).toBe(inode.fields.mode);
    expect(MEMFS_INODE_OFF_SIZE).toBe(inode.fields.size);
    expect(MEMFS_INODE_OFF_DATA_OFFSET).toBe(inode.fields.data_offset);
    expect(MEMFS_INODE_OFF_DATA_CAP).toBe(inode.fields.data_cap);
  });

  it("matches socket state constants and selected SocketSlot offsets", () => {
    expect(SOCKET_STATE_CONNECTED).toBe(readKernelConst(KERNEL_SOCKET_STATE_SOURCE, "SOCK_STATE_CONNECTED"));
    expect(SOCKET_STATE_SHUTDOWN).toBe(readKernelConst(KERNEL_SOCKET_STATE_SOURCE, "SOCK_STATE_SHUTDOWN"));
    const shutRd = readKernelConst(KERNEL_SOCKET_STATE_SOURCE, "SHUT_RD_MASK");
    const shutWr = readKernelConst(KERNEL_SOCKET_STATE_SOURCE, "SHUT_WR_MASK");
    expect(SOCKET_SHUT_RD_WR_MASK).toBe(shutRd | shutWr);

    const sockaddrStorageLen = readKernelConst(KERNEL_SOCKET_STATE_SOURCE, "SOCKADDR_STORAGE_LEN");
    const acceptQueueSize = readKernelConst(KERNEL_SOCKET_STATE_SOURCE, "ACCEPT_QUEUE_SIZE");
    const slot = readStructLayout(KERNEL_SOCKET_STATE_SOURCE, "SocketSlot", {
      ACCEPT_QUEUE_SIZE: acceptQueueSize,
      SOCKADDR_STORAGE_LEN: sockaddrStorageLen,
    });
    expect(SOCKET_SLOT_STATE_OFFSET).toBe(slot.fields.state);
    expect(SOCKET_SLOT_EXTERNAL_PEER_OFFSET).toBe(slot.fields.external_peer);
    expect(SOCKET_SLOT_RECV_BUF_IDX_OFFSET).toBe(slot.fields.recv_buf_idx);
    expect(SOCKET_SLOT_SHUTDOWN_MASK_OFFSET).toBe(slot.fields.shutdown_mask);
  });
});
