import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  FS_EFFECT_MEMFS_FILE_DATA,
  FS_EFFECT_MEMFS_MUTATION,
  FS_EFFECT_SHARED_OFD_STATE,
} from "../../../src/layout/fs-effects.js";
import {
  FS_OP_READ,
  FS_OP_WRITE,
  FS_OP_CLOSE,
  FS_OP_EXECVE,
  FS_OP_FSTAT,
  FS_OP_GETDENTS,
  FS_OP_LSEEK,
  FS_OP_MKDIR,
  FS_OP_MMAP,
  FS_OP_OPENAT,
  FS_OP_RENAME,
  FS_OP_TRUNCATE,
  FS_OP_UNLINK,
  FS_REQ_FLAG_PWRITE,
  FS_REQ_FLAG_WRITEV,
  FS_RING_COMPLETE_HEAD_OFFSET,
  FS_RING_COMPLETE_QUEUE_OFFSET,
  FS_RING_COMPLETE_QUEUE_SIZE,
  FS_RING_COMPLETE_TAIL_OFFSET,
  FS_RING_HEADER_BYTES,
  FS_RING_NOTIFY_OFFSET,
  FS_RING_REQUEST_BUF_PTR_OFFSET,
  FS_RING_REQUEST_BYTES,
  FS_RING_REQUEST_FD_OFFSET,
  FS_RING_REQUEST_FLAGS_OFFSET,
  FS_RING_REQUEST_LEN_OFFSET,
  FS_RING_REQUEST_MODE_OFFSET,
  FS_RING_REQUEST_OFFSET_OFFSET,
  FS_RING_REQUEST_OPCODE_OFFSET,
  FS_RING_REQUEST_PATH_PTR_OFFSET,
  FS_RING_REQUEST_VM_PTR_OFFSET,
  FS_RING_RESPONSE_BYTES,
  FS_RING_RESPONSE_RESULT_OFFSET,
  FS_RING_RESPONSE_VM_PTR_OFFSET,
  FS_RING_SUBMIT_HEAD_OFFSET,
  FS_RING_SUBMIT_QUEUE_SIZE,
  FS_RING_SUBMIT_TAIL_OFFSET,
  FS_RING_TOTAL_BYTES,
  fsRingCompletionEntryOffset,
  fsRingRequestEntryOffset,
} from "../../../src/layout/fs-ring.js";

const KERNEL_FS_RING_SOURCE = "../kernel/kernel/src/fs/ring_buffer.rs";
const KERNEL_FS_PENDING_SOURCE = "../kernel/kernel/src/syscall/fs/pending.rs";

const KERNEL_FS_RING_SOURCE_TEXT = readFileSync(KERNEL_FS_RING_SOURCE, "utf8");
const KERNEL_FS_PENDING_SOURCE_TEXT = readFileSync(KERNEL_FS_PENDING_SOURCE, "utf8");

function readKernelConstFrom(sourceText: string, name: string): number {
  const match = new RegExp(`pub const ${name}: (?:usize|u32) = ([^;]+);`).exec(sourceText);
  if (!match) {
    throw new Error(`kernel FS const not found: ${name}`);
  }
  const expression = match[1]
    .replace(/\bHEADER_SIZE\b/g, String(FS_RING_HEADER_BYTES))
    .replace(/\bSQ_SIZE\b/g, String(FS_RING_SUBMIT_QUEUE_SIZE))
    .replace(/\bCQ_SIZE\b/g, String(FS_RING_COMPLETE_QUEUE_SIZE))
    .replace(/\bREQ_SIZE\b/g, String(FS_RING_REQUEST_BYTES))
    .replace(/\bRESP_SIZE\b/g, String(FS_RING_RESPONSE_BYTES))
    .replace(/\bas u32\b/g, "");
  if (!/^[\d\s+*()|<]+$/.test(expression)) {
    throw new Error(`unsupported kernel FS ring const expression: ${name} = ${match[1]}`);
  }
  return Function(`"use strict"; return (${expression});`)() as number;
}

function readKernelConst(name: string): number {
  return readKernelConstFrom(KERNEL_FS_RING_SOURCE_TEXT, name);
}

function readKernelRequestFieldOffset(field: string): number {
  const constructor = /Some\(FsRequestInfo \{(?<body>[\s\S]*?)\n\s*\}\)/.exec(KERNEL_FS_RING_SOURCE_TEXT)
    ?.groups?.body;
  if (!constructor) {
    throw new Error("kernel FS request constructor not found");
  }
  const line = new RegExp(`${field}: \\*\\((?<expr>[^\\n]+)\\),`).exec(constructor)
    ?.groups?.expr;
  if (!line) {
    throw new Error(`kernel FS request field read not found: ${field}`);
  }
  if (line.startsWith("p as ")) {
    return 0;
  }
  const offset = /p\.add\((\d+)\)/.exec(line)?.[1];
  if (offset === undefined) {
    throw new Error(`kernel FS request field offset not found: ${field}`);
  }
  return Number.parseInt(offset, 10);
}

function readKernelResponseFieldOffset(field: "result" | "vm_ptr"): number {
  const pattern = field === "result"
    ? /\*\(\(cq_off as \*mut u8\) as \*mut i64\) = result;/
    : /\*\(\(cq_off as \*mut u8\)\.add\((\d+)\) as \*mut u32\) = target_vm_ptr;/;
  const match = pattern.exec(KERNEL_FS_RING_SOURCE_TEXT);
  if (!match) {
    throw new Error(`kernel FS response field write not found: ${field}`);
  }
  return field === "result" ? 0 : Number.parseInt(match[1], 10);
}

describe("FS ring layout", () => {
  it("matches the kernel FS ring constants", () => {
    expect(FS_RING_SUBMIT_QUEUE_SIZE).toBe(readKernelConst("SQ_SIZE"));
    expect(FS_RING_COMPLETE_QUEUE_SIZE).toBe(readKernelConst("CQ_SIZE"));
    expect(FS_RING_REQUEST_BYTES).toBe(readKernelConst("REQ_SIZE"));
    expect(FS_RING_RESPONSE_BYTES).toBe(readKernelConst("RESP_SIZE"));
    expect(FS_RING_HEADER_BYTES).toBe(readKernelConst("HEADER_SIZE"));
    expect(FS_RING_TOTAL_BYTES).toBe(readKernelConst("RING_BUFFER_SIZE"));
    expect(FS_OP_READ).toBe(readKernelConst("FS_OP_READ"));
    expect(FS_OP_WRITE).toBe(readKernelConst("FS_OP_WRITE"));
    expect(FS_OP_OPENAT).toBe(readKernelConst("FS_OP_OPENAT"));
    expect(FS_OP_CLOSE).toBe(readKernelConst("FS_OP_CLOSE"));
    expect(FS_OP_FSTAT).toBe(readKernelConst("FS_OP_FSTAT"));
    expect(FS_OP_MKDIR).toBe(readKernelConst("FS_OP_MKDIR"));
    expect(FS_OP_UNLINK).toBe(readKernelConst("FS_OP_UNLINK"));
    expect(FS_OP_RENAME).toBe(readKernelConst("FS_OP_RENAME"));
    expect(FS_OP_GETDENTS).toBe(readKernelConst("FS_OP_GETDENTS"));
    expect(FS_OP_TRUNCATE).toBe(readKernelConst("FS_OP_TRUNCATE"));
    expect(FS_OP_LSEEK).toBe(readKernelConst("FS_OP_LSEEK"));
    expect(FS_OP_MMAP).toBe(readKernelConst("FS_OP_MMAP"));
    expect(FS_OP_EXECVE).toBe(readKernelConst("FS_OP_EXECVE"));
    expect(FS_REQ_FLAG_PWRITE).toBe(readKernelConst("FS_REQ_FLAG_PWRITE"));
    expect(FS_REQ_FLAG_WRITEV).toBe(readKernelConst("FS_REQ_FLAG_WRITEV"));
    expect(FS_RING_SUBMIT_HEAD_OFFSET).toBe(readKernelConst("OFF_SUBMIT_HEAD"));
    expect(FS_RING_SUBMIT_TAIL_OFFSET).toBe(readKernelConst("OFF_SUBMIT_TAIL"));
    expect(FS_RING_COMPLETE_HEAD_OFFSET).toBe(readKernelConst("OFF_COMPLETE_HEAD"));
    expect(FS_RING_COMPLETE_TAIL_OFFSET).toBe(readKernelConst("OFF_COMPLETE_TAIL"));
    expect(FS_RING_NOTIFY_OFFSET).toBe(readKernelConst("OFF_NOTIFY"));
    expect(FS_RING_HEADER_BYTES).toBe(readKernelConst("OFF_SQ_BASE"));
    expect(FS_RING_COMPLETE_QUEUE_OFFSET).toBe(readKernelConst("OFF_CQ_BASE"));
  });

  it("matches kernel FS pending effect constants", () => {
    expect(FS_EFFECT_MEMFS_MUTATION).toBe(readKernelConstFrom(KERNEL_FS_PENDING_SOURCE_TEXT, "FS_EFFECT_MEMFS_MUTATION"));
    expect(FS_EFFECT_SHARED_OFD_STATE).toBe(readKernelConstFrom(KERNEL_FS_PENDING_SOURCE_TEXT, "FS_EFFECT_SHARED_OFD_STATE"));
    expect(FS_EFFECT_MEMFS_FILE_DATA).toBe(readKernelConstFrom(KERNEL_FS_PENDING_SOURCE_TEXT, "FS_EFFECT_MEMFS_FILE_DATA"));
  });

  it("matches kernel request and response field offsets", () => {
    expect(FS_RING_REQUEST_OPCODE_OFFSET).toBe(readKernelRequestFieldOffset("opcode"));
    expect(FS_RING_REQUEST_FD_OFFSET).toBe(readKernelRequestFieldOffset("fd"));
    expect(FS_RING_REQUEST_OFFSET_OFFSET).toBe(readKernelRequestFieldOffset("offset"));
    expect(FS_RING_REQUEST_LEN_OFFSET).toBe(readKernelRequestFieldOffset("len"));
    expect(FS_RING_REQUEST_BUF_PTR_OFFSET).toBe(readKernelRequestFieldOffset("buf_ptr"));
    expect(FS_RING_REQUEST_PATH_PTR_OFFSET).toBe(readKernelRequestFieldOffset("path_ptr"));
    expect(FS_RING_REQUEST_FLAGS_OFFSET).toBe(readKernelRequestFieldOffset("flags"));
    expect(FS_RING_REQUEST_MODE_OFFSET).toBe(readKernelRequestFieldOffset("mode"));
    expect(FS_RING_REQUEST_VM_PTR_OFFSET).toBe(readKernelRequestFieldOffset("vm_ptr"));
    expect(FS_RING_RESPONSE_RESULT_OFFSET).toBe(readKernelResponseFieldOffset("result"));
    expect(FS_RING_RESPONSE_VM_PTR_OFFSET).toBe(readKernelResponseFieldOffset("vm_ptr"));
  });

  it("computes entry offsets from the shared queue layout", () => {
    const ringBase = 0x2000;
    expect(fsRingRequestEntryOffset(ringBase, 0)).toBe(ringBase + FS_RING_HEADER_BYTES);
    expect(fsRingRequestEntryOffset(ringBase, 65)).toBe(
      ringBase + FS_RING_HEADER_BYTES + FS_RING_REQUEST_BYTES,
    );
    expect(fsRingCompletionEntryOffset(ringBase, 0)).toBe(
      ringBase + FS_RING_COMPLETE_QUEUE_OFFSET,
    );
    expect(fsRingCompletionEntryOffset(ringBase, 66)).toBe(
      ringBase + FS_RING_COMPLETE_QUEUE_OFFSET + 2 * FS_RING_RESPONSE_BYTES,
    );
  });
});
