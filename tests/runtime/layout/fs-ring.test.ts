import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  FS_OP_READ,
  FS_OP_WRITE,
  FS_RING_COMPLETE_HEAD_OFFSET,
  FS_RING_COMPLETE_QUEUE_OFFSET,
  FS_RING_COMPLETE_QUEUE_SIZE,
  FS_RING_COMPLETE_TAIL_OFFSET,
  FS_RING_HEADER_BYTES,
  FS_RING_REQUEST_BUF_PTR_OFFSET,
  FS_RING_REQUEST_BYTES,
  FS_RING_REQUEST_LEN_OFFSET,
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

function readKernelConst(name: string): number {
  const source = readFileSync(KERNEL_FS_RING_SOURCE, "utf8");
  const match = new RegExp(`pub const ${name}: (?:usize|u32) = ([^;]+);`).exec(source);
  if (!match) {
    throw new Error(`kernel FS ring const not found: ${name}`);
  }
  const expression = match[1]
    .replace(/\bHEADER_SIZE\b/g, String(FS_RING_HEADER_BYTES))
    .replace(/\bSQ_SIZE\b/g, String(FS_RING_SUBMIT_QUEUE_SIZE))
    .replace(/\bCQ_SIZE\b/g, String(FS_RING_COMPLETE_QUEUE_SIZE))
    .replace(/\bREQ_SIZE\b/g, String(FS_RING_REQUEST_BYTES))
    .replace(/\bRESP_SIZE\b/g, String(FS_RING_RESPONSE_BYTES))
    .replace(/\bas u32\b/g, "");
  if (!/^[\d\s+*()]+$/.test(expression)) {
    throw new Error(`unsupported kernel FS ring const expression: ${name} = ${match[1]}`);
  }
  return Function(`"use strict"; return (${expression});`)() as number;
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
    expect(FS_RING_SUBMIT_HEAD_OFFSET).toBe(readKernelConst("OFF_SUBMIT_HEAD"));
    expect(FS_RING_SUBMIT_TAIL_OFFSET).toBe(readKernelConst("OFF_SUBMIT_TAIL"));
    expect(FS_RING_COMPLETE_HEAD_OFFSET).toBe(readKernelConst("OFF_COMPLETE_HEAD"));
    expect(FS_RING_COMPLETE_TAIL_OFFSET).toBe(readKernelConst("OFF_COMPLETE_TAIL"));
    expect(FS_RING_HEADER_BYTES).toBe(readKernelConst("OFF_SQ_BASE"));
    expect(FS_RING_COMPLETE_QUEUE_OFFSET).toBe(readKernelConst("OFF_CQ_BASE"));
  });

  it("keeps request and response field offsets explicit", () => {
    expect(FS_RING_REQUEST_OPCODE_OFFSET).toBe(0);
    expect(FS_RING_REQUEST_LEN_OFFSET).toBe(16);
    expect(FS_RING_REQUEST_BUF_PTR_OFFSET).toBe(24);
    expect(FS_RING_REQUEST_PATH_PTR_OFFSET).toBe(32);
    expect(FS_RING_REQUEST_VM_PTR_OFFSET).toBe(48);
    expect(FS_RING_RESPONSE_RESULT_OFFSET).toBe(0);
    expect(FS_RING_RESPONSE_VM_PTR_OFFSET).toBe(8);
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
