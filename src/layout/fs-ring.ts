export const FS_RING_SUBMIT_HEAD_OFFSET = 0;
export const FS_RING_SUBMIT_TAIL_OFFSET = 4;
export const FS_RING_COMPLETE_HEAD_OFFSET = 8;
export const FS_RING_COMPLETE_TAIL_OFFSET = 12;
export const FS_RING_NOTIFY_OFFSET = 16;
export const FS_RING_HEADER_BYTES = 32;
export const FS_RING_SUBMIT_QUEUE_SIZE = 64;
export const FS_RING_COMPLETE_QUEUE_SIZE = 64;
export const FS_RING_REQUEST_BYTES = 64;
export const FS_RING_RESPONSE_BYTES = 16;
export const FS_RING_REQUEST_OPCODE_OFFSET = 0;
export const FS_RING_REQUEST_FD_OFFSET = 4;
export const FS_RING_REQUEST_OFFSET_OFFSET = 8;
export const FS_RING_REQUEST_LEN_OFFSET = 16;
export const FS_RING_REQUEST_BUF_PTR_OFFSET = 24;
export const FS_RING_REQUEST_PATH_PTR_OFFSET = 32;
export const FS_RING_REQUEST_FLAGS_OFFSET = 40;
export const FS_RING_REQUEST_MODE_OFFSET = 44;
export const FS_RING_REQUEST_VM_PTR_OFFSET = 48;
export const FS_RING_RESPONSE_RESULT_OFFSET = 0;
export const FS_RING_RESPONSE_VM_PTR_OFFSET = 8;
export const FS_RING_COMPLETE_QUEUE_OFFSET =
  FS_RING_HEADER_BYTES + FS_RING_SUBMIT_QUEUE_SIZE * FS_RING_REQUEST_BYTES;
export const FS_RING_TOTAL_BYTES =
  FS_RING_COMPLETE_QUEUE_OFFSET + FS_RING_COMPLETE_QUEUE_SIZE * FS_RING_RESPONSE_BYTES;

export const FS_OP_READ = 1;
export const FS_OP_WRITE = 2;
export const FS_OP_OPENAT = 3;
export const FS_OP_CLOSE = 4;
export const FS_OP_FSTAT = 5;
export const FS_OP_MKDIR = 6;
export const FS_OP_UNLINK = 7;
export const FS_OP_RENAME = 8;
export const FS_OP_GETDENTS = 9;
export const FS_OP_TRUNCATE = 10;
export const FS_OP_LSEEK = 11;
export const FS_REQ_FLAG_PWRITE = 1 << 0;
export const FS_REQ_FLAG_WRITEV = 1 << 1;

export function fsRingRequestEntryOffset(ringBase: number, pos: number): number {
  return ringBase
    + FS_RING_HEADER_BYTES
    + (pos % FS_RING_SUBMIT_QUEUE_SIZE) * FS_RING_REQUEST_BYTES;
}

export function fsRingCompletionEntryOffset(ringBase: number, pos: number): number {
  return ringBase
    + FS_RING_COMPLETE_QUEUE_OFFSET
    + (pos % FS_RING_COMPLETE_QUEUE_SIZE) * FS_RING_RESPONSE_BYTES;
}
