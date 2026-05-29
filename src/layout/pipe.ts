export const PIPE_HEADER_BYTES = 32;
export const PIPE_HEAD_OFFSET = 0;
export const PIPE_TAIL_OFFSET = 4;
export const PIPE_READ_OPEN_OFFSET = 8;
export const PIPE_WRITE_OPEN_OFFSET = 12;
export const PIPE_GENERATION_OFFSET = 20;
export const PIPE_DATA_OFFSET = PIPE_HEADER_BYTES;
export const PIPE_BUFFER_BYTES = 65_536;
export const PIPE_SLOT_BYTES = PIPE_HEADER_BYTES + PIPE_BUFFER_BYTES;

export const PIPE_SLOT_MODE_READ = 1 << 0;
export const PIPE_SLOT_MODE_WRITE = 1 << 1;
export const PIPE_SLOT_MODE_CONTROL = 1 << 2;

