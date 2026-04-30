const PIPE_BUF_SIZE = 65_536;
const PIPE_SLOT_SIZE = 32 + PIPE_BUF_SIZE;

const OFF_HEAD = 0;
const OFF_TAIL = 4;
const OFF_READ_OPEN = 8;
const OFF_WRITE_OPEN = 12;
const OFF_DATA = 32;

function slotBase(pipeBase: number, pipeSlot: number): number {
  return pipeBase + pipeSlot * PIPE_SLOT_SIZE;
}

export interface PipeReadResult {
  data: Uint8Array;
  eof: boolean;
}

export interface PipeWriteResult {
  written: number;
  broken: boolean;
}

export function readPipe(
  memory: WebAssembly.Memory,
  pipeBase: number,
  pipeSlot: number,
  maxLen = PIPE_BUF_SIZE,
): PipeReadResult {
  const base = slotBase(pipeBase, pipeSlot);
  const u32 = new Uint32Array(memory.buffer);
  const i32 = new Int32Array(memory.buffer);
  const u8 = new Uint8Array(memory.buffer);

  const headIdx = (base + OFF_HEAD) >>> 2;
  const tailIdx = (base + OFF_TAIL) >>> 2;
  const writeOpenIdx = (base + OFF_WRITE_OPEN) >>> 2;
  const head = Atomics.load(u32, headIdx);
  const tail = Atomics.load(u32, tailIdx);

  if (head === tail) {
    return {
      data: new Uint8Array(),
      eof: Atomics.load(i32, writeOpenIdx) <= 0,
    };
  }

  const available = tail >= head
    ? tail - head
    : PIPE_BUF_SIZE - head + tail;
  const readLen = Math.min(maxLen, available);
  const out = new Uint8Array(readLen);
  const dataBase = base + OFF_DATA;
  for (let i = 0; i < readLen; i++) {
    out[i] = u8[dataBase + ((head + i) % PIPE_BUF_SIZE)];
  }
  Atomics.store(u32, headIdx, (head + readLen) % PIPE_BUF_SIZE);
  return { data: out, eof: false };
}

export function writePipe(
  memory: WebAssembly.Memory,
  pipeBase: number,
  pipeSlot: number,
  data: Uint8Array,
): PipeWriteResult {
  const base = slotBase(pipeBase, pipeSlot);
  const u32 = new Uint32Array(memory.buffer);
  const i32 = new Int32Array(memory.buffer);
  const u8 = new Uint8Array(memory.buffer);

  const headIdx = (base + OFF_HEAD) >>> 2;
  const tailIdx = (base + OFF_TAIL) >>> 2;
  const readOpenIdx = (base + OFF_READ_OPEN) >>> 2;
  if (Atomics.load(i32, readOpenIdx) <= 0) {
    return { written: 0, broken: true };
  }

  const head = Atomics.load(u32, headIdx);
  const tail = Atomics.load(u32, tailIdx);
  const used = tail >= head
    ? tail - head
    : PIPE_BUF_SIZE - head + tail;
  const free = PIPE_BUF_SIZE - 1 - used;
  if (free <= 0) {
    return { written: 0, broken: false };
  }

  const writeLen = Math.min(free, data.length);
  const dataBase = base + OFF_DATA;
  for (let i = 0; i < writeLen; i++) {
    u8[dataBase + ((tail + i) % PIPE_BUF_SIZE)] = data[i];
  }
  Atomics.store(u32, tailIdx, (tail + writeLen) % PIPE_BUF_SIZE);
  return { written: writeLen, broken: false };
}
