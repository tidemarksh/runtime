import { expect, test } from "vite-plus/test";
import { readPipe, writePipe } from "../../../src/bridge/pipe.js";

const PIPE_SLOT_BYTES = 32 + 4096;
const PIPE_BASE = 0x1000;

function slotBase(pipeSlot: number): number {
  return PIPE_BASE + pipeSlot * PIPE_SLOT_BYTES;
}

function setPipeEnds(
  memory: WebAssembly.Memory,
  pipeSlot: number,
  readOpen: number,
  writeOpen: number,
): void {
  const view = new DataView(memory.buffer);
  const base = slotBase(pipeSlot);
  view.setInt32(base + 8, readOpen, true);
  view.setInt32(base + 12, writeOpen, true);
}

test("readPipe reports EOF only after buffered bytes have been drained", () => {
  const memory = new WebAssembly.Memory({ initial: 2, maximum: 2, shared: true });
  setPipeEnds(memory, 1, 1, 1);

  const firstWrite = writePipe(memory, PIPE_BASE, 1, new TextEncoder().encode("hello"));
  expect(firstWrite).toEqual({ written: 5, broken: false });

  setPipeEnds(memory, 1, 1, 0);

  const firstRead = readPipe(memory, PIPE_BASE, 1);
  expect(new TextDecoder().decode(firstRead.data)).toBe("hello");
  expect(firstRead.eof).toBe(false);

  const secondRead = readPipe(memory, PIPE_BASE, 1);
  expect(secondRead.data).toEqual(new Uint8Array());
  expect(secondRead.eof).toBe(true);
});

test("writePipe returns broken when the read end is already closed", () => {
  const memory = new WebAssembly.Memory({ initial: 2, maximum: 2, shared: true });
  setPipeEnds(memory, 1, 0, 1);

  const write = writePipe(memory, PIPE_BASE, 1, new TextEncoder().encode("hello"));

  expect(write).toEqual({ written: 0, broken: true });
});

test("writePipe returns backpressure without broken when the pipe buffer is full", () => {
  const memory = new WebAssembly.Memory({ initial: 2, maximum: 2, shared: true });
  const base = slotBase(1);
  const view = new DataView(memory.buffer);
  setPipeEnds(memory, 1, 1, 1);
  view.setUint32(base, 0, true);
  view.setUint32(base + 4, 4095, true);

  const write = writePipe(memory, PIPE_BASE, 1, new TextEncoder().encode("x"));

  expect(write).toEqual({ written: 0, broken: false });
});
