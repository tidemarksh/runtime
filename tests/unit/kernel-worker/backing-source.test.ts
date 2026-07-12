import { expect, test } from "vite-plus/test";
import { ChunkedBackingSource, Uint8ArrayBackingSource } from "../../../src/kernel-worker/backing-source.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

test("Uint8ArrayBackingSource does not expose mutable source storage", () => {
  const input = encoder.encode("abcdef");
  const source = new Uint8ArrayBackingSource(input);

  input[1] = "X".charCodeAt(0);
  const first = source.readRange(1, 3);
  expect(decoder.decode(first ?? new Uint8Array())).toBe("bcd");

  first![0] = "Y".charCodeAt(0);
  expect(decoder.decode(source.readRange(1, 3) ?? new Uint8Array())).toBe("bcd");
});

test("ChunkedBackingSource does not expose mutable chunk storage", () => {
  const chunk = encoder.encode("abcdef");
  const source = new ChunkedBackingSource(chunk.byteLength);

  source.putRange(0, chunk);
  chunk[0] = "X".charCodeAt(0);

  const first = source.readRange(0, 4);
  expect(decoder.decode(first ?? new Uint8Array())).toBe("abcd");

  first![1] = "Y".charCodeAt(0);
  expect(decoder.decode(source.readRange(0, 4) ?? new Uint8Array())).toBe("abcd");
});
