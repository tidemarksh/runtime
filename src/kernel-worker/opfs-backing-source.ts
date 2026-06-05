export interface OpfsBackingFileHandle {
  getFile(): Promise<Blob>;
}

export interface OpfsBackingSourceRange {
  byteLength: number;
  offset: number;
  data: Uint8Array;
}

function validateOpfsRange(byteLength: number, offset: number, length: number): void {
  if (!Number.isInteger(byteLength) || byteLength < 0) {
    throw new RangeError(`invalid OPFS backing source byte length: ${byteLength}`);
  }
  if (!Number.isInteger(offset) || offset < 0) {
    throw new RangeError(`invalid OPFS backing source offset: ${offset}`);
  }
  if (!Number.isInteger(length) || length < 0) {
    throw new RangeError(`invalid OPFS backing source range length: ${length}`);
  }
  if (offset + length > byteLength) {
    throw new RangeError(`OPFS backing source range exceeds source length: offset=${offset} length=${length}`);
  }
}

export async function readOpfsBackingSourceRange(
  handle: OpfsBackingFileHandle,
  byteLength: number,
  offset: number,
  length: number,
): Promise<OpfsBackingSourceRange> {
  validateOpfsRange(byteLength, offset, length);
  if (length === 0) {
    return { byteLength, offset, data: new Uint8Array() };
  }

  const file = await handle.getFile();
  if (file.size !== byteLength) {
    throw new Error(`OPFS backing source size changed: expected=${byteLength} actual=${file.size}`);
  }
  const slice = file.slice(offset, offset + length);
  const data = new Uint8Array(await slice.arrayBuffer());
  if (data.byteLength !== length) {
    throw new Error(`OPFS backing source range length mismatch: expected=${length} actual=${data.byteLength}`);
  }
  return { byteLength, offset, data };
}
