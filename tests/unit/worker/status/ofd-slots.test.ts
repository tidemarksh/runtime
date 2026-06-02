import { expect, test } from "vite-plus/test";
import {
  FD_ENTRY_BYTES,
  FD_ENTRY_OFD_ID_OFFSET,
} from "../../../../src/layout/fd.js";
import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_REFCOUNT_OFFSET,
  OFD_TYPE_OFFSET,
} from "../../../../src/layout/ofd.js";
import { applyStatusOfdSlots } from "../../../../src/worker/message-handler/status/ofd-slots.js";
import { maskRegularFileFdEntryRollbackGuestWrite } from "../../../../src/worker/ownership/ofd-slot.js";
import { makeHandle, makeKernel, makeStatusMessage } from "./helpers.js";

const SLOT_BYTES = 32;
const FD_ENTRY_FD_FLAGS_OFFSET = 4;

function makeOfdSlot(type: number, hostFd: number, ino: number, refcount: number) {
  const slot = new Uint8Array(SLOT_BYTES);
  const view = new DataView(slot.buffer);
  view.setInt32(OFD_TYPE_OFFSET, type, true);
  view.setInt32(OFD_HOST_FD_OFFSET, hostFd, true);
  view.setInt32(OFD_REFCOUNT_OFFSET, refcount, true);
  view.setUint32(OFD_INO_OFFSET, ino, true);
  return slot;
}

test("status ofd slots apply compatible snapshots into the runtime OFD table", () => {
  const handle = makeHandle();
  const slot = makeOfdSlot(1, 22, 33, 1);

  applyStatusOfdSlots(handle as never, makeStatusMessage({
    ofdSlots: [{ ofdId: 2, slot }],
  }) as never);

  const applied = new DataView(
    (handle.memory as WebAssembly.Memory).buffer,
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + 2 * SLOT_BYTES,
    SLOT_BYTES,
  );
  expect(applied.getInt32(OFD_TYPE_OFFSET, true)).toBe(1);
  expect(applied.getInt32(OFD_HOST_FD_OFFSET, true)).toBe(22);
  expect(applied.getUint32(OFD_INO_OFFSET, true)).toBe(33);
});

test("status ofd slots do not clobber protected runtime network OFDs", () => {
  const existing = makeOfdSlot(8, 44, 55, 1);
  const incoming = makeOfdSlot(1, 66, 77, 1);
  const handle = makeHandle({
    networkConnections: new Map([[5, { close: () => undefined }]]),
    networkConnectionOfds: new Map([[5, 3]]),
    kernel: makeKernel({
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 3 ? 8 : 0,
    }),
  });
  new Uint8Array((handle.memory as WebAssembly.Memory).buffer).set(
    existing,
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + 3 * SLOT_BYTES,
  );

  applyStatusOfdSlots(handle as never, makeStatusMessage({
    ofdSlots: [{ ofdId: 3, slot: incoming }],
  }) as never);

  const applied = new DataView(
    (handle.memory as WebAssembly.Memory).buffer,
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + 3 * SLOT_BYTES,
    SLOT_BYTES,
  );
  expect(applied.getInt32(OFD_TYPE_OFFSET, true)).toBe(8);
  expect(applied.getInt32(OFD_HOST_FD_OFFSET, true)).toBe(44);
});

test("status guest memory writes do not clear a live descriptor from a stale non-closing snapshot", () => {
  const processPtr = 0x500;
  const fd = 5;
  const fdEntryPtr = processPtr + fd * FD_ENTRY_BYTES;
  const handle = makeHandle({
    kernel: makeKernel({
      abi_sys_getdents64: () => 61n,
      thread_process_ptr: () => processPtr,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 7 ? 3 : 0,
    }),
  });
  const memory = new Uint8Array((handle.memory as WebAssembly.Memory).buffer);
  const view = new DataView(memory.buffer);
  view.setUint32(fdEntryPtr + FD_ENTRY_OFD_ID_OFFSET, 7, true);
  view.setUint32(fdEntryPtr + FD_ENTRY_FD_FLAGS_OFFSET, 1, true);

  const staleClear = new Uint8Array(FD_ENTRY_BYTES);
  const masked = maskRegularFileFdEntryRollbackGuestWrite(
    handle as never,
    makeStatusMessage({ syscallNr: 61n }) as never,
    fdEntryPtr,
    staleClear,
  );
  const maskedView = new DataView(masked.buffer, masked.byteOffset, masked.byteLength);

  expect(maskedView.getUint32(FD_ENTRY_OFD_ID_OFFSET, true)).toBe(7);
  expect(maskedView.getUint32(FD_ENTRY_FD_FLAGS_OFFSET, true)).toBe(1);
});

test("status guest memory writes do not replace a live descriptor with an invalid OFD sentinel", () => {
  const processPtr = 0x500;
  const fd = 2;
  const fdEntryPtr = processPtr + fd * FD_ENTRY_BYTES;
  const handle = makeHandle({
    kernel: makeKernel({
      thread_process_ptr: () => processPtr,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 3 ? 3 : 0,
    }),
  });
  const memory = new Uint8Array((handle.memory as WebAssembly.Memory).buffer);
  const view = new DataView(memory.buffer);
  view.setUint32(fdEntryPtr + FD_ENTRY_OFD_ID_OFFSET, 3, true);
  view.setUint32(fdEntryPtr + FD_ENTRY_FD_FLAGS_OFFSET, 1, true);

  const staleInvalid = new Uint8Array(FD_ENTRY_BYTES);
  new DataView(staleInvalid.buffer).setUint32(FD_ENTRY_OFD_ID_OFFSET, 0xffffffff, true);
  const masked = maskRegularFileFdEntryRollbackGuestWrite(
    handle as never,
    makeStatusMessage({
      syscallNr: 73n,
      fdEntries: [{ fd, ofdId: -1, fdFlags: 0 }],
    }) as never,
    fdEntryPtr,
    staleInvalid,
  );
  const maskedView = new DataView(masked.buffer, masked.byteOffset, masked.byteLength);

  expect(maskedView.getUint32(FD_ENTRY_OFD_ID_OFFSET, true)).toBe(3);
  expect(maskedView.getUint32(FD_ENTRY_FD_FLAGS_OFFSET, true)).toBe(1);
});

test("status guest memory writes do not let close clear a different live descriptor", () => {
  const processPtr = 0x500;
  const fd = 5;
  const fdEntryPtr = processPtr + fd * FD_ENTRY_BYTES;
  const handle = makeHandle({
    kernel: makeKernel({
      abi_sys_close: () => 57n,
      thread_process_ptr: () => processPtr,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 7 ? 3 : 0,
    }),
  });
  const memory = new Uint8Array((handle.memory as WebAssembly.Memory).buffer);
  const view = new DataView(memory.buffer);
  view.setUint32(fdEntryPtr + FD_ENTRY_OFD_ID_OFFSET, 7, true);
  view.setUint32(fdEntryPtr + FD_ENTRY_FD_FLAGS_OFFSET, 1, true);

  const staleClear = new Uint8Array(FD_ENTRY_BYTES);
  const masked = maskRegularFileFdEntryRollbackGuestWrite(
    handle as never,
    makeStatusMessage({
      syscallNr: 57n,
      fdTraceArg0: 6n,
      fdTraceResult: 0n,
    }) as never,
    fdEntryPtr,
    staleClear,
  );
  const maskedView = new DataView(masked.buffer, masked.byteOffset, masked.byteLength);

  expect(maskedView.getUint32(FD_ENTRY_OFD_ID_OFFSET, true)).toBe(7);
  expect(maskedView.getUint32(FD_ENTRY_FD_FLAGS_OFFSET, true)).toBe(1);
});
