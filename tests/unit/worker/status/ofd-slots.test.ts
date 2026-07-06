import { expect, test } from "vite-plus/test";
import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_REFCOUNT_OFFSET,
  OFD_TYPE_OFFSET,
} from "../../../../src/layout/ofd.js";
import { applyStatusOfdSlots } from "../../../../src/worker/message-handler/status/ofd-slots.js";
import { makeHandle, makeKernel, makeStatusMessage, seedNetworkSocketState } from "./helpers.js";

const SLOT_BYTES = 32;

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
    kernel: makeKernel({
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 3 ? 8 : 0,
    }),
  });
  seedNetworkSocketState(handle, {
    connections: [[5, { close: () => undefined }]],
    connectionOfds: [[5, 3]],
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
