import { expect, test } from "vite-plus/test";
import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_OFFSET_OFFSET,
  OFD_REFCOUNT_OFFSET,
  OFD_TYPE_OFFSET,
} from "../../../../src/layout/ofd.js";
import { applyStatusOfdSlots } from "../../../../src/worker/message-handler/status/ofd-slots.js";
import { makeHandle, makeKernel, makeStatusMessage, seedNetworkSocketState } from "./helpers.js";

const SLOT_BYTES = 32;

function makeOfdSlot(
  type: number,
  hostFd: number,
  ino: number,
  refcount: number,
  offset = 0n,
) {
  const slot = new Uint8Array(SLOT_BYTES);
  const view = new DataView(slot.buffer);
  view.setInt32(OFD_TYPE_OFFSET, type, true);
  view.setInt32(OFD_HOST_FD_OFFSET, hostFd, true);
  view.setInt32(OFD_REFCOUNT_OFFSET, refcount, true);
  view.setUint32(OFD_INO_OFFSET, ino, true);
  view.setBigInt64(OFD_OFFSET_OFFSET, offset, true);
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

test("status ofd slots can trust STATUS_FS regular file offsets", () => {
  let handle: ReturnType<typeof makeHandle> | undefined;
  handle = makeHandle({
    kernel: makeKernel({
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 4 ? 1 : 0,
      kernel_ofd_host_fd: (ofdId: number) => ofdId === 4 ? 22 : -1,
      kernel_ofd_ino: (ofdId: number) => ofdId === 4 ? 33 : 0,
      kernel_ofd_refcount: (ofdId: number) => ofdId === 4 ? 1 : 0,
      kernel_ofd_offset: (ofdId: number) => {
        if (ofdId !== 4) {
          return 0n;
        }
        if (!handle) {
          throw new Error("handle not initialized");
        }
        const view = new DataView(
          (handle.memory as WebAssembly.Memory).buffer,
          (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + 4 * SLOT_BYTES,
          SLOT_BYTES,
        );
        return view.getBigInt64(OFD_OFFSET_OFFSET, true);
      },
    }),
  });
  const current = makeOfdSlot(1, 22, 33, 1, 19n);
  const rewound = makeOfdSlot(1, 22, 33, 1, 0n);
  new Uint8Array((handle.memory as WebAssembly.Memory).buffer).set(
    current,
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + 4 * SLOT_BYTES,
  );
  const readOffset = () => new DataView(
    (handle.memory as WebAssembly.Memory).buffer,
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + 4 * SLOT_BYTES,
    SLOT_BYTES,
  ).getBigInt64(OFD_OFFSET_OFFSET, true);

  applyStatusOfdSlots(handle as never, makeStatusMessage({
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: rewound }],
  }) as never);

  expect(readOffset()).toBe(19n);

  applyStatusOfdSlots(handle as never, makeStatusMessage({
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: rewound }],
  }) as never, { trustRegularFileOfdOffsets: true });

  expect(readOffset()).toBe(0n);
});

test("status ofd slots apply eventfd trace offset without an OFD snapshot", () => {
  const eventfdType = 9;
  const ofdId = 4;
  const handle = makeHandle({
    kernel: makeKernel({
      abi_fd_type_eventfd: () => eventfdType,
    }),
  });
  new Uint8Array((handle.memory as WebAssembly.Memory).buffer).set(
    makeOfdSlot(eventfdType, 0, 0, 1, 0n),
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
  );
  const applied = new DataView(
    (handle.memory as WebAssembly.Memory).buffer,
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
    SLOT_BYTES,
  );

  applyStatusOfdSlots(handle as never, makeStatusMessage({
    fdTraceSyscallNr: 64n,
    fdTraceArg0OfdId: ofdId,
    fdTraceArg0OfdType: eventfdType,
    fdTraceArg0OfdOffset: 1n,
    fdTraceResult: 8n,
  }) as never);

  expect(applied.getBigInt64(OFD_OFFSET_OFFSET, true)).toBe(1n);
});
