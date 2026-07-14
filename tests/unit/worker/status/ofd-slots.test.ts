import { expect, test } from "vite-plus/test";
import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_OFFSET_OFFSET,
  OFD_REFCOUNT_OFFSET,
  OFD_STATUS_FLAGS_OFFSET,
  OFD_TYPE_OFFSET,
} from "../../../../src/layout/ofd.js";
import { applyStatusOfdSlots } from "../../../../src/worker/message-handler/status/ofd-slots.js";
import {
  createRuntimeStateBridge,
  publishRuntimeFdAuthorityEpoch,
} from "../../../../src/bridge/runtime-state.js";
import { makeHandle, makeKernel, makeStatusMessage, seedNetworkSocketState } from "./helpers.js";

const SLOT_BYTES = 32;
const O_WRONLY = 0x1;
const O_RDWR = 0x2;
const O_NONBLOCK = 0x800;

function makeOfdSlot(
  type: number,
  hostFd: number,
  ino: number,
  refcount: number,
  offset = 0n,
  statusFlags = 0,
) {
  const slot = new Uint8Array(SLOT_BYTES);
  const view = new DataView(slot.buffer);
  view.setInt32(OFD_TYPE_OFFSET, type, true);
  view.setInt32(OFD_HOST_FD_OFFSET, hostFd, true);
  view.setInt32(OFD_REFCOUNT_OFFSET, refcount, true);
  view.setInt32(OFD_STATUS_FLAGS_OFFSET, statusFlags, true);
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
  }) as never, {
    trustRegularFileOfdOffsets: true,
    authoritativeOfdIds: new Set(),
  });

  expect(readOffset()).toBe(19n);

  applyStatusOfdSlots(handle as never, makeStatusMessage({
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: rewound }],
  }) as never, {
    trustRegularFileOfdOffsets: true,
    authoritativeOfdIds: new Set([4]),
  });

  expect(readOffset()).toBe(0n);
});

test("status ofd slots preserve a newer canonical offset from an unchanged worker snapshot", () => {
  let handle: ReturnType<typeof makeHandle> | undefined;
  const slotView = (ofdId: number) => {
    if (!handle) {
      throw new Error("handle not initialized");
    }
    return new DataView(
      (handle.memory as WebAssembly.Memory).buffer,
      (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
      SLOT_BYTES,
    );
  };
  handle = makeHandle({
    kernel: makeKernel({
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_TYPE_OFFSET, true) : 0,
      kernel_ofd_host_fd: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_HOST_FD_OFFSET, true) : -1,
      kernel_ofd_ino: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getUint32(OFD_INO_OFFSET, true) : 0,
      kernel_ofd_refcount: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_REFCOUNT_OFFSET, true) : 0,
      kernel_ofd_offset: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getBigInt64(OFD_OFFSET_OFFSET, true) : 0n,
    }),
  });
  new Uint8Array((handle.memory as WebAssembly.Memory).buffer).set(
    makeOfdSlot(1, 22, 33, 1, 0n),
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + 4 * SLOT_BYTES,
  );

  applyStatusOfdSlots(handle as never, makeStatusMessage({
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: makeOfdSlot(1, 22, 33, 1, 1560n) }],
  }) as never, {
    trustRegularFileOfdOffsets: true,
    authoritativeOfdIds: new Set(),
  });

  expect(slotView(4).getBigInt64(OFD_OFFSET_OFFSET, true)).toBe(0n);
});

test("status ofd slots do not replace a live regular file identity from stale snapshots", () => {
  let handle: ReturnType<typeof makeHandle> | undefined;
  const slotView = (ofdId: number) => {
    if (!handle) {
      throw new Error("handle not initialized");
    }
    return new DataView(
      (handle.memory as WebAssembly.Memory).buffer,
      (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
      SLOT_BYTES,
    );
  };
  handle = makeHandle({
    kernel: makeKernel({
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_TYPE_OFFSET, true) : 0,
      kernel_ofd_host_fd: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_HOST_FD_OFFSET, true) : -1,
      kernel_ofd_ino: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getUint32(OFD_INO_OFFSET, true) : 0,
      kernel_ofd_refcount: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_REFCOUNT_OFFSET, true) : 0,
      kernel_ofd_offset: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getBigInt64(OFD_OFFSET_OFFSET, true) : 0n,
    }),
  });
  const current = makeOfdSlot(1, 22, 99, 1, 19n);
  const stale = makeOfdSlot(1, 22, 42, 1, 0n);
  new Uint8Array((handle.memory as WebAssembly.Memory).buffer).set(
    current,
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + 4 * SLOT_BYTES,
  );

  const changedFds = applyStatusOfdSlots(handle as never, makeStatusMessage({
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: stale }],
  }) as never, { trustRegularFileOfdOffsets: true });

  expect(slotView(4).getUint32(OFD_INO_OFFSET, true)).toBe(99);
  expect(slotView(4).getBigInt64(OFD_OFFSET_OFFSET, true)).toBe(19n);
  expect(changedFds).toEqual([]);
});

test("status openat replaces a reused regular file OFD identity", () => {
  let handle: ReturnType<typeof makeHandle> | undefined;
  const slotView = (ofdId: number) => {
    if (!handle) {
      throw new Error("handle not initialized");
    }
    return new DataView(
      (handle.memory as WebAssembly.Memory).buffer,
      (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
      SLOT_BYTES,
    );
  };
  handle = makeHandle({
    kernel: makeKernel({
      abi_sys_openat: () => 56n,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_TYPE_OFFSET, true) : 0,
      kernel_ofd_host_fd: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_HOST_FD_OFFSET, true) : -1,
      kernel_ofd_ino: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getUint32(OFD_INO_OFFSET, true) : 0,
      kernel_ofd_refcount: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_REFCOUNT_OFFSET, true) : 0,
      kernel_ofd_offset: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getBigInt64(OFD_OFFSET_OFFSET, true) : 0n,
    }),
  });
  new Uint8Array((handle.memory as WebAssembly.Memory).buffer).set(
    makeOfdSlot(1, -1, 42, 1, 19n),
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + 4 * SLOT_BYTES,
  );

  const message = makeStatusMessage({
    syscallNr: 56n,
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: makeOfdSlot(1, -1, 99, 1, 0n) }],
  });

  const ignoredFds = applyStatusOfdSlots(handle as never, message as never, {
    authoritativeOfdIds: new Set(),
  });

  expect(slotView(4).getUint32(OFD_INO_OFFSET, true)).toBe(42);
  expect(slotView(4).getBigInt64(OFD_OFFSET_OFFSET, true)).toBe(19n);
  expect(ignoredFds).toEqual([]);

  const changedFds = applyStatusOfdSlots(handle as never, message as never, {
    authoritativeOfdIds: new Set([4]),
  });

  expect(slotView(4).getUint32(OFD_INO_OFFSET, true)).toBe(99);
  expect(slotView(4).getBigInt64(OFD_OFFSET_OFFSET, true)).toBe(0n);
  expect(changedFds).toEqual([3]);
});

test("status resets a same-identity OFD regenerated without an OFD byte diff", () => {
  let handle: ReturnType<typeof makeHandle> | undefined;
  const slotView = (ofdId: number) => new DataView(
    (handle!.memory as WebAssembly.Memory).buffer,
    (handle!.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
    SLOT_BYTES,
  );
  handle = makeHandle({
    kernel: makeKernel({
      abi_sys_openat: () => 56n,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_TYPE_OFFSET, true) : 0,
      kernel_ofd_host_fd: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_HOST_FD_OFFSET, true) : -1,
      kernel_ofd_ino: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getUint32(OFD_INO_OFFSET, true) : 0,
      kernel_ofd_refcount: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_REFCOUNT_OFFSET, true) : 0,
      kernel_ofd_offset: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getBigInt64(OFD_OFFSET_OFFSET, true) : 0n,
    }),
  });
  new Uint8Array((handle.memory as WebAssembly.Memory).buffer).set(
    makeOfdSlot(1, -1, 42, 1, 4096n),
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + 4 * SLOT_BYTES,
  );

  const changedFds = applyStatusOfdSlots(handle as never, makeStatusMessage({
    syscallNr: 63n,
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0, generation: 12 }],
    ofdSlots: [{ ofdId: 4, slot: makeOfdSlot(1, -1, 42, 1, 0n) }],
  }) as never, {
    authoritativeOfdIds: new Set(),
    postedFdEntries: new Map([[3, { fd: 3, ofdId: 4, fdFlags: 0, generation: 11 }]]),
  });

  expect(slotView(4).getBigInt64(OFD_OFFSET_OFFSET, true)).toBe(0n);
  expect(changedFds).toEqual([]);
});

test("status applies an OFD identity changed earlier in the worker step", () => {
  let handle: ReturnType<typeof makeHandle> | undefined;
  const slotView = (ofdId: number) => {
    if (!handle) {
      throw new Error("handle not initialized");
    }
    return new DataView(
      (handle.memory as WebAssembly.Memory).buffer,
      (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
      SLOT_BYTES,
    );
  };
  handle = makeHandle({
    kernel: makeKernel({
      abi_sys_openat: () => 56n,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_TYPE_OFFSET, true) : 0,
      kernel_ofd_host_fd: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_HOST_FD_OFFSET, true) : -1,
      kernel_ofd_ino: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getUint32(OFD_INO_OFFSET, true) : 0,
      kernel_ofd_refcount: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_REFCOUNT_OFFSET, true) : 0,
      kernel_ofd_offset: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getBigInt64(OFD_OFFSET_OFFSET, true) : 0n,
    }),
  });
  new Uint8Array((handle.memory as WebAssembly.Memory).buffer).set(
    makeOfdSlot(1, -1, 42, 1, 19n),
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + 4 * SLOT_BYTES,
  );

  const changedFds = applyStatusOfdSlots(handle as never, makeStatusMessage({
    syscallNr: 63n,
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: makeOfdSlot(1, -1, 99, 1, 0n) }],
  }) as never, {
    authoritativeOfdIds: new Set([4]),
  });

  expect(slotView(4).getUint32(OFD_INO_OFFSET, true)).toBe(99);
  expect(slotView(4).getBigInt64(OFD_OFFSET_OFFSET, true)).toBe(0n);
  expect(changedFds).toEqual([3]);
});

test("status rejects a worker OFD identity after the posted slot was reused", () => {
  let handle: ReturnType<typeof makeHandle> | undefined;
  const slotView = (ofdId: number) => {
    if (!handle) {
      throw new Error("handle not initialized");
    }
    return new DataView(
      (handle.memory as WebAssembly.Memory).buffer,
      (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
      SLOT_BYTES,
    );
  };
  handle = makeHandle({
    kernel: makeKernel({
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_TYPE_OFFSET, true) : 0,
      kernel_ofd_host_fd: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_HOST_FD_OFFSET, true) : -1,
      kernel_ofd_ino: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getUint32(OFD_INO_OFFSET, true) : 0,
      kernel_ofd_refcount: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_REFCOUNT_OFFSET, true) : 0,
      kernel_ofd_offset: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getBigInt64(OFD_OFFSET_OFFSET, true) : 0n,
    }),
  });
  const posted = makeOfdSlot(1, -1, 42, 1, 0n);
  const stale = makeOfdSlot(1, -1, 42, 1, 4096n);
  new Uint8Array((handle.memory as WebAssembly.Memory).buffer).set(
    makeOfdSlot(1, -1, 99, 1, 0n),
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + 4 * SLOT_BYTES,
  );

  const changedFds = applyStatusOfdSlots(handle as never, makeStatusMessage({
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: stale }],
  }) as never, {
    trustRegularFileOfdOffsets: true,
    authoritativeOfdIds: new Set([4]),
    postedOfdSlots: new Map([[4, posted]]),
  });

  expect(slotView(4).getUint32(OFD_INO_OFFSET, true)).toBe(99);
  expect(slotView(4).getBigInt64(OFD_OFFSET_OFFSET, true)).toBe(0n);
  expect(changedFds).toEqual([]);
});

test("status rejects stale openat OFD identity after fd authority advances", () => {
  let handle: ReturnType<typeof makeHandle> | undefined;
  const slotView = (ofdId: number) => {
    if (!handle) {
      throw new Error("handle not initialized");
    }
    return new DataView(
      (handle.memory as WebAssembly.Memory).buffer,
      (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
      SLOT_BYTES,
    );
  };
  handle = makeHandle({
    runtimeBridge: createRuntimeStateBridge(),
    kernel: makeKernel({
      abi_sys_openat: () => 56,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 3 ? 4 : 0,
      process_fd_generation: (_vmPtr: number, fd: number) => fd === 3 ? 12 : 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_TYPE_OFFSET, true) : 0,
      kernel_ofd_host_fd: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_HOST_FD_OFFSET, true) : -1,
      kernel_ofd_ino: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getUint32(OFD_INO_OFFSET, true) : 0,
      kernel_ofd_refcount: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_REFCOUNT_OFFSET, true) : 0,
      kernel_ofd_offset: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getBigInt64(OFD_OFFSET_OFFSET, true) : 0n,
    }),
  });
  new Uint8Array((handle.memory as WebAssembly.Memory).buffer).set(
    makeOfdSlot(1, -1, 99, 1, 0n),
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + 4 * SLOT_BYTES,
  );
  publishRuntimeFdAuthorityEpoch(handle.runtimeBridge, 3);

  const changedFds = applyStatusOfdSlots(handle as never, makeStatusMessage({
    vmPtr: 0x1234,
    syscallNr: 56n,
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0, generation: 11 }],
    ofdSlots: [{ ofdId: 4, slot: makeOfdSlot(1, -1, 42, 1, 0n) }],
  }) as never, {
    authoritativeOfdIds: new Set([4]),
    postedFdEntries: new Map([[3, {
      fd: 3,
      ofdId: 4,
      fdFlags: 0,
      authorityEpoch: 0,
    }]]),
  });

  expect(slotView(4).getUint32(OFD_INO_OFFSET, true)).toBe(99);
  expect(changedFds).toEqual([]);
});

test("status compares worker OFD identity authority with the canonical process table", () => {
  let handle: ReturnType<typeof makeHandle> | undefined;
  const slotView = (ofdId: number) => {
    if (!handle) {
      throw new Error("handle not initialized");
    }
    return new DataView(
      (handle.memory as WebAssembly.Memory).buffer,
      (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
      SLOT_BYTES,
    );
  };
  handle = makeHandle({
    runtimeBridge: createRuntimeStateBridge(),
    kernel: makeKernel({
      abi_sys_openat: () => 56,
      process_current_pid: () => 7,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 3 ? 4 : 0,
      process_fd_generation: (vmPtr: number, fd: number) => fd === 3
        ? (vmPtr === 0x1234 ? 12 : 11)
        : 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_TYPE_OFFSET, true) : 0,
      kernel_ofd_host_fd: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_HOST_FD_OFFSET, true) : -1,
      kernel_ofd_ino: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getUint32(OFD_INO_OFFSET, true) : 0,
      kernel_ofd_refcount: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_REFCOUNT_OFFSET, true) : 0,
      kernel_ofd_offset: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getBigInt64(OFD_OFFSET_OFFSET, true) : 0n,
    }),
  });
  new Uint8Array((handle.memory as WebAssembly.Memory).buffer).set(
    makeOfdSlot(1, -1, 99, 1, 0n),
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + 4 * SLOT_BYTES,
  );
  publishRuntimeFdAuthorityEpoch(handle.runtimeBridge, 3);

  const changedFds = applyStatusOfdSlots(handle as never, makeStatusMessage({
    vmPtr: 0x5678,
    syscallNr: 56n,
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0, generation: 11 }],
    ofdSlots: [{ ofdId: 4, slot: makeOfdSlot(1, -1, 42, 1, 0n) }],
  }) as never, {
    authoritativeOfdIds: new Set([4]),
    postedFdEntries: new Map([[3, {
      fd: 3,
      ofdId: 4,
      fdFlags: 0,
      authorityEpoch: 0,
    }]]),
  });

  expect(slotView(4).getUint32(OFD_INO_OFFSET, true)).toBe(99);
  expect(changedFds).toEqual([]);
});

test("status accepts an OFD identity while posted fd authority is current", () => {
  let handle: ReturnType<typeof makeHandle> | undefined;
  const slotView = (ofdId: number) => {
    if (!handle) {
      throw new Error("handle not initialized");
    }
    return new DataView(
      (handle.memory as WebAssembly.Memory).buffer,
      (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
      SLOT_BYTES,
    );
  };
  handle = makeHandle({
    runtimeBridge: createRuntimeStateBridge(),
    kernel: makeKernel({
      abi_sys_openat: () => 56,
      process_current_pid: () => 7,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 3 ? 4 : 0,
      process_fd_generation: (vmPtr: number, fd: number) => fd === 3
        ? (vmPtr === 0x1234 ? 11 : 12)
        : 0,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_TYPE_OFFSET, true) : 0,
      kernel_ofd_host_fd: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_HOST_FD_OFFSET, true) : -1,
      kernel_ofd_ino: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getUint32(OFD_INO_OFFSET, true) : 0,
      kernel_ofd_refcount: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_REFCOUNT_OFFSET, true) : 0,
      kernel_ofd_offset: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getBigInt64(OFD_OFFSET_OFFSET, true) : 0n,
    }),
  });
  new Uint8Array((handle.memory as WebAssembly.Memory).buffer).set(
    makeOfdSlot(1, -1, 99, 1, 0n),
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + 4 * SLOT_BYTES,
  );

  const changedFds = applyStatusOfdSlots(handle as never, makeStatusMessage({
    vmPtr: 0x5678,
    syscallNr: 56n,
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0, generation: 12 }],
    ofdSlots: [{ ofdId: 4, slot: makeOfdSlot(1, -1, 42, 1, 0n) }],
  }) as never, {
    authoritativeOfdIds: new Set([4]),
    postedFdEntries: new Map([[3, {
      fd: 3,
      ofdId: 4,
      fdFlags: 0,
      authorityEpoch: 0,
    }]]),
  });

  expect(slotView(4).getUint32(OFD_INO_OFFSET, true)).toBe(42);
  expect(changedFds).toEqual([3]);
});

test("status does not apply regular-file authority filtering to pipe slots", () => {
  const pipeType = 7;
  const ofdId = 5;
  const handle = makeHandle({
    runtimeBridge: createRuntimeStateBridge(),
    kernel: makeKernel({
      abi_fd_type_file: () => 1,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fd === 4 ? ofdId : 0,
    }),
  });
  new Uint8Array((handle.memory as WebAssembly.Memory).buffer).set(
    makeOfdSlot(pipeType, -1, 0, 1, 0n),
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
  );
  publishRuntimeFdAuthorityEpoch(handle.runtimeBridge, 4);

  applyStatusOfdSlots(handle as never, makeStatusMessage({
    fdEntries: [{ fd: 4, ofdId, fdFlags: 0 }],
    ofdSlots: [{ ofdId, slot: makeOfdSlot(pipeType, -1, 0, 2, 0n) }],
  }) as never, {
    authoritativeOfdIds: new Set([ofdId]),
    postedFdEntries: new Map([[4, {
      fd: 4,
      ofdId,
      fdFlags: 0,
      authorityEpoch: 0,
    }]]),
  });

  const applied = new DataView(
    (handle.memory as WebAssembly.Memory).buffer,
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
    SLOT_BYTES,
  );
  expect(applied.getInt32(OFD_REFCOUNT_OFFSET, true)).toBe(2);
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

test("status ofd slots preserve live eventfd nonblock flags from stale snapshots", () => {
  const eventfdType = 9;
  const ofdId = 6;
  const handle = makeHandle({
    kernel: makeKernel({
      abi_fd_type_eventfd: () => eventfdType,
    }),
  });
  new Uint8Array((handle.memory as WebAssembly.Memory).buffer).set(
    makeOfdSlot(eventfdType, 0, 0, 1, 0n, O_RDWR | O_NONBLOCK),
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
  );
  const applied = new DataView(
    (handle.memory as WebAssembly.Memory).buffer,
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
    SLOT_BYTES,
  );

  applyStatusOfdSlots(handle as never, makeStatusMessage({
    fdEntries: [{ fd: 3, ofdId, fdFlags: 0, generation: 12 }],
    ofdSlots: [{
      ofdId,
      slot: makeOfdSlot(eventfdType, 0, 0, 1, 0n, O_RDWR),
    }],
    fdTraceSyscallNr: 101n,
    fdTraceArg0OfdId: ofdId,
    fdTraceResult: 0n,
  }) as never);

  expect(applied.getInt32(OFD_STATUS_FLAGS_OFFSET, true)).toBe(O_RDWR | O_NONBLOCK);
});

test("status ofd slots allow fcntl setfl to update live eventfd status flags", () => {
  const eventfdType = 9;
  const ofdId = 6;
  const handle = makeHandle({
    kernel: makeKernel({
      abi_fd_type_eventfd: () => eventfdType,
    }),
  });
  new Uint8Array((handle.memory as WebAssembly.Memory).buffer).set(
    makeOfdSlot(eventfdType, 0, 0, 1, 0n, O_RDWR),
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
  );
  const applied = new DataView(
    (handle.memory as WebAssembly.Memory).buffer,
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
    SLOT_BYTES,
  );

  applyStatusOfdSlots(handle as never, makeStatusMessage({
    fdEntries: [{ fd: 3, ofdId, fdFlags: 0 }],
    ofdSlots: [{
      ofdId,
      slot: makeOfdSlot(eventfdType, 0, 0, 1, 0n, O_RDWR | O_NONBLOCK),
    }],
    fdTraceSyscallNr: 25n,
    fdTraceArg0OfdId: ofdId,
    fdTraceArg1: 4n,
    fdTraceResult: 0n,
  }) as never);

  expect(applied.getInt32(OFD_STATUS_FLAGS_OFFSET, true)).toBe(O_RDWR | O_NONBLOCK);
});

test("status ofd slots preserve regular file access mode across fcntl setfl snapshots", () => {
  const fileType = 1;
  const ofdId = 6;
  const handle = makeHandle({
    kernel: makeKernel({
      abi_fd_type_file: () => fileType,
    }),
  });
  new Uint8Array((handle.memory as WebAssembly.Memory).buffer).set(
    makeOfdSlot(fileType, -1, 42, 1, 0n, O_WRONLY),
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
  );
  const applied = new DataView(
    (handle.memory as WebAssembly.Memory).buffer,
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
    SLOT_BYTES,
  );

  applyStatusOfdSlots(handle as never, makeStatusMessage({
    fdEntries: [{ fd: 3, ofdId, fdFlags: 0 }],
    ofdSlots: [{
      ofdId,
      slot: makeOfdSlot(fileType, -1, 42, 1, 0n, O_NONBLOCK),
    }],
    fdTraceSyscallNr: 25n,
    fdTraceArg0OfdId: ofdId,
    fdTraceArg1: 4n,
    fdTraceResult: 0n,
  }) as never);

  expect(applied.getInt32(OFD_STATUS_FLAGS_OFFSET, true)).toBe(O_WRONLY | O_NONBLOCK);
});

test("status ofd slots preserve regular file access mode across stale thread snapshots", () => {
  const fileType = 1;
  const ofdId = 6;
  const handle = makeHandle({
    kernel: makeKernel({
      abi_fd_type_file: () => fileType,
    }),
  });
  new Uint8Array((handle.memory as WebAssembly.Memory).buffer).set(
    makeOfdSlot(fileType, -1, 42, 1, 0n, O_RDWR),
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
  );
  const applied = new DataView(
    (handle.memory as WebAssembly.Memory).buffer,
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
    SLOT_BYTES,
  );

  applyStatusOfdSlots(handle as never, makeStatusMessage({
    fdEntries: [{ fd: 3, ofdId, fdFlags: 0, generation: 12 }],
    ofdSlots: [{
      ofdId,
      slot: makeOfdSlot(fileType, -1, 42, 1, 0n, 0),
    }],
    fdTraceSyscallNr: 98n,
    fdTraceResult: 0n,
  }) as never, {
    authoritativeOfdIds: new Set([ofdId]),
    postedFdEntries: new Map([[3, { fd: 3, ofdId, fdFlags: 0, generation: 11 }]]),
  });

  expect(applied.getInt32(OFD_STATUS_FLAGS_OFFSET, true)).toBe(O_RDWR);
});

test("status ofd slots apply access mode for a regenerated same-identity OFD", () => {
  const fileType = 1;
  const ofdId = 6;
  let handle: ReturnType<typeof makeHandle> | undefined;
  const slotView = () => new DataView(
    (handle!.memory as WebAssembly.Memory).buffer,
    (handle!.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
    SLOT_BYTES,
  );
  handle = makeHandle({
    kernel: makeKernel({
      abi_fd_type_file: () => fileType,
      abi_sys_openat: () => 56n,
      kernel_ofd_fd_type: (candidate: number) => candidate === ofdId
        ? slotView().getInt32(OFD_TYPE_OFFSET, true)
        : 0,
      kernel_ofd_host_fd: (candidate: number) => candidate === ofdId
        ? slotView().getInt32(OFD_HOST_FD_OFFSET, true)
        : -1,
      kernel_ofd_ino: (candidate: number) => candidate === ofdId
        ? slotView().getUint32(OFD_INO_OFFSET, true)
        : 0,
      kernel_ofd_refcount: (candidate: number) => candidate === ofdId
        ? slotView().getInt32(OFD_REFCOUNT_OFFSET, true)
        : 0,
      kernel_ofd_offset: (candidate: number) => candidate === ofdId
        ? slotView().getBigInt64(OFD_OFFSET_OFFSET, true)
        : 0n,
    }),
  });
  new Uint8Array((handle.memory as WebAssembly.Memory).buffer).set(
    makeOfdSlot(fileType, -1, 42, 1, 0n, 0),
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
  );
  const applied = new DataView(
    (handle.memory as WebAssembly.Memory).buffer,
    (handle.kernelState as { ofdTablePtr: number }).ofdTablePtr + ofdId * SLOT_BYTES,
    SLOT_BYTES,
  );

  applyStatusOfdSlots(handle as never, makeStatusMessage({
    syscallNr: 56n,
    fdEntries: [{ fd: 3, ofdId, fdFlags: 0, generation: 12 }],
    ofdSlots: [{
      ofdId,
      slot: makeOfdSlot(fileType, -1, 42, 1, 0n, O_RDWR),
    }],
  }) as never, {
    authoritativeOfdIds: new Set(),
    postedFdEntries: new Map([[3, { fd: 3, ofdId, fdFlags: 0, generation: 11 }]]),
  });

  expect(applied.getInt32(OFD_STATUS_FLAGS_OFFSET, true)).toBe(O_RDWR);
});
