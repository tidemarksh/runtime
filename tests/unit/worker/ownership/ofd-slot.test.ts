import { expect, test } from "vite-plus/test";
import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_OFFSET_OFFSET,
  OFD_REFCOUNT_OFFSET,
  OFD_TYPE_OFFSET,
} from "../../../../src/layout/ofd.js";
import { applyThreadOfdSlots } from "../../../../src/worker/ownership/ofd-slot.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

const slotBytes = 64;
const ofdTablePtr = 0x1000;

function writeRegularFileSlot(
  memory: WebAssembly.Memory,
  ofdId: number,
  offset: bigint,
  options: { hostFd?: number; ino?: number } = {},
): Uint8Array {
  const slot = new Uint8Array(slotBytes);
  const view = new DataView(slot.buffer);
  view.setInt32(OFD_TYPE_OFFSET, 3, true);
  view.setInt32(OFD_REFCOUNT_OFFSET, 1, true);
  view.setInt32(OFD_HOST_FD_OFFSET, options.hostFd ?? 0, true);
  view.setUint32(OFD_INO_OFFSET, options.ino ?? 42, true);
  view.setBigInt64(OFD_OFFSET_OFFSET, offset, true);
  new Uint8Array(memory.buffer).set(slot, ofdTablePtr + ofdId * slotBytes);
  return slot;
}

function makeHandle(memory: WebAssembly.Memory): ProcessHandle {
  return {
    memory,
    kernelState: { ofdTablePtr },
    kernel: {
      abi_max_fds: () => 16,
      abi_fd_type_file: () => 3,
      abi_sys_lseek: () => 62n,
      kernel_ofd_table_bytes: () => 16 * slotBytes,
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 4 ? 3 : 0,
    },
  } as unknown as ProcessHandle;
}

function currentOffset(memory: WebAssembly.Memory): bigint {
  return new DataView(memory.buffer).getBigInt64(
    ofdTablePtr + 4 * slotBytes + OFD_OFFSET_OFFSET,
    true,
  );
}

test("applyThreadOfdSlots preserves rollback guard unless regular file offset is trusted", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const handle = makeHandle(memory);
  writeRegularFileSlot(memory, 4, 4096n);
  const lowerOffsetSlot = writeRegularFileSlot(memory, 4, 64n);
  writeRegularFileSlot(memory, 4, 4096n);

  applyThreadOfdSlots(handle, {
    vmPtr: 0x1234,
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: lowerOffsetSlot }],
  });

  expect(currentOffset(memory)).toBe(4096n);

  applyThreadOfdSlots(handle, {
    vmPtr: 0x1234,
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: lowerOffsetSlot }],
    trustRegularFileOfdOffsets: true,
  });

  expect(currentOffset(memory)).toBe(64n);
});

test("applyThreadOfdSlots resets rollback history when a regular file OFD id is reused", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const handle = makeHandle(memory);
  writeRegularFileSlot(memory, 4, 4096n, { ino: 42 });
  const highOffsetSlot = writeRegularFileSlot(memory, 4, 8192n, { ino: 42 });
  writeRegularFileSlot(memory, 4, 4096n, { ino: 42 });

  applyThreadOfdSlots(handle, {
    vmPtr: 0x1234,
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: highOffsetSlot }],
  });

  expect(currentOffset(memory)).toBe(8192n);

  writeRegularFileSlot(memory, 4, 0n, { ino: 99 });
  const reusedOfdSlot = writeRegularFileSlot(memory, 4, 64n, { ino: 99 });
  writeRegularFileSlot(memory, 4, 0n, { ino: 99 });

  applyThreadOfdSlots(handle, {
    vmPtr: 0x1234,
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: reusedOfdSlot }],
  });

  expect(currentOffset(memory)).toBe(64n);
});
