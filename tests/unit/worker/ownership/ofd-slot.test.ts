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
  const offsetFloors = new Map<number, { hostFd: number; ino: number; offset: bigint }>();
  const slotView = (ofdId: number): DataView =>
    new DataView(memory.buffer, ofdTablePtr + ofdId * slotBytes, slotBytes);
  const kernel = {
    abi_max_fds: () => 16,
    abi_fd_type_file: () => 3,
    abi_sys_lseek: () => 62n,
    kernel_ofd_table_bytes: () => 16 * slotBytes,
    kernel_ofd_fd_type: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_TYPE_OFFSET, true) : 0,
    kernel_ofd_host_fd: (ofdId: number) => slotView(ofdId).getInt32(OFD_HOST_FD_OFFSET, true),
    kernel_ofd_ino: (ofdId: number) => slotView(ofdId).getUint32(OFD_INO_OFFSET, true),
    kernel_ofd_refcount: (ofdId: number) => slotView(ofdId).getInt32(OFD_REFCOUNT_OFFSET, true),
    kernel_ofd_offset: (ofdId: number) => slotView(ofdId).getBigInt64(OFD_OFFSET_OFFSET, true),
    supervisor_fd_entry_restore_action_apply: () => 1,
    supervisor_fd_entry_restore_action_skip: () => 2,
    supervisor_ofd_slot_restore_flag_referenced_by_fd_entry: () => 1,
    supervisor_ofd_slot_restore_flag_trust_regular_file_offset: () => 2,
    supervisor_ofd_slot_restore_action: (
      ofdId: number,
      candidateType: number,
      candidateHostFd: number,
      candidateIno: number,
      candidateOffset: bigint | number,
      syscallNr: bigint | number,
      flags: number,
    ) => {
      if (ofdId !== 4) {
        return 2;
      }
      const current = slotView(ofdId);
      const currentType = current.getInt32(OFD_TYPE_OFFSET, true);
      const currentRefcount = current.getInt32(OFD_REFCOUNT_OFFSET, true);
      if (currentType === 0 || currentRefcount <= 0) {
        return 1;
      }
      const referencedByFdEntry = (flags & 1) !== 0;
      if (!referencedByFdEntry) {
        if (candidateType === 0 || candidateType !== currentType) {
          return 2;
        }
        if (
          candidateHostFd !== current.getInt32(OFD_HOST_FD_OFFSET, true)
          || candidateIno !== current.getUint32(OFD_INO_OFFSET, true)
        ) {
          return 2;
        }
      }
      if ((flags & 2) !== 0 || currentType !== 3 || candidateType !== 3) {
        return 1;
      }
      const currentHostFd = current.getInt32(OFD_HOST_FD_OFFSET, true);
      const currentIno = current.getUint32(OFD_INO_OFFSET, true);
      if (candidateHostFd !== currentHostFd || candidateIno !== currentIno) {
        return 1;
      }
      const currentOffset = current.getBigInt64(OFD_OFFSET_OFFSET, true);
      const candidate = typeof candidateOffset === "bigint" ? candidateOffset : BigInt(candidateOffset);
      const floor = offsetFloors.get(ofdId);
      const maxSeen = floor?.hostFd === currentHostFd && floor.ino === currentIno ? floor.offset : currentOffset;
      const nextMax = candidate > currentOffset ? candidate : currentOffset;
      if (nextMax > maxSeen) {
        offsetFloors.set(ofdId, { hostFd: currentHostFd, ino: currentIno, offset: nextMax });
      }
      if (candidate < maxSeen) {
        return 2;
      }
      if (candidate >= currentOffset) {
        return 1;
      }
      return syscallNr === 62n ? 1 : 2;
    },
  };
  return {
    memory,
    kernelState: { ofdTablePtr },
    kernel,
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
