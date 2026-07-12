import { expect, test } from "vite-plus/test";
import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_OFFSET_OFFSET,
  OFD_REFCOUNT_OFFSET,
  OFD_TYPE_OFFSET,
} from "../../../../src/layout/ofd.js";
import { restoreRegularFileOffsetsForStep } from "../../../../src/worker/ownership/ofd-table.js";
import { applyThreadOfdSlots } from "../../../../src/worker/ownership/ofd-slot.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";
import { makeSupervisorAbiStubs } from "../support/supervisor-abi.js";

const slotBytes = 64;
const ofdTablePtr = 0x1000;
const ofdOffsetFloorPtr = 0x2000;
const ofdOffsetFloorBytes = 16 * BigUint64Array.BYTES_PER_ELEMENT;
const pipeType = 6;
const pipeBase = 0x4000;
const pipeSlotBytes = 32 + 65_536;
const pipeGenerationOffset = 20;

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
    kernel_ofd_offset_floor_ptr: () => ofdOffsetFloorPtr,
    kernel_ofd_offset_floor_bytes: () => ofdOffsetFloorBytes,
    kernel_ofd_fd_type: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_TYPE_OFFSET, true) : 0,
    kernel_ofd_host_fd: (ofdId: number) => slotView(ofdId).getInt32(OFD_HOST_FD_OFFSET, true),
    kernel_ofd_ino: (ofdId: number) => slotView(ofdId).getUint32(OFD_INO_OFFSET, true),
    kernel_ofd_refcount: (ofdId: number) => slotView(ofdId).getInt32(OFD_REFCOUNT_OFFSET, true),
    kernel_ofd_offset: (ofdId: number) => slotView(ofdId).getBigInt64(OFD_OFFSET_OFFSET, true),
    supervisor_ofd_fd_type: (ofdId: number) => ofdId === 4 ? slotView(ofdId).getInt32(OFD_TYPE_OFFSET, true) : 0,
    supervisor_ofd_host_fd: (ofdId: number) => slotView(ofdId).getInt32(OFD_HOST_FD_OFFSET, true),
    supervisor_ofd_ino: (ofdId: number) => slotView(ofdId).getUint32(OFD_INO_OFFSET, true),
    supervisor_ofd_refcount: (ofdId: number) => slotView(ofdId).getInt32(OFD_REFCOUNT_OFFSET, true),
    supervisor_ofd_offset: (ofdId: number) => slotView(ofdId).getBigInt64(OFD_OFFSET_OFFSET, true),
    supervisor_fd_entry_restore_action_apply: () => 1,
    supervisor_fd_entry_restore_action_skip: () => 2,
    supervisor_regular_file_offset_restore_action: (
      ofdId: number,
      snapshotHostFd: number,
      snapshotIno: number,
      snapshotOffset: bigint | number,
    ) => {
      if (ofdId !== 4) {
        return 2;
      }
      const current = slotView(ofdId);
      const currentOffset = current.getBigInt64(OFD_OFFSET_OFFSET, true);
      const candidate = typeof snapshotOffset === "bigint" ? snapshotOffset : BigInt(snapshotOffset);
      if (
        current.getInt32(OFD_TYPE_OFFSET, true) !== 3
        || current.getInt32(OFD_REFCOUNT_OFFSET, true) <= 0
        || current.getInt32(OFD_HOST_FD_OFFSET, true) !== snapshotHostFd
        || current.getUint32(OFD_INO_OFFSET, true) !== snapshotIno
        || currentOffset >= candidate
      ) {
        return 2;
      }
      return 1;
    },
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
      if (referencedByFdEntry) {
        if (candidateType === 0) {
          return 2;
        }
        if (
          candidateType !== currentType
          || currentType !== 3
          || candidateType !== 3
          || candidateHostFd !== current.getInt32(OFD_HOST_FD_OFFSET, true)
          || candidateIno !== current.getUint32(OFD_INO_OFFSET, true)
        ) {
          return 1;
        }
      } else {
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
    kernelState: { ofdTablePtr, ofdOffsetFloorPtr, ofdOffsetFloorBytes },
    kernel,
  } as unknown as ProcessHandle;
}

function currentOffset(memory: WebAssembly.Memory): bigint {
  return new DataView(memory.buffer).getBigInt64(
    ofdTablePtr + 4 * slotBytes + OFD_OFFSET_OFFSET,
    true,
  );
}

function currentIno(memory: WebAssembly.Memory): number {
  return new DataView(memory.buffer).getUint32(
    ofdTablePtr + 4 * slotBytes + OFD_INO_OFFSET,
    true,
  );
}

function writeOffsetFloor(memory: WebAssembly.Memory, ofdId: number, offset: bigint): void {
  new DataView(memory.buffer).setBigUint64(
    ofdOffsetFloorPtr + ofdId * BigUint64Array.BYTES_PER_ELEMENT,
    offset,
    true,
  );
}

function currentOffsetFloor(memory: WebAssembly.Memory, ofdId: number): bigint {
  return new DataView(memory.buffer).getBigUint64(
    ofdOffsetFloorPtr + ofdId * BigUint64Array.BYTES_PER_ELEMENT,
    true,
  );
}

function clearOfdSlot(memory: WebAssembly.Memory, ofdId: number): void {
  new Uint8Array(memory.buffer).fill(
    0,
    ofdTablePtr + ofdId * slotBytes,
    ofdTablePtr + (ofdId + 1) * slotBytes,
  );
}

function pipeOfdSlot(
  generation: number,
  options: { hostFd?: number; pipeSlot?: number } = {},
): Uint8Array {
  const slot = new Uint8Array(slotBytes);
  const view = new DataView(slot.buffer);
  view.setInt32(OFD_TYPE_OFFSET, pipeType, true);
  view.setInt32(OFD_REFCOUNT_OFFSET, 1, true);
  view.setInt32(OFD_HOST_FD_OFFSET, options.hostFd ?? -3, true);
  view.setUint32(OFD_INO_OFFSET, options.pipeSlot ?? 2, true);
  view.setBigInt64(OFD_OFFSET_OFFSET, BigInt(generation), true);
  return slot;
}

function writePipeOfdSlot(
  memory: WebAssembly.Memory,
  ofdId: number,
  generation: number,
  options: { hostFd?: number; pipeSlot?: number } = {},
): Uint8Array {
  const slot = pipeOfdSlot(generation, options);
  new Uint8Array(memory.buffer).set(slot, ofdTablePtr + ofdId * slotBytes);
  return slot;
}

function writePipeGeneration(
  memory: WebAssembly.Memory,
  pipeSlot: number,
  generation: number,
): void {
  new DataView(memory.buffer).setUint32(
    pipeBase + pipeSlot * pipeSlotBytes + pipeGenerationOffset,
    generation,
    true,
  );
}

function currentOfdGeneration(memory: WebAssembly.Memory, ofdId: number): bigint {
  return new DataView(memory.buffer).getBigInt64(
    ofdTablePtr + ofdId * slotBytes + OFD_OFFSET_OFFSET,
    true,
  );
}

function makePipeHandle(memory: WebAssembly.Memory): ProcessHandle {
  const slotView = (ofdId: number): DataView =>
    new DataView(memory.buffer, ofdTablePtr + ofdId * slotBytes, slotBytes);
  const kernel = makeSupervisorAbiStubs({
    memory,
    abi_max_fds: () => 16,
    abi_max_pipes: () => 64,
    abi_fd_type_file: () => 3,
    abi_fd_type_pipe: () => pipeType,
    abi_sys_lseek: () => 62n,
    kernel_ofd_table_bytes: () => 16 * slotBytes,
    kernel_pipe_base: () => pipeBase,
    kernel_pipe_region_bytes: () => 64 * pipeSlotBytes,
    kernel_ofd_fd_type: (ofdId: number) => slotView(ofdId).getInt32(OFD_TYPE_OFFSET, true),
    kernel_ofd_host_fd: (ofdId: number) => slotView(ofdId).getInt32(OFD_HOST_FD_OFFSET, true),
    kernel_ofd_ino: (ofdId: number) => slotView(ofdId).getUint32(OFD_INO_OFFSET, true),
    kernel_ofd_refcount: (ofdId: number) => slotView(ofdId).getInt32(OFD_REFCOUNT_OFFSET, true),
    kernel_ofd_offset: (ofdId: number) => slotView(ofdId).getBigInt64(OFD_OFFSET_OFFSET, true),
  });
  return {
    memory,
    kernelState: { ofdTablePtr, pipeBase },
    kernel,
  } as unknown as ProcessHandle;
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

  writeRegularFileSlot(memory, 4, 0n);
  restoreRegularFileOffsetsForStep(handle);

  expect(currentOffset(memory)).toBe(0n);

  writeRegularFileSlot(memory, 4, 4096n);

  applyThreadOfdSlots(handle, {
    vmPtr: 0x1234,
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: lowerOffsetSlot }],
    trustRegularFileOfdOffsets: true,
  });

  expect(currentOffset(memory)).toBe(64n);
});

test("trusted lseek offset transport resets the Kernel regular file offset floor", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const handle = makeHandle(memory);
  writeRegularFileSlot(memory, 4, 1282n);
  writeOffsetFloor(memory, 4, 1282n);
  const seekedSlot = writeRegularFileSlot(memory, 4, 0n);
  writeRegularFileSlot(memory, 4, 1282n);

  applyThreadOfdSlots(handle, {
    vmPtr: 0x1234,
    fdEntries: [{ fd: 15, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: seekedSlot }],
    syscallNr: 62n,
    trustRegularFileOfdOffsets: true,
  });

  expect(currentOffset(memory)).toBe(0n);
  expect(currentOffsetFloor(memory, 4)).toBe(0n);
});

test("applyThreadOfdSlots accepts referenced regular file identity replacement", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const handle = makeHandle(memory);
  writeRegularFileSlot(memory, 4, 7n, { ino: 99 });
  const replacementSlot = writeRegularFileSlot(memory, 4, 55n, { ino: 42 });
  writeRegularFileSlot(memory, 4, 7n, { ino: 99 });

  applyThreadOfdSlots(handle, {
    vmPtr: 0x1234,
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: replacementSlot }],
    trustRegularFileOfdOffsets: true,
    allowRegularFileIdentityReplacement: true,
  });

  expect(currentIno(memory)).toBe(42);
  expect(currentOffset(memory)).toBe(55n);
});

test("applyThreadOfdSlots rejects default regular file identity replacement from referenced snapshots", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const handle = makeHandle(memory);
  writeRegularFileSlot(memory, 4, 7n, { ino: 99 });
  const staleSlot = writeRegularFileSlot(memory, 4, 55n, { ino: 42 });
  writeRegularFileSlot(memory, 4, 7n, { ino: 99 });

  applyThreadOfdSlots(handle, {
    vmPtr: 0x1234,
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: staleSlot }],
    trustRegularFileOfdOffsets: true,
  });

  expect(currentIno(memory)).toBe(99);
  expect(currentOffset(memory)).toBe(7n);
});

test("applyThreadOfdSlots rejects unreferenced regular file identity replacement", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const handle = makeHandle(memory);
  writeRegularFileSlot(memory, 4, 7n, { ino: 99 });
  const staleSlot = writeRegularFileSlot(memory, 4, 55n, { ino: 42 });
  writeRegularFileSlot(memory, 4, 7n, { ino: 99 });

  applyThreadOfdSlots(handle, {
    vmPtr: 0x1234,
    fdEntries: [],
    ofdSlots: [{ ofdId: 4, slot: staleSlot }],
    trustRegularFileOfdOffsets: true,
  });

  expect(currentIno(memory)).toBe(99);
  expect(currentOffset(memory)).toBe(7n);
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

test("applyThreadOfdSlots can skip seeding the regular file offset restore snapshot", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const handle = makeHandle(memory);
  writeRegularFileSlot(memory, 4, 0n, { ino: 42 });
  const fsCompletionSlot = writeRegularFileSlot(memory, 4, 19n, { ino: 42 });
  writeRegularFileSlot(memory, 4, 0n, { ino: 42 });

  applyThreadOfdSlots(handle, {
    vmPtr: 0x1234,
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: fsCompletionSlot }],
    trustRegularFileOfdOffsets: true,
    recordRegularFileOfdOffsets: false,
  });

  expect(currentOffset(memory)).toBe(19n);

  writeRegularFileSlot(memory, 4, 0n, { ino: 42 });
  restoreRegularFileOffsetsForStep(handle);

  expect(currentOffset(memory)).toBe(0n);
});

test("applyThreadOfdSlots clears stale regular file offset restore snapshot when skipping seeding", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const handle = makeHandle(memory);
  writeRegularFileSlot(memory, 4, 0n, { ino: 42 });
  const staleSnapshotSlot = writeRegularFileSlot(memory, 4, 5n, { ino: 42 });
  writeRegularFileSlot(memory, 4, 0n, { ino: 42 });

  applyThreadOfdSlots(handle, {
    vmPtr: 0x1234,
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: staleSnapshotSlot }],
  });

  expect(currentOffset(memory)).toBe(5n);

  const fsCompletionSlot = writeRegularFileSlot(memory, 4, 19n, { ino: 42 });
  writeRegularFileSlot(memory, 4, 5n, { ino: 42 });

  applyThreadOfdSlots(handle, {
    vmPtr: 0x1234,
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: fsCompletionSlot }],
    trustRegularFileOfdOffsets: true,
    recordRegularFileOfdOffsets: false,
  });

  expect(currentOffset(memory)).toBe(19n);

  writeRegularFileSlot(memory, 4, 0n, { ino: 42 });
  restoreRegularFileOffsetsForStep(handle);

  expect(currentOffset(memory)).toBe(0n);
});

test("restoreRegularFileOffsetsForStep drops snapshots after a regular file OFD closes", () => {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const handle = makeHandle(memory);
  writeRegularFileSlot(memory, 4, 0n, { ino: 42 });
  const highOffsetSlot = writeRegularFileSlot(memory, 4, 512n, { ino: 42 });
  writeRegularFileSlot(memory, 4, 0n, { ino: 42 });

  applyThreadOfdSlots(handle, {
    vmPtr: 0x1234,
    fdEntries: [{ fd: 3, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: highOffsetSlot }],
  });

  expect(currentOffset(memory)).toBe(512n);

  clearOfdSlot(memory, 4);
  restoreRegularFileOffsetsForStep(handle);
  writeRegularFileSlot(memory, 4, 0n, { ino: 42 });
  restoreRegularFileOffsetsForStep(handle);

  expect(currentOffset(memory)).toBe(0n);
});

test("applyThreadOfdSlots rejects stale pipe generation snapshots", () => {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  const handle = makePipeHandle(memory);
  writePipeGeneration(memory, 2, 9);
  writePipeOfdSlot(memory, 4, 9);
  const staleSlot = pipeOfdSlot(8);

  applyThreadOfdSlots(handle, {
    vmPtr: 0x1234,
    fdEntries: [{ fd: 13, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: staleSlot }],
  });

  expect(currentOfdGeneration(memory, 4)).toBe(9n);

  const freshSlot = pipeOfdSlot(9);
  writePipeOfdSlot(memory, 4, 8);

  applyThreadOfdSlots(handle, {
    vmPtr: 0x1234,
    fdEntries: [{ fd: 13, ofdId: 4, fdFlags: 0 }],
    ofdSlots: [{ ofdId: 4, slot: freshSlot }],
  });

  expect(currentOfdGeneration(memory, 4)).toBe(9n);
});
