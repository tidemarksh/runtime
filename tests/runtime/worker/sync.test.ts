import { expect, test } from "vite-plus/test";
import { adoptKernelState } from "../../../src/worker/sync.js";

test("adoptKernelState preserves canonical pointer fields while keeping heapPtr monotonic", () => {
  const current = {
    heapBase: 0x200000,
    heapPtr: 0x210080,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memfsHeaderBytes: 0x20,
    memfsInodesBytes: 0x4000,
    memfsFirstChildBytes: 0x800,
    memfsDirEntriesBytes: 0x2000,
    memfsDataBytes: 0x400000,
    memStatePtr: 0x240000,
    epollTablePtr: 0x250000,
    socketTablePtr: 0x260000,
    socketUsed: 1,
    pipeBase: 0x270000,
    pipeUsed: 0n,
    netBufBase: 0x280000,
    netBufUsed: 0n,
    sigSlotsPtr: 0xc00000,
    processPendingPtr: 0xc10000,
    processRuntimeStatePtr: 0xc20000,
    timeStatePtr: 0xc30000,
  };
  const incoming = {
    ...current,
    heapBase: 0x300000,
    heapPtr: 0x210040,
    ofdTablePtr: 0x320000,
    memfsPtr: 0x330000,
    memfsHeaderBytes: 0x40,
    memfsInodesBytes: 0x8000,
    memfsFirstChildBytes: 0x1000,
    memfsDirEntriesBytes: 0x4000,
    memfsDataBytes: 0x800000,
    memStatePtr: 0x340000,
    epollTablePtr: 0x350000,
    socketTablePtr: 0x360000,
    socketUsed: 3,
    pipeBase: 0x370000,
    pipeUsed: 5n,
    netBufBase: 0x380000,
    netBufUsed: 7n,
    sigSlotsPtr: 0xd00000,
    processPendingPtr: 0xd10000,
    processRuntimeStatePtr: 0xd20000,
    timeStatePtr: 0xd30000,
  };

  expect(adoptKernelState(current, incoming)).toEqual({
    ...current,
    heapBase: incoming.heapBase,
    heapPtr: current.heapPtr,
    socketUsed: incoming.socketUsed,
    pipeUsed: incoming.pipeUsed,
    netBufUsed: incoming.netBufUsed,
  });
});
