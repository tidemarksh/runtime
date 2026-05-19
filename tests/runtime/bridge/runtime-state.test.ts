import { expect, test } from "vite-plus/test";
import {
  createRuntimeStateBridge,
  observeCurrentRuntimeBridgeEpoch,
  publishRuntimeBridgeEpoch,
  readPublishedRuntimeBridgeEpoch,
  observeRuntimeBridgeEpoch,
  publishKernelRuntimeState,
  readPublishedKernelRuntimeState,
} from "../../../src/bridge/runtime-state.js";

test("runtime bridge epoch publishes monotonically", () => {
  const bridge = createRuntimeStateBridge();

  expect(readPublishedRuntimeBridgeEpoch(bridge)).toBe(0);
  expect(publishRuntimeBridgeEpoch(bridge)).toBe(1);
  expect(readPublishedRuntimeBridgeEpoch(bridge)).toBe(1);
  expect(publishRuntimeBridgeEpoch(bridge)).toBe(2);
  observeRuntimeBridgeEpoch(bridge, 2);
  expect(readPublishedRuntimeBridgeEpoch(bridge)).toBe(2);
});

test("observeCurrentRuntimeBridgeEpoch returns the published epoch", () => {
  const bridge = createRuntimeStateBridge();

  publishRuntimeBridgeEpoch(bridge);
  publishRuntimeBridgeEpoch(bridge);

  expect(observeCurrentRuntimeBridgeEpoch(bridge)).toBe(2);
  expect(readPublishedRuntimeBridgeEpoch(bridge)).toBe(2);
});

test("runtime bridge state publication still round-trips with epoch slot present", () => {
  const bridge = createRuntimeStateBridge();
  const state = {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memfsHeaderBytes: 0,
    memfsInodesBytes: 0,
    memfsFirstChildBytes: 0,
    memfsDirEntriesBytes: 0,
    memfsDataBytes: 0,
    memStatePtr: 0x240000,
    epollTablePtr: 0x250000,
    socketTablePtr: 0x260000,
    socketUsed: 7,
    pipeBase: 0x270000,
    pipeUsed: 9n,
    netBufBase: 0x280000,
    netBufUsed: 11n,
    sigSlotsPtr: 0x290000,
    processPendingPtr: 0x2a0000,
    processRuntimeStatePtr: 0x2b0000,
    timeStatePtr: 0x2c0000,
  };

  publishKernelRuntimeState(bridge, state);

  expect(readPublishedKernelRuntimeState(bridge)).toEqual(state);
});
