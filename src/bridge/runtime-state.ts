import type { KernelRuntimeState } from "../abi.js";

const NOTIFY_INDEX = 0;
const VERSION_INDEX = 1;
const STATE_FIELD_COUNT = 16;

const FIELD_OFFSETS = {
  heapBase: 0,
  heapPtr: 8,
  ofdTablePtr: 16,
  memfsPtr: 24,
  memStatePtr: 32,
  epollTablePtr: 40,
  socketTablePtr: 48,
  socketUsed: 56,
  pipeBase: 64,
  pipeUsed: 72,
  netBufBase: 80,
  netBufUsed: 88,
  sigSlotsPtr: 96,
  processPendingPtr: 104,
  processRuntimeStatePtr: 112,
  timeStatePtr: 120,
} as const;

export interface RuntimeStateBridge {
  notifySab: SharedArrayBuffer;
  stateSab: SharedArrayBuffer;
}

export function createRuntimeStateBridge(): RuntimeStateBridge {
  return {
    notifySab: new SharedArrayBuffer(8),
    stateSab: new SharedArrayBuffer(STATE_FIELD_COUNT * 8),
  };
}

function notifyView(bridge: RuntimeStateBridge): Int32Array {
  return new Int32Array(bridge.notifySab);
}

function stateView(bridge: RuntimeStateBridge): DataView {
  return new DataView(bridge.stateSab);
}

function writeState(view: DataView, state: KernelRuntimeState): void {
  view.setUint32(FIELD_OFFSETS.heapBase, state.heapBase, true);
  view.setUint32(FIELD_OFFSETS.heapPtr, state.heapPtr, true);
  view.setUint32(FIELD_OFFSETS.ofdTablePtr, state.ofdTablePtr, true);
  view.setUint32(FIELD_OFFSETS.memfsPtr, state.memfsPtr, true);
  view.setUint32(FIELD_OFFSETS.memStatePtr, state.memStatePtr, true);
  view.setUint32(FIELD_OFFSETS.epollTablePtr, state.epollTablePtr, true);
  view.setUint32(FIELD_OFFSETS.socketTablePtr, state.socketTablePtr, true);
  view.setUint32(FIELD_OFFSETS.socketUsed, state.socketUsed, true);
  view.setUint32(FIELD_OFFSETS.pipeBase, state.pipeBase, true);
  view.setBigUint64(FIELD_OFFSETS.pipeUsed, state.pipeUsed, true);
  view.setUint32(FIELD_OFFSETS.netBufBase, state.netBufBase, true);
  view.setBigUint64(FIELD_OFFSETS.netBufUsed, state.netBufUsed, true);
  view.setUint32(FIELD_OFFSETS.sigSlotsPtr, state.sigSlotsPtr, true);
  view.setUint32(FIELD_OFFSETS.processPendingPtr, state.processPendingPtr, true);
  view.setUint32(FIELD_OFFSETS.processRuntimeStatePtr, state.processRuntimeStatePtr, true);
  view.setUint32(FIELD_OFFSETS.timeStatePtr, state.timeStatePtr, true);
}

export function publishKernelRuntimeState(
  bridge: RuntimeStateBridge,
  state: KernelRuntimeState,
  wake = false,
): void {
  const notify = notifyView(bridge);
  const version = Atomics.add(notify, VERSION_INDEX, 1) + 1;
  void version;
  writeState(stateView(bridge), state);
  Atomics.add(notify, VERSION_INDEX, 1);
  if (wake) {
    Atomics.add(notify, NOTIFY_INDEX, 1);
    Atomics.notify(notify, NOTIFY_INDEX);
  }
}

export function wakeRuntimeBridge(bridge: RuntimeStateBridge): void {
  const notify = notifyView(bridge);
  Atomics.add(notify, NOTIFY_INDEX, 1);
  Atomics.notify(notify, NOTIFY_INDEX);
}

export function waitForRuntimeBridgeWake(
  bridge: RuntimeStateBridge,
  timeoutMs?: number,
): "ok" | "not-equal" | "timed-out" {
  const notify = notifyView(bridge);
  const expected = Atomics.load(notify, NOTIFY_INDEX);
  if (timeoutMs === undefined) {
    return Atomics.wait(notify, NOTIFY_INDEX, expected);
  } else {
    return Atomics.wait(notify, NOTIFY_INDEX, expected, timeoutMs);
  }
}

export function readPublishedKernelRuntimeState(bridge: RuntimeStateBridge): KernelRuntimeState {
  const notify = notifyView(bridge);
  const view = stateView(bridge);

  for (;;) {
    const before = Atomics.load(notify, VERSION_INDEX);
    if ((before & 1) !== 0) {
      continue;
    }

    const state: KernelRuntimeState = {
      heapBase: view.getUint32(FIELD_OFFSETS.heapBase, true),
      heapPtr: view.getUint32(FIELD_OFFSETS.heapPtr, true),
      ofdTablePtr: view.getUint32(FIELD_OFFSETS.ofdTablePtr, true),
      memfsPtr: view.getUint32(FIELD_OFFSETS.memfsPtr, true),
      memStatePtr: view.getUint32(FIELD_OFFSETS.memStatePtr, true),
      epollTablePtr: view.getUint32(FIELD_OFFSETS.epollTablePtr, true),
      socketTablePtr: view.getUint32(FIELD_OFFSETS.socketTablePtr, true),
      socketUsed: view.getUint32(FIELD_OFFSETS.socketUsed, true),
      pipeBase: view.getUint32(FIELD_OFFSETS.pipeBase, true),
      pipeUsed: view.getBigUint64(FIELD_OFFSETS.pipeUsed, true),
      netBufBase: view.getUint32(FIELD_OFFSETS.netBufBase, true),
      netBufUsed: view.getBigUint64(FIELD_OFFSETS.netBufUsed, true),
      sigSlotsPtr: view.getUint32(FIELD_OFFSETS.sigSlotsPtr, true),
      processPendingPtr: view.getUint32(FIELD_OFFSETS.processPendingPtr, true),
      processRuntimeStatePtr: view.getUint32(FIELD_OFFSETS.processRuntimeStatePtr, true),
      timeStatePtr: view.getUint32(FIELD_OFFSETS.timeStatePtr, true),
    };

    const after = Atomics.load(notify, VERSION_INDEX);
    if (before === after) {
      return state;
    }
  }
}
