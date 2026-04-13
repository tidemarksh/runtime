import type { KernelRuntimeState } from "../abi.js";

const NOTIFY_INDEX = 0;
const EPOCH_INDEX = 1;
const STATE_VERSION_INDEX = 0;
const STATE_HEADER_BYTES = 8;
const STATE_FIELD_COUNT = 16;

const FIELD_OFFSETS = {
  heapBase: STATE_HEADER_BYTES + 0,
  heapPtr: STATE_HEADER_BYTES + 8,
  ofdTablePtr: STATE_HEADER_BYTES + 16,
  memfsPtr: STATE_HEADER_BYTES + 24,
  memStatePtr: STATE_HEADER_BYTES + 32,
  epollTablePtr: STATE_HEADER_BYTES + 40,
  socketTablePtr: STATE_HEADER_BYTES + 48,
  socketUsed: STATE_HEADER_BYTES + 56,
  pipeBase: STATE_HEADER_BYTES + 64,
  pipeUsed: STATE_HEADER_BYTES + 72,
  netBufBase: STATE_HEADER_BYTES + 80,
  netBufUsed: STATE_HEADER_BYTES + 88,
  sigSlotsPtr: STATE_HEADER_BYTES + 96,
  processPendingPtr: STATE_HEADER_BYTES + 104,
  processRuntimeStatePtr: STATE_HEADER_BYTES + 112,
  timeStatePtr: STATE_HEADER_BYTES + 120,
} as const;

export interface RuntimeStateBridge {
  notifySab: SharedArrayBuffer;
  stateSab: SharedArrayBuffer;
}

export function createRuntimeStateBridge(): RuntimeStateBridge {
  return {
    notifySab: new SharedArrayBuffer(8),
    stateSab: new SharedArrayBuffer(STATE_HEADER_BYTES + STATE_FIELD_COUNT * 8),
  };
}

function notifyView(bridge: RuntimeStateBridge): Int32Array {
  return new Int32Array(bridge.notifySab);
}

function stateView(bridge: RuntimeStateBridge): DataView {
  return new DataView(bridge.stateSab);
}

function stateVersionView(bridge: RuntimeStateBridge): Int32Array {
  return new Int32Array(bridge.stateSab, 0, 2);
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
  const version = Atomics.add(stateVersionView(bridge), STATE_VERSION_INDEX, 1) + 1;
  void version;
  writeState(stateView(bridge), state);
  Atomics.add(stateVersionView(bridge), STATE_VERSION_INDEX, 1);
  if (wake) {
    const notify = notifyView(bridge);
    Atomics.add(notify, NOTIFY_INDEX, 1);
    Atomics.notify(notify, NOTIFY_INDEX);
  }
}

export function wakeRuntimeBridge(bridge: RuntimeStateBridge): void {
  const notify = notifyView(bridge);
  Atomics.add(notify, NOTIFY_INDEX, 1);
  Atomics.notify(notify, NOTIFY_INDEX);
}

export function publishRuntimeBridgeEpoch(bridge: RuntimeStateBridge): number {
  const notify = notifyView(bridge);
  return Atomics.add(notify, EPOCH_INDEX, 1) + 1;
}

export function readPublishedRuntimeBridgeEpoch(bridge: RuntimeStateBridge): number {
  return Atomics.load(notifyView(bridge), EPOCH_INDEX);
}

export function observeRuntimeBridgeEpoch(
  bridge: RuntimeStateBridge,
  minimumEpoch: number,
): void {
  if (minimumEpoch <= 0) {
    return;
  }
  const notify = notifyView(bridge);
  while (Atomics.load(notify, EPOCH_INDEX) < minimumEpoch) {
    Atomics.notify(notify, EPOCH_INDEX);
  }
}

export function observeCurrentRuntimeBridgeEpoch(bridge: RuntimeStateBridge): number {
  const epoch = readPublishedRuntimeBridgeEpoch(bridge);
  observeRuntimeBridgeEpoch(bridge, epoch);
  return epoch;
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
  const versionView = stateVersionView(bridge);
  const view = stateView(bridge);

  for (;;) {
    const before = Atomics.load(versionView, STATE_VERSION_INDEX);
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

    const after = Atomics.load(versionView, STATE_VERSION_INDEX);
    if (before === after) {
      return state;
    }
  }
}
