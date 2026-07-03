import type { KernelRuntimeState } from "../abi.js";
import {
  RUNTIME_BRIDGE_DEBUG_STAGE_INDEX,
  RUNTIME_BRIDGE_DEBUG_VALUE_INDEX,
  RUNTIME_BRIDGE_EPOCH_INDEX,
  RUNTIME_BRIDGE_NOTIFY_INDEX,
  RUNTIME_STATE_FIELD_COUNT,
  RUNTIME_STATE_FIELD_OFFSETS,
  RUNTIME_STATE_HEADER_BYTES,
  RUNTIME_STATE_VERSION_INDEX,
} from "../layout/runtime-state.js";

export interface RuntimeStateBridge {
  notifySab: SharedArrayBuffer;
  stateSab: SharedArrayBuffer;
}

export function createRuntimeStateBridge(): RuntimeStateBridge {
  return {
    notifySab: new SharedArrayBuffer(16),
    stateSab: new SharedArrayBuffer(RUNTIME_STATE_HEADER_BYTES + RUNTIME_STATE_FIELD_COUNT * 8),
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

function canAccessField(view: DataView, offset: number): boolean {
  return offset + 4 <= view.byteLength;
}

function writeState(view: DataView, state: KernelRuntimeState): void {
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.heapBase, state.heapBase, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.heapPtr, state.heapPtr, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.ofdTablePtr, state.ofdTablePtr, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.memfsPtr, state.memfsPtr, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.memfsHeaderBytes, state.memfsHeaderBytes, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.memfsInodesBytes, state.memfsInodesBytes, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.memfsFirstChildBytes, state.memfsFirstChildBytes, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.memfsDirEntriesBytes, state.memfsDirEntriesBytes, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.memfsDataBytes, state.memfsDataBytes, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.memStatePtr, state.memStatePtr, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.epollTablePtr, state.epollTablePtr, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.socketTablePtr, state.socketTablePtr, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.socketUsed, state.socketUsed, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.pipeBase, state.pipeBase, true);
  view.setBigUint64(RUNTIME_STATE_FIELD_OFFSETS.pipeUsed, state.pipeUsed, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.netBufBase, state.netBufBase, true);
  view.setBigUint64(RUNTIME_STATE_FIELD_OFFSETS.netBufUsed, state.netBufUsed, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.sigSlotsPtr, state.sigSlotsPtr, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.processPendingPtr, state.processPendingPtr, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.processRuntimeStatePtr, state.processRuntimeStatePtr, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.timeStatePtr, state.timeStatePtr, true);
  if (canAccessField(view, RUNTIME_STATE_FIELD_OFFSETS.signalfdStatePtr)) {
    view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.signalfdStatePtr, state.signalfdStatePtr ?? 0, true);
  }
  if (!canAccessField(view, RUNTIME_STATE_FIELD_OFFSETS.socketStateBytes)) {
    return;
  }
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.ofdStatePtr, state.ofdStatePtr ?? 0, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.ofdStateBytes, state.ofdStateBytes ?? 0, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.ofdOffsetFloorPtr, state.ofdOffsetFloorPtr ?? 0, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.ofdOffsetFloorBytes, state.ofdOffsetFloorBytes ?? 0, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.socketStatePtr, state.socketStatePtr ?? 0, true);
  view.setUint32(RUNTIME_STATE_FIELD_OFFSETS.socketStateBytes, state.socketStateBytes ?? 0, true);
}

export function publishKernelRuntimeState(
  bridge: RuntimeStateBridge,
  state: KernelRuntimeState,
  wake = false,
): void {
  const version = Atomics.add(stateVersionView(bridge), RUNTIME_STATE_VERSION_INDEX, 1) + 1;
  void version;
  writeState(stateView(bridge), state);
  Atomics.add(stateVersionView(bridge), RUNTIME_STATE_VERSION_INDEX, 1);
  if (wake) {
    const notify = notifyView(bridge);
    Atomics.add(notify, RUNTIME_BRIDGE_NOTIFY_INDEX, 1);
    Atomics.notify(notify, RUNTIME_BRIDGE_NOTIFY_INDEX);
  }
}

export function wakeRuntimeBridge(bridge: RuntimeStateBridge): void {
  const notify = notifyView(bridge);
  Atomics.add(notify, RUNTIME_BRIDGE_NOTIFY_INDEX, 1);
  Atomics.notify(notify, RUNTIME_BRIDGE_NOTIFY_INDEX);
}

export function publishRuntimeBridgeEpoch(bridge: RuntimeStateBridge): number {
  const notify = notifyView(bridge);
  return Atomics.add(notify, RUNTIME_BRIDGE_EPOCH_INDEX, 1) + 1;
}

export function readPublishedRuntimeBridgeEpoch(bridge: RuntimeStateBridge): number {
  return Atomics.load(notifyView(bridge), RUNTIME_BRIDGE_EPOCH_INDEX);
}

export function publishRuntimeBridgeDebugStage(
  bridge: RuntimeStateBridge,
  stage: number,
  value = 0,
): void {
  const notify = notifyView(bridge);
  Atomics.store(notify, RUNTIME_BRIDGE_DEBUG_STAGE_INDEX, stage);
  Atomics.store(notify, RUNTIME_BRIDGE_DEBUG_VALUE_INDEX, value);
}

export function readRuntimeBridgeDebugStage(
  bridge: RuntimeStateBridge,
): { stage: number; value: number } {
  const notify = notifyView(bridge);
  return {
    stage: Atomics.load(notify, RUNTIME_BRIDGE_DEBUG_STAGE_INDEX),
    value: Atomics.load(notify, RUNTIME_BRIDGE_DEBUG_VALUE_INDEX),
  };
}

export function observeRuntimeBridgeEpoch(
  bridge: RuntimeStateBridge,
  minimumEpoch: number,
): void {
  if (minimumEpoch <= 0) {
    return;
  }
  const notify = notifyView(bridge);
  while (Atomics.load(notify, RUNTIME_BRIDGE_EPOCH_INDEX) < minimumEpoch) {
    Atomics.notify(notify, RUNTIME_BRIDGE_EPOCH_INDEX);
  }
}

export function observeCurrentRuntimeBridgeEpoch(bridge: RuntimeStateBridge): number {
  const epoch = readPublishedRuntimeBridgeEpoch(bridge);
  observeRuntimeBridgeEpoch(bridge, epoch);
  return epoch;
}

export function readPublishedKernelRuntimeState(bridge: RuntimeStateBridge): KernelRuntimeState {
  const versionView = stateVersionView(bridge);
  const view = stateView(bridge);

  for (;;) {
    const before = Atomics.load(versionView, RUNTIME_STATE_VERSION_INDEX);
    if ((before & 1) !== 0) {
      continue;
    }

    const state: KernelRuntimeState = {
      heapBase: view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.heapBase, true),
      heapPtr: view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.heapPtr, true),
      ofdTablePtr: view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.ofdTablePtr, true),
      memfsPtr: view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.memfsPtr, true),
      memfsHeaderBytes: view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.memfsHeaderBytes, true),
      memfsInodesBytes: view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.memfsInodesBytes, true),
      memfsFirstChildBytes: view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.memfsFirstChildBytes, true),
      memfsDirEntriesBytes: view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.memfsDirEntriesBytes, true),
      memfsDataBytes: view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.memfsDataBytes, true),
      memStatePtr: view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.memStatePtr, true),
      epollTablePtr: view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.epollTablePtr, true),
      socketTablePtr: view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.socketTablePtr, true),
      socketUsed: view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.socketUsed, true),
      pipeBase: view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.pipeBase, true),
      pipeUsed: view.getBigUint64(RUNTIME_STATE_FIELD_OFFSETS.pipeUsed, true),
      netBufBase: view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.netBufBase, true),
      netBufUsed: view.getBigUint64(RUNTIME_STATE_FIELD_OFFSETS.netBufUsed, true),
      sigSlotsPtr: view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.sigSlotsPtr, true),
      processPendingPtr: view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.processPendingPtr, true),
      processRuntimeStatePtr: view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.processRuntimeStatePtr, true),
      timeStatePtr: view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.timeStatePtr, true),
    };
    if (canAccessField(view, RUNTIME_STATE_FIELD_OFFSETS.socketStateBytes)) {
      const ofdStatePtr = view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.ofdStatePtr, true);
      const ofdStateBytes = view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.ofdStateBytes, true);
      const ofdOffsetFloorPtr = view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.ofdOffsetFloorPtr, true);
      const ofdOffsetFloorBytes = view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.ofdOffsetFloorBytes, true);
      const socketStatePtr = view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.socketStatePtr, true);
      const socketStateBytes = view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.socketStateBytes, true);
      if (ofdStatePtr > 0) {
        state.ofdStatePtr = ofdStatePtr;
      }
      if (ofdStateBytes > 0) {
        state.ofdStateBytes = ofdStateBytes;
      }
      if (ofdOffsetFloorPtr > 0) {
        state.ofdOffsetFloorPtr = ofdOffsetFloorPtr;
      }
      if (ofdOffsetFloorBytes > 0) {
        state.ofdOffsetFloorBytes = ofdOffsetFloorBytes;
      }
      if (socketStatePtr > 0) {
        state.socketStatePtr = socketStatePtr;
      }
      if (socketStateBytes > 0) {
        state.socketStateBytes = socketStateBytes;
      }
    }
    if (canAccessField(view, RUNTIME_STATE_FIELD_OFFSETS.signalfdStatePtr)) {
      const signalfdStatePtr = view.getUint32(RUNTIME_STATE_FIELD_OFFSETS.signalfdStatePtr, true);
      if (signalfdStatePtr > 0) {
        state.signalfdStatePtr = signalfdStatePtr;
      }
    }

    const after = Atomics.load(versionView, RUNTIME_STATE_VERSION_INDEX);
    if (before === after) {
      return state;
    }
  }
}
