import type { RuntimeNetworkConnection } from "../bridge/network.js";
import type { ProcessHandle } from "./types.js";

export interface NetworkSocketSlotSnapshot {
  offset: number;
  data: Uint8Array;
}

export interface NetworkRecvPayloadWatermark {
  recvBufIdx: number;
  head: number;
  tail: number;
  capacity: number;
  readBytes?: number;
  availableBytes?: number;
  deliveredBytes?: number;
  data: Uint8Array;
}

interface NetworkSocketState {
  connections: Map<number, RuntimeNetworkConnection>;
  connectionOfds: Map<number, number>;
  ofdSlotWatermarks: Map<number, Uint8Array>;
  pendingConnectFds: Set<number>;
  closedFds: Set<number>;
  closedSocketSlots: Map<number, NetworkSocketSlotSnapshot>;
  socketSlotWatermarks: Map<number, NetworkSocketSlotSnapshot>;
  recvHeadWatermarks: Map<number, number>;
  recvTailWatermarks: Map<number, number>;
  recvPayloadWatermarks: Map<number, NetworkRecvPayloadWatermark>;
}

const networkSocketStates = new WeakMap<ProcessHandle, NetworkSocketState>();

function networkSocketState(handle: ProcessHandle): NetworkSocketState {
  let state = networkSocketStates.get(handle);
  if (!state) {
    state = {
      connections: new Map(),
      connectionOfds: new Map(),
      ofdSlotWatermarks: new Map(),
      pendingConnectFds: new Set(),
      closedFds: new Set(),
      closedSocketSlots: new Map(),
      socketSlotWatermarks: new Map(),
      recvHeadWatermarks: new Map(),
      recvTailWatermarks: new Map(),
      recvPayloadWatermarks: new Map(),
    };
    networkSocketStates.set(handle, state);
  }
  return state;
}

export function networkConnectionOfds(handle: ProcessHandle): Map<number, number> {
  return networkSocketState(handle).connectionOfds;
}

export function networkConnections(handle: ProcessHandle): Map<number, RuntimeNetworkConnection> {
  return networkSocketState(handle).connections;
}

export function networkOfdSlotWatermarks(handle: ProcessHandle): Map<number, Uint8Array> {
  return networkSocketState(handle).ofdSlotWatermarks;
}

export function networkPendingConnectFds(handle: ProcessHandle): Set<number> {
  return networkSocketState(handle).pendingConnectFds;
}

export function networkClosedFds(handle: ProcessHandle): Set<number> {
  return networkSocketState(handle).closedFds;
}

export function networkClosedSocketSlots(
  handle: ProcessHandle,
): Map<number, NetworkSocketSlotSnapshot> {
  return networkSocketState(handle).closedSocketSlots;
}

export function networkSocketSlotWatermarks(
  handle: ProcessHandle,
): Map<number, NetworkSocketSlotSnapshot> {
  return networkSocketState(handle).socketSlotWatermarks;
}

export function networkRecvHeadWatermarks(handle: ProcessHandle): Map<number, number> {
  return networkSocketState(handle).recvHeadWatermarks;
}

export function networkRecvTailWatermarks(handle: ProcessHandle): Map<number, number> {
  return networkSocketState(handle).recvTailWatermarks;
}

export function networkRecvPayloadWatermarks(
  handle: ProcessHandle,
): Map<number, NetworkRecvPayloadWatermark> {
  return networkSocketState(handle).recvPayloadWatermarks;
}

export function copyNetworkSocketState(target: ProcessHandle, source: ProcessHandle): void {
  const targetState = networkSocketState(target);
  const sourceState = networkSocketState(source);
  targetState.connections = new Map(sourceState.connections);
  targetState.connectionOfds = new Map(sourceState.connectionOfds);
  targetState.ofdSlotWatermarks = new Map(
    [...sourceState.ofdSlotWatermarks].map(([ofdId, slot]) => [ofdId, slot.slice()]),
  );
  targetState.pendingConnectFds = new Set(sourceState.pendingConnectFds);
  targetState.closedFds = new Set(sourceState.closedFds);
  targetState.closedSocketSlots = new Map(
    [...sourceState.closedSocketSlots].map(([fd, slot]) => [fd, {
      offset: slot.offset,
      data: slot.data.slice(),
    }]),
  );
  targetState.socketSlotWatermarks = new Map(
    [...sourceState.socketSlotWatermarks].map(([fd, slot]) => [fd, {
      offset: slot.offset,
      data: slot.data.slice(),
    }]),
  );
  targetState.recvHeadWatermarks = new Map(sourceState.recvHeadWatermarks);
  targetState.recvTailWatermarks = new Map(sourceState.recvTailWatermarks);
  targetState.recvPayloadWatermarks = new Map(
    [...sourceState.recvPayloadWatermarks].map(([fd, snapshot]) => [fd, {
      recvBufIdx: snapshot.recvBufIdx,
      head: snapshot.head,
      tail: snapshot.tail,
      capacity: snapshot.capacity,
      readBytes: snapshot.readBytes,
      availableBytes: snapshot.availableBytes,
      deliveredBytes: snapshot.deliveredBytes,
      data: snapshot.data.slice(),
    }]),
  );
}
