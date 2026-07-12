import { expect, test } from "vite-plus/test";
import {
  appendPendingExitedThreadForNextStep,
  appendPendingAuthoritativeFdForNextStep,
  recordPostedFdEntriesForNextStatus,
  recordPostedOfdSlotsForNextStatus,
  takeChangedFdEntriesSincePosted,
  takeChangedFdEntriesWithBaselineSincePosted,
  takeChangedOfdSlotsSincePosted,
  takePendingAuthoritativeFdEntriesForNextStep,
  takePendingExitedThreadsForNextStep,
} from "../../../src/worker/thread-step-payload.js";
import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_OFFSET_OFFSET,
  OFD_TYPE_OFFSET,
} from "../../../src/layout/ofd.js";
import type { ThreadWorkerState } from "../../../src/worker/types.js";

test("exited-thread publications are deduplicated and consumed once", () => {
  const thread = {} as ThreadWorkerState;
  appendPendingExitedThreadForNextStep(thread, { vmPtr: 0x1234, tid: 2 });
  appendPendingExitedThreadForNextStep(thread, { vmPtr: 0x1234, tid: 2 });
  appendPendingExitedThreadForNextStep(thread, { vmPtr: 0x5678, tid: 3 });

  expect(takePendingExitedThreadsForNextStep(thread)).toEqual([
    { vmPtr: 0x1234, tid: 2 },
    { vmPtr: 0x5678, tid: 3 },
  ]);
  expect(takePendingExitedThreadsForNextStep(thread)).toEqual([]);
});

test("posted FD baseline treats generation as descriptor incarnation state", () => {
  const thread = {} as ThreadWorkerState;
  recordPostedFdEntriesForNextStatus(thread, [
    { fd: 8, ofdId: 9, fdFlags: 1, generation: 20 },
    { fd: 9, ofdId: 10, fdFlags: 1, generation: 21 },
  ]);

  expect(takeChangedFdEntriesSincePosted(thread, [
    { fd: 8, ofdId: 9, fdFlags: 1, generation: 200 },
    { fd: 9, ofdId: 0, fdFlags: 0, generation: 22 },
  ])).toEqual(new Set([8, 9]));
  expect(takeChangedFdEntriesSincePosted(thread, [
    { fd: 8, ofdId: 9, fdFlags: 1, generation: 201 },
    { fd: 9, ofdId: 0, fdFlags: 0, generation: 23 },
  ])).toEqual(new Set([8, 9]));
});

test("status without FD payload preserves the posted baseline", () => {
  const thread = {} as ThreadWorkerState;
  recordPostedFdEntriesForNextStatus(thread, [
    { fd: 9, ofdId: 10, fdFlags: 1, generation: 21 },
  ]);

  expect(takeChangedFdEntriesSincePosted(thread, undefined)).toEqual(new Set());
  expect(takeChangedFdEntriesSincePosted(thread, [
    { fd: 9, ofdId: 10, fdFlags: 1, generation: 99 },
  ])).toEqual(new Set([9]));
});

test("changed FD evidence retains the exact posted binding", () => {
  const thread = {} as ThreadWorkerState;
  const posted = { fd: 3, ofdId: 8, fdFlags: 1, generation: 12 };
  recordPostedFdEntriesForNextStatus(thread, [posted]);

  const changes = takeChangedFdEntriesWithBaselineSincePosted(thread, [
    { fd: 3, ofdId: 0, fdFlags: 0, generation: 13 },
  ]);

  expect(changes?.authoritativeFds).toEqual(new Set([3]));
  expect(changes?.postedFdEntries.get(3)).toEqual(posted);
});

test("next-step fd authority strips generation only from selected descriptors", () => {
  const thread = {} as ThreadWorkerState;
  appendPendingAuthoritativeFdForNextStep(thread, 6);

  expect(takePendingAuthoritativeFdEntriesForNextStep(thread, [
    { fd: 5, ofdId: 8, fdFlags: 0, generation: 12 },
    { fd: 6, ofdId: 7, fdFlags: 1, generation: 2 },
  ])).toEqual([
    { fd: 5, ofdId: 8, fdFlags: 0, generation: 12 },
    { fd: 6, ofdId: 7, fdFlags: 1 },
  ]);

  expect(takePendingAuthoritativeFdEntriesForNextStep(thread, [
    { fd: 6, ofdId: 7, fdFlags: 1, generation: 2 },
  ])).toEqual([
    { fd: 6, ofdId: 7, fdFlags: 1, generation: 2 },
  ]);
});

test("next-step fd authority preserves a same-ProcessState monotonic generation", () => {
  const thread = {} as ThreadWorkerState;
  appendPendingAuthoritativeFdForNextStep(thread, 6);

  expect(takePendingAuthoritativeFdEntriesForNextStep(thread, [
    { fd: 6, ofdId: 7, fdFlags: 1, generation: 12, monotonicGeneration: true },
  ])).toEqual([
    { fd: 6, ofdId: 7, fdFlags: 1, generation: 12, monotonicGeneration: true },
  ]);
});

function makeOfdSlot(ino: number, offset: bigint): Uint8Array {
  const slot = new Uint8Array(32);
  const view = new DataView(slot.buffer);
  view.setInt32(OFD_TYPE_OFFSET, 1, true);
  view.setInt32(OFD_HOST_FD_OFFSET, -1, true);
  view.setUint32(OFD_INO_OFFSET, ino, true);
  view.setBigInt64(OFD_OFFSET_OFFSET, offset, true);
  return slot;
}

test("posted OFD baseline identifies only slots changed by that worker step", () => {
  const thread = {} as ThreadWorkerState;
  recordPostedOfdSlotsForNextStatus(thread, [
    { ofdId: 4, slot: makeOfdSlot(40, 0n) },
    { ofdId: 5, slot: makeOfdSlot(50, 19n) },
  ]);

  expect(takeChangedOfdSlotsSincePosted(thread, [
    { ofdId: 4, slot: makeOfdSlot(40, 7n) },
    { ofdId: 5, slot: makeOfdSlot(50, 19n) },
  ])).toEqual(new Set([4]));
  expect(takeChangedOfdSlotsSincePosted(thread, [])).toEqual(new Set());
});

test("posted OFD baseline identifies identity changes made by that worker step", () => {
  const thread = {} as ThreadWorkerState;
  recordPostedOfdSlotsForNextStatus(thread, [
    { ofdId: 4, slot: makeOfdSlot(40, 0n) },
  ]);

  expect(takeChangedOfdSlotsSincePosted(thread, [
    { ofdId: 4, slot: makeOfdSlot(41, 0n) },
  ])).toEqual(new Set([4]));
});

test("OFD baseline rolls forward across multiple statuses from one worker step", () => {
  const thread = {} as ThreadWorkerState;
  recordPostedOfdSlotsForNextStatus(thread, [
    { ofdId: 4, slot: makeOfdSlot(40, 0n) },
  ]);

  expect(takeChangedOfdSlotsSincePosted(thread, [
    { ofdId: 4, slot: makeOfdSlot(41, 0n) },
  ])).toEqual(new Set([4]));
  expect(takeChangedOfdSlotsSincePosted(thread, [
    { ofdId: 4, slot: makeOfdSlot(41, 7n) },
  ])).toEqual(new Set([4]));
  expect(takeChangedOfdSlotsSincePosted(thread, [
    { ofdId: 4, slot: makeOfdSlot(41, 7n) },
  ])).toEqual(new Set());
});

test("status without OFD payload preserves the posted baseline", () => {
  const thread = {} as ThreadWorkerState;
  recordPostedOfdSlotsForNextStatus(thread, [
    { ofdId: 4, slot: makeOfdSlot(40, 0n) },
  ]);

  expect(takeChangedOfdSlotsSincePosted(thread, undefined)).toEqual(new Set());
  expect(takeChangedOfdSlotsSincePosted(thread, [
    { ofdId: 4, slot: makeOfdSlot(40, 0n) },
  ])).toEqual(new Set());
});
