import { expect, test } from "vite-plus/test";
import { appendPendingGuestMemoryWrites } from "../../../src/worker/guest-memory-writes.js";
import { snapshotPendingGuestMemoryWritesForNextStep } from "../../../src/worker/thread-step-payload.js";
import type { ThreadWorkerState } from "../../../src/worker/types.js";

function makeThread(): ThreadWorkerState {
  return {} as ThreadWorkerState;
}

test("appendPendingGuestMemoryWrites keeps more than thirty-two disjoint ranges", () => {
  const thread = makeThread();
  const writes = Array.from({ length: 40 }, (_value, index) => ({
    guestPtr: 0x1000 + index * 0x1000,
    bytes: Uint8Array.from([index]),
  }));

  appendPendingGuestMemoryWrites(thread, writes);

  const pendingWrites = snapshotPendingGuestMemoryWritesForNextStep(thread);
  expect(pendingWrites).toHaveLength(40);
  expect(pendingWrites[0]).toEqual({
    guestPtr: 0x1000,
    bytes: Uint8Array.from([0]),
  });
  expect(pendingWrites[39]).toEqual({
    guestPtr: 0x1000 + 39 * 0x1000,
    bytes: Uint8Array.from([39]),
  });
});

test("appendPendingGuestMemoryWrites coalesces overlapping ranges with latest bytes", () => {
  const thread = makeThread();

  appendPendingGuestMemoryWrites(thread, [
    { guestPtr: 0x1000, bytes: Uint8Array.from([1, 2, 3, 4]) },
    { guestPtr: 0x1002, bytes: Uint8Array.from([9, 8, 7]) },
  ]);

  expect(snapshotPendingGuestMemoryWritesForNextStep(thread)).toEqual([
    { guestPtr: 0x1000, bytes: Uint8Array.from([1, 2, 9, 8, 7]) },
  ]);
});
