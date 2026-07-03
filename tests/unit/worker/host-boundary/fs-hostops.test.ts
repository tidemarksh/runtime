import { expect, test } from "vite-plus/test";
import {
  appendPendingGuestMemoryWrites,
  formatGuestMemoryWriteWatchOverlap,
} from "../../../../src/worker/guest-memory-writes.js";
import {
  makeHostBoundaryThread,
} from "./helpers.js";

test("fs host boundary queues guest memory writes from supervisor-directed host work", () => {
  const thread = makeHostBoundaryThread(0x1234);

  appendPendingGuestMemoryWrites(thread, [
    { guestPtr: 0x1000, bytes: Uint8Array.from([1, 2, 3, 4]) },
    { guestPtr: 0x1002, bytes: Uint8Array.from([9, 8, 7]) },
    { guestPtr: 0x3000, bytes: Uint8Array.from([5]) },
  ]);

  expect(thread.pendingGuestMemoryWrites).toEqual([
    { guestPtr: 0x1000, bytes: Uint8Array.from([1, 2, 9, 8, 7]) },
    { guestPtr: 0x3000, bytes: Uint8Array.from([5]) },
  ]);
});

test("fs host boundary reports watched guest-memory write overlap diagnostics", () => {
  const overlap = formatGuestMemoryWriteWatchOverlap(
    "fs-hostop",
    [{ slot: 2, start: 0x1001, end: 0x1004 }],
    0x1000,
    Uint8Array.from([0xaa, 0xbb, 0xcc, 0xdd]),
  );

  expect(overlap).toBe("fs-hostop:slot=2:ptr=0x1000:len=4:overlap=0x1001..0x1004:sample=bbccdd");
});
