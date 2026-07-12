import { expect, test } from "vite-plus/test";
import {
  fdAuthorityEntriesChangedBetweenSnapshots,
  fdAuthorityEntriesMutatedInBothSnapshots,
  fdEntriesDifferingFromSnapshot,
  transferVforkExecveWorkers,
} from "../../../../src/worker/fork/vfork-execve.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

test("vfork execve publishes FD authority after workers reach their final owners", () => {
  const parentWorker = { name: "parent-worker" } as unknown as Worker;
  const childWorker = { name: "child-worker" } as unknown as Worker;
  const parentHandle = { pid: 1, worker: parentWorker } as ProcessHandle;
  const childHandle = { pid: 2, worker: childWorker } as ProcessHandle;
  const ownerByWorker = new Map<Worker, ProcessHandle>([[parentWorker, parentHandle]]);
  const events: string[] = [];

  transferVforkExecveWorkers(
    {
      attachThreadWorker: (handle, request) => {
        if (request.transferFrom) {
          expect(ownerByWorker.get(request.worker)).toBe(request.transferFrom);
          ownerByWorker.delete(request.worker);
        }
        ownerByWorker.set(request.worker, handle);
        events.push(`attach:${handle.pid}`);
      },
      markCanonicalFdEntriesForNextStep: (handle, entries) => {
        const ownedWorkers = [...ownerByWorker.entries()]
          .filter(([, owner]) => owner === handle)
          .map(([worker]) => worker);
        expect(ownedWorkers).toEqual([
          handle === parentHandle ? childWorker : parentWorker,
        ]);
        events.push(`mark:${handle.pid}:${entries.map((entry) => entry.fd).join(",")}`);
      },
      notifyForkProgress: (handle) => events.push(`notify:${handle.pid}`),
    },
    parentHandle,
    childHandle,
    parentWorker,
    childWorker,
    0x1234,
    [{ fd: 4 }],
    [{ fd: 1 }, { fd: 2 }],
  );

  expect(parentHandle.worker).toBe(childWorker);
  expect(events).toEqual([
    "attach:2",
    "attach:1",
    "mark:1:4",
    "mark:2:1,2",
    "notify:1",
  ]);
});

test("vfork execve limits transferred FD authority to entries changed by exec", () => {
  const parent = [
    { fd: 0, ofdId: 1, fdFlags: 0 },
    { fd: 1, ofdId: 2, fdFlags: 0 },
    { fd: 7, ofdId: 8, fdFlags: 1 },
    { fd: 20, ofdId: 21, fdFlags: 0 },
  ];
  const child = [
    { fd: 0, ofdId: 4, fdFlags: 0 },
    { fd: 1, ofdId: 2, fdFlags: 0 },
    { fd: 7, ofdId: 8, fdFlags: 1 },
  ];

  expect(fdEntriesDifferingFromSnapshot(parent, child).map((entry) => entry.fd)).toEqual([0, 20]);
  expect(fdEntriesDifferingFromSnapshot(child, parent).map((entry) => entry.fd)).toEqual([0]);
  expect(fdAuthorityEntriesChangedBetweenSnapshots(parent, child)).toEqual([{ fd: 0 }, { fd: 20 }]);
  expect(fdAuthorityEntriesMutatedInBothSnapshots(parent, child)).toEqual([{ fd: 0 }]);
});

test("vfork execve transfers FD authority when exec reuses the same OFD slot", () => {
  const parent = [{ fd: 0, ofdId: 4, fdFlags: 0, generation: 7 }];
  const child = [{ fd: 0, ofdId: 4, fdFlags: 0, generation: 9 }];

  expect(fdAuthorityEntriesChangedBetweenSnapshots(parent, child)).toEqual([{ fd: 0 }]);
  expect(fdAuthorityEntriesMutatedInBothSnapshots(parent, child)).toEqual([{ fd: 0 }]);
});
