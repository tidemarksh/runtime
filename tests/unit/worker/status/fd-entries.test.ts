import { expect, test } from "vite-plus/test";
import { applyStatusFdEntries } from "../../../../src/worker/message-handler/status/fd-entries.js";
import { makeHandle, makeKernel, makeStatusMessage } from "./helpers.js";

test("status fd entries restore valid descriptor bindings through the status owner", () => {
  const fdEntries = new Map<number, { ofdId: number; fdFlags: number }>();
  const handle = makeHandle({
    kernel: makeKernel({
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number, fdFlags: number) => {
        fdEntries.set(fd, { ofdId, fdFlags });
      },
    }),
  });

  applyStatusFdEntries(handle as never, makeStatusMessage({
    fdEntries: [
      { fd: 3, ofdId: 5, fdFlags: 1 },
      { fd: 99, ofdId: 6, fdFlags: 0 },
    ],
  }) as never);

  expect(fdEntries).toEqual(new Map([[3, { ofdId: 5, fdFlags: 1 }]]));
});

test("status fd entries preserve authoritative runtime network bindings", () => {
  const fdEntries = new Map<number, number>([[4, 11]]);
  const handle = makeHandle({
    networkConnections: new Map([[4, { close: () => undefined }]]),
    networkConnectionOfds: new Map([[4, 11]]),
    kernel: makeKernel({
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number) => {
        fdEntries.set(fd, ofdId);
      },
    }),
  });

  applyStatusFdEntries(handle as never, makeStatusMessage({
    fdEntries: [{ fd: 4, ofdId: 12, fdFlags: 0 }],
  }) as never);

  expect(fdEntries.get(4)).toBe(11);
});
