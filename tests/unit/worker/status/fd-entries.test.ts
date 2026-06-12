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

test("status fd entries mirror same-process thread descriptors to the owner fd table", () => {
  const calls: Array<{ vmPtr: number; fd: number; ofdId: number; fdFlags: number }> = [];
  const handle = makeHandle({
    pid: 7,
    vmPtr: 0x1234,
    kernel: makeKernel({
      abi_max_fds: () => 16,
      process_current_pid: (vmPtr: number) => (vmPtr === 0x1234 || vmPtr === 0x5678 ? 7 : 0),
      process_set_fd_entry: (vmPtr: number, fd: number, ofdId: number, fdFlags: number) => {
        calls.push({ vmPtr, fd, ofdId, fdFlags });
      },
    }),
  });

  applyStatusFdEntries(handle as never, makeStatusMessage({
    vmPtr: 0x5678,
    fdEntries: [{ fd: 9, ofdId: 10, fdFlags: 0 }],
  }) as never);

  expect(calls).toEqual([
    { vmPtr: 0x5678, fd: 9, ofdId: 10, fdFlags: 0 },
    { vmPtr: 0x1234, fd: 9, ofdId: 10, fdFlags: 0 },
  ]);
});

test("status fd entries do not mirror across a different process owner", () => {
  const calls: Array<{ vmPtr: number; fd: number; ofdId: number }> = [];
  const handle = makeHandle({
    pid: 7,
    vmPtr: 0x1234,
    kernel: makeKernel({
      abi_max_fds: () => 16,
      process_current_pid: (vmPtr: number) => (vmPtr === 0x1234 ? 7 : 8),
      process_set_fd_entry: (vmPtr: number, fd: number, ofdId: number) => {
        calls.push({ vmPtr, fd, ofdId });
      },
    }),
  });

  applyStatusFdEntries(handle as never, makeStatusMessage({
    vmPtr: 0x5678,
    fdEntries: [{ fd: 9, ofdId: 10, fdFlags: 0 }],
  }) as never);

  expect(calls).toEqual([{ vmPtr: 0x5678, fd: 9, ofdId: 10 }]);
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

test("status fd entries do not clear a live regular file from a stale non-closing snapshot", () => {
  const fdEntries = new Map<number, number>([[5, 7]]);
  const handle = makeHandle({
    kernel: makeKernel({
      abi_sys_write: () => 64n,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fdEntries.get(fd) ?? 0,
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number) => {
        if (ofdId === 0) {
          fdEntries.delete(fd);
        } else {
          fdEntries.set(fd, ofdId);
        }
      },
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 7 ? 1 : 0,
      kernel_ofd_refcount: (ofdId: number) => ofdId === 7 ? 1 : 0,
    }),
  });

  applyStatusFdEntries(handle as never, makeStatusMessage({
    syscallNr: 64n,
    fdEntries: [{ fd: 5, ofdId: 0, fdFlags: 0 }],
  }) as never);

  expect(fdEntries.get(5)).toBe(7);
});

test("status fd entries allow close to clear a regular file descriptor", () => {
  const fdEntries = new Map<number, number>([[5, 7]]);
  const handle = makeHandle({
    kernel: makeKernel({
      abi_sys_close: () => 57n,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fdEntries.get(fd) ?? 0,
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number) => {
        if (ofdId === 0) {
          fdEntries.delete(fd);
        } else {
          fdEntries.set(fd, ofdId);
        }
      },
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 7 ? 1 : 0,
      kernel_ofd_refcount: (ofdId: number) => ofdId === 7 ? 1 : 0,
    }),
  });

  applyStatusFdEntries(handle as never, makeStatusMessage({
    syscallNr: 57n,
    fdEntries: [{ fd: 5, ofdId: 0, fdFlags: 0 }],
  }) as never);

  expect(fdEntries.has(5)).toBe(false);
});
