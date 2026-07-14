import { expect, test } from "vite-plus/test";
import {
  OFD_HOST_FD_OFFSET,
  OFD_INO_OFFSET,
  OFD_TYPE_OFFSET,
} from "../../../../src/layout/ofd.js";
import { addProcessWorker } from "../../../../src/worker/process-worker-state.js";
import {
  processInheritedParentOutput,
  setProcessInheritedParentOutput,
} from "../../../../src/worker/process-stdio-state.js";
import {
  applyStatusFdEntries,
  markStatusFdChangesForNextStep,
} from "../../../../src/worker/message-handler/status/fd-entries.js";
import { snapshotPendingAuthoritativeFdsForNextStep } from "../../../../src/worker/thread-step-payload.js";
import type { ThreadWorkerState } from "../../../../src/worker/types.js";
import { makeHandle, makeKernel, makeStatusMessage, seedNetworkSocketState } from "./helpers.js";

test("status fd entries restore valid descriptor bindings through the status owner", () => {
  const fdEntries = new Map<number, { ofdId: number; fdFlags: number }>();
  const handle = makeHandle({
    kernel: makeKernel({
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number, fdFlags: number) => {
        fdEntries.set(fd, { ofdId, fdFlags });
      },
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 5 ? 1 : 0),
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

test("status fd entries apply only descriptors changed by the reporting worker step", () => {
  const fdEntries = new Map<number, number>([
    [8, 9],
    [9, 10],
  ]);
  const calls: Array<{ fd: number; ofdId: number }> = [];
  const handle = makeHandle({
    kernel: makeKernel({
      abi_max_fds: () => 16,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fdEntries.get(fd) ?? 0,
      supervisor_process_ofd_for_fd: (_vmPtr: number, fd: number) => fdEntries.get(fd) ?? 0,
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number) => {
        calls.push({ fd, ofdId });
        fdEntries.set(fd, ofdId);
      },
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 9 || ofdId === 10 ? 6 : 0),
    }),
  });

  const changedFds = applyStatusFdEntries(handle as never, makeStatusMessage({
    fdEntries: [
      { fd: 8, ofdId: 0, fdFlags: 0 },
      { fd: 9, ofdId: 0, fdFlags: 0 },
    ],
  }) as never, new Set([9]));

  expect(fdEntries.get(8)).toBe(9);
  expect(fdEntries.get(9)).toBe(0);
  expect(calls).toEqual([{ fd: 9, ofdId: 0 }]);
  expect(changedFds).toEqual([9]);
});

test("authoritative status fd changes propagate after shared memory already reflects them", () => {
  const handle = makeHandle({
    kernel: makeKernel({
      abi_max_fds: () => 16,
      process_fd_ofd_id: () => 0,
      supervisor_process_ofd_for_fd: () => 0,
      process_fd_flags: () => 0,
      supervisor_process_fd_flags_for_fd: () => 0,
    }),
  });

  const changedFds = applyStatusFdEntries(handle as never, makeStatusMessage({
    fdEntries: [{ fd: 11, ofdId: 0, fdFlags: 0 }],
  }) as never, new Set([11]));

  expect(changedFds).toEqual([11]);
});

test("status fd output redirection invalidates fork-time inherited output", () => {
  const fdEntries = new Map<number, number>([[1, 2]]);
  const handle = makeHandle({
    kernel: makeKernel({
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fdEntries.get(fd) ?? 0,
      supervisor_process_ofd_for_fd: (_vmPtr: number, fd: number) => fdEntries.get(fd) ?? 0,
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number) => {
        fdEntries.set(fd, ofdId);
      },
      kernel_ofd_fd_type: (ofdId: number) => ofdId === 2 || ofdId === 7 ? 3 : 0,
    }),
  });
  setProcessInheritedParentOutput(handle as never, true);

  const changedFds = applyStatusFdEntries(handle as never, makeStatusMessage({
    fdEntries: [{ fd: 1, ofdId: 7, fdFlags: 0 }],
  }) as never);

  expect(changedFds).toEqual([1]);
  expect(processInheritedParentOutput(handle as never)).toBe(false);
});

test("authoritative stale close does not clear a descriptor reused after the posted step", () => {
  const fdEntries = new Map<number, { fdFlags: number; ofdId: number }>([
    [3, { fdFlags: 0, ofdId: 12 }],
  ]);
  const calls: Array<{ fd: number; ofdId: number }> = [];
  const handle = makeHandle({
    kernel: makeKernel({
      abi_max_fds: () => 16,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fdEntries.get(fd)?.ofdId ?? 0,
      process_fd_flags: (_vmPtr: number, fd: number) => fdEntries.get(fd)?.fdFlags ?? 0,
      supervisor_process_ofd_for_fd: (_vmPtr: number, fd: number) => fdEntries.get(fd)?.ofdId ?? 0,
      supervisor_process_fd_flags_for_fd: (_vmPtr: number, fd: number) => fdEntries.get(fd)?.fdFlags ?? 0,
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number) => {
        calls.push({ fd, ofdId });
      },
      kernel_ofd_fd_type: () => 1,
    }),
  });

  const changedFds = applyStatusFdEntries(
    handle as never,
    makeStatusMessage({ fdEntries: [{ fd: 3, ofdId: 0, fdFlags: 0 }] }) as never,
    new Set([3]),
    new Map([[3, { fd: 3, ofdId: 8, fdFlags: 0 }]]),
  );

  expect(calls).toEqual([]);
  expect(changedFds).toEqual([]);
});

test("authoritative stale close does not clear the same OFD after its fd generation advances", () => {
  const calls: Array<{ fd: number; ofdId: number }> = [];
  const handle = makeHandle({
    kernel: makeKernel({
      abi_max_fds: () => 16,
      process_fd_ofd_id: () => 8,
      process_fd_flags: () => 0,
      process_fd_generation: () => 12,
      supervisor_process_ofd_for_fd: () => 8,
      supervisor_process_fd_flags_for_fd: () => 0,
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number) => {
        calls.push({ fd, ofdId });
      },
      kernel_ofd_fd_type: () => 1,
    }),
  });

  const changedFds = applyStatusFdEntries(
    handle as never,
    makeStatusMessage({ fdEntries: [{ fd: 3, ofdId: 0, fdFlags: 0, generation: 11 }] }) as never,
    new Set([3]),
    new Map([[3, { fd: 3, ofdId: 8, fdFlags: 0, generation: 11 }]]),
  );

  expect(calls).toEqual([]);
  expect(changedFds).toEqual([]);
});

test("authoritative stale close rejects reuse of the same OFD id with a new identity", () => {
  const calls: Array<{ fd: number; ofdId: number }> = [];
  const handle = makeHandle({
    kernel: makeKernel({
      abi_max_fds: () => 32,
      process_fd_ofd_id: () => 25,
      process_fd_flags: () => 0,
      supervisor_process_ofd_for_fd: () => 25,
      supervisor_process_fd_flags_for_fd: () => 0,
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number) => {
        calls.push({ fd, ofdId });
      },
      kernel_ofd_fd_type: () => 1,
      kernel_ofd_host_fd: () => -1,
      kernel_ofd_ino: () => 16906,
    }),
  });
  const postedSlot = new Uint8Array(32);
  const postedView = new DataView(postedSlot.buffer);
  postedView.setInt32(OFD_TYPE_OFFSET, 1, true);
  postedView.setInt32(OFD_HOST_FD_OFFSET, -1, true);
  postedView.setUint32(OFD_INO_OFFSET, 16905, true);

  const changedFds = applyStatusFdEntries(
    handle as never,
    makeStatusMessage({ fdEntries: [{ fd: 12, ofdId: 0, fdFlags: 0 }] }) as never,
    new Set([12]),
    new Map([[12, { fd: 12, ofdId: 25, fdFlags: 0 }]]),
    new Map([[25, postedSlot]]),
  );

  expect(calls).toEqual([]);
  expect(changedFds).toEqual([]);
});

test("authoritative close propagates after the reported OFD slot is already cleared", () => {
  const calls: Array<{ fd: number; ofdId: number }> = [];
  const handle = makeHandle({
    kernel: makeKernel({
      abi_max_fds: () => 32,
      process_fd_ofd_id: () => 17,
      process_fd_flags: () => 0,
      supervisor_process_ofd_for_fd: () => 17,
      supervisor_process_fd_flags_for_fd: () => 0,
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number) => {
        calls.push({ fd, ofdId });
      },
      kernel_ofd_fd_type: () => 0,
      kernel_ofd_host_fd: () => 0,
      kernel_ofd_ino: () => 0,
    }),
  });
  const postedSlot = new Uint8Array(32);
  const postedView = new DataView(postedSlot.buffer);
  postedView.setInt32(OFD_TYPE_OFFSET, 6, true);
  postedView.setInt32(OFD_HOST_FD_OFFSET, -2, true);
  postedView.setUint32(OFD_INO_OFFSET, 1, true);

  const changedFds = applyStatusFdEntries(
    handle as never,
    makeStatusMessage({ fdEntries: [{ fd: 7, ofdId: 0, fdFlags: 0 }] }) as never,
    new Set([7]),
    new Map([[7, { fd: 7, ofdId: 17, fdFlags: 0 }]]),
    new Map([[17, postedSlot]]),
  );

  expect(calls).toEqual([{ fd: 7, ofdId: 0 }]);
  expect(changedFds).toEqual([7]);
});

test("status fd entries reject bindings whose OFD slot is absent", () => {
  const calls: Array<{ fd: number; ofdId: number; fdFlags: number }> = [];
  const handle = makeHandle({
    kernel: makeKernel({
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number, fdFlags: number) => {
        calls.push({ fd, ofdId, fdFlags });
      },
      kernel_ofd_fd_type: () => 0,
    }),
  });

  applyStatusFdEntries(handle as never, makeStatusMessage({
    fdEntries: [{ fd: 2, ofdId: 3, fdFlags: 0 }],
  }) as never);

  expect(calls).toEqual([]);
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
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 10 ? 1 : 0),
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
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 10 ? 1 : 0),
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
    kernel: makeKernel({
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number) => {
        fdEntries.set(fd, ofdId);
      },
    }),
  });
  seedNetworkSocketState(handle, {
    connections: [[4, { close: () => undefined }]],
    connectionOfds: [[4, 11]],
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

test("status fd entries do not let a stale close clear a reused live descriptor", () => {
  const fdEntries = new Map<number, number>([[3, 4]]);
  const generations = new Map<number, number>([[3, 12]]);
  const setCalls: Array<{ fd: number; ofdId: number; fdFlags: number }> = [];
  const handle = makeHandle({
    kernel: makeKernel({
      abi_max_fds: () => 16,
      abi_sys_close: () => 57n,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fdEntries.get(fd) ?? 0,
      process_fd_flags: () => 0,
      process_fd_generation: (_vmPtr: number, fd: number) => generations.get(fd) ?? 0,
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number, fdFlags: number) => {
        setCalls.push({ fd, ofdId, fdFlags });
        fdEntries.set(fd, ofdId);
      },
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 4 ? 1 : 0),
      kernel_ofd_refcount: (ofdId: number) => (ofdId === 4 ? 1 : 0),
    }),
  });

  applyStatusFdEntries(handle as never, makeStatusMessage({
    syscallNr: 57n,
    fdEntries: [{ fd: 3, ofdId: 0, fdFlags: 0, generation: 11 }],
  }) as never);

  expect(fdEntries.get(3)).toBe(4);
  expect(setCalls).toEqual([]);
});

test("status fd entries allow fd-creating snapshots across local generations", () => {
  const fdEntries = new Map<number, number>([[4, 0]]);
  const generations = new Map<number, number>([[4, 3]]);
  const setCalls: Array<{ fd: number; ofdId: number; fdFlags: number }> = [];
  const handle = makeHandle({
    kernel: makeKernel({
      abi_max_fds: () => 16,
      abi_sys_openat: () => 56n,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fdEntries.get(fd) ?? 0,
      process_fd_flags: () => 0,
      process_fd_generation: (_vmPtr: number, fd: number) => generations.get(fd) ?? 0,
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number, fdFlags: number) => {
        setCalls.push({ fd, ofdId, fdFlags });
        fdEntries.set(fd, ofdId);
        generations.set(fd, (generations.get(fd) ?? 0) + 1);
      },
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 5 ? 1 : 0),
    }),
  });

  const authoritativeFds = applyStatusFdEntries(handle as never, makeStatusMessage({
    syscallNr: 56n,
    fdEntries: [{ fd: 4, ofdId: 5, fdFlags: 0, generation: 2 }],
  }) as never);

  expect(fdEntries.get(4)).toBe(5);
  expect(setCalls).toEqual([{ fd: 4, ofdId: 5, fdFlags: 0 }]);
  expect(authoritativeFds).toEqual([4]);
});

test("status fd changes become authoritative for every process worker", () => {
  const handle = makeHandle();
  const firstWorker = {} as Worker;
  const secondWorker = {} as Worker;
  const firstThread = {} as ThreadWorkerState;
  const secondThread = {} as ThreadWorkerState;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>([
    [firstWorker, firstThread],
    [secondWorker, secondThread],
  ]);
  addProcessWorker(handle as never, firstWorker);
  addProcessWorker(handle as never, secondWorker);

  markStatusFdChangesForNextStep(
    handle as never,
    threadWorkerStates,
    [4, 7],
  );

  expect(snapshotPendingAuthoritativeFdsForNextStep(firstThread)).toEqual([4, 7]);
  expect(snapshotPendingAuthoritativeFdsForNextStep(secondThread)).toEqual([4, 7]);
});

test("status fd entries do not resurrect a closed regular fd from a stale non-creating snapshot", () => {
  const fdEntries = new Map<number, number>([[6, 0]]);
  const generations = new Map<number, number>([[6, 4]]);
  const setCalls: Array<{ fd: number; ofdId: number; fdFlags: number }> = [];
  const handle = makeHandle({
    kernel: makeKernel({
      abi_max_fds: () => 16,
      abi_sys_read: () => 63n,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fdEntries.get(fd) ?? 0,
      process_fd_flags: () => 0,
      process_fd_generation: (_vmPtr: number, fd: number) => generations.get(fd) ?? 0,
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number, fdFlags: number) => {
        setCalls.push({ fd, ofdId, fdFlags });
        fdEntries.set(fd, ofdId);
        generations.set(fd, (generations.get(fd) ?? 0) + 1);
      },
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 9 ? 1 : 0),
    }),
  });

  applyStatusFdEntries(handle as never, makeStatusMessage({
    syscallNr: 63n,
    fdEntries: [{ fd: 6, ofdId: 9, fdFlags: 1, generation: 2 }],
  }) as never);

  expect(fdEntries.get(6)).toBe(0);
  expect(setCalls).toEqual([]);
});

test("status fd entries do not resurrect a closed pipe fd from a stale non-creating snapshot", () => {
  const fdEntries = new Map<number, number>([[7, 0]]);
  const generations = new Map<number, number>([[7, 3]]);
  const setCalls: Array<{ fd: number; ofdId: number; fdFlags: number }> = [];
  const handle = makeHandle({
    kernel: makeKernel({
      abi_max_fds: () => 16,
      abi_sys_write: () => 64n,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fdEntries.get(fd) ?? 0,
      process_fd_flags: () => 0,
      process_fd_generation: (_vmPtr: number, fd: number) => generations.get(fd) ?? 0,
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number, fdFlags: number) => {
        setCalls.push({ fd, ofdId, fdFlags });
        fdEntries.set(fd, ofdId);
        generations.set(fd, (generations.get(fd) ?? 0) + 1);
      },
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 8 ? 3 : 0),
    }),
  });

  applyStatusFdEntries(handle as never, makeStatusMessage({
    syscallNr: 64n,
    fdEntries: [{ fd: 7, ofdId: 8, fdFlags: 1, generation: 2 }],
  }) as never);

  expect(fdEntries.get(7)).toBe(0);
  expect(setCalls).toEqual([]);
});

test("status fd entries allow fd-creating syscalls to bind pipe fds across local generations", () => {
  const fdEntries = new Map<number, number>([[7, 0]]);
  const generations = new Map<number, number>([[7, 3]]);
  const setCalls: Array<{ fd: number; ofdId: number; fdFlags: number }> = [];
  const handle = makeHandle({
    kernel: makeKernel({
      abi_max_fds: () => 16,
      abi_sys_pipe2: () => 59n,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fdEntries.get(fd) ?? 0,
      process_fd_flags: () => 0,
      process_fd_generation: (_vmPtr: number, fd: number) => generations.get(fd) ?? 0,
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number, fdFlags: number) => {
        setCalls.push({ fd, ofdId, fdFlags });
        fdEntries.set(fd, ofdId);
        generations.set(fd, (generations.get(fd) ?? 0) + 1);
      },
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 8 ? 3 : 0),
    }),
  });

  applyStatusFdEntries(handle as never, makeStatusMessage({
    syscallNr: 59n,
    fdEntries: [{ fd: 7, ofdId: 8, fdFlags: 1, generation: 2 }],
  }) as never);

  expect(fdEntries.get(7)).toBe(8);
  expect(setCalls).toEqual([{ fd: 7, ofdId: 8, fdFlags: 1 }]);
});

test("status fd entries do not clear live epoll descriptors from stale non-closing snapshots", () => {
  const fdEntries = new Map<number, number>([
    [5, 7],
    [6, 8],
  ]);
  const handle = makeHandle({
    kernel: makeKernel({
      abi_max_fds: () => 16,
      abi_sys_write: () => 64n,
      abi_fd_type_epoll: () => 7,
      abi_fd_type_eventfd: () => 9,
      abi_fd_type_timerfd: () => 10,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fdEntries.get(fd) ?? 0,
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number) => {
        if (ofdId === 0) {
          fdEntries.delete(fd);
        } else {
          fdEntries.set(fd, ofdId);
        }
      },
      kernel_ofd_fd_type: (ofdId: number) => {
        if (ofdId === 7) {
          return 7;
        }
        if (ofdId === 8) {
          return 9;
        }
        return 0;
      },
      kernel_ofd_refcount: (ofdId: number) => (ofdId === 7 || ofdId === 8 ? 1 : 0),
    }),
  });

  applyStatusFdEntries(handle as never, makeStatusMessage({
    syscallNr: 64n,
    fdEntries: [
      { fd: 5, ofdId: 0, fdFlags: 0 },
      { fd: 6, ofdId: 0, fdFlags: 0 },
    ],
  }) as never);

  expect(fdEntries).toEqual(new Map([
    [5, 7],
    [6, 8],
  ]));
});

test("status fd entries allow close to clear a live epoll descriptor", () => {
  const fdEntries = new Map<number, number>([[5, 7]]);
  const handle = makeHandle({
    kernel: makeKernel({
      abi_max_fds: () => 16,
      abi_sys_close: () => 57n,
      abi_fd_type_epoll: () => 7,
      process_fd_ofd_id: (_vmPtr: number, fd: number) => fdEntries.get(fd) ?? 0,
      process_set_fd_entry: (_vmPtr: number, fd: number, ofdId: number) => {
        if (ofdId === 0) {
          fdEntries.delete(fd);
        } else {
          fdEntries.set(fd, ofdId);
        }
      },
      kernel_ofd_fd_type: (ofdId: number) => (ofdId === 7 ? 7 : 0),
      kernel_ofd_refcount: (ofdId: number) => (ofdId === 7 ? 1 : 0),
    }),
  });

  applyStatusFdEntries(handle as never, makeStatusMessage({
    syscallNr: 57n,
    fdEntries: [{ fd: 5, ofdId: 0, fdFlags: 0 }],
  }) as never);

  expect(fdEntries.has(5)).toBe(false);
});
