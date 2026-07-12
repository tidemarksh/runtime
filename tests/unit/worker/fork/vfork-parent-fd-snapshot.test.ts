import { expect, test } from "vite-plus/test";
import { snapshotVforkParentFdEntries } from "../../../../src/worker/fork-shared.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

test("vfork parent fd snapshot reads the kernel-preserved parent table", () => {
  const handle = {
    kernel: {
      abi_max_fds: () => 3,
      thread_vfork_parent_fd_ofd_id: (fd: number) => [1, 4, 7][fd],
      thread_vfork_parent_fd_flags: (fd: number) => [0, 2, 5][fd],
      thread_vfork_parent_fd_generation: (fd: number) => [3, 6, 9][fd],
      supervisor_process_ofd_for_fd: () => {
        throw new Error("child fd table must not be read");
      },
      supervisor_process_fd_flags_for_fd: () => {
        throw new Error("child fd table must not be read");
      },
    },
  } as unknown as ProcessHandle;

  expect(snapshotVforkParentFdEntries(handle, 0x1234)).toEqual([
    { fd: 0, ofdId: 1, fdFlags: 0, generation: 3 },
    { fd: 1, ofdId: 4, fdFlags: 2, generation: 6 },
    { fd: 2, ofdId: 7, fdFlags: 5, generation: 9 },
  ]);
});
