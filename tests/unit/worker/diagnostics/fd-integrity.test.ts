import { expect, test, vi } from "vitest";
import { assertProcessStandardFdIntegrity } from "../../../../src/worker/diagnostics/debug.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

test("standard fd integrity defers the failure snapshot on the healthy path", () => {
  const debugSnapshot = vi.fn(() => "failure snapshot");
  const handle = {
    pid: 1,
    vmPtr: 0x1000,
    kernel: {
      abi_max_fds: () => 1024,
      supervisor_process_ofd_for_fd: () => 0,
    },
  } as unknown as ProcessHandle;

  assertProcessStandardFdIntegrity(handle, "healthy", debugSnapshot, () => "pipe debug");

  expect(debugSnapshot).not.toHaveBeenCalled();
});
