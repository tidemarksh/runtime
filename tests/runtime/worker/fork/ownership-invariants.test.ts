import { expect, test, vi } from "vite-plus/test";
import {
  assertExecveChildReadyOwnership,
  assertForkChildReadyOwnership,
  assertVforkParentResumeOwnership,
} from "../../../../src/worker/ownership/invariants.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

function makeHandle(options: {
  pid?: number;
  vmPtr?: number;
  ownerPid?: number;
  processPtr?: number;
  fds?: Map<number, { ofdId: number; flags?: number; type?: number }>;
} = {}): ProcessHandle {
  const pid = options.pid ?? 42;
  const vmPtr = options.vmPtr ?? 0x1000;
  const ownerPid = options.ownerPid ?? pid;
  const processPtr = options.processPtr ?? 0x2000;
  const fds = options.fds ?? new Map<number, { ofdId: number; flags?: number; type?: number }>([
    [0, { ofdId: 1, type: 2 }],
    [1, { ofdId: 2, type: 2 }],
    [2, { ofdId: 3, type: 2 }],
  ]);
  return {
    pid,
    vmPtr,
    kernel: {
      process_current_pid: (candidateVmPtr: number) => candidateVmPtr === vmPtr ? ownerPid : 0,
      thread_process_ptr: (candidateVmPtr: number) => candidateVmPtr === vmPtr ? processPtr : 0,
      abi_max_fds: () => 8,
      abi_o_cloexec: () => 0x80000,
      process_fd_ofd_id: (_candidateVmPtr: number, fd: number) => fds.get(fd)?.ofdId ?? 0,
      process_fd_flags: (_candidateVmPtr: number, fd: number) => fds.get(fd)?.flags ?? 0,
      kernel_ofd_fd_type: (ofdId: number) => {
        for (const entry of fds.values()) {
          if (entry.ofdId === ofdId) {
            return entry.type ?? 1;
          }
        }
        return 0;
      },
    },
  } as unknown as ProcessHandle;
}

test("plain fork child ready validates ownership before child visibility", () => {
  const assertProcessStandardFdIntegrity = vi.fn();
  const handle = makeHandle();

  assertForkChildReadyOwnership(
    { assertProcessStandardFdIntegrity },
    handle,
    handle.vmPtr,
    "plain-fork-child-ready",
  );

  expect(assertProcessStandardFdIntegrity).toHaveBeenCalledWith(handle, "plain-fork-child-ready");
});

test("vfork parent resume rejects a thread that no longer belongs to the parent", () => {
  const assertProcessStandardFdIntegrity = vi.fn();
  const handle = makeHandle({ ownerPid: 99 });

  expect(() => assertVforkParentResumeOwnership(
    { assertProcessStandardFdIntegrity },
    handle,
    handle.vmPtr,
    "vfork-parent-restored",
  )).toThrow("thread owner pid drift");
  expect(assertProcessStandardFdIntegrity).not.toHaveBeenCalled();
});

test("execve child ready rejects retained CLOEXEC descriptors", () => {
  const assertProcessStandardFdIntegrity = vi.fn();
  const handle = makeHandle({
    fds: new Map([
      [0, { ofdId: 1, type: 2 }],
      [1, { ofdId: 2, type: 2 }],
      [2, { ofdId: 3, type: 2 }],
      [4, { ofdId: 4, flags: 0x80000, type: 1 }],
    ]),
  });

  expect(() => assertExecveChildReadyOwnership(
    { assertProcessStandardFdIntegrity },
    handle,
    handle.vmPtr,
    "vfork-child-ready",
  )).toThrow("retained CLOEXEC");
  expect(assertProcessStandardFdIntegrity).not.toHaveBeenCalled();
});
