import { expect, test, vi } from "vite-plus/test";
import {
  detachInheritedHostStdioFromChild,
  normalizeGuestMemoryPolicyLimitBytes,
} from "../../../../src/worker/fork/common.js";

test("detachInheritedHostStdioFromChild detaches guest host stdio without clearing hidden child pipe ids", () => {
  const processSetHostStdio = vi.fn();
  const child = {
    stdioOwner: {} as never,
    vmPtr: 0x1234,
    stdinPipe: 11,
    stdoutPipe: 12,
    stderrPipe: 13,
    kernel: {
      process_set_host_stdio: processSetHostStdio,
    },
  } as never;

  detachInheritedHostStdioFromChild(child);

  expect(processSetHostStdio).toHaveBeenCalledWith(0x1234, 0, 0, 0);
  expect(child.stdinPipe).toBe(11);
  expect(child.stdoutPipe).toBe(12);
  expect(child.stderrPipe).toBe(13);
});

test("detachInheritedHostStdioFromChild leaves standalone process host stdio untouched", () => {
  const processSetHostStdio = vi.fn();
  detachInheritedHostStdioFromChild({
    stdioOwner: null,
    vmPtr: 0x1234,
    kernel: {
      process_set_host_stdio: processSetHostStdio,
    },
  } as never);

  expect(processSetHostStdio).not.toHaveBeenCalled();
});

test("normalizeGuestMemoryPolicyLimitBytes preserves explicit policy independently of backing", () => {
  expect(normalizeGuestMemoryPolicyLimitBytes(0x7fff_ffff)).toBe(0x7fff_ffff);
  expect(normalizeGuestMemoryPolicyLimitBytes(192 * 1024 * 1024)).toBe(192 * 1024 * 1024);
  expect(normalizeGuestMemoryPolicyLimitBytes(-1)).toBe(0);
});
