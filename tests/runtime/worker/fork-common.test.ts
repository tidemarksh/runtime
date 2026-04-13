import { expect, test, vi } from "vite-plus/test";
import {
  computeGuestRamLimitBytes,
  detachInheritedHostStdioFromChild,
} from "../../../src/worker/fork/common.js";

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

test("computeGuestRamLimitBytes trims guest window below 2GiB when guestBase consumes linear-memory headroom", () => {
  expect(
    computeGuestRamLimitBytes(
      0x14b0_0000,
      192 * 1024 * 1024,
      32768,
    ),
  ).toBe(0x6b50_0000);
});

test("computeGuestRamLimitBytes preserves full 2GiB guest window when fork ceiling includes guestBase headroom", () => {
  expect(
    computeGuestRamLimitBytes(
      0x14b0_0000,
      192 * 1024 * 1024,
      40960,
    ),
  ).toBe(0x8000_0000);
});
