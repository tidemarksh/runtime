import { expect, test } from "vite-plus/test";
import { parseDebugGuestWriteWatchRegions } from "../../support/dynamic-startup-harness.js";

test("parseDebugGuestWriteWatchRegions ignores empty or invalid input", () => {
  expect(parseDebugGuestWriteWatchRegions(undefined)).toEqual([]);
  expect(parseDebugGuestWriteWatchRegions("")).toEqual([]);
  expect(parseDebugGuestWriteWatchRegions("0x20:0x10 invalid 0x10")).toEqual([]);
});

test("parseDebugGuestWriteWatchRegions parses up to two address ranges", () => {
  expect(
    parseDebugGuestWriteWatchRegions("0x1000:0x1020,0x2000:0x2010 0x3000:0x3010"),
  ).toEqual([
    { slot: 0, start: 0x1000, end: 0x1020 },
    { slot: 1, start: 0x2000, end: 0x2010 },
  ]);
});
