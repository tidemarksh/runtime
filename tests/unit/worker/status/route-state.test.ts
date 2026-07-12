import { expect, test } from "vite-plus/test";
import {
  statusProcessBelongsToHandle,
  statusProcessIncarnationMatches,
} from "../../../../src/worker/message-handler/status/route-state.js";

test("Kernel process identity takes precedence over a stale observed pid", () => {
  expect(statusProcessBelongsToHandle(4, 4, 2)).toBe(true);
});

test("Kernel process identity rejects a foreign status despite matching observed pid", () => {
  expect(statusProcessBelongsToHandle(4, 2, 4)).toBe(false);
});

test("observed process identity is used when Kernel identity is unavailable", () => {
  expect(statusProcessBelongsToHandle(4, null, 4)).toBe(true);
  expect(statusProcessBelongsToHandle(4, null, 2)).toBe(false);
  expect(statusProcessBelongsToHandle(4, null, null)).toBe(true);
});

test("a delayed vfork child status cannot mutate the restored parent incarnation", () => {
  expect(statusProcessIncarnationMatches(0x6000, 5, 0x5000, 1)).toBe(false);
  expect(statusProcessIncarnationMatches(0x5000, 1, 0x5000, 1)).toBe(true);
});
