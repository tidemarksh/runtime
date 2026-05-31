import { test, expect } from "vitest";
import { retainRuntimeLifecycleEvents } from "../../../../src/worker/diagnostics/lifecycle-log.js";

test("retainRuntimeLifecycleEvents returns a new array without mutating the input", () => {
  const events = ["start", "fork:1", "exit:1"];

  const retained = retainRuntimeLifecycleEvents(events);

  expect(retained).toEqual(events);
  expect(retained).not.toBe(events);
});

test("retainRuntimeLifecycleEvents keeps important entries and the recent tail", () => {
  const events = Array.from({ length: 5001 }, (_, index) => (
    index === 1 ? "fork:important" : `event:${index}`
  ));

  const retained = retainRuntimeLifecycleEvents(events);

  expect(retained).toContain("fork:important");
  expect(retained.at(-1)).toBe("event:5000");
  expect(retained.length).toBeLessThan(events.length);
});
