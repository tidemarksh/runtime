import { expect, test } from "vite-plus/test";
import { formatCompactDebugSuffix } from "../../../src/worker/message-handler/debug-summary.js";

test("formatCompactDebugSuffix does not confuse pc/sp/s11 with fault or signal fields", () => {
  const debug = [
    "fault_pc=0x0",
    "pc=0x83308",
    "sig_pc=0x4",
    "sig_sp=0x0",
    "sp=0x37d6be80",
    "sig_s11=0x0",
    "s11=0x37d08e00",
  ].join(" ");

  expect(formatCompactDebugSuffix(debug)).toBe(
    ":pc=0x83308:sp=0x37d6be80:s11=0x37d08e00:sig_pc=0x4:sig_sp=0x0:sig_s11=0x0",
  );
});

test("formatCompactDebugSuffix preserves quoted write_data payloads", () => {
  const debug = [
    "pc=0x4",
    "ra=0x6573a",
    "sp=0x37d55aa0",
    "s11=0x0",
    "write_fd=2",
    "write_count=31",
    "write_result=31",
    "write_data=\"fatal: bad g in signal handler\\\\n\"",
  ].join(" ");

  expect(formatCompactDebugSuffix(debug, [
    "pc",
    "ra",
    "sp",
    "s11",
    "write_fd",
    "write_count",
    "write_result",
    "write_data",
  ])).toBe(
    ":pc=0x4:ra=0x6573a:sp=0x37d55aa0:s11=0x0:write_fd=2:write_count=31:write_result=31:write_data=\"fatal: bad g in signal handler\\\\n\"",
  );
});
