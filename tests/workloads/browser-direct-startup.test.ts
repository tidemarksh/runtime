import { expect, test } from "vite-plus/test";
import {
  runBrowserDirectStartupEntry,
} from "../support/browser-direct-harness.js";
import { prepareGoBrowserDirectStartupEntry } from "../integration/targets/go/browser-direct-startup.js";
import { prepareNodeBrowserDirectStartupEntry } from "../integration/targets/node/browser-direct-startup.js";

for (const [label, entryFactory] of [
  ["node --version", prepareNodeBrowserDirectStartupEntry],
  ["go env GOROOT", prepareGoBrowserDirectStartupEntry],
] as const) {
  test(`browser direct startup local gate: ${label}`, async () => {
    const entry = entryFactory();
    if (!entry) {
      return;
    }

    const result = await runBrowserDirectStartupEntry(entry);
    if (!result) {
      return;
    }

    expect(
      result.exitCode,
      [
        `browser direct startup exit mismatch: ${entry.id}`,
        `exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `error=${result.error ?? "null"}`,
        `failure=${JSON.stringify(result.failureSnapshot)}`,
      ].join("\n"),
    ).toBe(entry.expectedExitCode);
    expect(
      result.output,
      [
        `browser direct startup output mismatch: ${entry.id}`,
        `exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `error=${result.error ?? "null"}`,
        `failure=${JSON.stringify(result.failureSnapshot)}`,
      ].join("\n"),
    ).toMatch(entry.outputPattern);
  }, 90_000);
}
