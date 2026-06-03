import { existsSync } from "node:fs";
import type { BrowserDirectStartupEntry } from "../../../support/browser-direct-harness.js";
import { logSkip } from "../../../support/runtime-workload.js";
import { GO_BINARY_PATH, buildGoRuntimeSnapshot } from "./support.js";

export function prepareGoBrowserDirectStartupEntry(): BrowserDirectStartupEntry | null {
  if (!GO_BINARY_PATH || !existsSync(GO_BINARY_PATH)) {
    return logSkip(`missing Go binary: ${GO_BINARY_PATH || "TIDEMARK_TEST_GO_BINARY"}`);
  }

  return {
    id: "browser-direct-go-env-goroot",
    snapshot: buildGoRuntimeSnapshot({
      includeToolchain: false,
      includeSampleModule: false,
    }),
    run: {
      executablePath: "/usr/local/go/bin/go",
      argv: ["/usr/local/go/bin/go", "env", "GOROOT"],
      env: [
        "GOROOT=/usr/local/go",
        "GOTELEMETRY=off",
        "PATH=/usr/local/go/bin:/bin",
      ],
      cwd: "/work",
    },
    runtimeOptions: {
      guestMemoryPolicyLimitBytes: 192 * 1024 * 1024,
      kernelWorkerMaxMemoryPages: 7168,
      maxProcessMemoryPages: 40960,
      maxForkMemoryPages: 40960,
    },
    expectedExitCode: 0,
    outputPattern: /\/usr\/local\/go/,
    timeoutMs: 30_000,
  };
}
