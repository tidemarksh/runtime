import { existsSync } from "node:fs";
import type { BrowserDirectStartupEntry } from "../../../support/browser-direct-harness.js";
import { logSkip } from "../../../support/runtime-workload.js";
import {
  buildNodeRuntimeSnapshot,
  NODE_BINARY_PATH,
  readNodeBinary,
} from "./support.js";

export function prepareNodeBrowserDirectStartupEntry(): BrowserDirectStartupEntry | null {
  if (!NODE_BINARY_PATH || !existsSync(NODE_BINARY_PATH)) {
    return logSkip(`missing Node binary: ${NODE_BINARY_PATH || "TIDEMARK_TEST_NODE_BINARY"}`);
  }

  const nodeBytes = readNodeBinary();
  if (!nodeBytes) {
    return null;
  }
  const snapshot = buildNodeRuntimeSnapshot(nodeBytes);
  if (!snapshot) {
    return null;
  }

  return {
    id: "browser-direct-node-version",
    snapshot,
    run: {
      executablePath: "/bin/node",
      argv: ["/bin/node", "--version"],
      env: [
        "PATH=/bin",
      ],
      cwd: "/work",
    },
    runtimeOptions: {
      guestMemoryPolicyLimitBytes: 2 * 1024 * 1024 * 1024,
      kernelWorkerMaxMemoryPages: 7168,
      maxProcessMemoryPages: 40960,
      maxForkMemoryPages: 40960,
    },
    expectedExitCode: 0,
    outputPattern: /\bv\d+\.\d+\.\d+\b/,
    timeoutMs: 30_000,
  };
}
