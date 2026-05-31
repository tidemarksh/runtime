import { registerToolchainExecGate } from "../../../support/toolchain-exec-test-runner.js";
import { cCompileLinkToolchainEntry } from "./toolchain-exec.js";

registerToolchainExecGate(cCompileLinkToolchainEntry);
