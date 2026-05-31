import { registerToolchainExecGate } from "../../../support/toolchain-exec-test-runner.js";
import { cppCompileLinkToolchainEntry } from "./toolchain-exec.js";

registerToolchainExecGate(cppCompileLinkToolchainEntry);
