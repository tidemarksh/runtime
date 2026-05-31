import { registerToolchainExecGate } from "../../../support/toolchain-exec-test-runner.js";
import { haskellCompileToolchainEntry } from "./toolchain-exec.js";

registerToolchainExecGate(haskellCompileToolchainEntry);
