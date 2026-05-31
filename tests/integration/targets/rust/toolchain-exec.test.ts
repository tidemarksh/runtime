import { registerToolchainExecGate } from "../../../support/toolchain-exec-test-runner.js";
import { cargoBuildToolchainEntry, rustcCompileToolchainEntry } from "./toolchain-exec.js";

registerToolchainExecGate(rustcCompileToolchainEntry);
registerToolchainExecGate(cargoBuildToolchainEntry);
