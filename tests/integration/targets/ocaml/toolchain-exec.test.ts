import { registerToolchainExecGate } from "../../../support/toolchain-exec-test-runner.js";
import { ocamlCompileToolchainEntry } from "./toolchain-exec.js";

registerToolchainExecGate(ocamlCompileToolchainEntry);
