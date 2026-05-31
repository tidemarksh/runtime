import { registerToolchainExecGate } from "../../../support/toolchain-exec-test-runner.js";
import {
  cmakeScriptToolchainEntry,
  makeBuildToolchainEntry,
  ninjaBuildToolchainEntry,
} from "./toolchain-exec.js";

registerToolchainExecGate(makeBuildToolchainEntry);
registerToolchainExecGate(cmakeScriptToolchainEntry);
registerToolchainExecGate(ninjaBuildToolchainEntry);
