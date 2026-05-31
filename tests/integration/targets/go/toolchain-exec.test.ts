import { registerToolchainExecGate } from "../../../support/toolchain-exec-test-runner.js";
import {
  goBuildToolchainEntry,
  goCompileVersionToolchainEntry,
  goHelperCombinedGoarchToolchainEntry,
  goToolCompileVersionSingleThreadToolchainEntry,
  goToolCompileVersionToolchainEntry,
} from "./toolchain-exec.js";

registerToolchainExecGate(goCompileVersionToolchainEntry);
registerToolchainExecGate(goHelperCombinedGoarchToolchainEntry);
registerToolchainExecGate(goToolCompileVersionToolchainEntry);
registerToolchainExecGate(goToolCompileVersionSingleThreadToolchainEntry);
registerToolchainExecGate(goBuildToolchainEntry);
