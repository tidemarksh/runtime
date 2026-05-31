import { registerDynamicStartupGate } from "../../../support/dynamic-startup-test-runner.js";
import { rubyDynamicStartupEntry } from "./dynamic-startup.js";

registerDynamicStartupGate(rubyDynamicStartupEntry);
