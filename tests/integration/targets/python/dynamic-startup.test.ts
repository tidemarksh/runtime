import { registerDynamicStartupGate } from "../../../support/dynamic-startup-test-runner.js";
import { pythonDynamicStartupEntry } from "./dynamic-startup.js";

registerDynamicStartupGate(pythonDynamicStartupEntry);
