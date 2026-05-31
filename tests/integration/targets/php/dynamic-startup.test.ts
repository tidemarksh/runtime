import { registerDynamicStartupGate } from "../../../support/dynamic-startup-test-runner.js";
import { phpDynamicStartupEntry } from "./dynamic-startup.js";

registerDynamicStartupGate(phpDynamicStartupEntry);
