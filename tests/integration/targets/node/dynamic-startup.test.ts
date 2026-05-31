import { registerDynamicStartupGate } from "../../../support/dynamic-startup-test-runner.js";
import { nodeDynamicStartupEntry } from "./dynamic-startup.js";

registerDynamicStartupGate(nodeDynamicStartupEntry);
