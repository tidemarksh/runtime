import { registerDynamicStartupGate } from "../../../support/dynamic-startup-test-runner.js";
import { javaDynamicStartupEntry } from "./dynamic-startup.js";

registerDynamicStartupGate(javaDynamicStartupEntry);
