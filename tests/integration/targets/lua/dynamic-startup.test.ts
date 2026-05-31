import { registerDynamicStartupGate } from "../../../support/dynamic-startup-test-runner.js";
import { luaDynamicStartupEntry } from "./dynamic-startup.js";

registerDynamicStartupGate(luaDynamicStartupEntry);
