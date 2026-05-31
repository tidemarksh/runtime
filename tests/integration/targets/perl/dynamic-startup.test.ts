import { registerDynamicStartupGate } from "../../../support/dynamic-startup-test-runner.js";
import { perlDynamicStartupEntry } from "./dynamic-startup.js";

registerDynamicStartupGate(perlDynamicStartupEntry);
