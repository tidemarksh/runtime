import { registerBrowserDirectStartupGate } from "../../../support/browser-direct-test-runner.js";
import { prepareNodeBrowserDirectStartupEntry } from "./browser-direct-startup.js";

registerBrowserDirectStartupGate("node --version", prepareNodeBrowserDirectStartupEntry);
