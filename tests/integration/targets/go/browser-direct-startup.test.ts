import { registerBrowserDirectStartupGate } from "../../../support/browser-direct-test-runner.js";
import { prepareGoBrowserDirectStartupEntry } from "./browser-direct-startup.js";

registerBrowserDirectStartupGate("go env GOROOT", prepareGoBrowserDirectStartupEntry);
