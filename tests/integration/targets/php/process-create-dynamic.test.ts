import { registerProcessCreateDynamicGate } from "../../../support/process-create-dynamic-test-runner.js";
import { phpDynamicStartupEntry } from "./dynamic-startup.js";

registerProcessCreateDynamicGate(phpDynamicStartupEntry, "/lib/ld-linux-riscv64-lp64d.so.1");
