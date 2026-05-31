import { registerProcessCreateDynamicGate } from "../../../support/process-create-dynamic-test-runner.js";
import { luaDynamicStartupEntry } from "./dynamic-startup.js";

registerProcessCreateDynamicGate(luaDynamicStartupEntry, "/lib/ld-linux-riscv64-lp64d.so.1");
