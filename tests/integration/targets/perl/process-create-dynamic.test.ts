import { registerProcessCreateDynamicGate } from "../../../support/process-create-dynamic-test-runner.js";
import { perlDynamicStartupEntry } from "./dynamic-startup.js";

registerProcessCreateDynamicGate(perlDynamicStartupEntry, "/lib/ld-linux-riscv64-lp64d.so.1");
