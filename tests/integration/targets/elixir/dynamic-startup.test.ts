import { registerDynamicStartupGate } from "../../../support/dynamic-startup-test-runner.js";
import { elixirDynamicStartupEntry } from "./dynamic-startup.js";

registerDynamicStartupGate(elixirDynamicStartupEntry);
