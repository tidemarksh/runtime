import { registerScriptRuntimeGate } from "../../../support/script-runtime-test-runner.js";
import { luaScriptRuntimeEntry } from "./script-runtime.js";

registerScriptRuntimeGate(luaScriptRuntimeEntry);
