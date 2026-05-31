import { registerScriptRuntimeGate } from "../../../support/script-runtime-test-runner.js";
import { elixirEvalScriptRuntimeEntry, erlEvalScriptRuntimeEntry } from "./script-runtime.js";

registerScriptRuntimeGate(erlEvalScriptRuntimeEntry);
registerScriptRuntimeGate(elixirEvalScriptRuntimeEntry);
