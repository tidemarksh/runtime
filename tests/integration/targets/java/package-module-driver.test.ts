import { registerPackageModuleDriverGate } from "../../../support/package-module-driver-test-runner.js";
import { javaClassLoadPackageModuleEntry } from "./package-module-driver.js";

registerPackageModuleDriverGate(javaClassLoadPackageModuleEntry);
