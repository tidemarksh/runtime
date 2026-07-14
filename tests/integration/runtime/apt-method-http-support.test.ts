import {
  registerAptMethodNetworkSupportTests,
  registerAptMethodStoreTests,
  registerAptMethodVerificationSupportTests,
  registerAptPackageCacheTests,
} from "../support/apt-method-http-suite.js";

registerAptMethodStoreTests();
registerAptMethodNetworkSupportTests();
registerAptPackageCacheTests();
registerAptMethodVerificationSupportTests();
