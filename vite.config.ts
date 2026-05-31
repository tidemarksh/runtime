import tsdownConfig from './tsdown.config.js';

import { defineConfig } from 'vite-plus';

export default defineConfig({
  pack: tsdownConfig,
  lint: {"options":{"typeAware":true,"typeCheck":true}},
  test: {
    setupFiles: ['./tests/support/integration-env.ts'],
  },
});
