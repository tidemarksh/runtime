import { defineConfig } from 'vite-plus/pack'

export default defineConfig({
  entry: [
    'src/index.ts',
    'src/thread-worker.ts',
    'src/kernel-worker.ts',
  ],
  dts: {
    tsgo: true,
  },
  exports: true,
  // ...config options
})
