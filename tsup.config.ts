/**
 * tsup build configuration for `@stackra/nestjs-queue`.
 *
 * Single entry — server-only NestJS code. Externals cover every
 * workspace and runtime dependency so they're not bundled.
 *
 * @module @stackra/nestjs-queue/build
 */

import { defineConfig } from 'tsup';

export default defineConfig({
  entry: { index: 'src/index.ts' },
  format: ['esm', 'cjs'],
  target: 'es2022',
  dts: true,
  sourcemap: true,
  clean: true,
  splitting: false,
  treeshake: true,
  external: [
    '@nestjs/bullmq',
    '@nestjs/common',
    '@nestjs/core',
    '@stackra/contracts',
    'bullmq',
    'reflect-metadata',
  ],
});
