import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { defineConfig, transformWithEsbuild } from 'vite';
import react from '@vitejs/plugin-react';

const here = dirname(fileURLToPath(import.meta.url));
const sharedSource = resolve(here, '../../src/lib').replace(/\\/g, '/');

// The established Dash source predates the .jsx convention and contains JSX
// in .js files. Convert only that shared source before Vite's import-analysis
// phase; the Dash webpack build remains unchanged.
const sharedJsx = {
    name: 'serverside-pivot-shared-jsx',
    enforce: 'pre' as const,
    async transform(code: string, id: string) {
        if (!id.replace(/\\/g, '/').startsWith(sharedSource) || !id.endsWith('.js')) return null;
        return transformWithEsbuild(code, id, { loader: 'jsx', jsx: 'automatic' });
    },
};

export default defineConfig({
    base: './',
    plugins: [sharedJsx, react({ include: [/src\/lib\/.*\.js$/, /\.[jt]sx?$/] })],
    resolve: {
        alias: {
            '@pivot': resolve(here, '../../src/lib'),
            // The shared source is built into this standalone React bundle.
            // Force all of it to use this bundle's React 18 instance.
            react: resolve(here, 'node_modules/react'),
            'react-dom': resolve(here, 'node_modules/react-dom'),
        },
    },
    define: {
        'process.env.NODE_ENV': JSON.stringify('production'),
    },
    build: {
        outDir: 'build',
        emptyOutDir: true,
        // The Python host reads this manifest instead of guessing among
        // hashed entries left behind by an in-place package upgrade.
        manifest: 'pivot-workspace-manifest.json',
        sourcemap: false,
        lib: {
            entry: './src/index.tsx',
            formats: ['es'],
            fileName: 'pivot-workspace-[hash]',
        },
        rollupOptions: {
            output: {
                entryFileNames: 'pivot-workspace-[hash].js',
                chunkFileNames: 'chunks/[name]-[hash].js',
                manualChunks: {
                    charts: ['echarts', 'echarts-for-react', 'echarts-gl'],
                    spreadsheet: ['xlsx'],
                },
            },
        },
    },
});
