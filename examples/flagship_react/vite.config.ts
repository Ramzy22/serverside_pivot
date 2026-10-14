import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { defineConfig } from 'vite';

const root = dirname(fileURLToPath(import.meta.url));
const runtimeUpstream = process.env.PIVOT_RUNTIME_UPSTREAM || 'http://127.0.0.1:8765';

export default defineConfig({
    root,
    server: {
        port: 5174,
        strictPort: true,
        proxy: {
            '/v1': {
                target: runtimeUpstream,
                changeOrigin: true,
            },
        },
    },
    preview: {
        port: 5174,
        strictPort: true,
        proxy: {
            '/v1': {
                target: runtimeUpstream,
                changeOrigin: true,
            },
        },
    },
    build: {
        outDir: 'dist',
        emptyOutDir: true,
    },
});
