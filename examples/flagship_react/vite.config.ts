import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { defineConfig } from 'vite';

const root = dirname(fileURLToPath(import.meta.url));

export default defineConfig({
    root,
    server: {
        port: 5174,
        strictPort: true,
        proxy: {
            '/v1': {
                target: 'http://127.0.0.1:8765',
                changeOrigin: true,
            },
        },
    },
    preview: {
        port: 5174,
        strictPort: true,
        proxy: {
            '/v1': {
                target: 'http://127.0.0.1:8765',
                changeOrigin: true,
            },
        },
    },
    build: {
        outDir: 'dist',
        emptyOutDir: true,
    },
});
