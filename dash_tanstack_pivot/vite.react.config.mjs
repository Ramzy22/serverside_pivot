import { resolve } from 'node:path';
import { defineConfig, transformWithEsbuild } from 'vite';
import react from '@vitejs/plugin-react';

const sharedSource = resolve(import.meta.dirname, 'src/lib').replace(/\\/g, '/');

// Existing Dash modules contain JSX in .js files. Convert the shared source
// before Vite parses imports; this leaves the Dash webpack pipeline untouched.
const sharedJsx = {
    name: 'serverside-pivot-shared-jsx',
    enforce: 'pre',
    async transform(code, id) {
        if (!id.replace(/\\/g, '/').startsWith(sharedSource) || !id.endsWith('.js')) return null;
        return transformWithEsbuild(code, id, { loader: 'jsx', jsx: 'transform' });
    },
};

export default defineConfig({
    plugins: [
        sharedJsx,
        react({
            include: [/src\/lib\/.*\.js$/, /\.[jt]sx?$/],
            // A classic transform uses the consumer's external React object.
            // It avoids coupling a bundled automatic JSX runtime to the React
            // version that happened to be installed during publication.
            jsxRuntime: 'classic',
        }),
    ],
    define: { 'process.env.NODE_ENV': JSON.stringify('production') },
    build: {
        outDir: 'dist/react',
        emptyOutDir: true,
        sourcemap: false,
        lib: {
            entry: resolve(import.meta.dirname, 'src/lib/react.ts'),
            formats: ['es'],
            fileName: 'index.mjs',
        },
        rollupOptions: {
            external: [
                'react',
                'react-dom',
                'react-dom/client',
                'styled-components',
            ],
            output: {
                entryFileNames: 'index.mjs',
                chunkFileNames: 'chunks/[name]-[hash].mjs',
            },
        },
    },
});
