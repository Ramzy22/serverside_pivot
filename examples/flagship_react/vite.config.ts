import { resolve } from 'node:path';
import { defineConfig } from '../../dash_tanstack_pivot/node_modules/vite/dist/node/index.js';

const root = resolve(import.meta.dirname);
const packageRoot = resolve(root, '../../dash_tanstack_pivot');
// The Dash package still retains React 16 as a legacy development dependency.
// The standalone and Streamlit hosts are React 18 applications, so this
// showcase deliberately resolves the React 18 peer set used by Streamlit.
const dependencies = resolve(packageRoot, 'streamlit_pivot/frontend/node_modules');

// The aliases let this repository example run before it has been packed or
// published. Its source still imports the public package specifier below;
// consumers replace only this development mapping with a normal npm install.
export default defineConfig({
    root,
    resolve: {
        alias: {
            'dash_tanstack_pivot/react': resolve(packageRoot, 'dist/react/index.mjs'),
            react: resolve(dependencies, 'react/index.js'),
            'react-dom/client': resolve(dependencies, 'react-dom/client.js'),
            'react-dom': resolve(dependencies, 'react-dom/index.js'),
            'styled-components': resolve(dependencies, 'styled-components/dist/styled-components.browser.esm.js'),
        },
    },
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
    build: {
        outDir: 'dist',
        emptyOutDir: true,
    },
});
