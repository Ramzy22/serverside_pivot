const path = require('path');
const WebpackDashDynamicImport = require('@plotly/webpack-dash-dynamic-import');

const packagejson = require('./package.json');

const dashLibraryName = packagejson.name.replace(/-/g, '_');

module.exports = {
    entry: { main: './src/lib/index.js' },
    output: {
        path: path.resolve(__dirname, dashLibraryName),
        filename: `${dashLibraryName}.min.js`,
        // Meaningful async names make the Dash assets diagnosable and allow CI
        // to hold ordinary feature chunks to a different budget than the
        // explicitly optional XLSX and WebGL renderers.
        chunkFilename: '[name].dash_tanstack_pivot.min.js',
        library: dashLibraryName,
        libraryTarget: 'window',
    },
    resolve: {
        extensions: ['.js', '.jsx', '.ts', '.tsx', '.json'],
    },
    externals: {
        react: 'React',
        'react-dom': 'ReactDOM',
        'plotly.js': 'Plotly',
        'prop-types': 'PropTypes',
    },
    module: {
        rules: [
            {
                test: /\.[jt]sx?$/,
                exclude: /node_modules\/(?!@tanstack)/,
                use: {
                    loader: 'babel-loader',
                    options: {
                        presets: [
                            '@babel/preset-env',
                            '@babel/preset-react',
                            '@babel/preset-typescript'
                        ],
                        plugins: [
                            '@babel/plugin-proposal-nullish-coalescing-operator',
                            '@babel/plugin-proposal-optional-chaining'
                        ]
                    }
                },
            },
            {
                test: /\.css$/,
                use: [
                    {
                        loader: 'style-loader',
                    },
                    {
                        loader: 'css-loader',
                    },
                ],
            },
        ],
    },
    optimization: {
        splitChunks: {
            name: false,
            cacheGroups: {
                async: {
                    chunks: 'async',
                    minSize: 0,
                    name(module, chunks, cacheGroupKey) {
                        return `${dashLibraryName}_async_${chunks[0].name}`;
                    }
                },
                shared: {
                    chunks: 'all',
                    minSize: 0,
                    minChunks: 2,
                    name: 'dash_tanstack_pivot_shared'
                }
            }
        }
    },
    plugins: [
        new WebpackDashDynamicImport()
    ]
};
