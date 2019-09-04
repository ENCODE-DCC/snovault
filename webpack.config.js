const path = require('path');
const webpack = require('webpack');
const env = process.env.NODE_ENV;

const PATHS = {
	static: path.resolve(__dirname, 'src/snowflakes/static'),
	build: path.resolve(__dirname, 'src/snowflakes/static/build'),
}

const plugins = [];
// don't include momentjs locales (large)
//plugins.push(new webpack.IgnorePlugin(/^\.\/locale$/, [/moment$/]));
let chunkFilename = '[name].js';
let mode = 'development';
const TerserPlugin = require('terser-webpack-plugin');
let optimization = {
	minimize: false,
};

if (env === 'production') {
	mode = 'production';
	// uglify code for production
    optimization.minimizer= [
        new TerserPlugin({
			terserOptions: {
				warning: true,
				mangle:true,
			}
		})
	];
	optimization.minimize= true;

	// add chunkhash to chunk names for production only (it's slower)
	chunkFilename = '[name].[chunkhash].js';
}

plugins.push(new webpack.LoaderOptionsPlugin({
	debug: true
}));


// Strip @jsx pragma in react-forms, which makes babel abort
// add babel to load .js files as ES6 and transpile JSX
const rules = [
	    {
	        test: /\.js$/,
	        include: path.resolve(__dirname, 'node_modules/react-forms'),
	        enforce: "pre",
	        loader: 'string-replace-loader',
	        options: {
	            search: '@jsx',
	            replace: 'jsx',
	        }
	    },
	    {
	        test: /\.js$/,
	        include: [
	            path.resolve(__dirname, 'src/snowflakes/static'),
	            path.resolve(__dirname, 'node_modules/react-forms'),
				path.resolve(__dirname, 'node_modules/terser-webpack-plugin/dist'),
	        ],
	        use: { 
				loader: 'babel-loader',
				options: {
					presets: ['@babel/preset-env', '@babel/preset-react', '@babel/flow']
				  }
			}
	    },

];

// const rules = [
//     {
//         test: /\.js$/,
//         include: [
//             PATHS.static,
//             path.resolve(__dirname, 'node_modules/dagre-d3'),
//             path.resolve(__dirname, 'node_modules/superagent'),
//         ],
//         use: {
//                 loader: 'babel-loader',
//             },       
//     },
//     {
//         test: /\.(jpg|png|gif)$/,
//         include: PATHS.images,
//         use: [
//             {
//                 loader: 'url-loader',
//                 options: {
//                     limit: 25000,
//                 },
//             }
//         ],
//     },
//     {
//         test: /\.scss$/,
//         use: [
//             MiniCssExtractPlugin.loader,
//             { loader: 'css-loader', options: { url: false, sourceMap: true } },
//             { loader: 'sass-loader', options: { sourceMap: true } }
//         ],
//     },
// ];

module.exports = [
	// for browser
	{
		context: PATHS.static,
		entry: {inline: './inline'},
		output: {
			path: PATHS.build,
			publicPath: '/static/build/',
			filename: '[name].js',
			chunkFilename: chunkFilename,
		},
		module: {
			rules,
		},
		mode,
		optimization,
		devtool: 'source-map',
		plugins: plugins
	},
	// for server-side rendering
	{
		entry: {
			renderer: './src/snowflakes/static/server.js',
		},
		target: 'node',
		// make sure compiled modules can use original __dirname
		node: {
			__dirname: true,
		},
		externals: [
			'brace',
			'brace/mode/json',
			'brace/theme/solarized_light',
			'd3',
			'dagre-d3',
            // avoid bundling babel transpiler, which is not used at runtime
            '@babel/register',
		],
		output: {
			path: PATHS.build,
			filename: '[name].js',
			libraryTarget: 'commonjs2',
			chunkFilename: chunkFilename,
		},
		module: {
			rules,
		},
		mode,
		optimization,
		devtool: 'source-map',
		plugins: plugins
	}
];
