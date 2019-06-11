/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

const path = require("path"),
  MiniCssExtractPlugin = require("mini-css-extract-plugin"),
  HtmlWebpackPlugin = require("html-webpack-plugin"),
  MonacoEditorWebpackPlugin = require("monaco-editor-webpack-plugin"),
  BundleAnalyzerPlugin = require("webpack-bundle-analyzer")
    .BundleAnalyzerPlugin,
  MONACO_DIR = path.resolve(__dirname, "./node_modules/monaco-editor");

module.exports = {
  entry: "./src/index.tsx",
  resolve: {
    extensions: [".ts", ".tsx", ".js"]
  },
  output: {
    path: path.resolve(__dirname, "dist"),
    filename:
      process.env.NODE_ENV === "development"
        ? "script/[name].[hash:8].js"
        : "script/[name].[contenthash:8].js"
  },
  module: {
    rules: [
      {
        test: /\.(ts|tsx|js)$/,
        use: {
          loader: "ts-loader"
        },
        exclude: /node_modules/
      },
      {
        test: /\.scss$/,
        use: [
          {
            loader: MiniCssExtractPlugin.loader,
            options: {
              hmr: process.env.NODE_ENV === "development",
              reloadAll: true
            }
          },
          "css-loader",
          "postcss-loader",
          "sass-loader"
        ]
      },
      {
        test: /\.css$/,
        include: MONACO_DIR,
        use: [
          {
            loader: MiniCssExtractPlugin.loader,
            options: {
              hmr: process.env.NODE_ENV === "development",
              reloadAll: true
            }
          },
          "css-loader"
        ]
      },
      {
        test: /\.(ttf|eot|woff)$/,
        use: {
          loader: "file-loader",
          options: {
            name: "[hash].[ext]",
            outputPath: "fonts",
            publicPath: "../fonts"
          }
        }
      }
    ]
  },
  plugins: [
    new MiniCssExtractPlugin({
      filename: "style/[name].[contenthash:8].css"
    }),
    new HtmlWebpackPlugin({
      inject: true,
      template: path.resolve(__dirname, "src", "index.html")
    }),
    new MonacoEditorWebpackPlugin(),
    new BundleAnalyzerPlugin({ analyzerMode: "static", openAnalyzer: false })
  ],
  devServer: {
    // Put your local `config.json` in the `.local` directory.
    contentBase: path.resolve(__dirname, ".local"),
    compress: true
  },
  devtool: process.env.NODE_ENV === "development" ? "source-map" : false
};
