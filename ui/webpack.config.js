/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

const path = require("path"),
  HtmlWebpackPlugin = require("html-webpack-plugin"),
  BundleAnalyzerPlugin = require("webpack-bundle-analyzer")
    .BundleAnalyzerPlugin;

module.exports = {
  entry: "./src/index.tsx",
  resolve: {
    extensions: [".ts", ".tsx", ".js"]
  },
  output: {
    path: path.resolve(__dirname, "dist"),
    filename: "[name].[hash:8].js"
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
        use: ["style-loader", "css-loader", "sass-loader"]
      },
      {
        test: /\.(ttf|eot|woff)$/,
        use: {
          loader: "file-loader",
          options: {
            name: "fonts/[hash].[ext]"
          }
        }
      }
    ]
  },
  plugins: [
    new HtmlWebpackPlugin({
      inject: true,
      template: path.resolve(__dirname, "src", "index.html")
    }),
    new BundleAnalyzerPlugin({ analyzerMode: "static", openAnalyzer: false })
  ],
  devServer: {
    contentBase: path.resolve(__dirname, "dist"),
    compress: true
  },
  devtool: "source-map"
};
