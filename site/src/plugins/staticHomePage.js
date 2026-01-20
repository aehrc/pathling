/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const fs = require("fs");
const path = require("path");

/**
 * A Docusaurus plugin that serves a custom static landing page at the root
 * path instead of a Docusaurus-generated page.
 *
 * During build: copies static/index.html to the output after the build.
 * During dev: intercepts requests to "/" and serves the static file.
 *
 * @returns {import("@docusaurus/types").Plugin} The plugin configuration.
 * @author John Grimes
 */
module.exports = function staticHomePagePlugin() {
  return {
    name: "static-home-page",

    async postBuild({ outDir, siteDir }) {
      const staticIndexPath = path.join(siteDir, "static", "index.html");
      const outputIndexPath = path.join(outDir, "index.html");

      if (fs.existsSync(staticIndexPath)) {
        fs.copyFileSync(staticIndexPath, outputIndexPath);
      }
    },

    configureWebpack(config, isServer, utils, content) {
      // Find and modify the CopyPlugin to exclude index.html from static.
      const CopyPlugin = config.plugins?.find(
        (p) => p.constructor.name === "CopyPlugin",
      );
      if (CopyPlugin && CopyPlugin.patterns) {
        CopyPlugin.patterns.forEach((pattern) => {
          if (!pattern.globOptions) {
            pattern.globOptions = {};
          }
          if (!pattern.globOptions.ignore) {
            pattern.globOptions.ignore = [];
          }
          pattern.globOptions.ignore.push("**/index.html");
        });
      }

      return {
        devServer: {
          setupMiddlewares(middlewares, devServer) {
            // Serve static index.html for root path during development.
            devServer.app.get("/", (req, res, next) => {
              const staticDir = devServer.options.static?.[0]?.directory;
              if (staticDir) {
                const staticIndexPath = path.join(staticDir, "index.html");
                if (fs.existsSync(staticIndexPath)) {
                  res.sendFile(staticIndexPath);
                  return;
                }
              }
              next();
            });
            return middlewares;
          },
        },
      };
    },
  };
};
