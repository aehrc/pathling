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

// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const { themes } = require("prism-react-renderer");
const lightCodeTheme = themes.github;
const darkCodeTheme = themes.dracula;

/** @type {import("@docusaurus/types").Config} */
const config = {
  title: "Pathling",
  tagline: "Analytics on FHIR&reg;",
  url: "https://pathling.csiro.au",
  baseUrl: "/",
  onBrokenLinks: "warn",
  onBrokenMarkdownLinks: "warn",
  favicon: "favicon.ico",

  organizationName: "aehrc",
  projectName: "pathling",
  trailingSlash: false,

  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  presets: [
    [
      "classic",
      /** @type {import("@docusaurus/preset-classic").Options} */
      ({
        docs: {
          sidebarPath: require.resolve("./sidebars.js"),
          editUrl: "https://github.com/aehrc/pathling/tree/main/site/",
          lastVersion: "current",
          versions: {
            current: {
              label: "8.0.2-SNAPSHOT",
              path: "/",
            },
            "7.2.0": {
              label: "7.2.0",
              path: "7.2.0",
              banner: "none",
              noIndex: true,
            },
          },
        },
        theme: {
          customCss: require.resolve("./src/css/custom.css"),
        },
        sitemap: {},
      }),
    ],
  ],

  themeConfig:
    /** @type {import("@docusaurus/preset-classic").ThemeConfig} */
    ({
      navbar: {
        title: null,
        logo: {
          alt: "Pathling",
          src: "assets/images/logo-colour.svg",
          srcDark: "assets/images/logo-colour-dark.svg",
          href: "https://pathling.csiro.au",
        },
        items: [
          {
            type: "doc",
            position: "left",
            docId: "index",
            label: "Overview",
          },
          {
            type: "docSidebar",
            position: "left",
            sidebarId: "libraries",
            label: "Libraries",
          },
          {
            type: "docSidebar",
            position: "left",
            sidebarId: "fhirpath",
            label: "FHIRPath",
          },
          {
            type: "docSidebar",
            position: "left",
            sidebarId: "server",
            label: "Server",
          },
          {
            label: "Roadmap",
            to: "/roadmap",
          },
          {
            type: "docsVersionDropdown",
            position: "right",
            dropdownActiveClassDisabled: true,
          },
          {
            href: "https://github.com/aehrc/pathling",
            label: "GitHub",
            position: "right",
          },
        ],
      },
      footer: {
        copyright: `This documentation is dedicated to the public domain via <a href="https://creativecommons.org/publicdomain/zero/1.0/">CC0</a>.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
        additionalLanguages: ["java", "scala", "yaml", "docker", "r"],
      },
      image: "/assets/images/social-preview.png",
    }),
};

module.exports = config;
