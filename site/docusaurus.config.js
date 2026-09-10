/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

const { execFileSync } = require("child_process");
const { themes } = require("prism-react-renderer");
const lightCodeTheme = themes.github;
const darkCodeTheme = themes.dracula;

/**
 * Finds the latest release version from the git tags that carry the given
 * prefix. Tags are used rather than the POMs because the POMs on main are
 * bumped to the next SNAPSHOT after each release, while the documentation
 * should describe the latest release.
 *
 * @param {string} prefix Tag prefix, e.g. `v` or `server-v`.
 * @returns {string} The highest version carrying the prefix, e.g. `9.9.0`.
 * @throws {Error} If no tag with the prefix exists, e.g. in a shallow clone.
 */
function latestReleaseVersion(prefix) {
  const tags = execFileSync(
    "git",
    ["tag", "--list", `${prefix}[0-9]*`, "--sort=-v:refname"],
    { cwd: __dirname, encoding: "utf8" },
  );
  const latest = tags.split("\n").find((tag) => tag !== "");
  if (!latest) {
    throw new Error(
      `No ${prefix}* release tag found. Fetch tags before building the site.`,
    );
  }
  return latest.slice(prefix.length);
}

// The core libraries and the server are versioned independently, so each
// documentation section reports the latest release of the artifact it
// describes.
const coreVersion = latestReleaseVersion("v");
const serverVersion = latestReleaseVersion("server-v");

/** @type {import("@docusaurus/types").Config} */
const config = {
  title: "Pathling",
  tagline: "Analytics on FHIR&reg;",
  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: "warn",
    },
  },
  themes: ["@docusaurus/theme-mermaid"],
  url: "https://pathling.csiro.au",
  baseUrl: "/",
  onBrokenLinks: "warn",
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
          routeBasePath: "docs",
          lastVersion: "current",
          versions: {
            current: {
              label: coreVersion,
              path: "",
            },
          },
        },
        theme: {
          customCss: require.resolve("./src/css/custom.css"),
        },
        blog: {
          showReadingTime: true,
          blogTitle: "Blog",
          blogDescription: "News and updates from the Pathling project",
          routeBasePath: "blog",
          editUrl: "https://github.com/aehrc/pathling/tree/main/site/",
        },
        sitemap: {},
        pages: {
          // Exclude index files so static/index.html can be served at root.
          exclude: ["**/index.{js,jsx,ts,tsx,md,mdx}"],
        },
      }),
    ],
  ],

  plugins: [
    require.resolve("./src/plugins/staticHomePage.js"),
    [
      "@docusaurus/plugin-content-docs",
      /** @type {import("@docusaurus/plugin-content-docs").Options} */
      ({
        id: "server",
        path: "server-docs",
        routeBasePath: "docs/server",
        sidebarPath: require.resolve("./sidebarsServer.js"),
        editUrl: "https://github.com/aehrc/pathling/tree/main/site/",
        lastVersion: "current",
        versions: {
          current: {
            label: serverVersion,
            path: "",
          },
        },
      }),
    ],
    [
      "@signalwire/docusaurus-plugin-llms-txt",
      {
        siteTitle: "Pathling",
        siteDescription:
          "Tools for FHIR analytics, built on Apache Spark. " +
          "Includes Python, R, Scala and Java libraries, plus a FHIR server.",
        depth: 2,
        content: {
          includeBlog: false,
          includePages: true,
          enableLlmsFullTxt: false,
        },
      },
    ],
  ],

  themeConfig:
    /** @type {import("@docusaurus/preset-classic").ThemeConfig} */
    ({
      // Without this, visiting an archived version is remembered in local
      // storage and the navbar links resolve to that version thereafter.
      docs: {
        versionPersistence: "none",
      },
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
            docsPluginId: "server",
            sidebarId: "server",
            label: "Server",
          },
          {
            label: "Roadmap",
            to: "/roadmap",
          },
          {
            to: "/blog",
            label: "Blog",
            position: "left",
          },
          // Each docs plugin instance has its own version dropdown. CSS in
          // custom.css shows only the one that matches the section being read.
          {
            type: "docsVersionDropdown",
            position: "right",
            className: "navbar__version navbar__version--core",
            dropdownActiveClassDisabled: true,
          },
          {
            type: "docsVersionDropdown",
            position: "right",
            docsPluginId: "server",
            className: "navbar__version navbar__version--server",
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
