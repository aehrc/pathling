---
layout: page title: Libraries nav_order: 7 parent: Documentation
---

# Libraries

A number of libraries have been created to help with the task of integrating
with Pathling from commonly-used languages.

## pathling-client

The [pathling-client](https://www.npmjs.com/package/pathling-client) NPM package
provides a JavaScript/TypeScript client for the Pathling REST API. It can be
used from the browser, or via Node.js.

It features support for `import`, `aggregate`, `search` and `extract`, along
with asynchronous processing and query of the server conformance statement and
SMART configuration.

## pathling-import

The [pathling-import](https://www.npmjs.com/package/pathling-import) NPM package
provides a set of functions written in TypeScript that facilitate the export of
resources from a FHIR server and import into Pathling, via a staging S3 bucket.
It is also designed to be used with AWS Lambda for serverless hosting and
execution of this functionality on AWS.

Next: [Roadmap](./roadmap.html)