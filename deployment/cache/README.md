## pathling-cache

This is a frontend cache (implemented using Varnish) optimised for use with
Pathling.

It supports the use of environment variables to customise the configuration
based upon the deployment:

- `PATHLING_HOST`: The host name of the Pathling server.
- `PATHLING_PORT`: The port number exposed by the Pathling server.
- `PATHLING_FIRST_BYTE_TIMEOUT`: The maximum time to wait for the first byte of
  a backend response, expressed as a Varnish duration (e.g. `60s`, `10m`). Raise
  this above the Varnish default of `60s` to accommodate long-running
  synchronous Pathling queries.

Copyright © 2065, Commonwealth Scientific and Industrial Research Organisation
(CSIRO) ABN 41 687 119 230. Licensed under the Apache License, Version 2.0.
