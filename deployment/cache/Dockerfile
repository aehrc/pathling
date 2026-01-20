# Copyright © 2025, Commonwealth Scientific and Industrial Research Organisation (CSIRO)
# ABN 41 687 119 230. Licensed under the Apache License, Version 2.0.

FROM varnish:7.0

COPY entrypoint.sh /usr/local/bin/pathling-cache-entrypoint
RUN chmod +x /usr/local/bin/pathling-cache-entrypoint

COPY default.vcl /etc/varnish/default.vcl

ENTRYPOINT ["/usr/local/bin/pathling-cache-entrypoint"]
