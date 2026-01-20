// Copyright © 2025, Commonwealth Scientific and Industrial Research Organisation (CSIRO)
// ABN 41 687 119 230. Licensed under the Apache License, Version 2.0.

vcl 4.0;
import std;

backend default {
  .host = "${PATHLING_HOST}";
  .port = "${PATHLING_PORT}";
}

sub vcl_recv {
  if (req.http.Cache-Control ~ "(private|no-cache|no-store)" || req.http.Pragma == "no-cache") {
    return (pass);
  }
}

sub vcl_req_authorization {
  return;
}

sub vcl_backend_response {
  // Respect backend cache-control headers that indicate the response should not be cached.
  if (beresp.http.Cache-Control ~ "(no-cache|no-store|private)") {
    set beresp.uncacheable = true;
    return (deliver);
  }
  set beresp.grace = 0s;
  set beresp.keep = 365d;
  set beresp.ttl = 1s;
  set beresp.do_gzip = true;
  if (beresp.status == 202) {
    set beresp.http.x-original-status = beresp.status;
    set beresp.status = 200;
  }
  return (deliver);
}

sub vcl_deliver {
  set resp.status = std.integer(resp.http.x-original-status, resp.status);
  unset resp.http.x-original-status;
}
