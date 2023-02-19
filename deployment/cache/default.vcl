// Copyright 2023 Commonwealth Scientific and Industrial Research
// Organisation (CSIRO) ABN 41 687 119 230.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
