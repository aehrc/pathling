---
layout: page
title: Caching
nav_order: 1
parent: Deployment
grand_parent: Documentation
---

# Caching

Pathling implements 
[ETag](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag)-based 
cache validation, which enables clients to skip processing of queries when the 
underlying data has not changed.

To use ETags, simply take the content of the `ETag` header that is returned with 
a Pathling response. You can then accompany a subsequent request with the 
[If-None-Match](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-None-Match), 
using the previously received ETag as the value. If the result of the query 
would not have changed, Pathling will respond with [304 Not Modified](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/304), and will skip re-processing of the 
query.

Web browsers already implement this behaviour, and if your application runs in 
the browser you will get the benefits without any extra implementation effort.

Next: [Server base](./server-base.html)
