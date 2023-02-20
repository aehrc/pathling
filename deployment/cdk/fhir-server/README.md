## Pathling CDK

This is a CDK construct for deploying Pathling to AWS.

It uses Fargate, ALB and S3, and includes a front-side cache.

Note that this construct currently assumes that the endpoint will be hosted at a
domain name that is managed within a Route 53 hosted zone. This is so that an
ACM certificate can be automatically created for the endpoint.

Copyright © 2023, Commonwealth Scientific and Industrial Research Organisation 
(CSIRO) ABN 41 687 119 230. Licensed under the 
[Apache License, version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
