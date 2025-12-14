# Pathling CDK

This is a CDK construct for deploying Pathling Server to AWS.

It uses ECS Fargate, Application Load Balancer, and S3 for storage. A Varnish
cache layer with EFS backing is included for improved performance.

## Usage

```typescript
import { PathlingStack } from "pathling-cdk";

new PathlingStack(app, "PathlingStack", {
  domainName: "pathling.example.com",
  domainZoneId: "Z1234567890ABC",
  domainZoneName: "example.com",
});
```

## Configuration

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `domainName` | Yes | - | The domain name for the Pathling service |
| `domainZoneId` | Yes | - | The Route 53 hosted zone ID |
| `domainZoneName` | Yes | - | The Route 53 hosted zone name |
| `image` | No | `ghcr.io/aehrc/pathling:latest` | The Pathling server Docker image |
| `cacheImage` | No | `ghcr.io/aehrc/pathling-cache:latest` | The Varnish cache Docker image |
| `deploymentName` | No | `""` | A name to identify this deployment |
| `bucketName` | No | Derived from `deploymentName` | The S3 bucket name for data storage |
| `bucketExists` | No | `false` | Whether to use an existing S3 bucket |
| `cpu` | No | `"2048"` | CPU units for the Fargate task |
| `memoryMiB` | No | `"4096"` | Memory (MiB) for the Fargate task |
| `maxHeapSize` | No | `"3096m"` | JVM max heap size |
| `additionalJavaOptions` | No | - | Additional JVM options |
| `cacheSize` | No | `"1G"` | Varnish cache size |
| `maxAzs` | No | - | Maximum availability zones for the VPC |
| `allowedOriginPatterns` | No | `"https://go.pathling.app"` | CORS allowed origin patterns |
| `sentryDsn` | No | - | Sentry DSN for error reporting |
| `sentryEnvironment` | No | `domainName` | Sentry environment name |
| `additionalConfiguration` | No | `{}` | Additional Pathling configuration |

## Architecture

The stack creates:

- A VPC with public and private subnets
- An ECS Fargate cluster
- A task definition with two containers:
  - Pathling server (port 8080)
  - Varnish cache (port 80)
- An Application Load Balancer with HTTPS
- An S3 bucket for data storage (warehouse and staging)
- An EFS file system for the Varnish cache
- Route 53 DNS records

## Copyright

Copyright © 2022-2025, Commonwealth Scientific and Industrial Research
Organisation (CSIRO) ABN 41 687 119 230. Licensed under the Apache License,
Version 2.0.
