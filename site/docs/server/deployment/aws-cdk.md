---
sidebar_position: 2
sidebar_label: AWS CDK
description: Instructions for deploying Pathling server on AWS using the CDK construct.
---

# AWS CDK

The Pathling CDK construct provides a simple way to deploy Pathling Server to
AWS using the [AWS Cloud Development Kit](https://aws.amazon.com/cdk/).

The construct creates a complete deployment including:

- A VPC with public and private subnets
- An ECS Fargate cluster
- A task definition with two containers:
  - Pathling server (port 8080)
  - Varnish cache (port 80)
- An Application Load Balancer with HTTPS
- An S3 bucket for data storage (warehouse and staging)
- An EFS file system for the Varnish cache
- Route 53 DNS records

## Installation

Install the construct from npm:

```bash
npm install pathling-cdk
```

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

| Property                  | Required | Default                              | Description                                    |
| ------------------------- | -------- | ------------------------------------ | ---------------------------------------------- |
| `domainName`              | Yes      | -                                    | The domain name for the Pathling service       |
| `domainZoneId`            | Yes      | -                                    | The Route 53 hosted zone ID                    |
| `domainZoneName`          | Yes      | -                                    | The Route 53 hosted zone name                  |
| `image`                   | No       | `ghcr.io/aehrc/pathling:latest`      | The Pathling server Docker image               |
| `cacheImage`              | No       | `ghcr.io/aehrc/pathling-cache:latest`| The Varnish cache Docker image                 |
| `deploymentName`          | No       | `""`                                 | A name to identify this deployment             |
| `bucketName`              | No       | Derived from `deploymentName`        | The S3 bucket name for data storage            |
| `bucketExists`            | No       | `false`                              | Whether to use an existing S3 bucket           |
| `cpu`                     | No       | `"2048"`                             | CPU units for the Fargate task                 |
| `memoryMiB`               | No       | `"4096"`                             | Memory (MiB) for the Fargate task              |
| `maxHeapSize`             | No       | `"3096m"`                            | JVM max heap size                              |
| `additionalJavaOptions`   | No       | -                                    | Additional JVM options                         |
| `cacheSize`               | No       | `"1G"`                               | Varnish cache size                             |
| `maxAzs`                  | No       | -                                    | Maximum availability zones for the VPC         |
| `allowedOriginPatterns`   | No       | `"https://go.pathling.app"`          | CORS allowed origin patterns                   |
| `sentryDsn`               | No       | -                                    | Sentry DSN for error reporting                 |
| `sentryEnvironment`       | No       | `domainName`                         | Sentry environment name                        |
| `additionalConfiguration` | No       | `{}`                                 | Additional Pathling configuration              |

## Example

This example creates a Pathling deployment with custom resource allocation and
an existing S3 bucket:

```typescript
import { PathlingStack } from "pathling-cdk";

new PathlingStack(app, "MyPathlingServer", {
  domainName: "pathling.myorg.com",
  domainZoneId: "Z1234567890ABC",
  domainZoneName: "myorg.com",
  deploymentName: "production",
  bucketName: "myorg-pathling-data",
  bucketExists: true,
  cpu: "4096",
  memoryMiB: "8192",
  maxHeapSize: "6g",
  allowedOriginPatterns: "https://app.myorg.com",
  additionalConfiguration: {
    "pathling.terminology.serverUrl": "https://tx.myorg.com/fhir",
    "logging.level.au.csiro.pathling": "info",
  },
});
```

## Data storage

The construct configures Pathling to use S3 for data storage:

- **Warehouse**: `s3://[bucket-name]/warehouse` - Where Pathling stores its data
- **Staging**: `s3://[bucket-name]/staging/` - Where you place files for import

To import data, upload NDJSON files to the staging directory and use the
[import operation](/docs/server/operations/import).
