/*
 * Copyright 2022-2025 Commonwealth Scientific and Industrial Research
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

import * as cdk from "aws-cdk-lib";
import { RemovalPolicy, Stack } from "aws-cdk-lib";
import * as apprunner from "aws-cdk-lib/aws-apprunner";
import {
  Effect,
  PolicyStatement,
  Role,
  ServicePrincipal,
} from "aws-cdk-lib/aws-iam";
import {
  BlockPublicAccess,
  Bucket,
  BucketEncryption,
  IBucket,
} from "aws-cdk-lib/aws-s3";
import { paramCase } from "change-case";
import { Construct } from "constructs";

// ECR Public image (App Runner requires ECR or ECR Public).
const DEFAULT_IMAGE = "public.ecr.aws/y4w7z7a1/pathling:latest";

export interface PathlingStackProps extends cdk.StackProps {
  image?: string;
  deploymentName?: string;
  bucketName?: string;
  bucketExists?: boolean;
  cpu?: string;
  memory?: string;
  maxHeapSize?: string;
  additionalJavaOptions?: string;
  allowedOrigins?: string;
  sentryDsn?: string;
  sentryEnvironment?: string;
  additionalConfiguration?: { [key: string]: string };
}

export class PathlingStack extends Stack {
  constructor(scope: Construct, id: string, props: PathlingStackProps) {
    super(scope, id, props);

    const {
      image = DEFAULT_IMAGE,
      deploymentName = "pathling",
      bucketName = paramCase(deploymentName),
      bucketExists = false,
      cpu = "2 vCPU",
      memory = "4 GB",
      maxHeapSize = "2800m",
      additionalJavaOptions,
      allowedOrigins = "https://go.pathling.app",
      sentryDsn,
      sentryEnvironment,
      additionalConfiguration = {},
    } = props;

    // Tag all resources.
    this.tags.setTag("PathlingDeployment", deploymentName);

    // Create or reference S3 bucket.
    const bucket: IBucket = bucketExists
      ? Bucket.fromBucketName(this, "Bucket", bucketName)
      : new Bucket(this, "Bucket", {
          bucketName,
          encryption: BucketEncryption.S3_MANAGED,
          enforceSSL: true,
          blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
          removalPolicy: RemovalPolicy.RETAIN,
        });

    // Create IAM role for App Runner instance.
    const instanceRole = new Role(this, "InstanceRole", {
      assumedBy: new ServicePrincipal("tasks.apprunner.amazonaws.com"),
    });
    instanceRole.addToPolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucketMultipartUploads",
          "s3:AbortMultipartUpload",
          "s3:ListBucket",
          "s3:DeleteObject",
          "s3:ListMultipartUploadParts",
        ],
        resources: [
          `arn:aws:s3:::${bucket.bucketName}/warehouse/*`,
          `arn:aws:s3:::${bucket.bucketName}/staging/*`,
          `arn:aws:s3:::${bucket.bucketName}`,
        ],
      }),
    );

    // Build JVM options.
    const jvmOptions = [
      "-Duser.timezone=UTC",
      `-Xmx${maxHeapSize}`,
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      additionalJavaOptions ?? "",
    ]
      .filter(Boolean)
      .join(" ");

    // Build environment variables.
    const environmentVariables: { [key: string]: string } = {
      JAVA_TOOL_OPTIONS: jvmOptions,
      "pathling.import.allowableSources": `s3://${bucket.bucketName}/staging/`,
      "pathling.storage.warehouseUrl": `s3://${bucket.bucketName}/warehouse`,
      "pathling.cors.allowedOrigins": allowedOrigins,
      "logging.level.au.csiro.pathling": "debug",
      ...(sentryDsn ? { "pathling.sentryDsn": sentryDsn } : {}),
      ...(sentryEnvironment
        ? { "pathling.sentryEnvironment": sentryEnvironment }
        : {}),
      ...additionalConfiguration,
    };

    // Convert to App Runner format.
    const runtimeEnvironmentVariables = Object.entries(environmentVariables).map(
      ([name, value]) => ({ name, value }),
    );

    // Create App Runner service using L1 construct.
    const service = new apprunner.CfnService(this, "Service", {
      serviceName: deploymentName,
      sourceConfiguration: {
        imageRepository: {
          imageIdentifier: image,
          imageRepositoryType: "ECR_PUBLIC",
          imageConfiguration: {
            port: "8080",
            runtimeEnvironmentVariables,
          },
        },
        autoDeploymentsEnabled: false,
      },
      instanceConfiguration: {
        cpu,
        memory,
        instanceRoleArn: instanceRole.roleArn,
      },
      healthCheckConfiguration: {
        protocol: "HTTP",
        path: "/fhir/metadata",
        interval: 5,
        timeout: 5,
        healthyThreshold: 1,
        unhealthyThreshold: 3,
      },
    });

    // Output the service URL.
    new cdk.CfnOutput(this, "ServiceUrl", {
      value: `https://${service.attrServiceUrl}`,
    });
  }
}
