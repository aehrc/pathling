/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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
import { RemovalPolicy } from "aws-cdk-lib";
import { Vpc } from "aws-cdk-lib/aws-ec2";
import {
  AwsLogDriver,
  Cluster,
  Compatibility,
  ContainerImage,
  TaskDefinition,
} from "aws-cdk-lib/aws-ecs";
import { ApplicationLoadBalancedFargateService } from "aws-cdk-lib/aws-ecs-patterns";
import { ApplicationProtocol } from "aws-cdk-lib/aws-elasticloadbalancingv2";
import { Effect, PolicyStatement } from "aws-cdk-lib/aws-iam";
import { RetentionDays } from "aws-cdk-lib/aws-logs";
import { HostedZone } from "aws-cdk-lib/aws-route53";
import {
  BlockPublicAccess,
  Bucket,
  BucketEncryption,
  IBucket,
} from "aws-cdk-lib/aws-s3";
import { paramCase, pascalCase } from "change-case";
import { Construct } from "constructs";

export interface PathlingServerProps extends cdk.StackProps {
  /** The image to use for the Pathling server container. */
  image?: string;

  /** The image to use for the cache container. */
  cacheImage?: string;

  /** The domain name that will be used to host the deployment. */
  domainName: string;

  /** The Route 53 hosted zone that is used to host the domain name. */
  domainZoneId: string;

  /** The name of the Route 53 hosted zone that is used to host the domain name. */
  domainZoneName: string;

  /** The name of the S3 bucket that will be used to store the Pathling server data. */
  bucketName?: string;

  /** Whether the S3 bucket already exists. */
  bucketExists?: boolean;

  /** The number of CPUs to allocate to the Pathling server container. */
  cpu?: string;

  /** The amount of memory to allocate to the Pathling server container. */
  memoryMiB?: string;

  /** The maximum heap size to allocate to the Pathling server container. */
  maxHeapSize?: string;

  /** Additional Java options to pass to the Pathling server container. */
  additionalJavaOptions?: string;

  /** The size of the cache to allocate to the cache container. */
  cacheSize?: string;

  /** The maximum number of availability zones to use for the deployment. */
  maxAzs?: number;

  /** The allowed origins for CORS requests to the server. */
  allowedOrigins?: string;

  /** The Sentry DSN to use for error reporting. */
  sentryDsn?: string;

  /** The Sentry environment to use for error reporting. */
  sentryEnvironment?: string;

  /** Additional configuration to pass to the Pathling server. */
  additionalConfiguration?: { [key: string]: string };
}

interface PathlingServerPropsResolved extends PathlingServerProps {
  image: string;
  cacheImage: string;
  bucketName: string;
  bucketExists: boolean;
  cpu: string;
  memoryMiB: string;
  maxHeapSize: string;
  cacheSize: string;
  allowedOrigins: string;
  additionalConfiguration: { [key: string]: string };
}

/**
 * A CDK stack that deploys a Pathling server, using Fargate and Application
 * Load Balancer.
 *
 * It also deploys an HTTP cache in front of the server.
 *
 * An S3 bucket is used (and optionally created) to support the storage of data
 * by the server.
 *
 * @param scope The parent construct.
 * @param id The ID of the stack, used for tagging.
 * @param props Options that control the configuration of the stack.
 * @author John Grimes
 */
export class PathlingServer extends Construct {
  private readonly deploymentName: string;

  constructor(scope: Construct, id: string, props: PathlingServerProps) {
    super(scope, id);
    const resolvedProps = this.applyDefaults(props),
      { bucketName, bucketExists, maxAzs } = resolvedProps;
    this.deploymentName = id;

    // Create a S3 bucket that will be used for the staging and warehouse locations.
    const bucket = bucketExists
      ? Bucket.fromBucketName(this, this.buildId("Bucket"), bucketName)
      : new Bucket(this, this.buildId("Bucket"), {
          bucketName: bucketName,
          encryption: BucketEncryption.S3_MANAGED,
          enforceSSL: true,
          blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
          removalPolicy: RemovalPolicy.RETAIN,
        });

    // Create a new VPC and cluster that will be used to run the service.
    const vpc = new Vpc(this, this.buildId("Vpc"), { maxAzs });
    const cluster = new Cluster(this, this.buildId("Cluster"), { vpc: vpc });

    // Create the Fargate service on ECS.
    this.service(resolvedProps, bucket, cluster);
  }

  private service(
    props: PathlingServerPropsResolved,
    bucket: IBucket,
    cluster: Cluster
  ): ApplicationLoadBalancedFargateService {
    const { domainName, domainZoneId, domainZoneName } = props;
    const appTaskDefinition = this.taskDefinition(props, bucket);

    // Create a service to wrap the application task.
    const service = new ApplicationLoadBalancedFargateService(
      this,
      this.buildId("AppService"),
      {
        cluster,
        taskDefinition: appTaskDefinition,
        protocol: ApplicationProtocol.HTTPS,
        redirectHTTP: true,
        domainName,
        domainZone: HostedZone.fromHostedZoneAttributes(
          this,
          this.buildId("HostedZone"),
          { hostedZoneId: domainZoneId, zoneName: domainZoneName }
        ),
        enableExecuteCommand: true,
      }
    );

    // Point the load balancer to the cache container.
    service.service.loadBalancerTarget({
      containerName: "pathling-cache",
      containerPort: 80,
    });

    // Define a health check based upon the capability statement.
    service.targetGroup.configureHealthCheck({
      path: "/fhir/metadata",
      port: "80",
      interval: cdk.Duration.seconds(20),
      timeout: cdk.Duration.seconds(5),
      healthyThresholdCount: 2,
      unhealthyThresholdCount: 10,
      healthyHttpCodes: "200,304",
    });

    return service;
  }

  private taskDefinition(
    {
      image,
      cacheImage,
      domainName,
      cpu,
      memoryMiB,
      maxHeapSize,
      additionalJavaOptions,
      cacheSize,
      allowedOrigins,
      sentryDsn,
      sentryEnvironment,
      additionalConfiguration,
    }: PathlingServerPropsResolved,
    bucket: IBucket
  ) {
    // Enables the application to access the S3 warehouse location.
    const warehouseAccessStatement = new PolicyStatement({
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
        `arn:aws:s3:::${bucket.bucketName}`,
      ],
    });

    // Enables the application to access the S3 staging location.
    const stagingAccessStatement = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        "s3:GetObject",
        "s3:ListBucketMultipartUploads",
        "s3:ListBucket",
      ],
      resources: [
        `arn:aws:s3:::${bucket.bucketName}/staging/*`,
        `arn:aws:s3:::${bucket.bucketName}`,
      ],
    });

    // Create a task definition that contains both the application and cache containers.
    const taskDefinition = new TaskDefinition(
      this,
      this.buildId("AppTaskDefinition"),
      {
        compatibility: Compatibility.FARGATE,
        cpu,
        memoryMiB,
        //
        // We haven't yet figured out how to build a Varnish image that can run on Fargate ARM.
        //
        // The error message is:
        // Error: Varnishd is already running (pid=1) (pidfile=/var/lib/varnish/varnishd/_.pid)
        //
        // The command we have been using to build it (locally on a Mac) is:
        // docker buildx build --tag
        // 865780493209.dkr.ecr.ap-southeast-2.amazonaws.com/pathling-cache --platform
        // linux/amd64,linux/arm64/v8 --push --no-cache .
        //
        // runtimePlatform: {
        //   operatingSystemFamily: OperatingSystemFamily.LINUX,
        //   cpuArchitecture: CpuArchitecture.ARM64,
        // },
      }
    );

    // Create the cache container.
    taskDefinition.addContainer(this.buildId("CacheContainer"), {
      containerName: "pathling-cache",
      image: ContainerImage.fromRegistry(cacheImage),
      portMappings: [{ containerPort: 80 }],
      environment: {
        PATHLING_HOST: "localhost",
        PATHLING_PORT: "8080",
        VARNISH_STORAGE: `file,/var/lib/varnish/cache,${cacheSize}`,
      },
      logging: AwsLogDriver.awsLogs({
        streamPrefix: "cache",
        logRetention: RetentionDays.ONE_MONTH,
      }),
    });

    // Create the application container.
    taskDefinition.addContainer(this.buildId("AppContainer"), {
      containerName: "pathling",
      image: ContainerImage.fromRegistry(image),
      portMappings: [{ containerPort: 8080 }],
      environment: {
        JAVA_TOOL_OPTIONS: `-Duser.timezone=UTC -Xmx${maxHeapSize} ${
          additionalJavaOptions ?? ""
        }`,
        "pathling.import.allowableSources": `s3://${bucket.bucketName}/staging/`,
        "pathling.storage.warehouseUrl": `s3://${bucket.bucketName}/warehouse`,
        "pathling.cors.allowedOrigins": allowedOrigins,
        "logging.level.au.csiro.pathling": "debug",
        "fs.s3a.aws.credentials.provider":
          "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper",
        ...(sentryDsn ? { "pathling.sentryDsn": sentryDsn } : {}),
        ...{ "pathling.sentryEnvironment": sentryEnvironment ?? domainName },
        ...additionalConfiguration,
      },
      logging: AwsLogDriver.awsLogs({
        streamPrefix: "server",
        logRetention: RetentionDays.ONE_MONTH,
      }),
    });

    // Add the policy statements to the task definition.
    taskDefinition.addToTaskRolePolicy(warehouseAccessStatement);
    taskDefinition.addToTaskRolePolicy(stagingAccessStatement);

    return taskDefinition;
  }

  private buildId(id: string): string {
    return `${this.deploymentName}${pascalCase(id)}`;
  }

  private applyDefaults(
    props: PathlingServerProps
  ): PathlingServerPropsResolved {
    const {
      image = "aehrc/pathling:6",
      cacheImage = "aehrc/pathling-cache:latest",
      bucketName,
      bucketExists = false,
      cpu = "2048",
      memoryMiB = "4096",
      maxHeapSize = "3096m",
      cacheSize = "1G",
      allowedOrigins = "https://go.pathling.app",
      additionalConfiguration = {},
    } = props;
    return {
      ...props,
      image,
      cacheImage,
      bucketName: bucketName || paramCase(this.deploymentName),
      bucketExists: bucketExists,
      cpu,
      memoryMiB,
      maxHeapSize,
      cacheSize,
      allowedOrigins,
      additionalConfiguration,
    };
  }
}
