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
import { Vpc } from "aws-cdk-lib/aws-ec2";
import {
  AwsLogDriver,
  Cluster,
  ContainerImage,
  FargateTaskDefinition,
} from "aws-cdk-lib/aws-ecs";
import { ApplicationLoadBalancedFargateService } from "aws-cdk-lib/aws-ecs-patterns";
import { FileSystem } from "aws-cdk-lib/aws-efs";
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

const DEFAULT_IMAGE = "ghcr.io/aehrc/pathling:latest";
const DEFAULT_CACHE_IMAGE = "ghcr.io/aehrc/pathling-cache:latest";

export interface PathlingStackProps extends cdk.StackProps {
  domainName: string;
  domainZoneId: string;
  domainZoneName: string;
  image?: string;
  cacheImage?: string;
  deploymentName?: string;
  bucketName?: string;
  bucketExists?: boolean;
  cpu?: string;
  memoryMiB?: string;
  maxHeapSize?: string;
  additionalJavaOptions?: string;
  cacheSize?: string;
  maxAzs?: number;
  allowedOrigins?: string;
  sentryDsn?: string;
  sentryEnvironment?: string;
  additionalConfiguration?: { [key: string]: string };
}

export interface PathlingStackPropsResolved extends PathlingStackProps {
  image: string;
  cacheImage: string;
  deploymentName: string;
  bucketName: string;
  bucketExists: boolean;
  cpu: string;
  memoryMiB: string;
  maxHeapSize: string;
  cacheSize: string;
  allowedOrigins: string;
  additionalConfiguration: { [key: string]: string };
}

export class PathlingStack extends Stack {
  private readonly deploymentName: string;

  constructor(scope: Construct, id: string, props: PathlingStackProps) {
    super(scope, id, props);
    const resolvedProps = this.applyDefaults(props),
      { deploymentName, bucketName, bucketExists, maxAzs } = resolvedProps;
    this.deploymentName = deploymentName;

    // All resources created by this stack will be tagged with a name of "PathlingDeployment" and a
    // value equal to the deployment name.
    this.tags.setTag("PathlingDeployment", this.deploymentName);

    // Create an S3 bucket that will be used for the staging and warehouse locations.
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

    // Create an EFS file system that will be used for the cache.
    const cacheFileSystem = new FileSystem(this, this.buildId("FileSystem"), {
      vpc,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // Create the Fargate service on ECS.
    const service = this.service(
      resolvedProps,
      bucket,
      cluster,
      cacheFileSystem,
    );

    // Allow access to EFS from the service.
    cacheFileSystem.connections.allowDefaultPortFrom(
      service.service.connections,
      `Allow traffic from Pathling service: ${deploymentName}`,
    );
  }

  private service(
    props: PathlingStackPropsResolved,
    bucket: IBucket,
    cluster: Cluster,
    cacheFileSystem: FileSystem,
  ): ApplicationLoadBalancedFargateService {
    const { domainName, domainZoneId, domainZoneName } = props;
    const appTaskDefinition = this.taskDefinition(
      props,
      bucket,
      cacheFileSystem,
    );

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
          { hostedZoneId: domainZoneId, zoneName: domainZoneName },
        ),
        enableExecuteCommand: true,
      },
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
      interval: cdk.Duration.seconds(30),
      timeout: cdk.Duration.seconds(10),
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
    }: PathlingStackPropsResolved,
    bucket: IBucket,
    cacheFileSystem: FileSystem,
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
    const taskDefinition = new FargateTaskDefinition(
      this,
      this.buildId("AppTaskDefinition"),
      {
        cpu: parseInt(cpu),
        memoryLimitMiB: parseInt(memoryMiB),
      },
    );

    // Create the cache container.
    const cacheContainer = taskDefinition.addContainer(
      this.buildId("CacheContainer"),
      {
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
      },
    );
    // Add a volume pointing to the cache EFS file system.
    taskDefinition.addVolume({
      name: "cache",
      efsVolumeConfiguration: { fileSystemId: cacheFileSystem.fileSystemId },
    });
    // Add a mount point for the cache volume.
    cacheContainer.addMountPoints({
      sourceVolume: "cache",
      containerPath: "/var/lib/varnish",
      readOnly: false,
    });

    // Build the JVM options string with required Spark/Hadoop flags.
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

    // Create the application container.
    taskDefinition.addContainer(this.buildId("AppContainer"), {
      containerName: "pathling",
      image: ContainerImage.fromRegistry(image),
      portMappings: [{ containerPort: 8080 }],
      environment: {
        JAVA_TOOL_OPTIONS: jvmOptions,
        "pathling.import.allowableSources": `s3://${bucket.bucketName}/staging/`,
        "pathling.storage.warehouseUrl": `s3://${bucket.bucketName}/warehouse`,
        "pathling.cors.allowedOrigins": allowedOrigins,
        "logging.level.au.csiro.pathling": "debug",
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

  private applyDefaults(props: PathlingStackProps): PathlingStackPropsResolved {
    const {
      image = DEFAULT_IMAGE,
      cacheImage = DEFAULT_CACHE_IMAGE,
      bucketName,
      bucketExists = false,
      deploymentName = "",
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
      deploymentName,
      bucketName: bucketName || paramCase(deploymentName),
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
