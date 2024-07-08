---
description: A set of functions written in TypeScript that facilitate the export of resources from a FHIR server and import into Pathling, via a staging S3 bucket. They can also be used as AWS Lambda functions.
---

# pathling-import

[pathling-import](https://www.npmjs.com/package/pathling-import) is a set of
functions written in TypeScript that facilitate the export of resources from a
FHIR server and import into Pathling, via a staging S3 bucket. They can also be
used as AWS Lambda functions.

## Use with AWS Lambda

This package includes a module named `handlers` which exports the functions
described below. AWS Lambda functions are configured using environment
variables - the supported variables are described for each function.

### fhirExport

Kicks off a bulk export operation at a FHIR endpoint.

**Input:** `[none]`

**Output:** `{ "statusUrl": [string] }`

**Configuration:**

- `SOURCE_ENDPOINT`: The base URL of the source FHIR server.
- `TYPES`: (optional) A comma-delimited set of resource types for which export
  will be requested.
- `SINCE`: A [FHIR instant](https://hl7.org/fhir/R4/datatypes.html#instant)
  formatted time used to filter the results based on their last updated time.
- `SOURCE_CLIENT_ID`, `SOURCE_CLIENT_SECRET`: Client credentials for the source
  token grant.
- `SOURCE_SCOPES`: Scopes to request as part of the source token grant.

### checkExportStatus

Gets the result of a FHIR bulk export operation.

**Input:** `{ "statusUrl": [string] }`

**Output:** `{ "result": [FHIR bulk export result] }`, or a `JobInProgressError`
thrown

Configuration:

- `SOURCE_ENDPOINT`: The base URL of the source FHIR server.
- `SOURCE_CLIENT_ID`, `SOURCE_CLIENT_SECRET`: Client credentials for the source
  token grant.
- `SOURCE_SCOPES`: Scopes to request as part of the source token grant.

### transferToS3

**Input:** `{ "result": [FHIR bulk export result] }`

**Output:** `{ "parameters": [Pathling import operation input parameters] }`

Downloads a FHIR bulk export and uploads it to S3.

**Configuration:**

- `SOURCE_ENDPOINT`: The base URL of the source FHIR server.
- `SOURCE_CLIENT_ID`, `SOURCE_CLIENT_SECRET`: Client credentials for the source
  token grant.
- `SOURCE_SCOPES`: Scopes to request as part of the source token grant.
- `STAGING_URL`: An S3 location that can be used to stage NDJSON files from the
  source ahead of import to Pathling.

### pathlingImport

Kicks off a Pathling import operation.

**Input:** `{ "parameters": [Pathling import operation input parameters] }`

**Output:** `{ "statusUrl": [string] }`

**Configuration:**

- `TARGET_ENDPOINT`: The base URL of the target Pathling server.
- `TARGET_CLIENT_ID`, `TARGET_CLIENT_SECRET`: Client credentials for the target
  token grant.
- `TARGET_SCOPES`: (optional, defaults to `user/*.write`) Scopes to request as
  part of the target token grant.

### AWS Step Functions

[AWS Step Functions](https://docs.aws.amazon.com/step-functions/latest/dg/welcome.html)
is an orchestration service that provides a convenient way of running the Lambda
functions that you create using the handlers.

Here is an example of some Amazon States Language configuration that can run
these functions to keep a FHIR endpoint in sync with Pathling:

```json
{
  "Comment": "A workflow that exports data from a FHIR server, stages it on S3 and imports it into a Pathling instance",
  "StartAt": "FHIR export",
  "States": {
    "FHIR export": {
      "Type": "Task",
      "Resource": "[lambda arn]",
      "Next": "Check export status",
      "Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ], "IntervalSeconds": 60, "MaxAttempts": 5
        }
      ]
    }, "Check export status": {
      "Type": "Task", "Resource": "[lambda arn]", "Retry": [
        {
          "ErrorEquals": [
            "JobInProgressError"
          ], "IntervalSeconds": 900, "MaxAttempts": 24, "BackoffRate": 1
        }, {
          "ErrorEquals": [
            "States.ALL"
          ], "IntervalSeconds": 60, "MaxAttempts": 5
        }
      ], "Next": "Transfer to S3"
    }, "Transfer to S3": {
      "Type": "Task", "Resource": "[lambda arn]", "Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ], "IntervalSeconds": 60, "MaxAttempts": 5
        }
      ], "Next": "Pathling import"
    }, "Pathling import": {
      "Type": "Task",
      "Resource": "[lambda arn]",
      "Next": "Check import result",
      "Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ], "IntervalSeconds": 60, "MaxAttempts": 5
        }
      ]
    }, "Check import result": {
      "Type": "Choice", "Choices": [
        {
          "Variable": "$.statusUrl",
          "IsNull": true,
          "Next": "Import not necessary"
        }
      ], "Default": "Check import status"
    }, "Import not necessary": {
      "Type": "Succeed"
    }, "Check import status": {
      "Type": "Task", "Resource": "[lambda arn]", "Retry": [
        {
          "ErrorEquals": [
            "JobInProgressError"
          ], "IntervalSeconds": 300, "MaxAttempts": 12, "BackoffRate": 1
        }, {
          "ErrorEquals": [
            "States.ALL"
          ], "IntervalSeconds": 60, "MaxAttempts": 5
        }
      ], "End": true
    }
  }
}
```

[CloudWatch Events](https://docs.aws.amazon.com/step-functions/latest/dg/tutorial-cloudwatch-events-target.html)
can be used to schedule the running of the step function at a specified
frequency.

### Sentry support

This program can also report any errors to Sentry - just set the `SENTRY_DSN`,
`SENTRY_ENVIRONMENT` (optional) and `SENTRY_RELEASE` (optional) environment
variables.

## Use as a Node.js command-line program

These functions can also be installed as an NPM package, using the following
command:

```
npm -g install pathling-import
```

This provides the ability to run the following command:

```
pathling-import [configuration file]
```

This command requires all of the environment variables described so far in this
document. It will orchestrate the entire sequence of functions, including retry
of job status checks and the S3 transfer. These additional optional variables
control the retry functionality:

- `EXPORT_RETRY_TIMES`, `TRANSFER_RETRY_TIMES`, `IMPORT_RETRY_TIMES` -
  (default: `24`) The maximum number of times each operation will be retried.
- `EXPORT_RETRY_WAIT`, `TRANSFER_RETRY_WAIT`, `IMPORT_RETRY_WAIT` -
  (default: `900`) The number of seconds to wait before retrying the operation.
- `EXPORT_RETRY_BACK_OFF`, `TRANSFER_RETRY_BACK_OFF`, `IMPORT_RETRY_BACK_OFF` -
  (default: `1.0`) The factor by which to multiply the wait upon each retry.

The configuration file argument is optional - you can configure the program
using either a JSON file or environment variables. If you are using a JSON file,
the variables translate to keys in lower camel case.

When running this command, you will also need to provide AWS credentials using
any of the methods described in
[Setting Credentials in Node.js](https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/setting-credentials-node.html)
.
The AWS credentials provided will require the `GetObject` and `PutObject`
permissions on the specified S3 location.

## Important notes

* This script assumes that both source and target endpoints can be accessed
  using an OAuth2 client credentials grant.
* The source FHIR endpoint must provide a
  [SMART configuration document](https://www.hl7.org/fhir/smart-app-launch/conformance.html#using-well-known)
  for discovery of its authentication configuration.
