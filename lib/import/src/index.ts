#!/usr/bin/env node
// noinspection JSUnusedGlobalSymbols

/**
 * This module provides a binary that can be used to run the importer as a Node.js command line
 * program.
 *
 * @author John Grimes
 */

import * as fs from "fs";
import pRetry, { FailedAttemptError } from "p-retry";
import {
  checkExportConfigured,
  checkImportStatusConfigured,
  Config,
  EnvironmentConfig,
  fhirExportConfigured,
  FileConfig,
  pathlingImportConfigured,
  RetryConfigType,
  transferToS3Configured,
} from "./config";

let config: Config;
const argument = process.argv[2],
  configFileName = argument ? argument : "pathling-import.config.json";
try {
  const configFile = fs.openSync(configFileName, "r");
  config = new FileConfig(configFile);
} catch (e) {
  console.info(
    "Configuration file not readable, will use environment variables: %s",
    e
  );
  config = new EnvironmentConfig();
}

async function run() {
  function retry(
    type: RetryConfigType,
    message: string,
    promise: () => Promise<any>
  ) {
    const retryConfig = config.getRetryConfig(type);

    return pRetry(
      (attemptCount) => {
        console.info("%s, attempt %d", message, attemptCount);
        return promise();
      },
      {
        retries: retryConfig.times,
        minTimeout: retryConfig.wait * 1000,
        factor: retryConfig.backOff,
        onFailedAttempt: (error: FailedAttemptError) =>
          console.info(
            "Attempt #%d not complete, %d retries left - %s",
            error.attemptNumber,
            error.retriesLeft,
            error.message
          ),
      }
    );
  }

  const kickoffResult = await fhirExportConfigured(config);
  const exportStatusResult = await retry(
    "export",
    "Checking export status",
    () => checkExportConfigured(config, { statusUrl: kickoffResult })
  );
  const transferToS3Result = await retry(
    "transfer",
    "Transferring data to S3",
    () => transferToS3Configured(config, { result: exportStatusResult })
  );
  const importResult = await pathlingImportConfigured(config, {
    parameters: transferToS3Result,
  });
  if (!importResult) {
    // This means that there was nothing to import, and we can skip the import status checking.
    return;
  }
  const importStatusResult = await retry(
    "import",
    "Importing data into Pathling",
    () => checkImportStatusConfigured(config, { statusUrl: importResult })
  );
  console.info("Import complete: %j", importStatusResult);
}

run().catch((error) => {
  console.error(error);
  process.exit(1);
});

process.on("unhandledRejection", (reason) => {
  console.error(reason);
});
process.on("uncaughtException", (reason) => {
  console.error(reason);
});
