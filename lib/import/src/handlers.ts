/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

// noinspection JSUnusedGlobalSymbols

/**
 *  AWS Lambda handlers for the various functions contained in this package.
 *
 *  @author John Grimes
 */

import { Handler } from "aws-lambda";
import { IParameters } from "@ahryman40k/ts-fhir-types/lib/R4";
import {
  checkExportConfigured,
  checkImportStatusConfigured,
  EnvironmentConfig,
  fhirExportConfigured,
  pathlingImportConfigured,
  transferToS3Configured,
} from "./config";
import { ImportResult } from "./import";
import { CheckStatusResult } from "./checkStatus";
import { ExportResult } from "./export";
import { initializeSentry } from "./sentry";

export interface ExportHandlerOutput {
  statusUrl: ExportResult;
}

export interface CheckStatusHandlerOutput {
  result: CheckStatusResult;
}

export interface ImportHandlerInput {
  parameters: IParameters;
}

export interface ImportHandlerOutput {
  statusUrl: ImportResult;
}

const config = new EnvironmentConfig();
initializeSentry(config);

/**
 * Lambda handler for bulk FHIR export.
 */
export const fhirExport: Handler<unknown, ExportHandlerOutput> = async () => ({
  statusUrl: await fhirExportConfigured(config),
});

/**
 * Lambda handler for checking bulk FHIR export status.
 */
export const checkExportStatus: Handler<
  ExportHandlerOutput,
  CheckStatusHandlerOutput
> = async (event) => ({
  result: await checkExportConfigured(config, event),
});

/**
 * Lambda handler for transferring bulk export to S3.
 */
export const transferToS3: Handler<
  CheckStatusHandlerOutput,
  ImportHandlerInput
> = async (event) => ({
  parameters: await transferToS3Configured(config, event),
});

/**
 * Lambda handler for import to Pathling.
 */
export const pathlingImport: Handler<ImportHandlerInput, ImportHandlerOutput> =
  async (event) => ({
    statusUrl: await pathlingImportConfigured(config, event),
  });

/**
 * Lambda handler for checking Pathling import status.
 */
export const checkImportStatus: Handler<
  ExportHandlerOutput,
  CheckStatusHandlerOutput
> = async (event) => ({
  result: await checkImportStatusConfigured(config, event),
});
