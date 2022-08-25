/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

/**
 * @author John Grimes
 */

import Case from "case";
import fs from "node:fs";
import { checkExportJobStatus, checkImportJobStatus } from "./checkStatus.js";
import { fhirBulkExport, FhirBulkResult } from "./export.js";
import {
  CheckStatusHandlerOutput,
  ExportHandlerOutput,
  ImportHandlerInput,
} from "./handlers";
import { importFromParameters } from "./import.js";
import { transferExportToS3 } from "./transferToS3.js";

export interface Config {
  getStringValue(name: string, optional?: boolean): string;

  getIntegerValue(name: string, optional?: boolean): number;

  getFloatValue(name: string, optional?: boolean): number;

  getRetryConfig(type: RetryConfigType): RetryConfig;
}

export interface RetryConfig {
  times: number;
  wait: number;
  backOff: number;
}

export type RetryConfigType = "export" | "transfer" | "import";

export const fhirExportConfigured = (config: Config) =>
  fhirBulkExport({
    endpoint: config.getStringValue("SOURCE_ENDPOINT"),
    resourceTypes: config.getStringValue("TYPES", true),
    since: config.getStringValue("SINCE"),
    clientId: config.getStringValue("SOURCE_CLIENT_ID"),
    clientSecret: config.getStringValue("SOURCE_CLIENT_SECRET"),
    scopes: config.getStringValue("SOURCE_SCOPES")
  });

export const checkExportConfigured = (
  config: Config,
  event: ExportHandlerOutput
) =>
  checkExportJobStatus({
    endpoint: config.getStringValue("SOURCE_ENDPOINT"),
    clientId: config.getStringValue("SOURCE_CLIENT_ID"),
    clientSecret: config.getStringValue("SOURCE_CLIENT_SECRET"),
    scopes: config.getStringValue("SOURCE_SCOPES"),
    statusUrl: event.statusUrl
  });

export const transferToS3Configured = (
  config: Config,
  event: CheckStatusHandlerOutput
) =>
  transferExportToS3({
    endpoint: config.getStringValue("SOURCE_ENDPOINT"),
    clientId: config.getStringValue("SOURCE_CLIENT_ID"),
    clientSecret: config.getStringValue("SOURCE_CLIENT_SECRET"),
    scopes: config.getStringValue("SOURCE_SCOPES"),
    result: event.result as FhirBulkResult,
    stagingUrl: config.getStringValue("STAGING_URL"),
    importMode: config.getStringValue("IMPORT_MODE", true)
  });

export const pathlingImportConfigured = (
  config: Config,
  event: ImportHandlerInput
) =>
  importFromParameters({
    endpoint: config.getStringValue("TARGET_ENDPOINT"),
    clientId: config.getStringValue("TARGET_CLIENT_ID"),
    clientSecret: config.getStringValue("TARGET_CLIENT_SECRET"),
    scopes: config.getStringValue("TARGET_SCOPES", true),
    parameters: event.parameters
  });

export const checkImportStatusConfigured = (
  config: Config,
  event: ExportHandlerOutput
) =>
  checkImportJobStatus({
    endpoint: config.getStringValue("TARGET_ENDPOINT"),
    clientId: config.getStringValue("TARGET_CLIENT_ID"),
    clientSecret: config.getStringValue("TARGET_CLIENT_SECRET"),
    scopes: config.getStringValue("TARGET_SCOPES", true),
    statusUrl: event.statusUrl
  });

export class FileConfig implements Config {
  readonly config: { [name: string]: string };

  constructor(fd: number) {
    this.config = JSON.parse(fs.readFileSync(fd).toString());
  }

  getValue(name: string, optional = false): any {
    const normalizedName = Case.camel(name),
      value = this.config[normalizedName];
    if (!optional && !value) {
      throw `Config value not set: ${normalizedName}`;
    }
    return value;
  }

  getStringValue(name: string, optional = false): string {
    const value = this.getValue(name, optional);
    if (!optional && typeof value !== "string") {
      throw `Expected string value: ${name}`;
    }
    return value;
  }

  getFloatValue(name: string, optional = false): number {
    const value = this.getValue(name, optional);
    if (!optional && typeof value !== "number") {
      throw `Expected float value: ${name}`;
    }
    return value;
  }

  getIntegerValue(name: string, optional = false): number {
    const value = this.getValue(name, optional);
    if (!optional && typeof value !== "number") {
      throw `Expected integer value: ${name}`;
    }
    return value;
  }

  getRetryConfig(type: RetryConfigType): RetryConfig {
    return getRetryConfig(this, type);
  }
}

export class EnvironmentConfig implements Config {
  getValue(name: string, optional = false): any {
    const normalizedName = Case.constant(name),
      value = process.env[normalizedName];
    if (!optional && !value) {
      throw `Environment variable not set: ${normalizedName}`;
    }
    return value;
  }

  getStringValue(name: string, optional = false): string {
    return this.getValue(name, optional);
  }

  getFloatValue(name: string, optional = false): number {
    const value = this.getValue(name, optional);
    return parseFloat(value);
  }

  getIntegerValue(name: string, optional = false): number {
    const value = this.getValue(name, optional);
    return parseInt(value);
  }

  getRetryConfig(type: RetryConfigType): RetryConfig {
    return getRetryConfig(this, type);
  }
}

function getRetryConfig(
  config: Config,
  operation: RetryConfigType
): RetryConfig {
  const times = config.getIntegerValue(`${operation}RetryTimes`, true);
  const wait = config.getIntegerValue(`${operation}RetryWait`, true);
  const backOff = config.getFloatValue(`${operation}RetryBackOff`, true);

  return {
    times: times ? times : 24,
    wait: wait ? wait : 900,
    backOff: backOff ? backOff : 1.0
  };
}
