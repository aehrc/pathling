/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

/**
 * @author John Grimes
 */

import Case from "case";
import fs from "node:fs";
import { checkExportJobStatus, checkImportJobStatus } from "./checkStatus.js";
import { MaybeAuthenticated } from "./common";
import { fhirBulkExport, FhirBulkResult } from "./export.js";
import {
  CheckStatusHandlerOutput,
  ExportHandlerOutput,
  ImportHandlerInput,
} from "./handlers";
import { importFromParameters } from "./import.js";
import { transferExportToS3 } from "./transferToS3.js";

export interface Config {
  getStringValue(name: string): string;

  getOptionalStringValue(name: string): string | undefined;

  getIntegerValue(name: string): number;

  getOptionalIntegerValue(name: string): number | undefined;

  getFloatValue(name: string): number;

  getOptionalFloatValue(name: string): number | undefined;

  getBooleanValue(name: string): boolean;

  getOptionalBooleanValue(name: string): boolean | undefined;

  getRetryConfig(type: RetryConfigType): RetryConfig;
}

export interface RetryConfig {
  times: number;
  wait: number;
  backOff: number;
}

export type RetryConfigType = "export" | "transfer" | "import";

type SupportedType = "string" | "number" | "boolean";

export const fhirExportConfigured = (config: Config) =>
  fhirBulkExport(
    maybeAuthenticated(config, "SOURCE", {
      endpoint: config.getStringValue("SOURCE_ENDPOINT"),
      resourceTypes: config.getOptionalStringValue("TYPES"),
      since: config.getStringValue("SINCE"),
    })
  );

export const checkExportConfigured = (
  config: Config,
  event: ExportHandlerOutput
) =>
  checkExportJobStatus(
    maybeAuthenticated(config, "SOURCE", {
      endpoint: config.getStringValue("SOURCE_ENDPOINT"),
      statusUrl: event.statusUrl,
    })
  );

export const transferToS3Configured = (
  config: Config,
  event: CheckStatusHandlerOutput
) =>
  transferExportToS3(
    maybeAuthenticated(config, "SOURCE", {
      endpoint: config.getStringValue("SOURCE_ENDPOINT"),
      result: event.result as FhirBulkResult,
      stagingUrl: config.getStringValue("STAGING_URL"),
      importMode: config.getOptionalStringValue("IMPORT_MODE"),
    })
  );

export const pathlingImportConfigured = (
  config: Config,
  event: ImportHandlerInput
) =>
  importFromParameters(
    maybeAuthenticated(config, "TARGET", {
      endpoint: config.getStringValue("TARGET_ENDPOINT"),
      parameters: event.parameters,
    })
  );

export const checkImportStatusConfigured = (
  config: Config,
  event: ExportHandlerOutput
) =>
  checkImportJobStatus(
    maybeAuthenticated(config, "TARGET", {
      endpoint: config.getStringValue("TARGET_ENDPOINT"),
      statusUrl: event.statusUrl,
    })
  );

export class FileConfig implements Config {
  readonly config: { [name: string]: string };

  constructor(fd: number) {
    this.config = JSON.parse(fs.readFileSync(fd).toString());
  }

  getValue(name: string, type: SupportedType): any {
    const value = this.getOptionalValue(name, type);
    if (value === null || value === undefined) {
      throw `Config value not set: ${name}`;
    }
    return value;
  }

  getOptionalValue(name: string, type: SupportedType): any {
    const normalizedName = Case.camel(name),
      value = this.config[normalizedName];
    if (value !== undefined && typeof value !== type) {
      throw `Expected ${type} value for ${name}: ${value}`;
    }
    return value;
  }

  getStringValue(name: string, optional = false): string {
    return this.getValue(name, "string");
  }

  getOptionalStringValue(name: string): string | undefined {
    return this.getOptionalValue(name, "string");
  }

  getFloatValue(name: string): number {
    return this.getValue(name, "number");
  }

  getOptionalFloatValue(name: string): number | undefined {
    return this.getOptionalValue(name, "number");
  }

  getIntegerValue(name: string, optional = false): number {
    return this.getValue(name, "number");
  }

  getOptionalIntegerValue(name: string): number | undefined {
    return this.getOptionalValue(name, "number");
  }

  getBooleanValue(name: string): boolean {
    return this.getValue(name, "boolean");
  }

  getOptionalBooleanValue(name: string): boolean | undefined {
    return this.getOptionalValue(name, "boolean");
  }

  getRetryConfig(type: RetryConfigType): RetryConfig {
    return getRetryConfig(this, type);
  }
}

export class EnvironmentConfig implements Config {
  private static parseFloat(value: string) {
    const number = parseFloat(value);
    if (isNaN(number)) {
      throw `Expected float value: ${value}`;
    }
    return number;
  }

  private static parseInt(value: string): number {
    const number = parseInt(value);
    if (isNaN(number)) {
      throw `Expected integer value: ${value}`;
    }
    return number;
  }

  private static parseBoolean(value: string): boolean {
    if (value !== "true" && value !== "false") {
      throw `Expected boolean value: ${value}`;
    }
    return value === "true";
  }

  getValue(name: string): string {
    const value = this.getOptionalValue(name);
    if (!value) {
      throw `Environment variable not set: ${name}`;
    }
    return value;
  }

  getOptionalValue(name: string): string | undefined {
    const normalizedName = Case.constant(name);
    return process.env[normalizedName];
  }

  getStringValue(name: string): string {
    return this.getValue(name);
  }

  getOptionalStringValue(name: string): string | undefined {
    return this.getOptionalValue(name);
  }

  getFloatValue(name: string): number {
    const value = this.getValue(name);
    return EnvironmentConfig.parseFloat(value);
  }

  getOptionalFloatValue(name: string): number | undefined {
    const value = this.getOptionalValue(name);
    return value ? EnvironmentConfig.parseFloat(value) : undefined;
  }

  getIntegerValue(name: string): number {
    const value = this.getValue(name);
    return EnvironmentConfig.parseInt(value);
  }

  getOptionalIntegerValue(name: string): number | undefined {
    const value = this.getOptionalValue(name);
    return value ? parseInt(value) : undefined;
  }

  getBooleanValue(name: string, optional?: boolean): boolean {
    const value = this.getValue(name);
    return EnvironmentConfig.parseBoolean(value);
  }

  getOptionalBooleanValue(name: string): boolean | undefined {
    const value = this.getOptionalValue(name);
    return value ? EnvironmentConfig.parseBoolean(value) : undefined;
  }

  getRetryConfig(type: RetryConfigType): RetryConfig {
    return getRetryConfig(this, type);
  }
}

function getRetryConfig(
  config: Config,
  operation: RetryConfigType
): RetryConfig {
  const times = config.getOptionalIntegerValue(`${operation}RetryTimes`);
  const wait = config.getOptionalIntegerValue(`${operation}RetryWait`);
  const backOff = config.getOptionalFloatValue(`${operation}RetryBackOff`);

  return {
    times: times ? times : 24,
    wait: wait ? wait : 900,
    backOff: backOff ? backOff : 1.0,
  };
}

function maybeAuthenticated<T>(
  config: Config,
  prefix: string,
  options: T
): T & MaybeAuthenticated {
  let authenticationDisabled =
    config.getOptionalStringValue(`${prefix}_AUTHENTICATION_ENABLED`) ===
    "false";
  if (authenticationDisabled) {
    return {
      ...options,
      authenticationEnabled: false,
    };
  } else {
    return {
      ...options,
      authenticationEnabled: true,
      clientId: config.getStringValue(`${prefix}_CLIENT_ID`),
      clientSecret: config.getStringValue(`${prefix}_CLIENT_SECRET`),
      scopes: config.getStringValue(`${prefix}_SCOPES`),
    };
  }
}
