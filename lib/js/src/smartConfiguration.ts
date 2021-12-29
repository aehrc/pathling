/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

import { getConfig, makeRequest } from "./common";
import {
  PathlingClientOptionsResolved,
  QueryOptions,
  QueryResult,
} from "./index";

export type SmartConfigurationQueryOptions = QueryOptions;

export type SmartConfiguration = {
  issuer: string;
  capabilities: string[];
  authorization_endpoint?: string;
  token_endpoint?: string;
  revocation_endpoint?: string;
};

/**
 * The structure of the aggregate result.
 */
export interface SmartConfigurationResult extends QueryResult {
  /**
   * A SMART configuration document.
   *
   * @see https://www.hl7.org/fhir/smart-app-launch/conformance.html
   */
  response: SmartConfiguration;
}

/**
 * A class that can be used to make requests to the SMART configuration document for a
 * Pathling server.
 *
 * @see https://www.hl7.org/fhir/smart-app-launch/conformance.html
 */
export class SmartConfigurationClient {
  readonly options: PathlingClientOptionsResolved;

  constructor(options: PathlingClientOptionsResolved) {
    this.options = options;
  }

  // noinspection JSUnusedGlobalSymbols
  /**
   * Send a SMART configuration request to this Pathling instance.
   */
  async request(
    options?: SmartConfigurationQueryOptions
  ): Promise<SmartConfigurationResult> {
    const config = getConfig(
      `${this.options.endpoint}/.well-known/smart-configuration`,
      new URLSearchParams(),
      options
    );

    return makeRequest(
      config,
      "Retrieving SMART configuration document",
      this.options,
      options
    );
  }
}
