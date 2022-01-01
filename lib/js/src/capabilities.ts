/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

import { ICapabilityStatement } from "@ahryman40k/ts-fhir-types/lib/R4";
import {
  PathlingClientOptionsResolved,
  QueryOptions,
  QueryResult,
} from "./index";
import { getConfig, makeRequest } from "./common";

export type CapabilitiesQueryOptions = QueryOptions;

/**
 * The structure of the aggregate result.
 */
export interface CapabilitiesResult extends QueryResult {
  /**
   * A CapabilityStatement resource.
   *
   * @see https://hl7.org/fhir/r4/capabilitystatement.html
   */
  response: ICapabilityStatement;
}

/**
 * A class that can be used to make requests to the capabilities operation of a
 * Pathling server.
 *
 * @see https://hl7.org/fhir/r4/http.html#capabilities
 */
export class CapabilitiesClient {
  readonly options: PathlingClientOptionsResolved;

  constructor(options: PathlingClientOptionsResolved) {
    this.options = options;
  }

  // noinspection JSUnusedGlobalSymbols
  /**
   * Send an aggregate request to this Pathling instance.
   */
  async request(
    options?: CapabilitiesQueryOptions
  ): Promise<CapabilitiesResult> {
    const config = getConfig(
      `${this.options.endpoint}/metadata`,
      new URLSearchParams(),
      options
    );

    return makeRequest(
      config,
      "Querying server capabilities",
      this.options,
      options
    );
  }
}
