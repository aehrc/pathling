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

import { Parameters } from "fhir/r4.js";
import { getConfig, makeRequest, postFhirConfig } from "./common.js";
import {
  PathlingClientOptionsResolved,
  QueryOptions,
  QueryResult,
} from "./index.js";

/**
 * The parameters that make up an extract query.
 */
export interface ExtractQuery {
  /**
   * The subject resource that will form the input context for all expressions
   * within the query.
   */
  subjectResource: string;

  /**
   * A list of column expressions. Each column is a FHIRPath expression that
   * defines a column within the result. The context is a single resource of the
   * subject resource type. The expression must return a materializable type.
   */
  columns: string[];

  /**
   * A list of filter expressions. A FHIRPath expression that can be evaluated
   * against each resource in the data set to determine whether it is included
   * within the result. The context is an individual resource of the type that
   * the search is being invoked against. The expression must evaluate to a
   * Boolean value.
   */
  filters?: string[];

  /**
   * The maximum number of rows to return.
   */
  limit?: number;
}

export interface ExtractQueryOptions extends QueryOptions {
  method?: "GET" | "POST";
}

/**
 * The structure of the extract result.
 */
export interface ExtractResult extends QueryResult {
  /**
   * A Parameters resource containing the results of the extract operation.
   *
   * @see https://pathling.csiro.au/docs/extract.html
   * @see https://www.hl7.org/fhir/R4/parameters.html
   */
  response: Parameters;

  /**
   * A URL at which the result of the operation can be retrieved.
   */
  url: string;
}

/**
 * A class that can be used to make requests to the extract operation of a
 * Pathling server.
 *
 * @see https://pathling.csiro.au/docs/extract.html
 */
export class ExtractClient {
  readonly options: PathlingClientOptionsResolved;

  constructor(options: PathlingClientOptionsResolved) {
    this.options = options;
  }

  // noinspection JSUnusedGlobalSymbols
  /**
   * Send an extract request to this Pathling instance.
   */
  async request(
    query: ExtractQuery,
    options?: ExtractQueryOptions
  ): Promise<ExtractResult> {
    const params = ExtractClient.searchFromQuery(query),
      getQueryTooLong =
        params.toString().length > this.options.maxGetQueryLength,
      usePostRequest = options?.method === "POST" || getQueryTooLong,
      url = `${this.options.endpoint}/${query.subjectResource}/$extract`,
      config = usePostRequest
        ? postFhirConfig(url, ExtractClient.parametersFromQuery(query), options)
        : getConfig(url, params, options);

    const result = await makeRequest<Parameters>(
      config,
      "Checking status of extract job",
      this.options,
      options
    );
    const resultUrl = result.response.parameter?.filter(
      (p) => p.name === "url"
    )[0];
    if (!resultUrl || !resultUrl.valueUrl) {
      throw new Error("Cannot find result URL in response");
    }
    return {
      ...result,
      url: resultUrl.valueUrl,
    };
  }

  /**
   * Convert an {@link ExtractQuery} object into the corresponding
   * {@link URLSearchParams} object.
   *
   * @private
   */
  private static searchFromQuery(query: ExtractQuery): URLSearchParams {
    const params = new URLSearchParams();
    query.columns.forEach((c) => params.append("column", c));
    if (query.filters) {
      query.filters.forEach((f: string) => params.append("filter", f));
    }
    if (query.limit) {
      params.append("limit", query.limit.toString());
    }
    return params;
  }

  /**
   * Convert an {@link ExtractQuery} object into the corresponding
   * {@link Parameters} resource.
   *
   * @private
   */
  private static parametersFromQuery(query: ExtractQuery): Parameters {
    return {
      resourceType: "Parameters",
      parameter: [
        ...query.columns.map((c: string) => ({
          name: "column",
          valueString: c,
        })),
        ...(query.filters
          ? query.filters.map((f: string) => ({
              name: "filter",
              valueString: f,
            }))
          : []),
        ...(query.limit
          ? [
              {
                name: "limit",
                valueInteger: query.limit,
              },
            ]
          : []),
      ],
    };
  }
}
