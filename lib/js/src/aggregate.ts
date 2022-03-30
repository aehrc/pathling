/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

import { Parameters } from "fhir/r4";
import { getConfig, makeRequest, postFhirConfig } from "./common";
import {
  PathlingClientOptionsResolved,
  QueryOptions,
  QueryResult,
} from "./index";

/**
 * The parameters that make up an aggregate query.
 */
export interface AggregateQuery {
  /**
   * The subject resource that will form the input context for all expressions
   * within the query.
   */
  subjectResource: string;

  /**
   * A list of aggregation expressions. You must provide at least one. An
   * aggregation expression is a FHIRPath expression that is used to calculate a
   * summary value from each grouping. The context is a collection of resources
   * of the type this operation was invoked against. The expression must
   * evaluate to a primitive value.
   */
  aggregations: string[];

  /**
   * A list of grouping expressions. A grouping expression is a FHIRPath
   * expression that can be evaluated against each resource in the data set to
   * determine which groupings it should be counted within. The context is an
   * individual resource of the type this operation was invoked against. The
   * expression must evaluate to a primitive value.
   */
  groupings?: string[];

  /**
   * A list of filter expressions. A FHIRPath expression that can be evaluated
   * against each resource in the data set to determine whether it is included
   * within the result. The context is an individual resource of the type this
   * operation was invoked against. The expression must evaluate to a singular
   * Boolean value. Multiple filters are combined using AND logic.
   */
  filters?: string[];
}

/**
 * Options that control the behaviour of the aggregate client.
 */
export interface AggregateQueryOptions extends QueryOptions {
  /**
   * Force the use of a specified HTTP method.
   */
  method?: "GET" | "POST";
}

/**
 * The structure of the aggregate result.
 */
export interface AggregateResult extends QueryResult {
  /**
   * A Parameters resource containing the results of the aggregate operation.
   *
   * @see https://pathling.csiro.au/docs/aggregate.html
   * @see https://www.hl7.org/fhir/R4/parameters.html
   */
  response: Parameters;
}

/**
 * A class that can be used to make requests to the aggregate operation of a
 * Pathling server.
 *
 * @see https://pathling.csiro.au/docs/aggregate.html
 */
export class AggregateClient {
  readonly options: PathlingClientOptionsResolved;

  constructor(options: PathlingClientOptionsResolved) {
    this.options = options;
  }

  // noinspection JSUnusedGlobalSymbols
  /**
   * Send an aggregate request to this Pathling instance.
   */
  async request(
    query: AggregateQuery,
    options?: AggregateQueryOptions
  ): Promise<AggregateResult> {
    const params = AggregateClient.searchFromQuery(query),
      getQueryTooLong =
        params.toString().length > this.options.maxGetQueryLength,
      usePostRequest = options?.method === "POST" || getQueryTooLong,
      url = `${this.options.endpoint}/${query.subjectResource}/$aggregate`,
      config = usePostRequest
        ? postFhirConfig(
            url,
            AggregateClient.parametersFromQuery(query),
            options
          )
        : getConfig(url, params, options);

    return makeRequest(
      config,
      "Checking status of aggregate job",
      this.options,
      options
    );
  }

  /**
   * Convert an {@link AggregateQuery} object into the corresponding
   * {@link URLSearchParams} object.
   *
   * @private
   */
  private static searchFromQuery(query: AggregateQuery): URLSearchParams {
    const params = new URLSearchParams();
    query.aggregations.forEach((e: string) => params.append("aggregation", e));
    if (query.groupings) {
      query.groupings.forEach((e: string) => params.append("grouping", e));
    }
    if (query.filters) {
      query.filters.forEach((e: string) => params.append("filter", e));
    }
    return params;
  }

  /**
   * Convert an {@link AggregateQuery} object into the corresponding
   * {@link Parameters} resource.
   *
   * @private
   */
  private static parametersFromQuery(query: AggregateQuery): Parameters {
    return {
      resourceType: "Parameters",
      parameter: [
        ...query.aggregations.map((a: string) => ({
          name: "aggregation",
          valueString: a,
        })),
        ...(query.groupings
          ? query.groupings.map((g: string) => ({
              name: "grouping",
              valueString: g,
            }))
          : []),
        ...(query.filters
          ? query.filters.map((f: string) => ({
              name: "filter",
              valueString: f,
            }))
          : []),
      ],
    };
  }
}
