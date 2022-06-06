/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

import { Bundle } from "fhir/r4";
import { getConfig, makeRequest, postFormConfig } from "./common";
import {
  PathlingClientOptionsResolved,
  QueryOptions,
  QueryResult
} from "./index";

/**
 * The parameters that make up a search query.
 */
export interface SearchQuery {
  /**
   * The subject resource that will form the input context for all expressions
   * within the query.
   */
  subjectResource: string;

  /**
   * A list of filter expressions. A FHIRPath expression that can be evaluated
   * against each resource in the data set to determine whether it is included
   * within the result. The context is an individual resource of the type that
   * the search is being invoked against. The expression must evaluate to a
   * Boolean value.
   */
  filters?: string[];

  /**
   * Any additional search parameters to include in the query.
   *
   * @see https://hl7.org/fhir/R4/search.html
   */
  additionalParams?: URLSearchParams;
}

/**
 * Options that control the behaviour of the search client.
 */
export interface SearchQueryOptions extends QueryOptions {
  /**
   * Force the use of a specified HTTP method.
   */
  method?: "GET" | "POST";
}

/**
 * The structure of the search result.
 */
export interface SearchResult extends QueryResult {
  /**
   * A Bundle resource containing the results of the search operation.
   *
   * @see https://pathling.csiro.au/docs/search.html
   * @see https://www.hl7.org/fhir/R4/bundle.html
   */
  response: Bundle;
}

/**
 * A class that can be used to make requests to the search operation of a
 * Pathling server.
 *
 * @see https://pathling.csiro.au/docs/search.html
 */
export class SearchClient {
  readonly options: PathlingClientOptionsResolved;

  constructor(options: PathlingClientOptionsResolved) {
    this.options = options;
  }

  /**
   * Send a search request to this Pathling instance.
   */
  async request(
    query: SearchQuery,
    options?: SearchQueryOptions
  ): Promise<SearchResult> {
    const params = SearchClient.searchFromQuery(query),
      getQueryTooLong =
        params.toString().length > this.options.maxGetQueryLength,
      usePostRequest = options?.method === "POST" || getQueryTooLong,
      url = `${this.options.endpoint}/${query.subjectResource}/_search`,
      config = usePostRequest
        ? postFormConfig(url, params, options)
        : getConfig(url, params, options);

    return makeRequest(
      config,
      "Checking status of search job",
      this.options,
      options
    );
  }

  /**
   * Convert a {@link SearchQuery} object into the corresponding
   * {@link URLSearchParams} object.
   *
   * @private
   */
  private static searchFromQuery(query: SearchQuery): URLSearchParams {
    const params = new URLSearchParams();
    if (query.filters) {
      params.append("_query", "fhirPath");
      query.filters.forEach((e: string) =>
        params.append("filter", escapeSearchParameter(e))
      );
    }
    if (query.additionalParams) {
      query.additionalParams.forEach((value: string, key: string) =>
        params.append(key, value)
      );
    }
    return params;
  }
}

/**
 * Applies the FHIR escaping rules for search parameters.
 *
 * @param param The parameter string to escape
 * @return The escaped parameter
 * @see https://www.hl7.org/fhir/R4/search.html#escaping
 */
export function escapeSearchParameter(param: string): string {
  let result = param.replace(/\\/g, "\\\\");
  result = result.replace(/\$/g, "\\$");
  result = result.replace(/,/g, "\\,");
  result = result.replace(/\|/g, "\\|");
  return result;
}
