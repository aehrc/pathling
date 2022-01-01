/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

import { AggregateClient } from "./aggregate";
import { SearchClient } from "./search";
import { ImportClient } from "./import";
import { ExtractClient } from "./extract";
import { CapabilitiesClient } from "./capabilities";
import { JobClient } from "./job";
import { SmartConfigurationClient } from "./smartConfiguration";

/**
 * Configuration options for a Pathling client instance.
 */
export interface PathlingClientOptions {
  /**
   * The FHIR endpoint of the Pathling server.
   */
  endpoint: string;

  /**
   * Configuration relating to the retry behaviour for async operations.
   */
  asyncRetry?: RetryConfig;

  /**
   * Whether to enable verbose logging.
   */
  verboseLogging?: boolean;

  /**
   * The maximum allowable length of a GET request before POST will be used.
   */
  maxGetQueryLength?: number;
}

/**
 * Configuration options for a Pathling client instance, with optionality
 * removed.
 */
export interface PathlingClientOptionsResolved extends PathlingClientOptions {
  asyncRetry: RetryConfig;
  maxGetQueryLength: number;
}

/**
 * Configuration relating to the behaviour of retry within async operations.
 */
export interface RetryConfig {
  /**
   * The maximum number of times to retry.
   */
  times: number;

  /**
   * The number of seconds to wait before retrying.
   */
  wait: number;

  /**
   * The factor by which to multiply the wait after each retry.
   */
  backOff: number;
}

/**
 * Options common to all types of Pathling queries.
 */
export interface QueryOptions {
  /**
   * An access token for accessing protected endpoints.
   */
  token?: string;

  /**
   * Whether to prefer asynchronous responses to requests. Useful when the
   * duration of a request might exceed HTTP timeouts.
   */
  preferAsync?: boolean;

  /**
   * A callback that will report the progress of unfinished async operations.
   */
  onProgress?: (progress: string) => unknown;
}

/**
 * The basic structure of a result common to all Pathling operations.
 */
export interface QueryResult {
  response: any;
  executionTime: number;
}

/**
 * A class that can be used to make requests to a Pathling server at a specified
 * endpoint.
 */
export default class PathlingClient {
  readonly options: PathlingClientOptionsResolved;
  readonly capabilities: CapabilitiesClient;
  readonly smartConfiguration: SmartConfigurationClient;
  readonly import: ImportClient;
  readonly aggregate: AggregateClient;
  readonly search: SearchClient;
  readonly extract: ExtractClient;
  readonly job: JobClient;

  constructor(options: PathlingClientOptions) {
    this.options = {
      ...options,
      asyncRetry: options.asyncRetry || { times: 60, wait: 1, backOff: 1.0 },
      maxGetQueryLength: 1500,
    };
    this.capabilities = new CapabilitiesClient(this.options);
    this.smartConfiguration = new SmartConfigurationClient(this.options);
    this.import = new ImportClient(this.options);
    this.aggregate = new AggregateClient(this.options);
    this.search = new SearchClient(this.options);
    this.extract = new ExtractClient(this.options);
    this.job = new JobClient();
  }
}
