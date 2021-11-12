import { IOperationOutcome, IParameters } from "@ahryman40k/ts-fhir-types/lib/R4";
import { PathlingClientOptionsResolved, QueryOptions, QueryResult } from "./index";
import { makeRequest, postFhirConfig } from "./common";

/**
 * The parameters that make up an import query.
 */
export interface ImportQuery {
  /**
   * A source FHIR NDJSON file containing resources to be included within this
   * import operation. Each file must contain only one type of resource.
   */
  source: ImportSource[];
}

/**
 * A source FHIR NDJSON file containing resources to be included within this
 * import operation. Each file must contain only one type of resource.
 */
export interface ImportSource {
  /**
   * The base FHIR resource type contained within this source file.
   *
   * @see http://hl7.org/fhir/ValueSet/resource-types
   */
  resourceType: string;

  /**
   * A URL that can be used to retrieve this source file.
   */
  url: string;
}

/**
 * Options that control the behaviour of the import client.
 */
export type ImportQueryOptions = QueryOptions;

/**
 * The structure of the import result.
 */
export interface ImportResult extends QueryResult {
  /**
   * An OperationOutcome resource describing the outcome of the import.
   *
   * @see https://www.hl7.org/fhir/r4/operationoutcome.html
   */
  response: IOperationOutcome;
}

/**
 * A class that can be used to make requests to the import operation of a
 * Pathling server.
 *
 * @see https://pathling.csiro.au/docs/import.html
 */
export class ImportClient {
  readonly options: PathlingClientOptionsResolved;

  constructor(options: PathlingClientOptionsResolved) {
    this.options = options;
  }

  /**
   * Send an import request to this Pathling instance.
   */
  async request(
    query: ImportQuery,
    options?: ImportQueryOptions
  ): Promise<ImportResult> {
    return this.requestWithParams(
      ImportClient.parametersFromQuery(query),
      options
    );
  }

  /**
   * Send an import request to this Pathling instance, with a Parameters
   * resource as input.
   *
   * @see https://pathling.csiro.au/docs/import.html
   * @see https://www.hl7.org/fhir/R4/parameters.html
   */
  async requestWithParams(
    params: IParameters,
    options?: ImportQueryOptions
  ): Promise<ImportResult> {
    return makeRequest(
      postFhirConfig(`${this.options.endpoint}/$import`, params, options),
      "Checking status of import job",
      this.options
    );
  }

  /**
   * Convert an {@link ImportQuery} object into the corresponding
   * {@link IParameters} resource.
   *
   * @private
   */
  private static parametersFromQuery(query: ImportQuery): IParameters {
    return {
      resourceType: "Parameters",
      parameter: query.source.map((s: ImportSource) => ({
        name: "source",
        part: [
          {
            name: "resourceType",
            valueString: s.resourceType,
          },
          {
            name: "url",
            valueString: s.url,
          },
        ],
      })),
    };
  }
}
