/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

import axios from "axios";
import { QueryOptions } from "./index";
import { FHIR_CONTENT_TYPE } from "./common";

/**
 * A class that can be used to check the progress of an asynchronous job.
 */
export class JobClient {
  // noinspection JSUnusedGlobalSymbols
  /**
   * Check the status of a job, using its job status URL.
   *
   * @returns The response body, if the job is complete.
   * @throws {JobInProgressError} if the job is incomplete.
   */
  async request(url: string, options?: QueryOptions): Promise<any> {
    const auth = { Authorization: `Bearer ${options?.token}` },
      config = {
        url,
        headers: {
          Accept: FHIR_CONTENT_TYPE,
          ...(options?.token ? auth : {}),
        },
      },
      response = await axios.request<any>(config);

    if (response.status === 200) {
      return response.data;
    } else if (response.status === 202) {
      const progress = response.headers["x-progress"];
      const message = progress ? progress : "(no progress message)";
      throw new JobInProgressError(`Job in progress: ${message}`, progress);
    } else {
      throw `Unexpected status: ${response.status} ${response.statusText}`;
    }
  }
}

/**
 * An error that is raised when we check on the status of a job that is not yet
 * finished.
 */
export class JobInProgressError extends Error {
  readonly progress: string;

  constructor(message: string, progress: string) {
    super(message);
    this.name = "JobInProgressError";
    this.progress = progress;
  }
}
