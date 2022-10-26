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

import axios from "axios";
import { FHIR_CONTENT_TYPE } from "./common";
import { QueryOptions } from "./index";

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
          ...(options?.token ? auth : {})
        }
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
  readonly progress: string | undefined;

  constructor(message: string, progress?: string) {
    super(message);
    this.name = "JobInProgressError";
    this.progress = progress;
  }
}
