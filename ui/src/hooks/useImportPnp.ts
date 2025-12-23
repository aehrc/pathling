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
 *
 * Author: John Grimes
 */

import { importPnpKickOff, jobStatus, jobCancel } from "../api";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useAsyncJob } from "./useAsyncJob";
import type { UseImportPnpFn } from "../types/hooks";

/**
 * Execute a passthrough (PnP) import operation with polling.
 */
export const useImportPnp: UseImportPnpFn = (options) => {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;

  return useAsyncJob(
    () => ({
      kickOff: () =>
        importPnpKickOff(fhirBaseUrl!, {
          exportUrl: options.sources[0],
          exportType: "dynamic",
          saveMode: "merge",
          accessToken,
        }),
      getJobId: (result) => result.jobId,
      checkStatus: (jobId) => jobStatus(fhirBaseUrl!, { jobId, accessToken }),
      isComplete: (status) => status.status === "complete",
      getResult: () => undefined,
      cancel: (jobId) => jobCancel(fhirBaseUrl!, { jobId, accessToken }),
      pollingInterval: 3000,
    }),
    {
      onProgress: options.onProgress,
      onComplete: options.onComplete,
      onError: options.onError,
    },
  );
}
