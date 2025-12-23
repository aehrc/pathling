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

import { useMutation } from "@tanstack/react-query";
import { deleteResource } from "../api";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import type { UseDeleteFn, UseDeleteVariables } from "../types/hooks";

/**
 * Delete a FHIR resource.
 *
 * @param options - Optional callbacks for success and error.
 * @returns Mutation result for deleting resources.
 */
export const useDelete: UseDeleteFn = (options) => {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;

  return useMutation<void, Error, UseDeleteVariables>({
    mutationFn: (variables) =>
      deleteResource(fhirBaseUrl!, {
        resourceType: variables.resourceType,
        id: variables.id,
        accessToken,
      }),
    onSuccess: options?.onSuccess,
    onError: options?.onError,
  });
}
