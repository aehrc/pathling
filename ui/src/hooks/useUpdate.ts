/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

import { useMutation } from "@tanstack/react-query";

import { update } from "../api";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";

import type { UseMutationResult } from "@tanstack/react-query";
import type { Resource } from "fhir/r4";

/**
 * Options for useUpdate hook.
 */
export interface UseUpdateOptions {
  /** Callback on successful update. */
  onSuccess?: (resource: Resource) => void;
  /** Callback on error. */
  onError?: (error: Error) => void;
}

/**
 * Variables for useUpdate mutation.
 */
export interface UseUpdateVariables {
  /** The resource to update (must include id). */
  resource: Resource;
}

/**
 * Result of useUpdate hook.
 */
export type UseUpdateResult = UseMutationResult<
  Resource,
  Error,
  UseUpdateVariables
>;

/**
 * Update an existing FHIR resource.
 */
export type UseUpdateFn = (options?: UseUpdateOptions) => UseUpdateResult;

/**
 * Update an existing FHIR resource.
 *
 * @param options - Optional callbacks for success and error.
 * @returns Mutation result for updating resources.
 */
export const useUpdate: UseUpdateFn = (options) => {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;

  return useMutation<Resource, Error, UseUpdateVariables>({
    mutationFn: (variables) => {
      if (!variables.resource.id) {
        throw new Error("Resource must have an id for update");
      }
      return update(fhirBaseUrl!, {
        resourceType: variables.resource.resourceType,
        id: variables.resource.id,
        resource: variables.resource,
        accessToken,
      });
    },
    onSuccess: options?.onSuccess,
    onError: options?.onError,
  });
};
