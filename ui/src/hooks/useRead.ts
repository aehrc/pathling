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

import { useQuery } from "@tanstack/react-query";

import { read } from "../api";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";

import type { UseReadFn } from "../types/hooks";
import type { Resource } from "fhir/r4";

/**
 * Read a single FHIR resource by type and ID.
 *
 * @param options - Read options including resource type and ID.
 * @returns Query result with the Resource.
 */
export const useRead: UseReadFn = (options) => {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;

  return useQuery<Resource, Error>({
    queryKey: ["read", options.resourceType, options.id],
    queryFn: () =>
      read(fhirBaseUrl!, {
        resourceType: options.resourceType,
        id: options.id,
        accessToken,
      }),
    enabled: options.enabled !== false && !!fhirBaseUrl && !!options.id,
  });
};
