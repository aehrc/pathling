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
import { useCallback, useState } from "react";

import { viewRun, viewRunStored } from "../api";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import {
  streamToText,
  parseNdjsonResponse,
  extractColumns,
} from "../utils/ndjson";

/**
 * A ViewDefinition resource.
 */
export interface ViewDefinition {
  resourceType: "ViewDefinition";
  id?: string;
  name?: string;
  resource: string;
  status: string;
  select: Array<{
    column?: Array<{ path: string; name: string }>;
    forEach?: string;
    forEachOrNull?: string;
    select?: unknown[];
  }>;
  where?: Array<{ path: string }>;
}

/**
 * Request to execute a ViewDefinition.
 */
export interface ViewRunRequest {
  /** Execution mode: stored (by ID) or inline (by JSON). */
  mode: "stored" | "inline";
  /** ID of a stored ViewDefinition (required when mode is "stored"). */
  viewDefinitionId?: string;
  /** JSON string of the ViewDefinition (required when mode is "inline"). */
  viewDefinitionJson?: string;
  /** Maximum rows to return. Defaults to 10. */
  limit?: number;
}

/**
 * Result of executing a ViewDefinition.
 */
export interface ViewDefinitionResult {
  /** Column names extracted from the first result row. */
  columns: string[];
  /** Array of result rows. */
  rows: Record<string, unknown>[];
}

/**
 * Options for useViewRun hook.
 */
export interface UseViewRunOptions {
  /** Callback on successful execution. */
  onSuccess?: (result: ViewDefinitionResult) => void;
  /** Callback on error. */
  onError?: (error: Error) => void;
}

/**
 * Result of useViewRun hook.
 */
export interface UseViewRunResult {
  /** Current status of the mutation. */
  status: "idle" | "pending" | "success" | "error";
  /** The execution result when successful. */
  result: ViewDefinitionResult | undefined;
  /** Error object when failed. */
  error: Error | null;
  /** The request that produced the current result. */
  lastRequest: ViewRunRequest | undefined;
  /** Execute a ViewDefinition. */
  execute: (request: ViewRunRequest) => void;
  /** Reset all state. */
  reset: () => void;
  /** Whether execution is in progress. */
  isPending: boolean;
}

/**
 * Execute a ViewDefinition and return parsed results.
 */
export type UseViewRunFn = (options?: UseViewRunOptions) => UseViewRunResult;

/**
 * Execute a ViewDefinition and return parsed results.
 *
 * Provides imperative execution via the `execute` function. Handles stream
 * consumption and NDJSON parsing internally.
 *
 * @param options - Optional callbacks for success and error events.
 * @returns Hook result with status, result, error, and control functions.
 */
export const useViewRun: UseViewRunFn = (options) => {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;
  const [lastRequest, setLastRequest] = useState<ViewRunRequest | undefined>(
    undefined,
  );

  const mutation = useMutation<ViewDefinitionResult, Error, ViewRunRequest>({
    mutationFn: async (request: ViewRunRequest) => {
      if (!fhirBaseUrl) {
        throw new Error("FHIR base URL is not configured");
      }

      let stream: ReadableStream;

      if (request.mode === "stored" && request.viewDefinitionId) {
        stream = await viewRunStored(fhirBaseUrl, {
          viewDefinitionId: request.viewDefinitionId,
          limit: request.limit ?? 10,
          accessToken,
        });
      } else if (request.mode === "inline" && request.viewDefinitionJson) {
        const parsed = JSON.parse(request.viewDefinitionJson);
        stream = await viewRun(fhirBaseUrl, {
          viewDefinition: parsed,
          limit: request.limit ?? 10,
          accessToken,
        });
      } else {
        throw new Error("Invalid request: missing view definition ID or JSON");
      }

      const ndjsonText = await streamToText(stream);
      const rows = parseNdjsonResponse(ndjsonText);
      const columns = extractColumns(rows);

      setLastRequest(request);
      return { columns, rows };
    },
    onSuccess: options?.onSuccess,
    onError: options?.onError,
  });

  const execute = useCallback(
    (request: ViewRunRequest) => {
      mutation.mutate(request);
    },
    [mutation],
  );

  const reset = useCallback(() => {
    mutation.reset();
    setLastRequest(undefined);
  }, [mutation]);

  return {
    status: mutation.status,
    result: mutation.data,
    error: mutation.error,
    lastRequest,
    execute,
    reset,
    isPending: mutation.isPending,
  };
};
