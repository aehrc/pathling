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

import { sqlQueryRun } from "../api";
import { config } from "../config";
import { readSqlQueryResponse } from "./sqlQueryHelpers";
import { useAuth } from "../contexts/AuthContext";

import type { SqlQueryRequest, SqlQueryResult } from "../types/sqlQuery";

/**
 * Options for {@link useSqlQueryRun}.
 */
export interface UseSqlQueryRunOptions {
  /** Callback fired on a successful execution. */
  onSuccess?: (result: SqlQueryResult) => void;
  /** Callback fired on error. */
  onError?: (error: Error) => void;
}

/**
 * Result of {@link useSqlQueryRun}.
 */
export interface UseSqlQueryRunResult {
  /** Current status of the underlying mutation. */
  status: "idle" | "pending" | "success" | "error";
  /** The execution result when successful. */
  result: SqlQueryResult | undefined;
  /** Error object when failed. */
  error: Error | null;
  /** Snapshot of the request that produced the current state. */
  lastRequest: SqlQueryRequest | undefined;
  /** Execute a SQL query. */
  execute: (request: SqlQueryRequest) => void;
  /** Reset all state to idle. */
  reset: () => void;
  /** Whether execution is in progress. */
  isPending: boolean;
}

/**
 * Executes a SQL on FHIR `$sqlquery-run` request and returns a parsed,
 * format-aware result.
 *
 * The hook is a thin wrapper over {@link sqlQueryRun} and
 * {@link readSqlQueryResponse}; the bulk of the logic lives in those pure
 * helpers, in line with the project's React rules.
 *
 * @param options - Optional callbacks for success and error events.
 * @returns Hook result with status, result, error, and control functions.
 */
export function useSqlQueryRun(
  options?: UseSqlQueryRunOptions,
): UseSqlQueryRunResult {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;
  const [lastRequest, setLastRequest] = useState<SqlQueryRequest | undefined>(
    undefined,
  );

  const mutation = useMutation<SqlQueryResult, Error, SqlQueryRequest>({
    mutationFn: async (request: SqlQueryRequest) => {
      if (!fhirBaseUrl) {
        throw new Error("FHIR base URL is not configured");
      }
      setLastRequest(request);
      const response =
        request.mode === "stored"
          ? await sqlQueryRun(fhirBaseUrl, {
              mode: "stored",
              libraryId: request.libraryId,
              format: request.format,
              limit: request.limit,
              header: request.header,
              bindings: request.bindings,
              parameterTypes: request.parameterTypes,
              accessToken,
            })
          : await sqlQueryRun(fhirBaseUrl, {
              mode: "inline",
              library: request.library,
              format: request.format,
              limit: request.limit,
              header: request.header,
              bindings: request.bindings,
              parameterTypes: request.parameterTypes,
              accessToken,
            });

      const format = request.format ?? "ndjson";
      return readSqlQueryResponse(response, format);
    },
    onSuccess: options?.onSuccess,
    onError: options?.onError,
  });

  const execute = useCallback(
    (request: SqlQueryRequest) => {
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
}
