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

/**
 * Hook for the synchronous `$sql-run` operation. One hook serves every kind of
 * subject, because the operation itself is subject-polymorphic: a
 * ViewDefinition, a SQLQuery and a SQLView all go to the same endpoint and
 * come back in the same set of formats.
 *
 * @author John Grimes
 */

import { useMutation } from "@tanstack/react-query";
import { useCallback, useState } from "react";

import { sqlRun, sqlRunStored } from "../api";
import { config } from "../config";
import { buildBindingsResource, readSqlQueryResponse } from "./sqlQueryHelpers";
import { useAuth } from "../contexts/AuthContext";

import type { SqlRunFormat, SubjectSource } from "../api";
import type {
  SqlQueryParameterType,
  SqlQueryResult,
  SqlQueryRuntimeBindings,
} from "../types/sqlQuery";

/**
 * A single `$sql-run` request.
 */
export interface SqlRunRequest {
  /** How the subject is named. */
  subject: SubjectSource;
  /** Output format; the server defaults to ndjson when omitted. */
  format?: SqlRunFormat;
  /** Maximum rows to return. */
  limit?: number;
  /** Whether CSV output carries a header row. */
  header?: boolean;
  /** Values bound to the declared parameters, for a SQL subject only. */
  bindings?: SqlQueryRuntimeBindings;
  /** Declared parameter types, keyed by name. */
  parameterTypes?: Record<string, SqlQueryParameterType>;
  /** Patient ids restricting the data the subject reads. */
  patientIds?: string[];
  /** Group ids restricting the data the subject reads. */
  groupIds?: string[];
  /** Restricts to resources updated at or after this instant. */
  since?: string;
}

/** Options for {@link useSqlRun}. */
export interface UseSqlRunOptions {
  /** Callback fired on a successful execution. */
  onSuccess?: (result: SqlQueryResult) => void;
  /** Callback fired on error. */
  onError?: (error: Error) => void;
}

/** Result of {@link useSqlRun}. */
export interface UseSqlRunResult {
  /** Current status of the underlying mutation. */
  status: "idle" | "pending" | "success" | "error";
  /** The execution result when successful. */
  result: SqlQueryResult | undefined;
  /** Error object when failed. */
  error: Error | null;
  /** Snapshot of the request that produced the current state. */
  lastRequest: SqlRunRequest | undefined;
  /** Runs a subject. */
  execute: (request: SqlRunRequest) => void;
  /** Resets all state to idle. */
  reset: () => void;
  /** Whether execution is in progress. */
  isPending: boolean;
}

/**
 * Runs a subject via `$sql-run` and returns a parsed, format-aware result.
 *
 * A stored subject with no runtime bindings is sent as a GET, which is the
 * form the specification prefers and the one a user can paste into a browser;
 * anything carrying a resource or a binding must be a POST.
 *
 * @param options - Optional callbacks for success and error events.
 * @returns Hook result with status, result, error, and control functions.
 */
export function useSqlRun(options?: UseSqlRunOptions): UseSqlRunResult {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;
  const [lastRequest, setLastRequest] = useState<SqlRunRequest | undefined>(
    undefined,
  );

  const mutation = useMutation<SqlQueryResult, Error, SqlRunRequest>({
    mutationFn: async (request: SqlRunRequest) => {
      if (!fhirBaseUrl) {
        throw new Error("FHIR base URL is not configured");
      }
      setLastRequest(request);

      const parameters = buildBindingsResource(
        request.bindings,
        request.parameterTypes,
      );
      const response =
        request.subject.kind === "reference" && !parameters
          ? await sqlRunStored(fhirBaseUrl, {
              reference: request.subject.reference,
              format: request.format,
              limit: request.limit,
              header: request.header,
              patientIds: request.patientIds,
              groupIds: request.groupIds,
              since: request.since,
              accessToken,
            })
          : await sqlRun(fhirBaseUrl, {
              subject: request.subject,
              format: request.format,
              limit: request.limit,
              header: request.header,
              parameters,
              patientIds: request.patientIds,
              groupIds: request.groupIds,
              since: request.since,
              accessToken,
            });

      return readSqlQueryResponse(response, request.format ?? "ndjson");
    },
    onSuccess: options?.onSuccess,
    onError: options?.onError,
  });

  const execute = useCallback(
    (request: SqlRunRequest) => {
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
