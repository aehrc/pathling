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

import { useCallback } from "react";

import { checkResponse } from "../api/utils";
import { useAuth } from "../contexts/AuthContext";

/**
 * Hook that provides authenticated file download functionality.
 *
 * Handles Bearer token injection and blob downloads. Errors (including 401)
 * are passed to the onError callback; 401 errors are also handled globally
 * to trigger re-authentication.
 *
 * @param onError - Optional callback for error reporting. Called with the
 * error when the download fails.
 * @returns A function that downloads a file from the given URL. The function
 * takes the URL to fetch and the filename to use for the downloaded file.
 *
 * @example
 * const downloadFile = useDownloadFile(setError);
 * await downloadFile("https://example.com/file.ndjson", "export.ndjson");
 */
export function useDownloadFile(
  onError?: (error: Error) => void,
): (url: string, filename: string) => Promise<void> {
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;

  return useCallback(
    async (url: string, filename: string) => {
      try {
        const headers: HeadersInit = {};
        if (accessToken) {
          headers.Authorization = `Bearer ${accessToken}`;
        }

        const response = await fetch(url, { headers });
        await checkResponse(response, "Download");

        const blob = await response.blob();
        const downloadUrl = window.URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = downloadUrl;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        window.URL.revokeObjectURL(downloadUrl);
      } catch (err) {
        const error = err instanceof Error ? err : new Error("Download failed");
        onError?.(error);
        // Re-throw so the global handler can catch UnauthorizedError.
        throw error;
      }
    },
    [accessToken, onError],
  );
}
