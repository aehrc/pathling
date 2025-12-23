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
import { useAuth } from "../contexts/AuthContext";
import { useUnauthorizedHandler } from "./useUnauthorizedHandler";

/**
 * Hook that provides authenticated file download functionality.
 *
 * Handles Bearer token injection, 401 error handling, and blob downloads.
 * The download is triggered by programmatically creating and clicking an
 * anchor element, which prompts the browser's save dialog.
 *
 * @param onError - Optional callback for error reporting. Called with the
 * error message when a non-401 error occurs.
 * @returns A function that downloads a file from the given URL. The function
 * takes the URL to fetch and the filename to use for the downloaded file.
 *
 * @example
 * const downloadFile = useDownloadFile(setError);
 * await downloadFile("https://example.com/file.ndjson", "export.ndjson");
 */
export function useDownloadFile(
  onError?: (message: string) => void,
): (url: string, filename: string) => Promise<void> {
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;
  const handleUnauthorizedError = useUnauthorizedHandler();

  return useCallback(
    async (url: string, filename: string) => {
      try {
        const headers: HeadersInit = {};
        if (accessToken) {
          headers.Authorization = `Bearer ${accessToken}`;
        }

        const response = await fetch(url, { headers });

        if (response.status === 401) {
          handleUnauthorizedError();
          return;
        }

        if (!response.ok) {
          throw new Error(`Download failed: ${response.status}`);
        }

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
        onError?.(err instanceof Error ? err.message : "Download failed");
      }
    },
    [accessToken, handleUnauthorizedError, onError],
  );
}
