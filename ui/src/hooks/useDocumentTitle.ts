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
 * Hook for setting the document title based on server name.
 *
 * @author John Grimes
 */

import { useEffect } from "react";

import { useServerCapabilities } from "./useServerCapabilities";
import { config } from "../config";

/**
 * Updates the document title to the server name from the CapabilityStatement.
 * Falls back to the hostname if no server name is available.
 */
export function useDocumentTitle() {
  const { data: capabilities } = useServerCapabilities(config.fhirBaseUrl);

  useEffect(() => {
    const serverName = capabilities?.serverName;
    // Use server name if available, otherwise fall back to hostname.
    document.title = serverName ? serverName : window.location.hostname;
  }, [capabilities?.serverName]);
}
