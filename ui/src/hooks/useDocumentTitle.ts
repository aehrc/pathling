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
