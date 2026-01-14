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
    if (serverName) {
      document.title = serverName;
    } else {
      // Fall back to the hostname.
      document.title = window.location.hostname;
    }
  }, [capabilities?.serverName]);
}
