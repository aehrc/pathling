/**
 * Page for browsing FHIR resources with FHIRPath filters.
 *
 * @author John Grimes
 */

import { Box, Flex, Spinner, Text } from "@radix-ui/themes";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { LoginRequired } from "../components/auth/LoginRequired";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { ResourceResultList } from "../components/resources/ResourceResultList";
import { ResourceSearchForm } from "../components/resources/ResourceSearchForm";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useResourceSearch } from "../hooks/useResourceSearch";
import { useServerCapabilities } from "../hooks/useServerCapabilities";
import { UnauthorizedError } from "../types/errors";
import type { SearchRequest } from "../types/search";

export function Resources() {
  const { fhirBaseUrl } = config;
  const { isAuthenticated, clearSessionAndPromptLogin } = useAuth();

  const [searchRequest, setSearchRequest] = useState<SearchRequest | null>(null);
  const unauthorizedHandledRef = useRef(false);

  // Fetch server capabilities to determine if auth is required.
  const { data: capabilities, isLoading: isLoadingCapabilities } =
    useServerCapabilities(fhirBaseUrl);

  // Extract resource types from capabilities.
  const resourceTypes = useMemo(() => {
    if (!capabilities?.resources) return [];
    return capabilities.resources.map((r) => r.type).sort();
  }, [capabilities]);

  // Execute the search query.
  const { data: searchResult, isLoading: isSearching, error: searchError } = useResourceSearch(
    searchRequest,
  );

  // Handle 401 errors by clearing session and prompting for re-authentication.
  const handleUnauthorizedError = useCallback(() => {
    if (unauthorizedHandledRef.current) return;
    unauthorizedHandledRef.current = true;
    clearSessionAndPromptLogin();
  }, [clearSessionAndPromptLogin]);

  // Reset the unauthorized flag when user becomes authenticated.
  useEffect(() => {
    if (isAuthenticated) {
      unauthorizedHandledRef.current = false;
    }
  }, [isAuthenticated]);

  // Handle search errors.
  useEffect(() => {
    if (searchError instanceof UnauthorizedError) {
      handleUnauthorizedError();
    }
  }, [searchError, handleUnauthorizedError]);

  const handleSearch = useCallback((request: SearchRequest) => {
    setSearchRequest(request);
  }, []);

  // Show loading state while checking server capabilities.
  if (isLoadingCapabilities) {
    return (
      <>
        <Flex align="center" gap="2">
          <Spinner />
          <Text>Checking server capabilities...</Text>
        </Flex>
        <SessionExpiredDialog />
      </>
    );
  }

  // Show login prompt if authentication is required but not authenticated.
  if (capabilities?.authRequired && !isAuthenticated) {
    return <LoginRequired />;
  }

  // Determine actual error to display (ignore unauthorized since it's handled separately).
  const displayError =
    searchError && !(searchError instanceof UnauthorizedError) ? searchError : null;

  // Show search form and results.
  return (
    <>
      <Flex gap="6" direction={{ initial: "column", md: "row" }}>
        <Box style={{ flex: 1 }}>
          <ResourceSearchForm
            onSubmit={handleSearch}
            isLoading={isSearching}
            disabled={false}
            resourceTypes={resourceTypes}
          />
        </Box>

        <Box style={{ flex: 1 }}>
          <ResourceResultList
            resources={searchResult?.resources}
            total={searchResult?.total}
            isLoading={isSearching}
            error={displayError}
            hasSearched={searchRequest !== null}
            fhirBaseUrl={fhirBaseUrl}
          />
        </Box>
      </Flex>
      <SessionExpiredDialog />
    </>
  );
}
