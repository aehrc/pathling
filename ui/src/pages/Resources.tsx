/**
 * Page for browsing FHIR resources with FHIRPath filters.
 *
 * @author John Grimes
 */

import { Box, Flex, Spinner, Text } from "@radix-ui/themes";
import { useEffect, useState } from "react";
import { deleteResource } from "../api";
import { LoginRequired } from "../components/auth/LoginRequired";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { DeleteConfirmationDialog } from "../components/resources/DeleteConfirmationDialog";
import { ResourceResultList } from "../components/resources/ResourceResultList";
import { ResourceSearchForm } from "../components/resources/ResourceSearchForm";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useToast } from "../contexts/ToastContext";
import { useFhirPathSearch, useServerCapabilities, useUnauthorizedHandler } from "../hooks";
import { UnauthorizedError } from "../types/errors";
import type { SearchRequest } from "../types/search";

interface DeleteTarget {
  resourceType: string;
  resourceId: string;
  summary: string | null;
}

export function Resources() {
  const { fhirBaseUrl } = config;
  const { client, isAuthenticated } = useAuth();
  const { showToast } = useToast();
  const accessToken = client?.state.tokenResponse?.access_token;
  const handleUnauthorizedError = useUnauthorizedHandler();

  const [searchRequest, setSearchRequest] = useState<SearchRequest | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<DeleteTarget | null>(null);
  const [isDeleting, setIsDeleting] = useState(false);

  // Fetch server capabilities to determine if auth is required.
  const { data: capabilities, isLoading: isLoadingCapabilities } =
    useServerCapabilities(fhirBaseUrl);

  // Execute the search query.
  const {
    resources,
    total,
    isLoading: isSearching,
    error: searchError,
    refetch,
  } = useFhirPathSearch({
    resourceType: searchRequest?.resourceType ?? "",
    filters: searchRequest?.filters ?? [],
    enabled: !!searchRequest?.resourceType,
  });

  // Handle search errors.
  useEffect(() => {
    if (searchError instanceof UnauthorizedError) {
      handleUnauthorizedError();
    }
  }, [searchError, handleUnauthorizedError]);

  const handleSearch = (request: SearchRequest) => {
    setSearchRequest(request);
  };

  // Handle delete button click - open confirmation dialog.
  const handleDeleteClick = (resourceType: string, resourceId: string, summary: string | null) => {
    setDeleteTarget({ resourceType, resourceId, summary });
  };

  // Handle delete confirmation - perform the delete.
  const handleDeleteConfirm = async () => {
    if (!deleteTarget) return;

    setIsDeleting(true);
    try {
      await deleteResource(fhirBaseUrl!, {
        resourceType: deleteTarget.resourceType,
        id: deleteTarget.resourceId,
        accessToken,
      });
      showToast(
        "Resource deleted",
        `${deleteTarget.resourceType}/${deleteTarget.resourceId}`,
      );
      setDeleteTarget(null);
      // Refresh the search results.
      await refetch();
    } catch (err) {
      if (err instanceof UnauthorizedError) {
        handleUnauthorizedError();
      } else {
        showToast(
          "Delete failed",
          err instanceof Error ? err.message : "An error occurred",
        );
      }
    } finally {
      setIsDeleting(false);
    }
  };

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
            resourceTypes={capabilities?.resourceTypes ?? []}
          />
        </Box>

        <Box style={{ flex: 1 }}>
          <ResourceResultList
            resources={resources}
            total={total}
            isLoading={isSearching}
            error={displayError}
            hasSearched={searchRequest !== null}
            fhirBaseUrl={fhirBaseUrl}
            onDelete={handleDeleteClick}
          />
        </Box>
      </Flex>
      <SessionExpiredDialog />
      {deleteTarget && (
        <DeleteConfirmationDialog
          open={!!deleteTarget}
          onOpenChange={(open) => {
            if (!open) setDeleteTarget(null);
          }}
          resourceType={deleteTarget.resourceType}
          resourceId={deleteTarget.resourceId}
          resourceSummary={deleteTarget.summary}
          onConfirm={handleDeleteConfirm}
          isDeleting={isDeleting}
        />
      )}
    </>
  );
}
