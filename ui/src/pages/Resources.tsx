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
 * Page for browsing FHIR resources with FHIRPath filters.
 *
 * @author John Grimes
 */

import { Box, Flex, Spinner, Text } from "@radix-ui/themes";
import { useState } from "react";

import { deleteResource } from "../api";
import { LoginRequired } from "../components/auth/LoginRequired";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { DeleteConfirmationDialog } from "../components/resources/DeleteConfirmationDialog";
import { ResourceResultList } from "../components/resources/ResourceResultList";
import { ResourceSearchForm } from "../components/resources/ResourceSearchForm";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useToast } from "../contexts/ToastContext";
import { useFhirPathSearch, useServerCapabilities } from "../hooks";

import type { SearchParamCapability } from "../hooks/useServerCapabilities";
import type { SearchRequest } from "../types/search";

interface DeleteTarget {
  resourceType: string;
  resourceId: string;
  summary: string | null;
}

/**
 * Page component for browsing and managing FHIR resources.
 *
 * @returns The resources page component.
 */
export function Resources() {
  const { fhirBaseUrl } = config;
  const { client, isAuthenticated } = useAuth();
  const { showToast } = useToast();
  const accessToken = client?.state.tokenResponse?.access_token;

  const [searchRequest, setSearchRequest] = useState<SearchRequest | null>(null);
  const [showDeleteDialog, setShowDeleteDialog] = useState<DeleteTarget | null>(null);
  const [isDeleting, setIsDeleting] = useState(false);

  // Fetch server capabilities to determine if auth is required.
  const { data: capabilities, isLoading: isLoadingCapabilities } =
    useServerCapabilities(fhirBaseUrl);

  // Build search parameters mapping from capabilities.
  let searchParams: Record<string, SearchParamCapability[]> | undefined;
  if (capabilities?.resources) {
    searchParams = {};
    for (const resource of capabilities.resources) {
      searchParams[resource.type] = resource.searchParams;
    }
  }

  // Execute the search query. 401 errors handled globally.
  const {
    resources,
    total,
    isLoading: isSearching,
    error: searchError,
    refetch,
  } = useFhirPathSearch({
    resourceType: searchRequest?.resourceType ?? "",
    filters: searchRequest?.filters ?? [],
    searchParams: searchRequest?.params,
    enabled: !!searchRequest?.resourceType,
  });

  const handleSearch = (request: SearchRequest) => {
    setSearchRequest(request);
  };

  // Handle delete button click - open confirmation dialog.
  const handleDeleteClick = (resourceType: string, resourceId: string, summary: string | null) => {
    setShowDeleteDialog({ resourceType, resourceId, summary });
  };

  // Handle delete confirmation - perform the delete. 401 errors handled globally.
  const handleDeleteConfirm = async () => {
    if (!showDeleteDialog) return;

    setIsDeleting(true);
    try {
      await deleteResource(fhirBaseUrl!, {
        resourceType: showDeleteDialog.resourceType,
        id: showDeleteDialog.resourceId,
        accessToken,
      });
      showToast(
        "Resource deleted",
        `${showDeleteDialog.resourceType}/${showDeleteDialog.resourceId}`,
      );
      setShowDeleteDialog(null);
      // Refresh the search results.
      refetch();
    } catch (err) {
      showToast("Delete failed", err instanceof Error ? err.message : "An error occurred");
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
            searchParams={searchParams}
          />
        </Box>

        <Box style={{ flex: 1 }}>
          <ResourceResultList
            resources={resources}
            total={total}
            isLoading={isSearching}
            error={searchError}
            hasSearched={searchRequest !== null}
            fhirBaseUrl={fhirBaseUrl}
            onDelete={handleDeleteClick}
          />
        </Box>
      </Flex>
      <SessionExpiredDialog />
      {showDeleteDialog && (
        <DeleteConfirmationDialog
          open={!!showDeleteDialog}
          onOpenChange={(open) => {
            if (!open) setShowDeleteDialog(null);
          }}
          resourceType={showDeleteDialog.resourceType}
          resourceId={showDeleteDialog.resourceId}
          resourceSummary={showDeleteDialog.summary}
          onConfirm={handleDeleteConfirm}
          isDeleting={isDeleting}
        />
      )}
    </>
  );
}
