/**
 * Page for managing import operations.
 *
 * @author John Grimes
 */

import { Box, Flex, Spinner, Tabs, Text } from "@radix-ui/themes";
import { useState } from "react";

import { LoginRequired } from "../components/auth/LoginRequired";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { ImportCard } from "../components/import/ImportCard";
import { ImportForm } from "../components/import/ImportForm";
import { ImportPnpForm } from "../components/import/ImportPnpForm";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useServerCapabilities } from "../hooks";

import type { ImportJob, ImportRequest } from "../types/import";
import type { ImportPnpRequest } from "../types/importPnp";

/**
 *
 */
export function Import() {
  const { fhirBaseUrl } = config;
  const { isAuthenticated } = useAuth();

  // Track all import jobs.
  const [imports, setImports] = useState<ImportJob[]>([]);

  // Track error messages for display.
  const [, setError] = useState<string | null>(null);

  // Fetch server capabilities to determine if auth is required.
  const { data: capabilities, isLoading: isLoadingCapabilities } =
    useServerCapabilities(fhirBaseUrl);

  /**
   * Handles submission of a standard import.
   *
   * @param request - The import request configuration.
   */
  const handleStandardImport = (request: ImportRequest) => {
    const newImport: ImportJob = {
      id: crypto.randomUUID(),
      type: "standard",
      request,
      createdAt: new Date(),
    };
    setImports((prev) => [newImport, ...prev]);
  };

  /**
   * Handles submission of a PnP import.
   *
   * @param request - The PnP import request configuration.
   */
  const handlePnpImport = (request: ImportPnpRequest) => {
    const newImport: ImportJob = {
      id: crypto.randomUUID(),
      type: "pnp",
      request,
      createdAt: new Date(),
    };
    setImports((prev) => [newImport, ...prev]);
  };

  /**
   * Handles closing/removing an import card.
   *
   * @param id - The ID of the import to close.
   */
  const handleCloseImport = (id: string) => {
    setImports((prev) => prev.filter((job) => job.id !== id));
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

  // Show import form (either auth not required or user is authenticated).
  return (
    <>
      <Flex gap="6" direction={{ initial: "column", md: "row" }}>
        <Tabs.Root defaultValue="urls" style={{ flex: 1 }}>
          <Tabs.List>
            <Tabs.Trigger value="urls">Import from URLs</Tabs.Trigger>
            <Tabs.Trigger value="fhir-server">Import from FHIR server</Tabs.Trigger>
          </Tabs.List>

          <Box pt="4">
            <Tabs.Content value="urls">
              <ImportForm
                onSubmit={handleStandardImport}
                isSubmitting={false}
                disabled={false}
                resourceTypes={capabilities?.resourceTypes ?? []}
              />
            </Tabs.Content>

            <Tabs.Content value="fhir-server">
              <ImportPnpForm onSubmit={handlePnpImport} isSubmitting={false} disabled={false} />
            </Tabs.Content>
          </Box>
        </Tabs.Root>

        <Flex direction="column" gap="3" mt="4" style={{ flex: 1 }}>
          {imports.map((job) => (
            <ImportCard
              key={job.id}
              job={job}
              onError={(message) => setError(message)}
              onClose={() => handleCloseImport(job.id)}
            />
          ))}
        </Flex>
      </Flex>

      <SessionExpiredDialog />
    </>
  );
}
