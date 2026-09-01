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
 * Shared guard that gates page content on server capabilities and
 * authentication. Every page fetches capabilities to learn whether the server
 * requires a login, shows a consistent loading state while that check runs, and
 * mounts the session-expiry dialog. This component centralises that shared
 * boilerplate so individual pages only supply their own content.
 *
 * @author John Grimes
 */

import { Flex, Spinner, Text } from "@radix-ui/themes";

import { LoginRequired } from "./LoginRequired";
import { SessionExpiredDialog } from "./SessionExpiredDialog";
import { config } from "../../config";
import { useAuth } from "../../contexts/AuthContext";
import { useServerCapabilities } from "../../hooks/useServerCapabilities";

import type { ServerCapabilities } from "../../hooks/useServerCapabilities";
import type { ReactNode } from "react";

interface CapabilityGuardProps {
  /**
   * Renders the page content once capabilities are loaded and access is
   * permitted. The resolved capabilities are passed through so pages can derive
   * resource types and search parameters from them.
   */
  children: (capabilities: ServerCapabilities | undefined) => ReactNode;
}

/**
 * Gates its children behind the server capability check and authentication.
 *
 * @param props - The component props.
 * @param props.children - Render function invoked with the resolved
 *   capabilities when access is permitted.
 * @returns The loading state, a login prompt, or the rendered children.
 */
export function CapabilityGuard({ children }: Readonly<CapabilityGuardProps>) {
  const { fhirBaseUrl } = config;
  const { isAuthenticated } = useAuth();
  const { data: capabilities, isLoading: isLoadingCapabilities } =
    useServerCapabilities(fhirBaseUrl);

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

  // Access permitted: render the page content with the resolved capabilities.
  return (
    <>
      {children(capabilities)}
      <SessionExpiredDialog />
    </>
  );
}
