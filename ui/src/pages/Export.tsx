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
 * Page for managing bulk export operations.
 * Supports multiple concurrent exports, each displayed in its own card.
 *
 * @author John Grimes
 */

import { Box, Flex } from "@radix-ui/themes";
import { useState } from "react";

import { CapabilityGuard } from "../components/auth/CapabilityGuard";
import { ExportCard } from "../components/export/ExportCard";
import { ExportForm } from "../components/export/ExportForm";
import { buildSearchParamMap } from "../hooks";

import type { ExportRequest } from "../types/export";

interface ExportJob {
  id: string;
  request: ExportRequest;
  createdAt: Date;
}

/**
 * Page component for managing bulk data exports.
 *
 * @returns The export page component.
 */
export function Export() {
  const [exports, setExports] = useState<ExportJob[]>([]);

  const handleExport = (request: ExportRequest) => {
    const newExport: ExportJob = {
      id: crypto.randomUUID(),
      request,
      createdAt: new Date(),
    };
    // Prepend new exports so most recent appears first.
    setExports((prev) => [newExport, ...prev]);
  };

  const handleCloseExport = (id: string) => {
    setExports((prev) => prev.filter((exportJob) => exportJob.id !== id));
  };

  // Show export form and any active/completed export cards.
  return (
    <CapabilityGuard>
      {(capabilities) => {
        const searchParams = buildSearchParamMap(capabilities);

        return (
          <Flex gap="6" direction={{ initial: "column", md: "row" }}>
            <Box style={{ flex: 1 }}>
              <ExportForm
                onSubmit={handleExport}
                resourceTypes={capabilities?.resourceTypes ?? []}
                searchParams={searchParams}
              />
            </Box>

            <Flex direction="column" gap="3" style={{ flex: 1 }}>
              {exports.map((exportJob) => (
                <ExportCard
                  key={exportJob.id}
                  request={exportJob.request}
                  createdAt={exportJob.createdAt}
                  onClose={() => handleCloseExport(exportJob.id)}
                />
              ))}
            </Flex>
          </Flex>
        );
      }}
    </CapabilityGuard>
  );
}
