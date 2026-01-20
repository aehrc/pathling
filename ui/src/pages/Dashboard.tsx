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
 * Dashboard page displaying server information from the CapabilityStatement.
 *
 * @author John Grimes
 */

import { CheckCircledIcon, CrossCircledIcon } from "@radix-ui/react-icons";
import {
  Badge,
  Box,
  Card,
  DataList,
  Flex,
  Heading,
  Separator,
  Spinner,
  Table,
  Text,
} from "@radix-ui/themes";

import { config } from "../config";
import { useServerCapabilities } from "../hooks/useServerCapabilities";

/**
 * Displays server information from the CapabilityStatement.
 *
 * @returns The dashboard page component.
 */
export function Dashboard() {
  const { fhirBaseUrl } = config;
  const { data: capabilities, isLoading, error } = useServerCapabilities(fhirBaseUrl);

  if (isLoading) {
    return (
      <Flex align="center" gap="2">
        <Spinner />
        <Text>Loading server information...</Text>
      </Flex>
    );
  }

  if (error) {
    return <Text color="red">{error.message}</Text>;
  }

  if (!capabilities) {
    return <Text color="gray">No server information available.</Text>;
  }

  return (
    <Flex gap="6" direction={{ initial: "column", lg: "row" }}>
      <Box style={{ flex: 1 }}>
        <Card mb="4">
          <Heading size="4" mb="3">
            Server information
          </Heading>

          <DataList.Root>
            <DataList.Item>
              <DataList.Label minWidth="120px">Server name</DataList.Label>
              <DataList.Value>{capabilities.serverName || "Unknown"}</DataList.Value>
            </DataList.Item>

            {capabilities.serverVersion && (
              <DataList.Item>
                <DataList.Label minWidth="120px">Version</DataList.Label>
                <DataList.Value>{capabilities.serverVersion}</DataList.Value>
              </DataList.Item>
            )}

            <DataList.Item>
              <DataList.Label minWidth="120px">FHIR version</DataList.Label>
              <DataList.Value>{capabilities.fhirVersion || "Unknown"}</DataList.Value>
            </DataList.Item>

            {capabilities.publisher && (
              <DataList.Item>
                <DataList.Label minWidth="120px">Publisher</DataList.Label>
                <DataList.Value>{capabilities.publisher}</DataList.Value>
              </DataList.Item>
            )}

            <DataList.Item>
              <DataList.Label minWidth="120px">Authentication</DataList.Label>
              <DataList.Value>
                <Flex align="center" gap="1">
                  {capabilities.authRequired ? (
                    <>
                      <CheckCircledIcon color="var(--green-9)" />
                      <Text color="green">SMART on FHIR</Text>
                    </>
                  ) : (
                    <>
                      <CrossCircledIcon color="var(--gray-9)" />
                      <Text color="gray">Not required</Text>
                    </>
                  )}
                </Flex>
              </DataList.Value>
            </DataList.Item>
          </DataList.Root>

          {capabilities.description && (
            <>
              <Separator my="3" size="4" />
              <Text size="2" color="gray">
                {capabilities.description}
              </Text>
            </>
          )}
        </Card>

        {capabilities.operations && capabilities.operations.length > 0 && (
          <Card>
            <Heading size="4" mb="3">
              System operations
            </Heading>
            <Flex gap="2" wrap="wrap">
              {capabilities.operations.map((op) => (
                <Badge key={op.name} size="2" variant="soft">
                  ${op.name}
                </Badge>
              ))}
            </Flex>
          </Card>
        )}
      </Box>

      {capabilities.resources && capabilities.resources.length > 0 && (
        <Box style={{ flex: 1 }}>
          <Card>
            <Heading size="4" mb="3">
              Supported resources
            </Heading>
            <Table.Root layout="fixed">
              <Table.Header>
                <Table.Row>
                  <Table.ColumnHeaderCell>Resource</Table.ColumnHeaderCell>
                  <Table.ColumnHeaderCell>Interactions</Table.ColumnHeaderCell>
                </Table.Row>
              </Table.Header>
              <Table.Body>
                {capabilities.resources.map((resource) => (
                  <Table.Row key={resource.type}>
                    <Table.Cell
                      style={{
                        maxWidth: "50%",
                        overflow: "hidden",
                        textWrap: "nowrap",
                        textOverflow: "ellipsis",
                      }}
                    >
                      <Text weight="medium">{resource.type}</Text>
                    </Table.Cell>
                    <Table.Cell>
                      <Flex gap="1" wrap="wrap">
                        {resource.operations.map((op) => (
                          <Badge
                            key={op}
                            size="1"
                            variant="soft"
                            color={op.startsWith("$") ? "teal" : "gray"}
                          >
                            {op}
                          </Badge>
                        ))}
                      </Flex>
                    </Table.Cell>
                  </Table.Row>
                ))}
              </Table.Body>
            </Table.Root>
          </Card>
        </Box>
      )}
    </Flex>
  );
}
