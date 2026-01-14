/**
 * Form for executing ViewDefinitions.
 *
 * @author John Grimes
 */

import { CopyIcon, PlayIcon, UploadIcon } from "@radix-ui/react-icons";
import {
  Box,
  Button,
  Callout,
  Card,
  Flex,
  Heading,
  IconButton,
  Select,
  Spinner,
  Tabs,
  Text,
  TextArea,
  Tooltip,
} from "@radix-ui/themes";
import { useState } from "react";

import { useClipboard } from "../../hooks";
import { useViewDefinitions } from "../../hooks/useViewDefinitions";

import type { ViewRunRequest } from "../../types/hooks";
import type { CreateViewDefinitionResult } from "../../types/sqlOnFhir";

interface SqlOnFhirFormProps {
  onExecute: (request: ViewRunRequest) => void;
  onSaveToServer: (json: string) => Promise<CreateViewDefinitionResult>;
  isExecuting: boolean;
  isSaving: boolean;
  disabled?: boolean;
}

const EXAMPLE_VIEW_DEFINITION = `{
  "resourceType": "ViewDefinition",
  "name": "example_view",
  "resource": "Patient",
  "select": [
    {
      "column": [
        { "path": "id", "name": "patient_id" },
        { "path": "gender", "name": "gender" }
      ]
    }
  ]
}`;

/**
 * Form for selecting and executing ViewDefinitions.
 *
 * @param root0 - The component props.
 * @param root0.onExecute - Callback when view is executed.
 * @param root0.onSaveToServer - Callback to save view definition to server.
 * @param root0.isExecuting - Whether execution is in progress.
 * @param root0.isSaving - Whether save is in progress.
 * @param root0.disabled - Whether the form is disabled.
 * @returns The SQL on FHIR form component.
 */
export function SqlOnFhirForm({
  onExecute,
  onSaveToServer,
  isExecuting,
  isSaving,
  disabled = false,
}: SqlOnFhirFormProps) {
  const [activeTab, setActiveTab] = useState<"stored" | "custom">("stored");
  const [selectedViewDefinitionId, setSelectedViewDefinitionId] = useState<string>("");
  const [customJson, setCustomJson] = useState<string>("");
  const [saveError, setSaveError] = useState<Error | null>(null);

  const { data: viewDefinitions, isLoading: isLoadingViewDefinitions } = useViewDefinitions();
  const copyToClipboard = useClipboard();

  const handleExecute = () => {
    if (activeTab === "stored" && selectedViewDefinitionId) {
      onExecute({
        mode: "stored",
        viewDefinitionId: selectedViewDefinitionId,
      });
    } else if (activeTab === "custom" && customJson.trim()) {
      onExecute({
        mode: "inline",
        viewDefinitionJson: customJson,
      });
    }
  };

  const canExecute =
    (activeTab === "stored" && selectedViewDefinitionId) ||
    (activeTab === "custom" && customJson.trim());

  const handleSaveToServer = async () => {
    setSaveError(null);
    try {
      const result = await onSaveToServer(customJson);
      // Switch to stored tab and select the new ViewDefinition.
      setActiveTab("stored");
      setSelectedViewDefinitionId(result.id);
    } catch (err) {
      setSaveError(err instanceof Error ? err : new Error("Failed to save"));
    }
  };

  return (
    <Card>
      <Flex direction="column" gap="4">
        <Heading size="4">SQL on FHIR</Heading>

        <Tabs.Root
          value={activeTab}
          onValueChange={(value) => setActiveTab(value as "stored" | "custom")}
        >
          <Tabs.List>
            <Tabs.Trigger value="stored">Select view definition</Tabs.Trigger>
            <Tabs.Trigger value="custom">Provide JSON</Tabs.Trigger>
          </Tabs.List>

          <Box pt="4">
            <Tabs.Content value="stored">
              <Box>
                <Box mb="2">
                  <Text as="label" size="2" weight="medium">
                    View definition
                  </Text>
                </Box>
                {isLoadingViewDefinitions ? (
                  <Flex align="center" gap="2" py="2">
                    <Spinner size="1" />
                    <Text size="2" color="gray">
                      Loading view definitions...
                    </Text>
                  </Flex>
                ) : viewDefinitions && viewDefinitions.length > 0 ? (
                  <Select.Root
                    value={selectedViewDefinitionId}
                    onValueChange={setSelectedViewDefinitionId}
                  >
                    <Select.Trigger
                      style={{ width: "100%" }}
                      placeholder="Select a view definition"
                    />
                    <Select.Content>
                      {viewDefinitions.map((vd) => (
                        <Select.Item key={vd.id} value={vd.id}>
                          {vd.name}
                        </Select.Item>
                      ))}
                    </Select.Content>
                  </Select.Root>
                ) : (
                  <Text size="2" color="gray" as="p">
                    No view definitions found on the server. You can use the "Provide JSON" tab to
                    execute a view definition directly.
                  </Text>
                )}
                {viewDefinitions &&
                  viewDefinitions.length > 0 &&
                  (selectedViewDefinitionId ? (
                    <Box mt="2" style={{ position: "relative" }}>
                      <Tooltip content="Copy to clipboard">
                        <IconButton
                          size="1"
                          variant="ghost"
                          aria-label="Copy to clipboard"
                          onClick={() =>
                            copyToClipboard(
                              viewDefinitions.find((vd) => vd.id === selectedViewDefinitionId)
                                ?.json ?? "",
                            )
                          }
                          style={{ position: "absolute", top: 8, right: 8, zIndex: 1 }}
                        >
                          <CopyIcon />
                        </IconButton>
                      </Tooltip>
                      <TextArea
                        readOnly
                        size="1"
                        rows={16}
                        value={
                          viewDefinitions.find((vd) => vd.id === selectedViewDefinitionId)?.json ??
                          ""
                        }
                        style={{ fontFamily: "monospace" }}
                      />
                    </Box>
                  ) : (
                    <Text size="1" color="gray" mt="2">
                      Select a view definition that has been loaded into the server.
                    </Text>
                  ))}
              </Box>
            </Tabs.Content>

            <Tabs.Content value="custom">
              <Box>
                <Box mb="2">
                  <Text as="label" size="2" weight="medium">
                    View definition JSON
                  </Text>
                </Box>
                <TextArea
                  size="1"
                  resize="vertical"
                  rows={16}
                  placeholder={EXAMPLE_VIEW_DEFINITION}
                  value={customJson}
                  onChange={(e) => setCustomJson(e.target.value)}
                  style={{ fontFamily: "monospace" }}
                />
                <Text size="1" color="gray" mt="2">
                  Enter a valid view definition resource in JSON format.
                </Text>
                {saveError && (
                  <Callout.Root color="red" mt="2" size="1">
                    <Callout.Text>{saveError.message}</Callout.Text>
                  </Callout.Root>
                )}
              </Box>
            </Tabs.Content>
          </Box>
        </Tabs.Root>

        <Flex gap="3">
          <Button
            size="3"
            onClick={handleExecute}
            disabled={disabled || isExecuting || !canExecute}
            style={{ flex: 1 }}
          >
            <PlayIcon />
            {isExecuting ? "Executing..." : "Execute"}
          </Button>
          {activeTab === "custom" && (
            <Button
              size="3"
              variant="soft"
              onClick={handleSaveToServer}
              disabled={disabled || isSaving || !customJson.trim()}
              style={{ flex: 1 }}
            >
              <UploadIcon />
              {isSaving ? "Saving..." : "Save to server"}
            </Button>
          )}
        </Flex>
      </Flex>
    </Card>
  );
}
