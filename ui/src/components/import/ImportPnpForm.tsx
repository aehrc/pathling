/**
 * Form for configuring and starting a ping and pull import job.
 *
 * @author John Grimes
 */

import { UploadIcon } from "@radix-ui/react-icons";
import { Box, Button, Card, Flex, Heading, Select, Text, TextField } from "@radix-ui/themes";
import { useState } from "react";
import type { ImportFormat, SaveMode } from "../../types/import";
import { IMPORT_FORMATS } from "../../types/import";
import type { ExportType, ImportPnpRequest } from "../../types/importPnp";
import { SaveModeField } from "./SaveModeField";

interface ImportPnpFormProps {
  onSubmit: (request: ImportPnpRequest) => void;
  isSubmitting: boolean;
  disabled: boolean;
}

export function ImportPnpForm({ onSubmit, isSubmitting, disabled }: ImportPnpFormProps) {
  const [exportUrl, setExportUrl] = useState("");
  const [saveMode, setSaveMode] = useState<SaveMode>("overwrite");
  const [inputFormat, setInputFormat] = useState<ImportFormat>("application/fhir+ndjson");
  const exportType: ExportType = "dynamic";

  const handleSubmit = () => {
    const request: ImportPnpRequest = {
      exportUrl,
      exportType,
      saveMode,
      inputFormat,
    };
    onSubmit(request);
  };

  const isValid = exportUrl.trim() !== "";

  return (
    <Card>
      <Flex direction="column" gap="4">
        <Heading size="4">New import</Heading>

        <Box>
          <Box mb="2">
            <Text as="label" size="2" weight="medium">
              Input format
            </Text>
          </Box>
          <Select.Root
            value={inputFormat}
            onValueChange={(value) => setInputFormat(value as ImportFormat)}
          >
            <Select.Trigger style={{ width: "100%" }} />
            <Select.Content>
              {IMPORT_FORMATS.map((format) => (
                <Select.Item key={format.value} value={format.value}>
                  {format.label}
                </Select.Item>
              ))}
            </Select.Content>
          </Select.Root>
        </Box>

        <Box>
          <Box mb="2">
            <Text as="label" size="2" weight="medium">
              Export URL
            </Text>
          </Box>
          <TextField.Root
            placeholder="e.g., https://example.org/fhir/$export"
            value={exportUrl}
            onChange={(e) => setExportUrl(e.target.value)}
          />
          <Text size="1" color="gray" mt="1">
            The bulk export endpoint URL of the remote FHIR server.
          </Text>
        </Box>

        <SaveModeField value={saveMode} onChange={setSaveMode} />

        <Button size="3" onClick={handleSubmit} disabled={disabled || isSubmitting || !isValid}>
          <UploadIcon />
          {isSubmitting ? "Starting import..." : "Start import"}
        </Button>
      </Flex>
    </Card>
  );
}
