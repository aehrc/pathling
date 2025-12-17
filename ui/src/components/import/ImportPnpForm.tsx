/**
 * Form for configuring and starting a ping and pull import job.
 *
 * @author John Grimes
 */

import { useState } from "react";
import {
  Box,
  Button,
  Card,
  Flex,
  Heading,
  RadioCards,
  Select,
  Text,
  TextField,
} from "@radix-ui/themes";
import { UploadIcon } from "@radix-ui/react-icons";
import type { ImportFormat } from "../../types/import";
import { IMPORT_FORMATS } from "../../types/import";
import type { ExportType, ImportPnpRequest, PnpSaveMode } from "../../types/importPnp";
import { EXPORT_TYPES, PNP_SAVE_MODES } from "../../types/importPnp";

interface ImportPnpFormProps {
  onSubmit: (request: ImportPnpRequest) => void;
  isSubmitting: boolean;
  disabled: boolean;
}

export function ImportPnpForm({ onSubmit, isSubmitting, disabled }: ImportPnpFormProps) {
  const [exportUrl, setExportUrl] = useState("");
  const [inputSource, setInputSource] = useState("");
  const [exportType, setExportType] = useState<ExportType>("dynamic");
  const [saveMode, setSaveMode] = useState<PnpSaveMode>("overwrite");
  const [inputFormat, setInputFormat] = useState<ImportFormat>("application/fhir+ndjson");

  const handleSubmit = () => {
    const request: ImportPnpRequest = {
      exportUrl,
      exportType,
      inputSource,
      saveMode,
      inputFormat,
    };
    onSubmit(request);
  };

  const isValid = exportUrl.trim() !== "" && inputSource.trim() !== "";

  return (
    <Card>
      <Flex direction="column" gap="4">
        <Heading size="4">New import</Heading>

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

        <Box>
          <Box mb="2">
            <Text as="label" size="2" weight="medium">
              Input source
            </Text>
          </Box>
          <TextField.Root
            placeholder="e.g., https://example.org/fhir"
            value={inputSource}
            onChange={(e) => setInputSource(e.target.value)}
          />
          <Text size="1" color="gray" mt="1">
            URI identifying the source of the imported data.
          </Text>
        </Box>

        <Box>
          <Box mb="2">
            <Text as="label" size="2" weight="medium">
              Export type
            </Text>
          </Box>
          <RadioCards.Root
            value={exportType}
            onValueChange={(v) => setExportType(v as ExportType)}
            columns="2"
            gap="2"
          >
            {EXPORT_TYPES.map((option) => (
              <RadioCards.Item value={option.value} key={option.value}>
                <Flex direction="column" width="100%">
                  <Text weight="medium">{option.label}</Text>
                  <Text size="1" color="gray">
                    {option.description}
                  </Text>
                </Flex>
              </RadioCards.Item>
            ))}
          </RadioCards.Root>
        </Box>

        <Box>
          <Box mb="2">
            <Text as="label" size="2" weight="medium">
              Save mode
            </Text>
          </Box>
          <RadioCards.Root
            value={saveMode}
            onValueChange={(v) => setSaveMode(v as PnpSaveMode)}
            columns="2"
            gap="2"
          >
            {PNP_SAVE_MODES.map((option) => (
              <RadioCards.Item value={option.value} key={option.value}>
                <Flex direction="column" width="100%">
                  <Text weight="medium">{option.label}</Text>
                  <Text size="1" color="gray">
                    {option.description}
                  </Text>
                </Flex>
              </RadioCards.Item>
            ))}
          </RadioCards.Root>
        </Box>

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

        <Button size="3" onClick={handleSubmit} disabled={disabled || isSubmitting || !isValid}>
          <UploadIcon />
          {isSubmitting ? "Starting import..." : "Start import"}
        </Button>
      </Flex>
    </Card>
  );
}
