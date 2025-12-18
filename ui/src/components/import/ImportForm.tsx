/**
 * Form for configuring and starting an import job.
 *
 * @author John Grimes
 */

import { Cross2Icon, PlusIcon, UploadIcon } from "@radix-ui/react-icons";
import {
  Box,
  Button,
  Card,
  Flex,
  Heading,
  IconButton,
  RadioCards,
  Select,
  Text,
  TextField,
} from "@radix-ui/themes";
import { useState } from "react";
import type { ImportFormat, ImportInput, ImportMode, ImportRequest } from "../../types/import";
import { IMPORT_FORMATS, IMPORT_MODES } from "../../types/import";

interface ImportFormProps {
  onSubmit: (request: ImportRequest) => void;
  isSubmitting: boolean;
  disabled: boolean;
  resourceTypes: string[];
}

const DEFAULT_INPUT: ImportInput = { type: "Patient", url: "" };

export function ImportForm({ onSubmit, isSubmitting, disabled, resourceTypes }: ImportFormProps) {
  const [inputFormat, setInputFormat] = useState<ImportFormat>("application/fhir+ndjson");
  const [inputSource, setInputSource] = useState("");
  const [mode, setMode] = useState<ImportMode>("overwrite");
  const [inputs, setInputs] = useState<ImportInput[]>([{ ...DEFAULT_INPUT }]);

  const handleSubmit = () => {
    const request: ImportRequest = {
      inputFormat,
      inputSource,
      input: inputs.filter((input) => input.url.trim() !== ""),
      mode,
    };
    onSubmit(request);
  };

  const addInput = () => {
    setInputs([...inputs, { ...DEFAULT_INPUT }]);
  };

  const removeInput = (index: number) => {
    if (inputs.length > 1) {
      setInputs(inputs.filter((_, i) => i !== index));
    }
  };

  const updateInput = (index: number, field: keyof ImportInput, value: string) => {
    setInputs(inputs.map((input, i) => (i === index ? { ...input, [field]: value } : input)));
  };

  const isValid = inputSource.trim() !== "" && inputs.some((input) => input.url.trim() !== "");

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
              Input source
            </Text>
          </Box>
          <TextField.Root
            placeholder="e.g., https://example.com/data-source"
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
              Import mode
            </Text>
          </Box>
          <RadioCards.Root
            value={mode}
            onValueChange={(v) => setMode(v as ImportMode)}
            columns="2"
            gap="2"
          >
            {IMPORT_MODES.map((modeOption) => (
              <RadioCards.Item value={modeOption.value} key={modeOption.value}>
                <Flex direction="column" width="100%">
                  <Text weight="medium">{modeOption.label}</Text>
                  <Text size="1" color="gray">
                    {modeOption.description}
                  </Text>
                </Flex>
              </RadioCards.Item>
            ))}
          </RadioCards.Root>
        </Box>

        <Box>
          <Flex justify="between" align="center" mb="2">
            <Text as="label" size="2" weight="medium">
              Input files
            </Text>
            <Button size="1" variant="soft" onClick={addInput}>
              <PlusIcon />
              Add input
            </Button>
          </Flex>
          <Flex direction="column" gap="2">
            {inputs.map((input, index) => (
              <Flex key={index} gap="2" align="end">
                <Box style={{ width: 160 }}>
                  {index === 0 && (
                    <Text size="1" color="gray" mb="1" as="div">
                      Resource type
                    </Text>
                  )}
                  <Select.Root
                    value={input.type}
                    onValueChange={(value) => updateInput(index, "type", value)}
                  >
                    <Select.Trigger style={{ width: "100%" }} />
                    <Select.Content>
                      {resourceTypes.map((type) => (
                        <Select.Item key={type} value={type}>
                          {type}
                        </Select.Item>
                      ))}
                    </Select.Content>
                  </Select.Root>
                </Box>
                <Box style={{ flex: 1 }}>
                  {index === 0 && (
                    <Text size="1" color="gray" mb="1" as="div">
                      URL
                    </Text>
                  )}
                  <TextField.Root
                    placeholder="e.g., s3a://bucket/Patient.ndjson"
                    value={input.url}
                    onChange={(e) => updateInput(index, "url", e.target.value)}
                  />
                </Box>
                <IconButton
                  size="2"
                  variant="soft"
                  color="red"
                  onClick={() => removeInput(index)}
                  disabled={inputs.length === 1}
                >
                  <Cross2Icon />
                </IconButton>
              </Flex>
            ))}
          </Flex>
          <Text size="1" color="gray" mt="1">
            Supported URL schemes: http://, https://, s3a://, hdfs://, file://
          </Text>
        </Box>

        <Button size="3" onClick={handleSubmit} disabled={disabled || isSubmitting || !isValid}>
          <UploadIcon />
          {isSubmitting ? "Starting import..." : "Start import"}
        </Button>
      </Flex>
    </Card>
  );
}
