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
  Select,
  Text,
  TextField,
} from "@radix-ui/themes";
import { useRef, useState } from "react";

import { SaveModeField } from "./SaveModeField";
import { IMPORT_FORMATS } from "../../types/import";

import type { ImportFormat, ImportRequest, SaveMode } from "../../types/import";

interface ImportInputWithId {
  id: number;
  type: string;
  url: string;
}

interface ImportFormProps {
  onSubmit: (request: ImportRequest) => void;
  isSubmitting: boolean;
  disabled: boolean;
  resourceTypes: string[];
}

/**
 *
 * @param root0
 * @param root0.onSubmit
 * @param root0.isSubmitting
 * @param root0.disabled
 * @param root0.resourceTypes
 */
export function ImportForm({ onSubmit, isSubmitting, disabled, resourceTypes }: ImportFormProps) {
  const idCounter = useRef(1);
  const [inputFormat, setInputFormat] = useState<ImportFormat>("application/fhir+ndjson");
  const [saveMode, setSaveMode] = useState<SaveMode>("overwrite");
  const [inputs, setInputs] = useState<ImportInputWithId[]>([{ id: 0, type: "Patient", url: "" }]);

  const handleSubmit = () => {
    const request: ImportRequest = {
      inputFormat,
      input: inputs
        .filter((input) => input.url.trim() !== "")
        .map(({ type, url }) => ({ type, url })),
      saveMode,
    };
    onSubmit(request);
  };

  const addInput = () => {
    setInputs([...inputs, { id: idCounter.current++, type: "Patient", url: "" }]);
  };

  const removeInput = (id: number) => {
    if (inputs.length > 1) {
      setInputs(inputs.filter((input) => input.id !== id));
    }
  };

  const updateInput = (id: number, field: "type" | "url", value: string) => {
    setInputs(inputs.map((input) => (input.id === id ? { ...input, [field]: value } : input)));
  };

  const isValid = inputs.some((input) => input.url.trim() !== "");

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
              <Flex key={input.id} gap="2" align="end">
                <Box style={{ width: 160 }}>
                  {index === 0 && (
                    <Text size="1" color="gray" mb="1" as="div">
                      Resource type
                    </Text>
                  )}
                  <Select.Root
                    value={input.type}
                    onValueChange={(value) => updateInput(input.id, "type", value)}
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
                    onChange={(e) => updateInput(input.id, "url", e.target.value)}
                  />
                </Box>
                <IconButton
                  size="2"
                  variant="soft"
                  color="red"
                  onClick={() => removeInput(input.id)}
                  disabled={inputs.length === 1}
                >
                  <Cross2Icon />
                </IconButton>
              </Flex>
            ))}
          </Flex>
          <Text size="1" color="gray" mt="1">
            Supported URL schemes: s3a://, hdfs://, file://
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
