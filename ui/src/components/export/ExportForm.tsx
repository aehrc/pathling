/**
 * Form for configuring and starting a bulk export job.
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
  Select,
  Text,
  TextField,
  Checkbox,
  ScrollArea,
} from "@radix-ui/themes";
import { PlayIcon } from "@radix-ui/react-icons";
import type { ExportLevel, ExportRequest } from "../../types/export";

interface ExportFormProps {
  onSubmit: (request: ExportRequest) => void;
  isSubmitting: boolean;
  disabled: boolean;
  resourceTypes: string[];
}

const EXPORT_LEVELS: { value: ExportLevel; label: string }[] = [
  { value: "system", label: "System (all resources)" },
  { value: "patient-type", label: "Patient type (all patients)" },
  { value: "patient-instance", label: "Patient instance (single patient)" },
  { value: "group", label: "Group (group members)" },
];

export function ExportForm({
  onSubmit,
  isSubmitting,
  disabled,
  resourceTypes,
}: ExportFormProps) {
  const [level, setLevel] = useState<ExportLevel>("system");
  const [selectedTypes, setSelectedTypes] = useState<string[]>([]);
  const [since, setSince] = useState("");
  const [until, setUntil] = useState("");
  const [elements, setElements] = useState("");
  const [patientId, setPatientId] = useState("");
  const [groupId, setGroupId] = useState("");

  const handleSubmit = () => {
    const request: ExportRequest = {
      level,
      resourceTypes: selectedTypes.length > 0 ? selectedTypes : undefined,
      since: since || undefined,
      until: until || undefined,
      elements: elements || undefined,
      patientId: level === "patient-instance" ? patientId : undefined,
      groupId: level === "group" ? groupId : undefined,
    };
    onSubmit(request);
  };

  const toggleResourceType = (type: string) => {
    setSelectedTypes((prev) =>
      prev.includes(type) ? prev.filter((t) => t !== type) : [...prev, type],
    );
  };

  const selectAllTypes = () => {
    setSelectedTypes([...resourceTypes]);
  };

  const clearAllTypes = () => {
    setSelectedTypes([]);
  };

  return (
    <Card>
      <Flex direction="column" gap="4">
        <Heading size="4">New export</Heading>

        <Box>
          <Text as="label" size="2" weight="medium" mb="1">
            Export level
          </Text>
          <Select.Root value={level} onValueChange={(value) => setLevel(value as ExportLevel)}>
            <Select.Trigger style={{ width: "100%" }} />
            <Select.Content>
              {EXPORT_LEVELS.map((l) => (
                <Select.Item key={l.value} value={l.value}>
                  {l.label}
                </Select.Item>
              ))}
            </Select.Content>
          </Select.Root>
        </Box>

        {level === "patient-instance" && (
          <Box>
            <Text as="label" size="2" weight="medium" mb="1">
              Patient ID
            </Text>
            <TextField.Root
              placeholder="e.g., patient-123"
              value={patientId}
              onChange={(e) => setPatientId(e.target.value)}
            />
          </Box>
        )}

        {level === "group" && (
          <Box>
            <Text as="label" size="2" weight="medium" mb="1">
              Group ID
            </Text>
            <TextField.Root
              placeholder="e.g., group-456"
              value={groupId}
              onChange={(e) => setGroupId(e.target.value)}
            />
          </Box>
        )}

        <Box>
          <Flex justify="between" align="center" mb="2">
            <Text as="label" size="2" weight="medium">
              Resource types
            </Text>
            <Flex gap="2">
              <Text size="1" color="blue" style={{ cursor: "pointer" }} onClick={selectAllTypes}>
                Select all
              </Text>
              <Text size="1" color="gray">
                |
              </Text>
              <Text size="1" color="blue" style={{ cursor: "pointer" }} onClick={clearAllTypes}>
                Clear
              </Text>
            </Flex>
          </Flex>
          <ScrollArea
            style={{
              height: 150,
              border: "1px solid var(--gray-5)",
              borderRadius: "var(--radius-2)",
            }}
          >
            <Box p="2">
              <Flex wrap="wrap" gap="2">
                {resourceTypes.map((type) => (
                  <Flex key={type} align="center" gap="1" pr="2">
                    <Checkbox
                      checked={selectedTypes.includes(type)}
                      onCheckedChange={() => toggleResourceType(type)}
                    />
                    <Text size="2">{type}</Text>
                  </Flex>
                ))}
              </Flex>
            </Box>
          </ScrollArea>
          <Text size="1" color="gray" mt="1">
            Leave empty to export all supported resource types.
          </Text>
        </Box>

        <Flex gap="4">
          <Box style={{ flex: 1 }}>
            <Text as="label" size="2" weight="medium" mb="1">
              Since
            </Text>
            <TextField.Root
              type="datetime-local"
              value={since}
              onChange={(e) => setSince(e.target.value)}
            />
            <Text size="1" color="gray" mt="1">
              Only resources updated after this time.
            </Text>
          </Box>
          <Box style={{ flex: 1 }}>
            <Text as="label" size="2" weight="medium" mb="1">
              Until
            </Text>
            <TextField.Root
              type="datetime-local"
              value={until}
              onChange={(e) => setUntil(e.target.value)}
            />
            <Text size="1" color="gray" mt="1">
              Only resources updated before this time.
            </Text>
          </Box>
        </Flex>

        <Box>
          <Text as="label" size="2" weight="medium" mb="1">
            Elements (optional)
          </Text>
          <TextField.Root
            placeholder="e.g., id,meta,name"
            value={elements}
            onChange={(e) => setElements(e.target.value)}
          />
          <Text size="1" color="gray" mt="1">
            Comma-separated list of element names to include.
          </Text>
        </Box>

        <Button
          size="3"
          onClick={handleSubmit}
          disabled={
            disabled ||
            isSubmitting ||
            (level === "patient-instance" && !patientId) ||
            (level === "group" && !groupId)
          }
        >
          <PlayIcon />
          {isSubmitting ? "Starting export..." : "Start export"}
        </Button>
      </Flex>
    </Card>
  );
}
