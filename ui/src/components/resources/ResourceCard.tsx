/**
 * Card component for displaying a single FHIR resource.
 *
 * @author John Grimes
 */

import { CopyIcon, ExternalLinkIcon, TrashIcon } from "@radix-ui/react-icons";
import {
  Badge,
  Box,
  Card,
  Code,
  Flex,
  IconButton,
  ScrollArea,
  Text,
  Tooltip,
} from "@radix-ui/themes";
import type { Resource } from "fhir/r4";
import { useState } from "react";
import { useClipboard } from "../../hooks";

interface ResourceCardProps {
  resource: Resource;
  fhirBaseUrl: string;
  onDelete: (resourceType: string, resourceId: string, summary: string | null) => void;
}

/**
 * Extracts a human-readable summary from a FHIR resource based on its type.
 */
function getResourceSummary(resource: Resource): string | null {
  const r = resource as unknown as Record<string, unknown>;

  // Patient: name.
  if (resource.resourceType === "Patient" && r.name) {
    const names = r.name as Array<{ family?: string; given?: string[] }>;
    if (names.length > 0) {
      const name = names[0];
      const given = name.given?.join(" ") ?? "";
      const family = name.family ?? "";
      return `${given} ${family}`.trim() || null;
    }
  }

  // Practitioner: name.
  if (resource.resourceType === "Practitioner" && r.name) {
    const names = r.name as Array<{ family?: string; given?: string[]; prefix?: string[] }>;
    if (names.length > 0) {
      const name = names[0];
      const prefix = name.prefix?.join(" ") ?? "";
      const given = name.given?.join(" ") ?? "";
      const family = name.family ?? "";
      return `${prefix} ${given} ${family}`.trim() || null;
    }
  }

  // Organization: name.
  if (resource.resourceType === "Organization" && r.name) {
    return r.name as string;
  }

  // Observation: code display.
  if (resource.resourceType === "Observation" && r.code) {
    const code = r.code as { coding?: Array<{ display?: string }>; text?: string };
    return code.text ?? code.coding?.[0]?.display ?? null;
  }

  // Condition: code display.
  if (resource.resourceType === "Condition" && r.code) {
    const code = r.code as { coding?: Array<{ display?: string }>; text?: string };
    return code.text ?? code.coding?.[0]?.display ?? null;
  }

  // Medication: code display.
  if (resource.resourceType === "Medication" && r.code) {
    const code = r.code as { coding?: Array<{ display?: string }>; text?: string };
    return code.text ?? code.coding?.[0]?.display ?? null;
  }

  // MedicationRequest: medication display.
  if (resource.resourceType === "MedicationRequest") {
    const med = r.medicationCodeableConcept as
      | { coding?: Array<{ display?: string }>; text?: string }
      | undefined;
    if (med) {
      return med.text ?? med.coding?.[0]?.display ?? null;
    }
  }

  // Procedure: code display.
  if (resource.resourceType === "Procedure" && r.code) {
    const code = r.code as { coding?: Array<{ display?: string }>; text?: string };
    return code.text ?? code.coding?.[0]?.display ?? null;
  }

  // Encounter: type display.
  if (resource.resourceType === "Encounter" && r.type) {
    const types = r.type as Array<{ coding?: Array<{ display?: string }>; text?: string }>;
    if (types.length > 0) {
      return types[0].text ?? types[0].coding?.[0]?.display ?? null;
    }
  }

  return null;
}

export function ResourceCard({ resource, fhirBaseUrl, onDelete }: ResourceCardProps) {
  const [expanded, setExpanded] = useState(false);
  const summary = getResourceSummary(resource);
  const copyToClipboard = useClipboard();

  const handleOpenResource = () => {
    const url = `${fhirBaseUrl}/${resource.resourceType}/${resource.id}`;
    window.open(url, "_blank", "noopener,noreferrer");
  };

  const handleDelete = () => {
    if (resource.resourceType && resource.id) {
      onDelete(resource.resourceType, resource.id, summary);
    }
  };

  return (
    <Card>
      <Flex direction="column" gap="2">
        <Flex justify="between" align="start">
          <Flex gap="1" style={{ width: "100%" }}>
            <Badge
              color="teal"
              style={{ cursor: "pointer", userSelect: "none" }}
              onClick={() => setExpanded(!expanded)}
            >
              {resource.resourceType}
            </Badge>
            <Text
              size="2"
              weight="medium"
              style={{
                flexGrow: 1,
                overflow: "hidden",
                textWrap: "nowrap",
                textOverflow: "ellipsis",
                cursor: "pointer",
                userSelect: "none",
              }}
              onClick={() => setExpanded(!expanded)}
            >
              {resource.id}
            </Text>
            <Flex gap="1" align="center">
              <Tooltip content="Open in FHIR server">
                <IconButton size="1" variant="ghost" onClick={handleOpenResource}>
                  <ExternalLinkIcon />
                </IconButton>
              </Tooltip>
              <Tooltip content="Delete resource">
                <IconButton size="1" variant="ghost" color="red" onClick={handleDelete}>
                  <TrashIcon />
                </IconButton>
              </Tooltip>
            </Flex>
          </Flex>
        </Flex>

        {summary && (
          <Text
            size="2"
            color="gray"
            style={{ cursor: "pointer", userSelect: "none" }}
            onClick={() => setExpanded(!expanded)}
          >
            {summary}
          </Text>
        )}

        {expanded && (
          <Box mt="2" style={{ position: "relative" }}>
            <Tooltip content="Copy to clipboard">
              <IconButton
                size="1"
                variant="ghost"
                aria-label="Copy to clipboard"
                onClick={() => copyToClipboard(JSON.stringify(resource, null, 2))}
                style={{ position: "absolute", top: 8, right: 8, zIndex: 1 }}
              >
                <CopyIcon />
              </IconButton>
            </Tooltip>
            <ScrollArea style={{ maxHeight: 300 }}>
              <Code size="1" style={{ display: "block", whiteSpace: "pre-wrap", padding: "6px" }}>
                {JSON.stringify(resource, null, 2)}
              </Code>
            </ScrollArea>
          </Box>
        )}
      </Flex>
    </Card>
  );
}
