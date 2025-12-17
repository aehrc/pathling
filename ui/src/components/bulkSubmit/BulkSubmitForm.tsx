/**
 * Form for configuring and starting a bulk submit operation.
 *
 * @author John Grimes
 */

import { useState, useCallback } from "react";
import {
  Box,
  Button,
  Card,
  Flex,
  Heading,
  IconButton,
  RadioCards,
  Text,
  TextField,
} from "@radix-ui/themes";
import { PaperPlaneIcon, PlusIcon, TrashIcon } from "@radix-ui/react-icons";
import type {
  BulkSubmitRequest,
  BulkSubmitStatus,
  FileRequestHeader,
} from "../../types/bulkSubmit";
import { SUBMISSION_STATUSES } from "../../types/bulkSubmit";

interface BulkSubmitFormProps {
  onSubmit: (request: BulkSubmitRequest) => void;
  isSubmitting: boolean;
  disabled: boolean;
}

function generateSubmissionId(): string {
  return crypto.randomUUID();
}

export function BulkSubmitForm({ onSubmit, isSubmitting, disabled }: BulkSubmitFormProps) {
  const [submitterSystem, setSubmitterSystem] = useState("");
  const [submitterValue, setSubmitterValue] = useState("");
  const [submissionId, setSubmissionId] = useState(generateSubmissionId);
  const [submissionStatus, setSubmissionStatus] = useState<BulkSubmitStatus>("complete");
  const [manifestUrl, setManifestUrl] = useState("");
  const [fhirBaseUrl, setFhirBaseUrl] = useState("");
  const [fileRequestHeaders, setFileRequestHeaders] = useState<FileRequestHeader[]>([]);
  const [metadataLabel, setMetadataLabel] = useState("");
  const [metadataDescription, setMetadataDescription] = useState("");

  const handleSubmit = () => {
    const request: BulkSubmitRequest = {
      submissionId,
      submitter: {
        system: submitterSystem,
        value: submitterValue,
      },
      submissionStatus,
    };

    if (manifestUrl.trim()) {
      request.manifestUrl = manifestUrl;
    }
    if (fhirBaseUrl.trim()) {
      request.fhirBaseUrl = fhirBaseUrl;
    }
    if (fileRequestHeaders.length > 0) {
      request.fileRequestHeaders = fileRequestHeaders;
    }
    if (metadataLabel.trim() || metadataDescription.trim()) {
      request.metadata = {};
      if (metadataLabel.trim()) {
        request.metadata.label = metadataLabel;
      }
      if (metadataDescription.trim()) {
        request.metadata.description = metadataDescription;
      }
    }

    onSubmit(request);
  };

  const addFileRequestHeader = useCallback(() => {
    setFileRequestHeaders((prev) => [...prev, { headerName: "", headerValue: "" }]);
  }, []);

  const removeFileRequestHeader = useCallback((index: number) => {
    setFileRequestHeaders((prev) => prev.filter((_, i) => i !== index));
  }, []);

  const updateFileRequestHeader = useCallback(
    (index: number, field: "headerName" | "headerValue", value: string) => {
      setFileRequestHeaders((prev) =>
        prev.map((header, i) => (i === index ? { ...header, [field]: value } : header)),
      );
    },
    [],
  );

  const regenerateSubmissionId = useCallback(() => {
    setSubmissionId(generateSubmissionId());
  }, []);

  // Validation: submitter required, manifest URL required when status is complete.
  const isValid =
    submitterSystem.trim() !== "" &&
    submitterValue.trim() !== "" &&
    submissionId.trim() !== "" &&
    (submissionStatus !== "complete" || manifestUrl.trim() !== "");

  return (
    <Card>
      <Flex direction="column" gap="4">
        <Heading size="4">New submission</Heading>

        <Box>
          <Box mb="2">
            <Text as="label" size="2" weight="medium">
              Submitter system
            </Text>
          </Box>
          <TextField.Root
            placeholder="e.g., http://example.org/submitters"
            value={submitterSystem}
            onChange={(e) => setSubmitterSystem(e.target.value)}
          />
          <Text size="1" color="gray" mt="1">
            The system URI that identifies the submitter.
          </Text>
        </Box>

        <Box>
          <Box mb="2">
            <Text as="label" size="2" weight="medium">
              Submitter value
            </Text>
          </Box>
          <TextField.Root
            placeholder="e.g., my-submitter-id"
            value={submitterValue}
            onChange={(e) => setSubmitterValue(e.target.value)}
          />
          <Text size="1" color="gray" mt="1">
            The unique identifier for the submitter within the system.
          </Text>
        </Box>

        <Box>
          <Box mb="2">
            <Text as="label" size="2" weight="medium">
              Submission ID
            </Text>
          </Box>
          <Flex gap="2">
            <TextField.Root
              style={{ flex: 1 }}
              value={submissionId}
              onChange={(e) => setSubmissionId(e.target.value)}
            />
            <Button variant="soft" onClick={regenerateSubmissionId}>
              Regenerate
            </Button>
          </Flex>
          <Text size="1" color="gray" mt="1">
            A unique identifier for this submission.
          </Text>
        </Box>

        <Box>
          <Box mb="2">
            <Text as="label" size="2" weight="medium">
              Submission status
            </Text>
          </Box>
          <RadioCards.Root
            value={submissionStatus}
            onValueChange={(v) => setSubmissionStatus(v as BulkSubmitStatus)}
            columns="2"
            gap="2"
          >
            {SUBMISSION_STATUSES.map((option) => (
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
              Manifest URL
              {submissionStatus === "complete" && (
                <Text color="red" ml="1">
                  *
                </Text>
              )}
            </Text>
          </Box>
          <TextField.Root
            placeholder="e.g., https://example.org/fhir/$export-poll-status"
            value={manifestUrl}
            onChange={(e) => setManifestUrl(e.target.value)}
          />
          <Text size="1" color="gray" mt="1">
            The URL of the bulk export manifest file.
            {submissionStatus === "complete" && " Required when status is Complete."}
          </Text>
        </Box>

        <Box>
          <Box mb="2">
            <Text as="label" size="2" weight="medium">
              FHIR base URL
            </Text>
          </Box>
          <TextField.Root
            placeholder="e.g., https://example.org/fhir"
            value={fhirBaseUrl}
            onChange={(e) => setFhirBaseUrl(e.target.value)}
          />
          <Text size="1" color="gray" mt="1">
            The base URL of the FHIR server that produced the manifest (optional).
          </Text>
        </Box>

        <Box>
          <Box mb="2">
            <Flex justify="between" align="center">
              <Text as="label" size="2" weight="medium">
                File request headers
              </Text>
              <IconButton size="1" variant="soft" onClick={addFileRequestHeader}>
                <PlusIcon />
              </IconButton>
            </Flex>
          </Box>
          {fileRequestHeaders.length === 0 ? (
            <Text size="1" color="gray">
              No custom headers configured. Click + to add headers for authenticated file downloads.
            </Text>
          ) : (
            <Flex direction="column" gap="2">
              {fileRequestHeaders.map((header, index) => (
                <Flex key={index} gap="2" align="center">
                  <TextField.Root
                    style={{ flex: 1 }}
                    placeholder="Header name"
                    value={header.headerName}
                    onChange={(e) => updateFileRequestHeader(index, "headerName", e.target.value)}
                  />
                  <TextField.Root
                    style={{ flex: 2 }}
                    placeholder="Header value"
                    value={header.headerValue}
                    onChange={(e) => updateFileRequestHeader(index, "headerValue", e.target.value)}
                  />
                  <IconButton
                    size="1"
                    variant="soft"
                    color="red"
                    onClick={() => removeFileRequestHeader(index)}
                  >
                    <TrashIcon />
                  </IconButton>
                </Flex>
              ))}
            </Flex>
          )}
        </Box>

        <Box>
          <Box mb="2">
            <Text as="label" size="2" weight="medium">
              Metadata (optional)
            </Text>
          </Box>
          <Flex direction="column" gap="2">
            <TextField.Root
              placeholder="Label"
              value={metadataLabel}
              onChange={(e) => setMetadataLabel(e.target.value)}
            />
            <TextField.Root
              placeholder="Description"
              value={metadataDescription}
              onChange={(e) => setMetadataDescription(e.target.value)}
            />
          </Flex>
          <Text size="1" color="gray" mt="1">
            Optional metadata to associate with this submission.
          </Text>
        </Box>

        <Button size="3" onClick={handleSubmit} disabled={disabled || isSubmitting || !isValid}>
          <PaperPlaneIcon />
          {isSubmitting ? "Submitting..." : "Submit"}
        </Button>
      </Flex>
    </Card>
  );
}
