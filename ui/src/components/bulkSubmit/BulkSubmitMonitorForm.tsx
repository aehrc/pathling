/**
 * Form for monitoring an existing bulk submission.
 *
 * @author John Grimes
 */

import { MagnifyingGlassIcon } from "@radix-ui/react-icons";
import { Box, Button, Card, Flex, Heading, Text, TextField } from "@radix-ui/themes";
import { useState } from "react";

import type { SubmitterIdentifier } from "../../types/bulkSubmit";

interface BulkSubmitMonitorFormProps {
  onMonitor: (submissionId: string, submitter: SubmitterIdentifier) => void;
  isSubmitting: boolean;
  disabled: boolean;
}

/**
 *
 * @param root0
 * @param root0.onMonitor
 * @param root0.isSubmitting
 * @param root0.disabled
 */
export function BulkSubmitMonitorForm({
  onMonitor,
  isSubmitting,
  disabled,
}: BulkSubmitMonitorFormProps) {
  const [submitterSystem, setSubmitterSystem] = useState("");
  const [submitterValue, setSubmitterValue] = useState("");
  const [submissionId, setSubmissionId] = useState("");

  const handleSubmit = () => {
    onMonitor(submissionId, {
      system: submitterSystem,
      value: submitterValue,
    });
  };

  const isValid =
    submitterSystem.trim() !== "" && submitterValue.trim() !== "" && submissionId.trim() !== "";

  return (
    <Card>
      <Flex direction="column" gap="4">
        <Heading size="4">Monitor submission</Heading>

        <Text size="2" color="gray">
          Enter the details of an existing submission to monitor its status.
        </Text>

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
        </Box>

        <Box>
          <Box mb="2">
            <Text as="label" size="2" weight="medium">
              Submission ID
            </Text>
          </Box>
          <TextField.Root
            placeholder="e.g., 550e8400-e29b-41d4-a716-446655440000"
            value={submissionId}
            onChange={(e) => setSubmissionId(e.target.value)}
          />
        </Box>

        <Button size="3" onClick={handleSubmit} disabled={disabled || isSubmitting || !isValid}>
          <MagnifyingGlassIcon />
          {isSubmitting ? "Starting..." : "Start monitoring"}
        </Button>
      </Flex>
    </Card>
  );
}
