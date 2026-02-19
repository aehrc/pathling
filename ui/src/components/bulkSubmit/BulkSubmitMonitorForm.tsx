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
 * Form for monitoring an existing bulk submission.
 *
 * @author John Grimes
 */

import { MagnifyingGlassIcon } from "@radix-ui/react-icons";
import { Box, Button, Card, Flex, Heading, Text, TextField } from "@radix-ui/themes";
import { useState } from "react";

import { FieldLabel } from "../FieldLabel";

import type { SubmitterIdentifier } from "../../types/bulkSubmit";

interface BulkSubmitMonitorFormProps {
  onMonitor: (submissionId: string, submitter: SubmitterIdentifier) => void;
  isSubmitting: boolean;
  disabled: boolean;
}

/**
 * Form for entering and monitoring existing bulk submissions.
 *
 * @param root0 - The component props.
 * @param root0.onMonitor - Callback when monitoring is requested.
 * @param root0.isSubmitting - Whether a submission is in progress.
 * @param root0.disabled - Whether the form is disabled.
 * @returns The bulk submit monitor form component.
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
          <FieldLabel mb="2">Submitter system</FieldLabel>
          <TextField.Root
            placeholder="e.g., http://example.org/submitters"
            value={submitterSystem}
            onChange={(e) => setSubmitterSystem(e.target.value)}
          />
        </Box>

        <Box>
          <FieldLabel mb="2">Submitter value</FieldLabel>
          <TextField.Root
            placeholder="e.g., my-submitter-id"
            value={submitterValue}
            onChange={(e) => setSubmitterValue(e.target.value)}
          />
        </Box>

        <Box>
          <FieldLabel mb="2">Submission ID</FieldLabel>
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
