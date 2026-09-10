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
 * Progress indicator shared by the active state of asynchronous job cards. It
 * renders a determinate progress bar when a numeric percentage is available,
 * and falls back to an indeterminate spinner with a caller-supplied label
 * while the server has not yet reported progress.
 *
 * @author John Grimes
 */

import { ReloadIcon } from "@radix-ui/react-icons";
import { Box, Flex, Progress, Text } from "@radix-ui/themes";

interface JobProgressIndicatorProps {
  /** Percentage complete (0-100), or null/undefined when no numeric progress is available yet. */
  progress: number | null | undefined;
  /** Label shown beside the spinner while waiting for numeric progress. */
  pendingLabel: string;
  /** Size of the progress bar. Defaults to "2". */
  size?: "1" | "2" | "3";
}

/**
 * Renders the progress state of an active job as either a bar or a spinner.
 *
 * @param props - The component props.
 * @param props.progress - Percentage complete, or null/undefined when unknown.
 * @param props.pendingLabel - Label shown beside the spinner while unknown.
 * @param props.size - Size of the progress bar. Defaults to "2".
 * @returns The progress bar when progress is numeric, otherwise the spinner.
 */
export function JobProgressIndicator({
  progress,
  pendingLabel,
  size = "2",
}: Readonly<JobProgressIndicatorProps>) {
  if (progress != null) {
    return (
      <Box>
        <Flex justify="between" mb="1">
          <Text size="1" color="gray">
            Progress
          </Text>
          <Text size="1" color="gray">
            {progress}%
          </Text>
        </Flex>
        <Progress size={size} value={progress} />
      </Box>
    );
  }

  return (
    <Flex align="center" gap="2">
      <ReloadIcon style={{ animation: "spin 1s linear infinite" }} />
      <Text size="2" color="gray">
        {pendingLabel}
      </Text>
    </Flex>
  );
}
