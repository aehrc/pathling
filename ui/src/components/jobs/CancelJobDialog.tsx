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
 * Confirmation dialog shown before cancelling an in-progress job, warning that partial results will
 * be discarded. Finished jobs are removed without confirmation and so never open this dialog.
 *
 * @author John Grimes
 */

import { AlertDialog, Button, Flex, Spinner } from "@radix-ui/themes";

import { formatJobStartTime } from "./jobsPresentation";

import type { JobSummary } from "../../api/jobs";

interface CancelJobDialogProps {
  /** The job pending confirmation, or null when the dialog is closed. */
  job: JobSummary | null;
  /** Whether the cancellation request is in flight. */
  isCancelling: boolean;
  /** Called when the user confirms cancellation. */
  onConfirm: () => void;
  /** Called when the dialog's open state changes. */
  onOpenChange: (open: boolean) => void;
}

/**
 * Confirmation dialog for cancelling an in-progress job.
 *
 * @param root0 - The component props.
 * @param root0.job - The job pending confirmation, or null when closed.
 * @param root0.isCancelling - Whether the cancellation request is in flight.
 * @param root0.onConfirm - Called when the user confirms cancellation.
 * @param root0.onOpenChange - Called when the dialog's open state changes.
 * @returns The cancel confirmation dialog.
 */
export function CancelJobDialog({
  job,
  isCancelling,
  onConfirm,
  onOpenChange,
}: Readonly<CancelJobDialogProps>) {
  return (
    <AlertDialog.Root open={job !== null} onOpenChange={onOpenChange}>
      <AlertDialog.Content maxWidth="480px">
        <AlertDialog.Title>Cancel job?</AlertDialog.Title>
        <AlertDialog.Description size="2">
          {job
            ? `This will stop the ${job.operation} job started at ${formatJobStartTime(
                job.startTime,
              )} and delete any results it has produced so far. This cannot be undone.`
            : null}
        </AlertDialog.Description>
        <Flex gap="3" mt="4" justify="end">
          <AlertDialog.Cancel>
            <Button variant="soft" color="gray">
              Keep running
            </Button>
          </AlertDialog.Cancel>
          <Button color="red" onClick={onConfirm} disabled={isCancelling}>
            {isCancelling ? <Spinner /> : null}
            Cancel job
          </Button>
        </Flex>
      </AlertDialog.Content>
    </AlertDialog.Root>
  );
}
