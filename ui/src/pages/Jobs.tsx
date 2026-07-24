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
 * Page listing the caller's asynchronous jobs, backed directly by the server's $jobs operation with
 * automatic refresh and per-row cancellation.
 *
 * @author John Grimes
 */

import { Box, Heading, Text } from "@radix-ui/themes";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { jobCancel } from "../api";
import { CapabilityGuard } from "../components/auth/CapabilityGuard";
import { CancelJobDialog } from "../components/jobs/CancelJobDialog";
import { requiresCancelConfirmation } from "../components/jobs/jobsPresentation";
import { JobsTable } from "../components/jobs/JobsTable";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useToast } from "../contexts/ToastContext";
import { JOBS_QUERY_KEY, useJobsList } from "../hooks";

import type { JobSummary } from "../api";

/**
 * Page component listing the caller's jobs with live refresh and cancellation.
 *
 * @returns The jobs page component.
 */
export function Jobs() {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;
  const queryClient = useQueryClient();
  const { showToast } = useToast();

  const jobsQuery = useJobsList();

  // The job awaiting cancellation confirmation, or null when no dialog is open.
  const [pendingJob, setPendingJob] = useState<JobSummary | null>(null);

  const cancelMutation = useMutation<void, Error, JobSummary>({
    mutationFn: (job) => jobCancel(fhirBaseUrl, { pollingUrl: job.url, accessToken }),
    onSuccess: (_data, job) => {
      void queryClient.invalidateQueries({ queryKey: JOBS_QUERY_KEY });
      showToast(
        job.status === "in-progress" ? "Job cancelled" : "Job removed",
        `The ${job.operation} job has been removed.`,
      );
    },
    onError: (error) => {
      // On failure the job stays in the list; the toast explains why.
      showToast("Could not cancel job", error.message);
    },
    onSettled: () => {
      setPendingJob(null);
    },
  });

  const handleCancelJob = (job: JobSummary) => {
    // In-progress jobs are confirmed first; finished jobs are removed immediately.
    if (requiresCancelConfirmation(job)) {
      setPendingJob(job);
    } else {
      cancelMutation.mutate(job);
    }
  };

  const handleConfirmCancel = () => {
    if (pendingJob) {
      cancelMutation.mutate(pendingJob);
    }
  };

  return (
    <CapabilityGuard>
      {() => (
        <>
          <Box mb="4">
            <Heading size="6" mb="1">
              Jobs
            </Heading>
            <Text size="2" color="gray">
              Background jobs you own on this server. The list refreshes automatically while jobs
              are running.
            </Text>
          </Box>

          <JobsTable
            jobs={jobsQuery.data ?? []}
            isLoading={jobsQuery.isLoading}
            error={jobsQuery.error}
            onRetry={() => void jobsQuery.refetch()}
            onCancelJob={handleCancelJob}
          />

          <CancelJobDialog
            job={pendingJob}
            isCancelling={cancelMutation.isPending}
            onConfirm={handleConfirmCancel}
            onOpenChange={(open) => {
              if (!open) {
                setPendingJob(null);
              }
            }}
          />
        </>
      )}
    </CapabilityGuard>
  );
}
