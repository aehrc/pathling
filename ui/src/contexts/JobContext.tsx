/**
 * Context for managing job state (both export and import) using useReducer.
 *
 * @author John Grimes
 */

import { createContext, useContext, useReducer, useMemo, type ReactNode } from "react";
import type {
  Job,
  ExportJob,
  ImportJob,
  ImportPnpJob,
  BulkSubmitJob,
  ViewExportJob,
  JobStatus,
} from "../types/job";
import type { StatusManifest } from "../types/bulkSubmit";
import type { ExportManifest } from "../types/export";
import type { ImportManifest } from "../types/import";
import type { ViewExportManifest } from "../types/viewExport";

interface JobState {
  jobs: Job[];
}

type JobAction =
  | { type: "ADD_JOB"; payload: Job }
  | { type: "UPDATE_JOB"; payload: { id: string; updates: Partial<Job> } }
  | { type: "REMOVE_JOB"; payload: string }
  | { type: "CLEAR_JOBS" };

interface JobContextValue extends JobState {
  addJob: (job: Omit<Job, "createdAt">) => void;
  updateJobStatus: (id: string, status: JobStatus) => void;
  updateJobProgress: (id: string, progress: number) => void;
  updateJobPollUrl: (id: string, pollUrl: string) => void;
  updateJobManifest: (
    id: string,
    manifest: ExportManifest | ImportManifest | StatusManifest | ViewExportManifest,
  ) => void;
  updateJobError: (id: string, error: Error) => void;
  removeJob: (id: string) => void;
  clearJobs: () => void;
  getJob: (id: string) => Job | undefined;
  getExportJobs: () => ExportJob[];
  getImportJobs: () => ImportJob[];
  getImportPnpJobs: () => ImportPnpJob[];
  getBulkSubmitJobs: () => BulkSubmitJob[];
  getViewExportJobs: () => ViewExportJob[];
}

const JobContext = createContext<JobContextValue | null>(null);

function jobReducer(state: JobState, action: JobAction): JobState {
  switch (action.type) {
    case "ADD_JOB":
      return {
        ...state,
        jobs: [action.payload, ...state.jobs],
      };
    case "UPDATE_JOB":
      return {
        ...state,
        jobs: state.jobs.map((job) =>
          job.id === action.payload.id ? { ...job, ...action.payload.updates } : job,
        ) as Job[],
      };
    case "REMOVE_JOB":
      return {
        ...state,
        jobs: state.jobs.filter((job) => job.id !== action.payload),
      };
    case "CLEAR_JOBS":
      return {
        ...state,
        jobs: [],
      };
    default:
      return state;
  }
}

export function JobProvider({ children }: { children: ReactNode }) {
  const [state, dispatch] = useReducer(jobReducer, { jobs: [] });

  const addJob = (job: Omit<Job, "createdAt">) => {
    dispatch({
      type: "ADD_JOB",
      payload: { ...job, createdAt: new Date() } as Job,
    });
  };

  const updateJobStatus = (id: string, status: JobStatus) => {
    dispatch({
      type: "UPDATE_JOB",
      payload: { id, updates: { status } },
    });
  };

  const updateJobProgress = (id: string, progress: number) => {
    dispatch({
      type: "UPDATE_JOB",
      payload: { id, updates: { progress } },
    });
  };

  const updateJobPollUrl = (id: string, pollUrl: string) => {
    dispatch({
      type: "UPDATE_JOB",
      payload: { id, updates: { pollUrl } },
    });
  };

  const updateJobManifest = (
    id: string,
    manifest: ExportManifest | ImportManifest | StatusManifest | ViewExportManifest,
  ) => {
    dispatch({
      type: "UPDATE_JOB",
      payload: {
        id,
        updates: { manifest, status: "completed" } as Partial<Job>,
      },
    });
  };

  const updateJobError = (id: string, error: Error) => {
    dispatch({
      type: "UPDATE_JOB",
      payload: { id, updates: { error, status: "failed" } },
    });
  };

  const removeJob = (id: string) => {
    dispatch({ type: "REMOVE_JOB", payload: id });
  };

  const clearJobs = () => {
    dispatch({ type: "CLEAR_JOBS" });
  };

  const getJob = (id: string) => {
    return state.jobs.find((job) => job.id === id);
  };

  const getExportJobs = useMemo(
    () => () => state.jobs.filter((job): job is ExportJob => job.type === "export"),
    [state.jobs],
  );

  const getImportJobs = useMemo(
    () => () => state.jobs.filter((job): job is ImportJob => job.type === "import"),
    [state.jobs],
  );

  const getImportPnpJobs = useMemo(
    () => () => state.jobs.filter((job): job is ImportPnpJob => job.type === "import-pnp"),
    [state.jobs],
  );

  const getBulkSubmitJobs = useMemo(
    () => () => state.jobs.filter((job): job is BulkSubmitJob => job.type === "bulk-submit"),
    [state.jobs],
  );

  const getViewExportJobs = useMemo(
    () => () => state.jobs.filter((job): job is ViewExportJob => job.type === "view-export"),
    [state.jobs],
  );

  return (
    <JobContext.Provider
      value={{
        ...state,
        addJob,
        updateJobStatus,
        updateJobProgress,
        updateJobPollUrl,
        updateJobManifest,
        updateJobError,
        removeJob,
        clearJobs,
        getJob,
        getExportJobs,
        getImportJobs,
        getImportPnpJobs,
        getBulkSubmitJobs,
        getViewExportJobs,
      }}
    >
      {children}
    </JobContext.Provider>
  );
}

export function useJobs(): JobContextValue {
  const context = useContext(JobContext);
  if (!context) {
    throw new Error("useJobs must be used within a JobProvider");
  }
  return context;
}
