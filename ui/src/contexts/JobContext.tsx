/**
 * Context for managing job state (both export and import) using useReducer.
 *
 * @author John Grimes
 */

import { createContext, useContext, useReducer, useCallback, useMemo, type ReactNode } from "react";
import type { Job, ExportJob, ImportJob, ImportPnpJob, JobStatus } from "../types/job";
import type { ExportManifest } from "../types/export";
import type { ImportManifest } from "../types/import";

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
  updateJobManifest: (id: string, manifest: ExportManifest | ImportManifest) => void;
  updateJobError: (id: string, error: string) => void;
  removeJob: (id: string) => void;
  clearJobs: () => void;
  getJob: (id: string) => Job | undefined;
  getExportJobs: () => ExportJob[];
  getImportJobs: () => ImportJob[];
  getImportPnpJobs: () => ImportPnpJob[];
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

  const addJob = useCallback((job: Omit<Job, "createdAt">) => {
    dispatch({
      type: "ADD_JOB",
      payload: { ...job, createdAt: new Date() } as Job,
    });
  }, []);

  const updateJobStatus = useCallback((id: string, status: JobStatus) => {
    dispatch({
      type: "UPDATE_JOB",
      payload: { id, updates: { status } },
    });
  }, []);

  const updateJobProgress = useCallback((id: string, progress: number) => {
    dispatch({
      type: "UPDATE_JOB",
      payload: { id, updates: { progress } },
    });
  }, []);

  const updateJobManifest = useCallback((id: string, manifest: ExportManifest | ImportManifest) => {
    dispatch({
      type: "UPDATE_JOB",
      payload: {
        id,
        updates: { manifest, status: "completed" } as Partial<Job>,
      },
    });
  }, []);

  const updateJobError = useCallback((id: string, error: string) => {
    dispatch({
      type: "UPDATE_JOB",
      payload: { id, updates: { error, status: "failed" } },
    });
  }, []);

  const removeJob = useCallback((id: string) => {
    dispatch({ type: "REMOVE_JOB", payload: id });
  }, []);

  const clearJobs = useCallback(() => {
    dispatch({ type: "CLEAR_JOBS" });
  }, []);

  const getJob = useCallback(
    (id: string) => {
      return state.jobs.find((job) => job.id === id);
    },
    [state.jobs],
  );

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

  return (
    <JobContext.Provider
      value={{
        ...state,
        addJob,
        updateJobStatus,
        updateJobProgress,
        updateJobManifest,
        updateJobError,
        removeJob,
        clearJobs,
        getJob,
        getExportJobs,
        getImportJobs,
        getImportPnpJobs,
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
