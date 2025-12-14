/**
 * Context for managing export job state using useReducer.
 *
 * @author John Grimes
 */

import {
  createContext,
  useContext,
  useReducer,
  useCallback,
  type ReactNode,
} from "react";
import type { ExportJob, ExportManifest } from "../types/export";

type JobStatus = "pending" | "in_progress" | "completed" | "failed" | "cancelled";

interface JobState {
  jobs: ExportJob[];
}

type JobAction =
  | { type: "ADD_JOB"; payload: ExportJob }
  | { type: "UPDATE_JOB"; payload: { id: string; updates: Partial<ExportJob> } }
  | { type: "REMOVE_JOB"; payload: string }
  | { type: "CLEAR_JOBS" };

interface JobContextValue extends JobState {
  addJob: (job: Omit<ExportJob, "createdAt">) => void;
  updateJobStatus: (id: string, status: JobStatus) => void;
  updateJobProgress: (id: string, progress: number) => void;
  updateJobManifest: (id: string, manifest: ExportManifest) => void;
  updateJobError: (id: string, error: string) => void;
  removeJob: (id: string) => void;
  clearJobs: () => void;
  getJob: (id: string) => ExportJob | undefined;
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
          job.id === action.payload.id
            ? { ...job, ...action.payload.updates }
            : job
        ),
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

  const addJob = useCallback((job: Omit<ExportJob, "createdAt">) => {
    dispatch({
      type: "ADD_JOB",
      payload: { ...job, createdAt: new Date() },
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

  const updateJobManifest = useCallback(
    (id: string, manifest: ExportManifest) => {
      dispatch({
        type: "UPDATE_JOB",
        payload: { id, updates: { manifest, status: "completed" } },
      });
    },
    []
  );

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
    [state.jobs]
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
