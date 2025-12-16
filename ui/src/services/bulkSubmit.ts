/**
 * Service for FHIR bulk submit operations.
 *
 * @author John Grimes
 */

import type { Binary, Parameters } from "fhir/r4";
import { UnauthorizedError } from "../types/errors";
import type {
  BulkSubmitRequest,
  BulkSubmitStatusRequest,
  StatusManifest,
  SubmitterIdentifier,
} from "../types/bulkSubmit";

interface KickOffResult {
  submissionId: string;
  status: string;
}

interface PollResult {
  status: "in_progress" | "completed";
  manifest?: StatusManifest;
}

/**
 * Sends a bulk submit request to create or update a submission. This is a synchronous operation
 * that returns an acknowledgement with the submission status.
 */
export async function kickOffBulkSubmit(
  fhirBaseUrl: string,
  accessToken: string | undefined,
  request: BulkSubmitRequest,
): Promise<KickOffResult> {
  const url = `${fhirBaseUrl}/$bulk-submit`;

  const headers: HeadersInit = {
    "Content-Type": "application/fhir+json",
    Accept: "application/fhir+json",
  };
  if (accessToken) {
    headers.Authorization = `Bearer ${accessToken}`;
  }

  const parameters = buildBulkSubmitParameters(request);

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(parameters),
  });

  if (response.status === 401) {
    throw new UnauthorizedError();
  }
  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(`Bulk submit failed: ${response.status} - ${errorBody}`);
  }

  // Parse the Parameters response to extract submissionId and status.
  const responseBody = (await response.json()) as Parameters;
  const submissionId =
    responseBody.parameter?.find((p) => p.name === "submissionId")?.valueString ??
    request.submissionId;
  const status =
    responseBody.parameter?.find((p) => p.name === "status")?.valueString ?? "unknown";

  return {
    submissionId,
    status,
  };
}

/**
 * Polls the bulk submit status endpoint.
 */
export async function pollBulkSubmitStatus(
  fhirBaseUrl: string,
  accessToken: string | undefined,
  request: BulkSubmitStatusRequest,
): Promise<PollResult> {
  const url = `${fhirBaseUrl}/$bulk-submit-status`;

  const headers: HeadersInit = {
    "Content-Type": "application/fhir+json",
    Accept: "application/fhir+json",
    Prefer: "respond-async",
  };
  if (accessToken) {
    headers.Authorization = `Bearer ${accessToken}`;
  }

  const parameters = buildStatusParameters(request);

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(parameters),
  });

  if (response.status === 202) {
    return { status: "in_progress" };
  }

  if (response.status === 200) {
    const responseBody: unknown = await response.json();
    const manifest = parseBinaryManifest(responseBody);
    return { status: "completed", manifest };
  }

  if (response.status === 401) {
    throw new UnauthorizedError();
  }

  const errorBody = await response.text();
  throw new Error(`Status poll failed: ${response.status} - ${errorBody}`);
}

/**
 * Aborts a submission by sending an "aborted" status.
 */
export async function abortBulkSubmit(
  fhirBaseUrl: string,
  accessToken: string | undefined,
  submissionId: string,
  submitter: SubmitterIdentifier,
): Promise<void> {
  await kickOffBulkSubmit(fhirBaseUrl, accessToken, {
    submissionId,
    submitter,
    submissionStatus: "aborted",
  });
}

/**
 * Builds a FHIR Parameters resource for a bulk submit request.
 */
function buildBulkSubmitParameters(request: BulkSubmitRequest): Parameters {
  const params: Parameters = {
    resourceType: "Parameters",
    parameter: [
      { name: "submissionId", valueString: request.submissionId },
      {
        name: "submitter",
        valueIdentifier: {
          system: request.submitter.system,
          value: request.submitter.value,
        },
      },
      {
        name: "submissionStatus",
        valueCoding: { code: request.submissionStatus },
      },
    ],
  };

  if (request.manifestUrl) {
    params.parameter!.push({
      name: "manifestUrl",
      valueString: request.manifestUrl,
    });
  }
  if (request.fhirBaseUrl) {
    params.parameter!.push({
      name: "fhirBaseUrl",
      valueString: request.fhirBaseUrl,
    });
  }
  if (request.replacesManifestUrl) {
    params.parameter!.push({
      name: "replacesManifestUrl",
      valueString: request.replacesManifestUrl,
    });
  }
  if (request.fileRequestHeaders && request.fileRequestHeaders.length > 0) {
    for (const header of request.fileRequestHeaders) {
      params.parameter!.push({
        name: "fileRequestHeader",
        part: [
          { name: "headerName", valueString: header.headerName },
          { name: "headerValue", valueString: header.headerValue },
        ],
      });
    }
  }
  if (request.metadata) {
    const metadataParts = [];
    if (request.metadata.label) {
      metadataParts.push({ name: "label", valueString: request.metadata.label });
    }
    if (request.metadata.description) {
      metadataParts.push({
        name: "description",
        valueString: request.metadata.description,
      });
    }
    if (metadataParts.length > 0) {
      params.parameter!.push({ name: "metadata", part: metadataParts });
    }
  }

  return params;
}

/**
 * Builds a FHIR Parameters resource for a status check request.
 */
function buildStatusParameters(request: BulkSubmitStatusRequest): Parameters {
  return {
    resourceType: "Parameters",
    parameter: [
      { name: "submissionId", valueString: request.submissionId },
      {
        name: "submitter",
        valueIdentifier: {
          system: request.submitter.system,
          value: request.submitter.value,
        },
      },
    ],
  };
}

/**
 * Parses a Binary resource response containing a status manifest.
 */
function parseBinaryManifest(responseBody: unknown): StatusManifest {
  if (isBinaryResource(responseBody)) {
    const decodedData = atob(responseBody.data);
    return JSON.parse(decodedData) as StatusManifest;
  }
  return responseBody as StatusManifest;
}

/**
 * Type guard to check if a response is a FHIR Binary resource with data.
 */
function isBinaryResource(value: unknown): value is Binary & { data: string } {
  return (
    typeof value === "object" &&
    value !== null &&
    "resourceType" in value &&
    (value as { resourceType: string }).resourceType === "Binary" &&
    "data" in value &&
    typeof (value as { data: unknown }).data === "string"
  );
}
