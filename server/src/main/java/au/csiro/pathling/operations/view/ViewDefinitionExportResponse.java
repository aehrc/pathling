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

package au.csiro.pathling.operations.view;

import au.csiro.pathling.OperationResponse;
import au.csiro.pathling.operations.export.ExportManifest;
import au.csiro.pathling.operations.export.ExportManifestOutput;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.List;
import lombok.Getter;
import org.hl7.fhir.r4.model.Parameters;

/**
 * Represents the completion manifest returned at the result URL of a {@code $viewdefinition-export}
 * operation. The manifest follows the SQL on FHIR shape: a required {@code exportId} and {@code
 * status}, the echoed {@code clientTrackingId} and {@code _format}, the export timing fields, and
 * one {@code output} per exported view with a {@code name} and one or more {@code location}
 * download URLs.
 *
 * @author John Grimes
 */
@Getter
public class ViewDefinitionExportResponse implements OperationResponse<Parameters> {

  @Nonnull private final String serverBaseUrl;

  @Nonnull private final List<ViewExportOutput> outputs;

  /** The server-assigned export job identifier. */
  @Nonnull private final String exportId;

  /** The client-supplied tracking identifier, echoed only when present. */
  @Nullable private final String clientTrackingId;

  /** The effective output format code (e.g. {@code ndjson}). */
  @Nonnull private final String format;

  /** The time the export job was created (kick-off time). */
  @Nonnull private final Instant exportStartTime;

  /** The time the manifest was built on completion. */
  @Nonnull private final Instant exportEndTime;

  /**
   * Creates a new ViewDefinitionExportResponse.
   *
   * @param serverBaseUrl the FHIR server base URL (used for constructing result URLs)
   * @param outputs the list of view export outputs
   * @param exportId the server-assigned export job identifier
   * @param clientTrackingId the client-supplied tracking identifier, or null if none was supplied
   * @param format the effective output format code
   * @param exportStartTime the export job creation (kick-off) time
   * @param exportEndTime the time the manifest is built on completion
   */
  @SuppressWarnings("java:S107")
  public ViewDefinitionExportResponse(
      @Nonnull final String serverBaseUrl,
      @Nonnull final List<ViewExportOutput> outputs,
      @Nonnull final String exportId,
      @Nullable final String clientTrackingId,
      @Nonnull final String format,
      @Nonnull final Instant exportStartTime,
      @Nonnull final Instant exportEndTime) {
    this.serverBaseUrl = serverBaseUrl;
    this.outputs = outputs;
    this.exportId = exportId;
    this.clientTrackingId = clientTrackingId;
    this.format = format;
    this.exportStartTime = exportStartTime;
    this.exportEndTime = exportEndTime;
  }

  @Nonnull
  @Override
  public Parameters toOutput() {
    // Delegate to the shared manifest builder, mapping each view output to the generic export
    // output shape. Both export operations produce the identical manifest, so the construction
    // lives in one place.
    final List<ExportManifestOutput> manifestOutputs =
        outputs.stream()
            .map(output -> new ExportManifestOutput(output.name(), output.fileUrls()))
            .toList();
    return new ExportManifest(
            serverBaseUrl,
            exportId,
            clientTrackingId,
            format,
            exportStartTime,
            exportEndTime,
            manifestOutputs)
        .toParameters();
  }
}
