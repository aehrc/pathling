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
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import lombok.Getter;
import org.apache.http.client.utils.URIBuilder;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;

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
    final Parameters parameters = new Parameters();

    // Ensure the base URL ends with a slash for proper URL construction.
    final String normalizedBaseUrl =
        serverBaseUrl.endsWith("/") ? serverBaseUrl : serverBaseUrl + "/";

    parameters.addParameter().setName("exportId").setValue(new StringType(exportId));
    parameters.addParameter().setName("status").setValue(new CodeType("completed"));

    // Echo the client tracking id only when one was supplied at kick-off.
    if (clientTrackingId != null && !clientTrackingId.isBlank()) {
      parameters
          .addParameter()
          .setName("clientTrackingId")
          .setValue(new StringType(clientTrackingId));
    }

    parameters.addParameter().setName("_format").setValue(new CodeType(format));

    parameters
        .addParameter()
        .setName("exportStartTime")
        .setValue(new InstantType(Date.from(exportStartTime)));
    parameters
        .addParameter()
        .setName("exportEndTime")
        .setValue(new InstantType(Date.from(exportEndTime)));

    // Whole seconds between start and end, never negative.
    final long durationSeconds =
        Math.max(0L, Duration.between(exportStartTime, exportEndTime).toSeconds());
    parameters
        .addParameter()
        .setName("exportDuration")
        .setValue(new IntegerType((int) durationSeconds));

    // One output per view, with a name and one or more location parts.
    for (final ViewExportOutput output : outputs) {
      final ParametersParameterComponent outputParam = parameters.addParameter().setName("output");
      outputParam.addPart().setName("name").setValue(new StringType(output.name()));
      for (final String fileUrl : output.fileUrls()) {
        outputParam
            .addPart()
            .setName("location")
            .setValue(new UriType(buildResultUrl(normalizedBaseUrl, fileUrl)));
      }
    }

    return parameters;
  }

  /**
   * Converts a local file URL to a remote result URL.
   *
   * @param baseUrl the normalised base server URL
   * @param localUrl the local file URL containing the job ID and filename
   * @return the remote result URL
   */
  @Nonnull
  private static String buildResultUrl(
      @Nonnull final String baseUrl, @Nonnull final String localUrl) {
    try {
      final String[] parts = localUrl.split("/jobs/")[1].split("/");
      final String jobUuid = parts[0];
      final String file = parts[1];

      return new URIBuilder(baseUrl + "$result")
          .addParameter("job", jobUuid)
          .addParameter("file", file)
          .build()
          .toString();
    } catch (final URISyntaxException e) {
      throw new InternalErrorException(e);
    }
  }
}
