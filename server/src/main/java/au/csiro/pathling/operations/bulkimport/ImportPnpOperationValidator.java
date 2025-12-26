/*
 * Copyright 2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.operations.bulkimport;

import au.csiro.pathling.ParamUtil;
import au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult;
import au.csiro.pathling.config.PnpConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.library.io.SaveMode;
import au.csiro.pathling.operations.OperationValidation;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.UrlType;
import org.springframework.stereotype.Component;

/**
 * Validates ping and pull import operation requests in FHIR Parameters format.
 *
 * @author John Grimes
 */
@Slf4j
@Component
public class ImportPnpOperationValidator {

  private static final String EXPORT_TYPE_DYNAMIC = "dynamic";
  private static final String EXPORT_TYPE_STATIC = "static";

  @Nonnull
  private final ServerConfiguration serverConfiguration;

  /**
   * Constructor for ImportPnpOperationValidator.
   *
   * @param serverConfiguration the server configuration
   */
  public ImportPnpOperationValidator(@Nonnull final ServerConfiguration serverConfiguration) {
    this.serverConfiguration = serverConfiguration;
  }

  /**
   * Validates a ping and pull import request from a FHIR Parameters resource.
   *
   * @param requestDetails the request details
   * @param parameters the FHIR Parameters resource
   * @return the validation result containing the ImportPnpRequest and any issues
   */
  public PreAsyncValidationResult<ImportPnpRequest> validateParametersRequest(
      @Nonnull final RequestDetails requestDetails,
      @Nonnull final Parameters parameters
  ) {
    // Validate that PnP configuration is present.
    final PnpConfiguration pnpConfig =
        serverConfiguration.getImport() != null
        ? serverConfiguration.getImport().getPnp()
        : null;
    if (pnpConfig == null) {
      throw new InvalidUserInputError(
          "Ping and pull import is not configured. Please configure pathling.import.pnp settings.");
    }

    // Validate that at least one authentication method is configured.
    if ((pnpConfig.getPrivateKeyJwk() == null || pnpConfig.getPrivateKeyJwk().isBlank())
        && (pnpConfig.getClientSecret() == null || pnpConfig.getClientSecret().isBlank())) {
      throw new InvalidUserInputError(
          "Ping and pull authentication is not configured. Please provide either privateKeyJwk or clientSecret.");
    }

    // Extract exportUrl parameter (required).
    final String exportUrl = Objects.requireNonNull(
        ParamUtil.extractFromPart(
            parameters.getParameter(),
            "exportUrl",
            UrlType.class,
            UrlType::getValue,
            false,
            null,
            false,
            new InvalidUserInputError("Missing required parameter: exportUrl")
        ),
        "exportUrl must not be null"
    );

    // Extract exportType parameter (optional, defaults to "dynamic").
    final String exportType = ParamUtil.extractFromPart(
        parameters.getParameter(),
        "exportType",
        Coding.class,
        Coding::getCode,
        true,
        EXPORT_TYPE_DYNAMIC,
        false,
        new InvalidUserInputError("Invalid exportType")
    );

    // Validate exportType.
    if (!EXPORT_TYPE_DYNAMIC.equals(exportType) && !EXPORT_TYPE_STATIC.equals(exportType)) {
      throw new InvalidUserInputError(
          "Invalid exportType: %s. Must be 'dynamic' or 'static'.".formatted(exportType));
    }

    // Note: inputSource parameter is accepted but ignored for backwards compatibility.

    // Extract mode parameter (optional, defaults to OVERWRITE).
    final SaveMode saveMode = ParamUtil.extractFromPart(
        parameters.getParameter(),
        "mode",
        Coding.class,
        coding -> SaveMode.fromCode(coding.getCode()),
        true,
        SaveMode.OVERWRITE,
        false,
        new InvalidUserInputError("Unknown mode.")
    );

    // Extract inputFormat parameter (optional, defaults to NDJSON).
    final ImportFormat importFormat = ParamUtil.extractFromPart(
        parameters.getParameter(),
        "inputFormat",
        Coding.class,
        coding -> parseImportFormat(coding.getCode()),
        true,
        ImportFormat.NDJSON,
        false,
        new InvalidUserInputError("Unknown format.")
    );

    final ImportPnpRequest importPnpRequest = new ImportPnpRequest(
        requestDetails.getCompleteUrl(),
        exportUrl,
        exportType,
        saveMode,
        importFormat
    );

    final List<OperationOutcome.OperationOutcomeIssueComponent> issues = Stream.of(
            OperationValidation.validateAcceptHeader(requestDetails, false),
            OperationValidation.validatePreferHeader(requestDetails, false))
        .flatMap(Collection::stream)
        .toList();

    return new PreAsyncValidationResult<>(importPnpRequest, issues);
  }

  /**
   * Parses an import format string from MIME type (e.g., "application/fhir+ndjson").
   *
   * @param formatString the format string
   * @return the ImportFormat
   */
  private ImportFormat parseImportFormat(final String formatString) {
    if (formatString == null || formatString.isBlank()) {
      return ImportFormat.NDJSON; // Default.
    }
    try {
      return ImportFormat.fromCode(formatString);
    } catch (final IllegalArgumentException e) {
      throw new InvalidUserInputError(e.getMessage());
    }
  }

}
