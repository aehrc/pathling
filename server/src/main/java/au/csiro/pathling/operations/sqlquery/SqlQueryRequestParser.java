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

package au.csiro.pathling.operations.sqlquery;

import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Validates and normalises the raw HTTP inputs of a {@code $sqlquery-run} invocation into a {@link
 * SqlQueryRequest}. Has no Spark dependency; performs only structural FHIR-level validation and
 * parsing.
 */
@Slf4j
@Component
public class SqlQueryRequestParser {

  @Nonnull private final SqlQueryLibraryParser libraryParser;

  /**
   * Constructs a new SqlQueryRequestParser.
   *
   * @param libraryParser parser for the SQLQuery Library profile
   */
  @Autowired
  public SqlQueryRequestParser(@Nonnull final SqlQueryLibraryParser libraryParser) {
    this.libraryParser = libraryParser;
  }

  /**
   * Parses the raw inputs into a validated {@link SqlQueryRequest}.
   *
   * @param queryResource the inline Library resource carrying the SQLQuery
   * @param format the explicit {@code _format} parameter, if any
   * @param acceptHeader the HTTP {@code Accept} header value, used as a fallback for {@code format}
   * @param includeHeader whether to include a CSV header row; {@code null} defaults to {@code true}
   * @param limit optional row cap
   * @param parameterValues raw runtime parameter bindings
   * @return the validated request
   * @throws InvalidRequestException if the inputs are not a valid SQLQuery invocation
   */
  @Nonnull
  @SuppressWarnings("java:S107")
  public SqlQueryRequest parse(
      @Nonnull final IBaseResource queryResource,
      @Nullable final String format,
      @Nullable final String acceptHeader,
      @Nullable final BooleanType includeHeader,
      @Nullable final IntegerType limit,
      @Nullable final List<ParametersParameterComponent> parameterValues) {

    final Library library = castToLibrary(queryResource);

    log.info(
        "SQLQuery Library has {} relatedArtifact entries", library.getRelatedArtifact().size());
    for (final org.hl7.fhir.r4.model.RelatedArtifact ra : library.getRelatedArtifact()) {
      log.info(
          "  relatedArtifact: type={}, label={}, resource={}",
          ra.getType(),
          ra.getLabel(),
          ra.getResource());
    }

    final ParsedSqlQuery parsedQuery = libraryParser.parse(library);
    log.info(
        "Parsed SQL query: {} view references, {} parameters",
        parsedQuery.getViewReferences().size(),
        parsedQuery.getDeclaredParameters().size());

    final SqlQueryOutputFormat outputFormat = selectOutputFormat(format, acceptHeader);
    final boolean shouldIncludeHeader = includeHeader == null || includeHeader.booleanValue();
    final Integer limitValue =
        (limit != null && limit.getValue() != null) ? limit.getValue() : null;
    final List<ParametersParameterComponent> safeParameterValues =
        parameterValues != null ? parameterValues : List.of();

    return new SqlQueryRequest(
        parsedQuery, outputFormat, shouldIncludeHeader, limitValue, safeParameterValues);
  }

  @Nonnull
  private Library castToLibrary(@Nonnull final IBaseResource resource) {
    if (resource instanceof final Library library) {
      return library;
    }
    throw new InvalidRequestException(
        "Expected a Library resource but received: " + resource.fhirType());
  }

  @Nonnull
  private SqlQueryOutputFormat selectOutputFormat(
      @Nullable final String format, @Nullable final String acceptHeader) {
    if (format != null && !format.isBlank()) {
      return SqlQueryOutputFormat.fromString(format);
    }
    return SqlQueryOutputFormat.fromAcceptHeader(acceptHeader);
  }
}
