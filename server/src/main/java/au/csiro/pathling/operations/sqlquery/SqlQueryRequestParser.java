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
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Base64BinaryType;
import org.hl7.fhir.r4.model.BaseDateTimeType;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.PrimitiveType;
import org.hl7.fhir.r4.model.TimeType;
import org.hl7.fhir.r4.model.Type;
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

  /** FHIR primitive types whose runtime value is treated as a Java {@code String}. */
  private static final Set<String> STRING_LIKE_TYPES =
      Set.of("string", "code", "id", "uri", "url", "canonical", "oid", "uuid");

  /** FHIR primitive types whose runtime value is treated as a Java {@code Integer}. */
  private static final Set<String> INTEGER_LIKE_TYPES =
      Set.of("integer", "unsignedInt", "positiveInt");

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
   * @param parameters runtime parameter bindings as a {@code Parameters} resource
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
      @Nullable final Parameters parameters) {

    final Library library = castToLibrary(queryResource);

    if (log.isDebugEnabled()) {
      for (final org.hl7.fhir.r4.model.RelatedArtifact ra : library.getRelatedArtifact()) {
        log.debug(
            "  relatedArtifact: type={}, label={}, resource={}",
            ra.getType(),
            ra.getLabel(),
            ra.getResource());
      }
    }

    final ParsedSqlQuery parsedQuery = libraryParser.parse(library);
    log.debug(
        "Parsed SQLQuery Library: {} relatedArtifact entries, {} view references, {} parameters",
        library.getRelatedArtifact().size(),
        parsedQuery.getViewReferences().size(),
        parsedQuery.getDeclaredParameters().size());

    final SqlQueryOutputFormat outputFormat = selectOutputFormat(format, acceptHeader);
    final boolean shouldIncludeHeader = includeHeader == null || includeHeader.booleanValue();
    final Integer limitValue =
        (limit != null && limit.getValue() != null) ? limit.getValue() : null;
    final Map<String, Object> parameterBindings =
        bindParameters(parameters, parsedQuery.getDeclaredParameters());

    return new SqlQueryRequest(
        parsedQuery, outputFormat, shouldIncludeHeader, limitValue, parameterBindings);
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

  /**
   * Validates the runtime parameter bindings against the declarations in {@code Library.parameter}
   * and converts each {@code value[x]} to a typed Java object.
   *
   * @param parameters the runtime bindings, may be {@code null}
   * @param declaredParameters the declarations from the SQLQuery Library
   * @return an ordered map of name → typed Java value
   * @throws InvalidRequestException if a binding is malformed, names a parameter that was not
   *     declared, or supplies a value of the wrong FHIR type
   */
  @Nonnull
  private Map<String, Object> bindParameters(
      @Nullable final Parameters parameters,
      @Nonnull final java.util.List<SqlParameterDeclaration> declaredParameters) {

    if (parameters == null || parameters.getParameter().isEmpty()) {
      return Map.of();
    }

    final Map<String, String> declaredByName = new LinkedHashMap<>();
    for (final SqlParameterDeclaration declaration : declaredParameters) {
      declaredByName.put(declaration.getName(), declaration.getType());
    }

    final Map<String, Object> bindings = new LinkedHashMap<>();
    for (final ParametersParameterComponent binding : parameters.getParameter()) {
      final String name = binding.getName();
      if (name == null || name.isBlank()) {
        throw new InvalidRequestException(
            "Each runtime parameter binding must have a non-empty name");
      }
      final String declaredType = declaredByName.get(name);
      if (declaredType == null) {
        throw new InvalidRequestException(
            "Runtime parameter '"
                + name
                + "' is not declared in the SQLQuery Library's parameter list");
      }
      final Type value = binding.getValue();
      if (value == null) {
        throw new InvalidRequestException("Runtime parameter '" + name + "' has no value[x]");
      }
      bindings.put(name, convertTypedValue(name, declaredType, value));
    }
    return bindings;
  }

  /**
   * Converts a runtime {@code value[x]} to the Java type matching the declared FHIR type, after
   * checking the wire FHIR type matches the declaration.
   */
  @Nonnull
  private Object convertTypedValue(
      @Nonnull final String name, @Nonnull final String declaredType, @Nonnull final Type value) {

    final String wireType = value.fhirType();
    if (!declaredType.equals(wireType)) {
      throw new InvalidRequestException(
          "Runtime parameter '"
              + name
              + "' was declared as '"
              + declaredType
              + "' but supplied as '"
              + wireType
              + "'");
    }

    if (INTEGER_LIKE_TYPES.contains(declaredType)) {
      return ((IntegerType) value).getValue();
    }
    if (STRING_LIKE_TYPES.contains(declaredType)) {
      return ((PrimitiveType<?>) value).getValueAsString();
    }
    return switch (declaredType) {
      case "boolean" -> ((BooleanType) value).getValue();
      case "decimal" -> ((DecimalType) value).getValue();
      case "date" -> LocalDate.parse(((DateType) value).getValueAsString());
      case "dateTime", "instant" -> ((BaseDateTimeType) value).getValue().toInstant();
      case "time" -> LocalTime.parse(((TimeType) value).getValueAsString());
      case "base64Binary" -> ((Base64BinaryType) value).getValue();
      default ->
          throw new InvalidRequestException(
              "Runtime parameter '"
                  + name
                  + "' declares unsupported FHIR type '"
                  + declaredType
                  + "'");
    };
  }
}
