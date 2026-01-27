/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.column;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.regexp_extract;
import static org.apache.spark.sql.functions.when;

import jakarta.annotation.Nonnull;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Column;

/**
 * Utility class for working with Reference columns in SQL operations.
 *
 * <p>Wraps Reference column operations and provides methods for:
 *
 * <ul>
 *   <li>Type extraction from reference strings and type fields
 *   <li>Validation of reference formats and type names
 *   <li>Handling both singular and array reference fields
 * </ul>
 *
 * <p>This class centralizes all Reference-related SQL column operations, similar to how {@link
 * QuantityValue} handles Quantity operations.
 *
 * @author John Grimes
 */
public class ReferenceValue {

  /**
   * Regex pattern for extracting resource type from reference strings.
   *
   * <p>Matches resource types in various reference formats:
   *
   * <ul>
   *   <li>Relative: "Patient/123" → "Patient"
   *   <li>Absolute: "http://example.org/fhir/Patient/123" → "Patient"
   *   <li>Canonical: "http://hl7.org/fhir/ValueSet/my-valueset" → "ValueSet"
   * </ul>
   */
  private static final String REFERENCE_TYPE_PATTERN = "(?:^|/)([A-Z][a-zA-Z]+)(?:/|$|\\|)";

  /**
   * Regex pattern for validating FHIR resource type names.
   *
   * <p>A valid FHIR resource type name must start with a capital letter followed by one or more
   * letters (e.g., "Patient", "Observation", "ValueSet").
   */
  private static final String FHIR_TYPE_NAME_PATTERN = "^[A-Z][a-zA-Z]+$";

  @Nonnull private final ColumnRepresentation referenceColumn;

  @Nonnull private final ColumnRepresentation typeColumn;

  private ReferenceValue(
      @Nonnull final ColumnRepresentation referenceColumn,
      @Nonnull final ColumnRepresentation typeColumn) {
    this.referenceColumn = referenceColumn;
    this.typeColumn = typeColumn;
  }

  /**
   * Creates a ReferenceValue wrapper for the specified Reference column representations.
   *
   * @param referenceColumn the Reference.reference column representation
   * @param typeColumn the Reference.type column representation
   * @return a ReferenceValue instance
   */
  @Nonnull
  public static ReferenceValue of(
      @Nonnull final ColumnRepresentation referenceColumn,
      @Nonnull final ColumnRepresentation typeColumn) {
    return new ReferenceValue(referenceColumn, typeColumn);
  }

  /**
   * Extracts the resource type from singular reference and type columns.
   *
   * <p>Type extraction logic:
   *
   * <ol>
   *   <li>Use {@code type} if present and valid
   *   <li>Parse resource type from {@code reference} string
   *   <li>Return null if neither provides a valid type
   * </ol>
   *
   * <p>Filtering rules:
   *
   * <ul>
   *   <li>Exclude contained references (starting with {@code #})
   *   <li>Validate extracted type matches FHIR resource type pattern
   * </ul>
   *
   * @param reference The Reference.reference column (may contain SQL null values)
   * @param type The Reference.type column (may contain SQL null values)
   * @return A column containing the extracted type string, or null for unresolvable references
   */
  @Nonnull
  public static Column extractTypeFromColumns(
      @Nonnull final Column reference, @Nonnull final Column type) {
    final Column parsedType = regexp_extract(reference, REFERENCE_TYPE_PATTERN, 1);
    final Column extractedType = coalesce(type, parsedType);
    return validateTypeFormat(extractedType);
  }

  /**
   * Extracts the resource type from this Reference, with priority given to the explicit type field.
   *
   * <p>Type extraction logic:
   *
   * <ol>
   *   <li>Use {@code Reference.type} if present and valid
   *   <li>Parse resource type from {@code Reference.reference} string
   *   <li>Return null for individual references that cannot be resolved
   * </ol>
   *
   * <p>Handles both singular and array columns - if reference column is an array, type column is
   * also assumed to be an array.
   *
   * @return A column representation containing the extracted type string(s)
   */
  @Nonnull
  public ColumnRepresentation extractType() {
    final UnaryOperator<Column> arrayLogic =
        refArray ->
            org.apache.spark.sql.functions.zip_with(
                refArray, typeColumn.getValue(), ReferenceValue::extractTypeFromColumns);

    final UnaryOperator<Column> singularLogic =
        ref -> extractTypeFromColumns(ref, typeColumn.getValue());

    return referenceColumn.vectorize(arrayLogic, singularLogic);
  }

  /**
   * Validates that a type string matches the FHIR resource type name pattern and is not null.
   *
   * @param type the type column to validate
   * @return the validated type column, or null if invalid
   */
  private static Column validateTypeFormat(@Nonnull final Column type) {
    final Column isValidType = type.isNotNull().and(type.rlike(FHIR_TYPE_NAME_PATTERN));
    return when(isValidType, type).otherwise(lit(null));
  }
}
