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

package au.csiro.pathling.projection;

import au.csiro.pathling.encoders.ValueFunctions;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import java.util.Objects;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * The result of evaluating a {@link RequestedColumn} as part of a {@link ProjectionClause}.
 *
 * @param collection The result of evaluating the column.
 * @param requestedColumn The column that was requested to be included in the projection.
 * @author John Grimes
 */
public record ProjectedColumn(
    @Nonnull Collection collection, @Nonnull RequestedColumn requestedColumn) {

  /**
   * Gets the column value from the collection and aliases it with the requested name. If a SQL type
   * is specified in the requested column, the column value will be cast to that type.
   *
   * @return The column value with the appropriate alias
   */
  @Nonnull
  public Column getValue() {
    // If a type was asserted for the column, check that the collection is of that type.
    requestedColumn
        .type()
        .ifPresent(
            requestedType ->
                collection
                    .getFhirType()
                    .ifPresent(
                        actualType -> {
                          if (!requestedType.equals(actualType)) {
                            throw new IllegalArgumentException(
                                "Collection "
                                    + collection
                                    + " has type "
                                    + actualType
                                    + ", expected "
                                    + requestedType);
                          }
                        }));
    final Column rawResult;
    try {
      rawResult =
          Materializable.getExternalValue(
              requestedColumn.collection() ? collection.asPlural() : collection.asSingular());
    } catch (final UnsupportedOperationException e) {
      // Re-throw with column context to help users identify which column caused the error.
      final String originalMessage = e.getMessage();
      final String contextualMessage =
          "Column '"
              + requestedColumn.name()
              + "' with path '"
              + requestedColumn.path().toExpression()
              + "': "
              + originalMessage.substring(0, 1).toLowerCase()
              + originalMessage.substring(1);
      throw new UnsupportedOperationException(contextualMessage, e);
    }
    // A path that navigates beyond the encoded schema has a null-typed value. Without a SQL type
    // annotation to cast to, it is given the type that the column has when the data is present.
    final Column typedResult =
        requestedColumn
            .sqlType()
            .map(rawResult::try_cast)
            .orElseGet(
                () ->
                    findSqlType()
                        .map(type -> ValueFunctions.castIfNullType(rawResult, type))
                        .orElse(rawResult));
    return typedResult.alias(requestedColumn.name());
  }

  /**
   * Derives the Spark SQL type for this column using only static metadata. This is the static
   * analogue of {@link #getValue()}: it applies the same precedence order for type selection but
   * inspects declared annotations rather than resolving the underlying column expression.
   *
   * <p>Precedence:
   *
   * <ol>
   *   <li>Explicit {@code sqlType} annotation on the requested column.
   *   <li>Declared FHIR {@code type} annotation mapped via {@link FhirPathType#forFhirType}.
   *   <li>The resolved {@link FhirPathType} on the collection (requires {@link Materializable}).
   * </ol>
   *
   * <p>A FHIR type is mapped to the type of the value that {@link #getValue()} produces for it,
   * which differs from the internal FHIRPath representation for decimals (rendered as strings to
   * preserve their precision) and base64Binary values (decoded to binary). When {@code
   * collection()} is {@code true}, the element type is wrapped in {@link
   * DataTypes#createArrayType}.
   *
   * @return The Spark {@link DataType} for this column.
   * @throws UnsupportedOperationException If the collection is not {@link Materializable} and no
   *     explicit type annotation is present, or if no type information is available at all.
   */
  @Nonnull
  public DataType getSqlType() {
    return findSqlType()
        .orElseThrow(
            () ->
                collection instanceof Materializable
                    ? new UnsupportedOperationException(
                        "Cannot derive SQL type for column '"
                            + requestedColumn.name()
                            + "': no sqlType annotation, FHIR type annotation, or resolved"
                            + " FhirPathType")
                    : new UnsupportedOperationException(
                        "Cannot obtain value for non-primitive collection of FHIR type: "
                            + collection.getFhirType().map(Objects::toString).orElse("unknown")));
  }

  /**
   * Derives the Spark SQL type for this column as described in {@link #getSqlType()}, if there is
   * enough type information to do so.
   *
   * @return The Spark {@link DataType} for this column, or empty if it cannot be derived
   */
  @Nonnull
  private Optional<DataType> findSqlType() {
    return requestedColumn
        .sqlType()
        .or(
            () ->
                requestedColumn
                    .type()
                    .flatMap(
                        fhirType ->
                            FhirPathType.forFhirType(fhirType)
                                .map(type -> valueType(type, Optional.of(fhirType)))))
        .or(
            () ->
                collection instanceof Materializable
                    ? collection.getType().map(type -> valueType(type, collection.getFhirType()))
                    : Optional.empty())
        .map(type -> requestedColumn.collection() ? DataTypes.createArrayType(type) : type);
  }

  /**
   * Maps a FHIRPath type to the Spark SQL type of the column values that it produces.
   *
   * @param type The FHIRPath type
   * @param fhirType The FHIR type, which distinguishes base64Binary from other string types
   * @return The Spark SQL type of the column values
   */
  @Nonnull
  private static DataType valueType(
      @Nonnull final FhirPathType type, @Nonnull final Optional<FHIRDefinedType> fhirType) {
    if (FhirPathType.DECIMAL.equals(type)) {
      return DataTypes.StringType;
    } else if (fhirType.filter(FHIRDefinedType.BASE64BINARY::equals).isPresent()) {
      return DataTypes.BinaryType;
    } else {
      return type.getSqlDataType();
    }
  }
}
