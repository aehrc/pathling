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

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.views.ColumnTag;
import jakarta.annotation.Nonnull;
import java.util.Objects;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * The result of evaluating a {@link RequestedColumn} as part of a {@link ProjectionClause}.
 *
 * @param collection The result of evaluating the column.
 * @param requestedColumn The column that was requested to be included in the projection.
 */
public record ProjectedColumn(
    @Nonnull Collection collection, @Nonnull RequestedColumn requestedColumn) {

  /**
   * The one declared FHIR type that is not cast on output. A decimal is output as its literal text,
   * and a cast would change that (decision 76). {@link #getSqlType()} still reports {@code
   * DECIMAL(32,6)} for it.
   */
  private static final FHIRDefinedType UNCAST_DECLARED_TYPE = FHIRDefinedType.DECIMAL;

  /**
   * Gets the column value from the collection and aliases it with the requested name. If a SQL type
   * is specified in the requested column, the column value will be cast to that type. Otherwise, if
   * a FHIR type is declared on the column, the value is cast to the SQL type of that FHIR type
   * (FR-028), so that a column over an element absent from the input schema has the declared type.
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
    return requestedColumn
        .sqlType()
        .map(sqlType -> castToSqlType(rawResult, sqlType))
        .or(() -> declaredOutputType().map(rawResult::cast))
        .orElse(rawResult)
        .alias(requestedColumn.name());
  }

  /**
   * Casts a column to the SQL type requested by its tag. An instant cast to a timestamp without a
   * time zone is cast to a timestamp first. An instant is a point in time, held as text carrying
   * its offset on both layouts (decision 81), and a direct cast from text would discard the offset,
   * keeping the wall time instead. Through a timestamp, it gives the point in time in the session
   * time zone. Every other value keeps the direct cast, so text of any other type keeps its wall
   * time.
   *
   * @param value the column to cast
   * @param sqlType the requested SQL type
   * @return the cast column
   */
  @Nonnull
  private Column castToSqlType(@Nonnull final Column value, @Nonnull final DataType sqlType) {
    final boolean instant =
        collection.getFhirType().filter(FHIRDefinedType.INSTANT::equals).isPresent();
    final boolean withoutTimeZone =
        DataTypes.TimestampNTZType.equals(sqlType)
            || sqlType instanceof final ArrayType arrayType
                && DataTypes.TimestampNTZType.equals(arrayType.elementType());
    if (instant && withoutTimeZone) {
      final DataType withTimeZone =
          sqlType instanceof ArrayType
              ? DataTypes.createArrayType(DataTypes.TimestampType)
              : DataTypes.TimestampType;
      return value.try_cast(withTimeZone).try_cast(sqlType);
    }
    return value.try_cast(sqlType);
  }

  /**
   * Gets the SQL type that the declared FHIR type of the column gives its output, if a FHIR type is
   * declared and is one that is cast on output.
   *
   * @return the SQL type of the declared FHIR type, as an array for a collection column
   */
  @Nonnull
  private Optional<DataType> declaredOutputType() {
    return requestedColumn
        .type()
        .filter(type -> !UNCAST_DECLARED_TYPE.equals(type))
        .flatMap(FhirPathType::forFhirType)
        .map(FhirPathType::getSqlDataType)
        .map(type -> requestedColumn.collection() ? DataTypes.createArrayType(type) : type);
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
   * <p>When {@code collection()} is {@code true}, the element type is wrapped in {@link
   * DataTypes#createArrayType}.
   *
   * @return The Spark {@link DataType} for this column.
   * @throws UnsupportedOperationException If the collection is not {@link Materializable} and no
   *     explicit type annotation is present, or if no type information is available at all.
   */
  @Nonnull
  public DataType getSqlType() {
    final DataType elementType =
        requestedColumn
            .sqlType()
            .or(
                () ->
                    requestedColumn
                        .type()
                        .flatMap(FhirPathType::forFhirType)
                        .map(FhirPathType::getSqlDataType))
            .or(
                () -> {
                  if (!(collection instanceof Materializable)) {
                    throw new UnsupportedOperationException(
                        "Cannot obtain value for non-primitive collection of FHIR type: "
                            + collection.getFhirType().map(Objects::toString).orElse("unknown"));
                  }
                  return collection.getType().map(FhirPathType::getSqlDataType);
                })
            .orElseThrow(
                () ->
                    new UnsupportedOperationException(
                        "Cannot derive the SQL type of column '"
                            + requestedColumn.name()
                            + "' with path '"
                            + requestedColumn.path().toExpression()
                            + "', because the path carries no type information. Declare the"
                            + " column's FHIR type with \"type\", or its SQL type with the '"
                            + ColumnTag.ANSI_TYPE_TAG
                            + "' tag."));
    return requestedColumn.collection() ? DataTypes.createArrayType(elementType) : elementType;
  }
}
