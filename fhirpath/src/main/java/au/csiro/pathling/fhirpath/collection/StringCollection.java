/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.collection;

import static au.csiro.pathling.fhirpath.literal.StringLiteral.unescapeFhirPathString;
import static au.csiro.pathling.utilities.Strings.unSingleQuote;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.column.ColumnCtx;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.unsafe.types.UTF8String;
import org.hl7.fhir.r4.model.Base64BinaryType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.MarkdownType;
import org.hl7.fhir.r4.model.OidType;
import org.hl7.fhir.r4.model.PrimitiveType;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.UuidType;

/**
 * Represents a FHIRPath expression which refers to a string typed element.
 *
 * @author John Grimes
 */
public class StringCollection extends Collection implements Materializable<PrimitiveType>,
    StringCoercible {

  public StringCollection(@Nonnull final Column column, @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition) {
    super(column, type, fhirType, definition);
  }

  /**
   * Returns a new instance with the specified column and definition.
   *
   * @param column The column to use
   * @param definition The definition to use
   * @return A new instance of {@link StringCollection}
   */
  @Nonnull
  public static StringCollection build(@Nonnull final Column column,
      @Nonnull final Optional<NodeDefinition> definition) {
    return new StringCollection(column, Optional.of(FhirPathType.STRING),
        Optional.of(FHIRDefinedType.STRING), definition);
  }

  /**
   * Returns a new instance with the specified column.
   */
  @Nonnull
  public static StringCollection build(@Nonnull final Column column) {
    return new StringCollection(column, Optional.of(FhirPathType.STRING),
        Optional.of(FHIRDefinedType.STRING), Optional.empty());
  }

  /**
   * Returns a new instance with the specified column context.
   *
   * @param column The column context to use
   * @return A new instance of {@link StringCollection}
   */
  @Nonnull
  public static StringCollection build(@Nonnull final ColumnCtx column) {
    return build(column.getValue());
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param fhirPath The FHIRPath representation of the literal
   * @return A new instance of {@link StringCollection}
   */
  @Nonnull
  public static StringCollection fromLiteral(@Nonnull final String fhirPath) {
    final String value = parseStringLiteral(fhirPath);
    return StringCollection.build(lit(value), Optional.empty());
  }

  @Nonnull
  public static String parseStringLiteral(final @Nonnull String fhirPath) {
    // Remove the surrounding single quotes and unescape the string according to the rules within
    // the FHIRPath specification.
    String value = unSingleQuote(fhirPath);
    value = unescapeFhirPathString(value);
    return value;
  }

  @Nonnull
  public String toLiteralValue() {
    return Optional.of(getColumn().expr())
        .filter(Literal.class::isInstance)
        .map(Literal.class::cast)
        .map(Literal::value)
        .filter(UTF8String.class::isInstance)
        .map(Objects::toString)
        .orElseThrow(() -> new IllegalStateException(
            "Cannot convert column to literal value: " + getColumn()));
  }

  /**
   * Gets a value from a row for a String or String literal.
   *
   * @param row The {@link Row} from which to extract the value
   * @param columnNumber The column number to extract the value from
   * @param fhirType The FHIR type to assume when extracting the value
   * @return A {@link PrimitiveType}, or the absence of a value
   */
  @Nonnull
  public static Optional<PrimitiveType> valueFromRow(@Nonnull final Row row, final int columnNumber,
      @Nonnull final Optional<FHIRDefinedType> fhirType) {
    if (row.isNullAt(columnNumber)) {
      return Optional.empty();
    }
    if (fhirType.isEmpty()) {
      return Optional.of(new StringType(row.getString(columnNumber)));
    }
    switch (fhirType.get()) {
      case URI:
        return Optional.of(new UriType(row.getString(columnNumber)));
      case CODE:
        return Optional.of(new CodeType(row.getString(columnNumber)));
      case OID:
        return Optional.of(new OidType(row.getString(columnNumber)));
      case ID:
        return Optional.of(new IdType(row.getString(columnNumber)));
      case UUID:
        return Optional.of(new UuidType(row.getString(columnNumber)));
      case MARKDOWN:
        return Optional.of(new MarkdownType(row.getString(columnNumber)));
      case BASE64BINARY:
        return Optional.of(new Base64BinaryType(row.getString(columnNumber)));
      default:
        return Optional.of(new StringType(row.getString(columnNumber)));
    }
  }

  @Nonnull
  @Override
  public Optional<PrimitiveType> getFhirValueFromRow(@Nonnull final Row row,
      final int columnNumber) {
    return valueFromRow(row, columnNumber, getFhirType());
  }

  @Nonnull
  @Override
  public StringCollection asStringPath() {
    return this;
  }
}
