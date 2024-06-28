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

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Row;
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

  public StringCollection(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition) {
    super(columnRepresentation, type, fhirType, definition);
  }

  /**
   * Returns a new instance with the specified columnCtx and definition.
   *
   * @param columnRepresentation The columnCtx to use
   * @param definition The definition to use
   * @return A new instance of {@link StringCollection}
   */
  @Nonnull
  public static StringCollection build(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<NodeDefinition> definition) {
    return new StringCollection(columnRepresentation, Optional.of(FhirPathType.STRING),
        Optional.of(FHIRDefinedType.STRING), definition);
  }

  /**
   * Returns a new instance with the specified column.
   */
  @Nonnull
  public static StringCollection build(@Nonnull final ColumnRepresentation columnRepresentation) {
    return build(columnRepresentation, Optional.empty());
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param stringLiteral The FHIRPath representation of the literal
   * @return A new instance of {@link StringCollection}
   */
  @Nonnull
  public static StringCollection fromLiteral(@Nonnull final String stringLiteral) {
    // TODO: Rationalise this with fromValue.
    return fromValue(parseStringLiteral(stringLiteral));
  }

  /**
   * Returns a new instance based upon a literal represented by a {@link StringType}.
   * <p>
   * This is required for the reflection-based instantiation of collections used in
   * {@link au.csiro.pathling.view.ProjectionContext#of}.
   *
   * @param value The value to use
   * @return A new instance of {@link StringCollection}
   */
  @Nonnull
  public static StringCollection fromValue(@Nonnull final StringType value) {
    return StringCollection.fromValue(value.getValue());
  }

  /**
   * Returns a new instance based upon a literal value.
   *
   * @param value The value to use
   * @return A new instance of {@link StringCollection}
   */
  @Nonnull
  public static StringCollection fromValue(@Nonnull final String value) {
    return StringCollection.build(DefaultRepresentation.literal(value));
  }

  /**
   * Returns a new instance based upon a {@link Base64BinaryType}.
   *
   * @param value The value to use
   * @return A new instance of {@link StringCollection}
   */
  @Nonnull
  public static StringCollection fromValue(@Nonnull final Base64BinaryType value) {
    return StringCollection.fromValue(value.getValueAsString());
  }

  /**
   * Returns a new instance based upon a {@link CodeType}.
   *
   * @param value The value to use
   * @return A new instance of {@link StringCollection}
   */
  @Nonnull
  public static StringCollection fromValue(@Nonnull final CodeType value) {
    return StringCollection.fromValue(value.getValueAsString());
  }

  /**
   * Returns a new instance based upon a {@link IdType}.
   *
   * @param value The value to use
   * @return A new instance of {@link StringCollection}
   */
  @Nonnull
  public static StringCollection fromValue(@Nonnull final IdType value) {
    return StringCollection.fromValue(value.getValueAsString());
  }

  /**
   * Returns a new instance based upon a {@link OidType}.
   *
   * @param value The value to use
   * @return A new instance of {@link StringCollection}
   */
  @Nonnull
  public static StringCollection fromValue(@Nonnull final OidType value) {
    return StringCollection.fromValue(value.getValueAsString());
  }

  /**
   * Returns a new instance based upon a {@link UriType).
   *
   * @param value The value to use
   * @return A new instance of {@link StringCollection}
   */
  @Nonnull
  public static StringCollection fromValue(@Nonnull final UriType value) {
    return StringCollection.fromValue(value.getValueAsString());
  }

  /**
   * Returns a new instance based upon a {@link org.hl7.fhir.r4.model.UrlType}.
   *
   * @param value The value to use
   * @return A new instance of {@link StringCollection}
   */
  @Nonnull
  public static StringCollection fromValue(@Nonnull final org.hl7.fhir.r4.model.UrlType value) {
    return StringCollection.fromValue(value.getValueAsString());
  }

  /**
   * Returns a new instance based upon a {@link UuidType}.
   *
   * @param value The value to use
   * @return A new instance of {@link StringCollection}
   */
  @Nonnull
  public static StringCollection fromValue(@Nonnull final UuidType value) {
    return StringCollection.fromValue(value.getValueAsString());
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
    return getColumn().asStringValue()
        .orElseThrow(() -> new IllegalStateException(
            "Cannot convert column to literal value: " + this.getColumn()));
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
