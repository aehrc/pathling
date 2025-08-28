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

package au.csiro.pathling.fhirpath.collection;

import static au.csiro.pathling.fhirpath.literal.StringLiteral.unescapeFhirPathString;
import static au.csiro.pathling.utilities.Strings.unSingleQuote;

import au.csiro.pathling.annotations.UsedByReflection;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.Numeric;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.comparison.Comparable;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Base64BinaryType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.OidType;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.UuidType;

/**
 * Represents a FHIRPath expression which refers to a string typed element.
 *
 * @author John Grimes
 */
@SuppressWarnings("TypeMayBeWeakened")
public class StringCollection extends Collection implements Comparable, Numeric, StringCoercible,
    Materializable {

  /**
   * Creates a new StringCollection with the specified parameters.
   *
   * @param columnRepresentation the column representation for this collection
   * @param type the FHIRPath type of this collection
   * @param fhirType the FHIR defined type of this collection
   * @param definition the node definition for this collection
   * @param extensionMapColumn the extension map column for this collection
   */
  protected StringCollection(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition,
      @Nonnull final Optional<Column> extensionMapColumn) {
    super(columnRepresentation, type, fhirType, definition, extensionMapColumn);
  }

  /**
   * Returns a new instance with the specified columnCtx and definition.
   *
   * @param columnRepresentation The columnCtx to use
   * @param fhirDefinedType the FHIR type of the collection
   * @return A new instance of {@link StringCollection}
   */
  @Nonnull
  public static StringCollection build(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final FHIRDefinedType fhirDefinedType) {
    return new StringCollection(columnRepresentation, Optional.of(FhirPathType.STRING),
        Optional.of(fhirDefinedType), Optional.empty(), Optional.empty());
  }

  /**
   * Returns a new instance with the specified column representation and no definition.
   *
   * @param columnRepresentation The column representation to use
   * @return A new instance with the specified column
   */
  @Nonnull
  public static StringCollection build(@Nonnull final ColumnRepresentation columnRepresentation) {
    return build(columnRepresentation, FHIRDefinedType.STRING);
  }


  /**
   * Returns an empty string collection.
   *
   * @return A new instance of StringCollection
   */
  @Nonnull
  public static StringCollection empty() {
    return build(DefaultRepresentation.empty());
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param stringLiteral The FHIRPath representation of the literal
   * @return A new instance of StringCollection
   */
  @Nonnull
  public static StringCollection fromLiteral(@Nonnull final String stringLiteral) {
    return fromValue(parseStringLiteral(stringLiteral));
  }

  /**
   * Returns a new instance based upon a literal represented by a {@link StringType}.
   * <p>
   * This is required for the reflection-based instantiation of collections used in
   * {@link au.csiro.pathling.projection.ProjectionContext#of}.
   *
   * @param value The value to use
   * @return A new instance of StringCollection
   */
  @Nonnull
  @UsedByReflection
  public static StringCollection fromValue(@Nonnull final StringType value) {
    return StringCollection.fromValue(value.getValue());
  }

  /**
   * Returns a new instance based upon a literal value.
   *
   * @param value The value to use
   * @return A new instance of StringCollection
   */
  @Nonnull
  public static StringCollection fromValue(@Nonnull final String value) {
    return fromValue(value, FHIRDefinedType.STRING);
  }

  /**
   * Returns a new instance based upon a literal value with specified FHIR type.
   *
   * @param value The value to use
   * @param fhirDefinedType The FHIR defined type for the collection
   * @return A new instance of {@link StringCollection}
   */
  @Nonnull
  public static StringCollection fromValue(@Nonnull final String value,
      @Nonnull final FHIRDefinedType fhirDefinedType) {
    return StringCollection.build(DefaultRepresentation.literal(value), fhirDefinedType);
  }

  /**
   * Returns a new instance based upon a {@link Base64BinaryType}.
   *
   * @param value The value to use
   * @return A new instance of StringCollection
   */
  @UsedByReflection
  @Nonnull
  public static StringCollection fromValue(@Nonnull final Base64BinaryType value) {
    // special case for Base64BinaryType, as it needs to be decoded
    return StringCollection.build(
        DefaultRepresentation.literal(value.getValue()),
        FHIRDefinedType.BASE64BINARY);
  }

  /**
   * Returns a new instance based upon a {@link CodeType}.
   *
   * @param value The value to use
   * @return A new instance of StringCollection
   */
  @UsedByReflection
  @Nonnull
  public static StringCollection fromValue(@Nonnull final CodeType value) {
    return StringCollection.fromValue(value.getValueAsString(), FHIRDefinedType.CODE);
  }

  /**
   * Returns a new instance based upon a {@link IdType}.
   *
   * @param value The value to use
   * @return A new instance of StringCollection
   */
  @UsedByReflection
  @Nonnull
  public static StringCollection fromValue(@Nonnull final IdType value) {
    return StringCollection.fromValue(value.getValueAsString(), FHIRDefinedType.ID);
  }

  /**
   * Returns a new instance based upon a {@link OidType}.
   *
   * @param value The value to use
   * @return A new instance of StringCollection
   */
  @UsedByReflection
  @Nonnull
  public static StringCollection fromValue(@Nonnull final OidType value) {
    return StringCollection.fromValue(value.getValueAsString(), FHIRDefinedType.OID);
  }

  /**
   * Returns a new instance based upon a {@link UriType}.
   *
   * @param value The value to use
   * @return A new instance of StringCollection
   */
  @UsedByReflection
  @Nonnull
  public static StringCollection fromValue(@Nonnull final UriType value) {
    return StringCollection.fromValue(value.getValueAsString(), FHIRDefinedType.URI);
  }

  /**
   * Returns a new instance based upon a {@link org.hl7.fhir.r4.model.UrlType}.
   *
   * @param value The value to use
   * @return A new instance of StringCollection
   */
  @UsedByReflection
  @Nonnull
  public static StringCollection fromValue(@Nonnull final org.hl7.fhir.r4.model.UrlType value) {
    return StringCollection.fromValue(value.getValueAsString(), FHIRDefinedType.URL);
  }

  /**
   * Returns a new instance based upon a {@link UuidType}.
   *
   * @param value The value to use
   * @return A new instance of StringCollection
   */
  @UsedByReflection
  @Nonnull
  public static StringCollection fromValue(@Nonnull final UuidType value) {
    return StringCollection.fromValue(value.getValueAsString(), FHIRDefinedType.UUID);
  }

  /**
   * Returns a new instance based upon a {@link org.hl7.fhir.r4.model.CanonicalType}.
   *
   * @param value The value to use
   * @return A new instance of StringCollection
   */
  @UsedByReflection
  @Nonnull
  public static StringCollection fromValue(
      @Nonnull final org.hl7.fhir.r4.model.CanonicalType value) {
    return StringCollection.fromValue(value.getValueAsString(), FHIRDefinedType.CANONICAL);
  }

  // fromValue for MarkdownType

  /**
   * Returns a new instance based upon a {@link org.hl7.fhir.r4.model.MarkdownType}.
   *
   * @param value The value to use
   * @return A new instance of {@link StringCollection}
   */
  @UsedByReflection
  @Nonnull
  public static StringCollection fromValue(
      @Nonnull final org.hl7.fhir.r4.model.MarkdownType value) {
    return StringCollection.fromValue(value.getValueAsString(), FHIRDefinedType.MARKDOWN);
  }

  /**
   * Parses a FHIRPath string literal into a {@link String}.
   *
   * @param fhirPath The FHIRPath representation of the literal
   * @return The parsed {@link String}
   */
  @Nonnull
  public static String parseStringLiteral(final @Nonnull String fhirPath) {
    // Remove the surrounding single quotes and unescape the string according to the rules within
    // the FHIRPath specification.
    String value = unSingleQuote(fhirPath);
    value = unescapeFhirPathString(value);
    return value;
  }

  /**
   * Cast the column value of this collection to a literal.
   *
   * @return The column value as a literal
   */
  @Nonnull
  public String toLiteralValue() {
    return getColumn().asStringValue()
        .orElseThrow(() -> new IllegalStateException(
            "Cannot convert column to literal value: " + this.getColumn()));
  }

  @Nonnull
  @Override
  public StringCollection asStringPath() {
    return (StringCollection) asSingular();
  }

  @Override
  public boolean isComparableTo(@Nonnull final Comparable path) {
    return path instanceof StringCollection;
  }

  @Override
  public @Nonnull Function<Numeric, Collection> getMathOperation(
      @Nonnull final Numeric.MathOperation operation) {
    if (operation == MathOperation.ADDITION) {
      return numeric -> mapColumn(c -> functions.concat(c, numeric.getColumn().getValue()));
    } else {
      throw new UnsupportedFhirPathFeatureError(
          "Operation not supported on String: " + operation);
    }
  }

  @Override
  public @Nonnull Collection negate() {
    throw new InvalidUserInputError("Negation is not supported on Strings");
  }

  @Nonnull
  @Override
  public Column toExternalValue() {
    // special case to convert base64String back to Binary
    return getFhirType()
        .filter(FHIRDefinedType.BASE64BINARY::equals)
        .map(t -> new DefaultRepresentation(getColumnValue()).transform(functions::unbase64)
            .getValue())
        .orElseGet(Materializable.super::toExternalValue);
  }

}
