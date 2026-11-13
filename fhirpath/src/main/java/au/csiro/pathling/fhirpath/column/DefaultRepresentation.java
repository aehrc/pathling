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

package au.csiro.pathling.fhirpath.column;

import static au.csiro.pathling.sql.SqlFunctions.let;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.encoders.ColumnFunctions;
import au.csiro.pathling.encoders.ValueFunctions;
import au.csiro.pathling.fhirpath.definition.PrimitiveMetadata;
import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.annotation.AnnotationKind;
import au.csiro.pathling.schema.annotation.kinds.DecimalNumericKind;
import au.csiro.pathling.utilities.SystemDecimal;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Describes a representation where collections of values are represented as arrays in the dataset.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
public class DefaultRepresentation extends ColumnRepresentation {

  private static final DefaultRepresentation EMPTY_REPRESENTATION =
      new DefaultRepresentation(functions.lit(null));

  /**
   * Gets the empty representation.
   *
   * @return A singleton instance of an empty representation
   */
  @Nonnull
  public static DefaultRepresentation empty() {
    return EMPTY_REPRESENTATION;
  }

  /** The column value represented by this object. */
  @Setter(AccessLevel.PROTECTED)
  @Nonnull
  private Column value;

  /**
   * Create a new {@link ColumnRepresentation} from a literal value.
   *
   * @param value The value to represent
   * @return A new {@link ColumnRepresentation} representing the value
   */
  @Nonnull
  public static ColumnRepresentation literal(@Nonnull final Object value) {
    if (value instanceof final byte[] ba) {
      return fromBinaryColumn(functions.lit(ba));
    } else {
      // Otherwise use the default representation.
      return new DefaultRepresentation(lit(value));
    }
  }

  /**
   * Creates a new {@link ColumnRepresentation} that represents a binary column as a base64 encoded
   * string.
   *
   * @param column a column containing binary data
   * @return A new {@link ColumnRepresentation} representing the binary data as a base64 encoded
   *     string.
   */
  @Nonnull
  public static ColumnRepresentation fromBinaryColumn(@Nonnull final Column column) {
    return new DefaultRepresentation(column).transform(functions::base64);
  }

  @Override
  @Nonnull
  public DefaultRepresentation copyOf(@Nonnull final Column newValue) {
    return new DefaultRepresentation(newValue);
  }

  @Override
  @Nonnull
  public DefaultRepresentation vectorize(
      @Nonnull final UnaryOperator<Column> arrayExpression,
      @Nonnull final UnaryOperator<Column> singularExpression) {
    return copyOf(ValueFunctions.ifArray(value, arrayExpression, singularExpression));
  }

  @Override
  @Nonnull
  public DefaultRepresentation flatten() {
    return copyOf(ValueFunctions.unnest(value));
  }

  /**
   * Traverses to a field of the structure, or of every structure in the array, that this
   * representation holds. The field is referenced through the tolerant traversal expression, so the
   * traversal yields a null of the given type where the input schema does not carry the field.
   *
   * <p>Where this representation holds an array, a present field is read from every element, which
   * yields an array of the field's type, so the null that stands for an absent field is given that
   * array shape too. Whether the field is present or absent, and whether a repeating parent is
   * present or absent, the traversal then yields one type (decision 88).
   *
   * <p>A decimal and a date are read as a structure of their stored text and the annotations the
   * schema carries, with no annotation computed (design §4.1). A decimal whose number the engine's
   * System.Decimal cannot hold is null: only whether its number decodes is tested.
   *
   * @param fieldName the name of the field to traverse to
   * @param fhirType the FHIR type of the field
   * @param fallback the type of the null that stands for the field where it is absent
   * @return the flattened result of the traversal
   */
  @Override
  @Nonnull
  public ColumnRepresentation traverse(
      @Nonnull final String fieldName,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final DataType fallback) {
    if (isDecimal(fhirType) || isDate(fhirType)) {
      return traverseStored(fieldName, fhirType, fallback, false);
    }
    final Column field =
        ValueFunctions.ifArray(
            value,
            array ->
                ColumnFunctions.resolveOrNull(
                    array, fieldName, DataTypes.createArrayType(fallback)),
            singular -> ColumnFunctions.resolveOrNull(singular, fieldName, fallback));
    return decodeField(copyOf(field).removeNulls().flatten(), fhirType);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Every primitive is read as {@link #storedValues} reads it, paired with its metadata group.
   */
  @Override
  @Nonnull
  public ColumnRepresentation traverseWithMetadata(
      @Nonnull final String fieldName,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final DataType fallback) {
    return traverseStored(fieldName, fhirType, fallback, true);
  }

  /**
   * Traverses to a primitive read as the engine holds it from the values and the metadata groups
   * beside them, as {@link #storedValues} reads them: a decimal or a date, or, with its id and
   * extensions, any primitive.
   *
   * <p>Each parent's values are read from the field and its metadata group, so that the read names
   * only the annotations, a repeating field's annotations paired with its values by position. A
   * parent is evaluated once for both, so that a parent with side effects, such as a trace, fires
   * once.
   */
  @Nonnull
  private ColumnRepresentation traverseStored(
      @Nonnull final String fieldName,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final DataType fallback,
      final boolean withMetadata) {
    final boolean repeating = fallback instanceof ArrayType;
    final Function<Boolean, UnaryOperator<Column>> stored =
        keepAbsent ->
            parent ->
                let(
                    parent,
                    p ->
                        storedValues(
                            ColumnFunctions.resolveOrNull(p, fieldName, fallback),
                            groupOf(p, fieldName),
                            fhirType,
                            repeating,
                            withMetadata,
                            keepAbsent));
    // Without its id and extensions, a repeating primitive under a repeating parent keeps its items
    // without a value as null items, as the engine has always given them (decision 152 changes
    // nothing that is read without the metadata).
    final UnaryOperator<Column> underRepeating = stored.apply(!withMetadata);
    final UnaryOperator<Column> underSingular = stored.apply(false);
    if (isDecimal(fhirType) && !repeating) {
      // Only the parents without an element are left out, so that a decimal whose number does not
      // decode stays as a null item, as decoding its text alone gave.
      return vectorize(
              array ->
                  functions.transform(
                      functions.filter(
                          array,
                          parent ->
                              isElement(
                                  ColumnFunctions.resolveOrNull(parent, fieldName, fallback),
                                  groupOf(parent, fieldName),
                                  withMetadata)),
                      underRepeating::apply),
              underSingular)
          .flatten();
    }
    // Only the parents without an element are left out.
    return vectorize(
            array ->
                functions.filter(
                    functions.transform(array, underRepeating::apply), Column::isNotNull),
            underSingular)
        .flatten();
  }

  /** The metadata group of a field of a parent, null where the schema has none. */
  @Nonnull
  private static Column groupOf(@Nonnull final Column parent, @Nonnull final String fieldName) {
    return ColumnFunctions.resolveOrNull(
        parent, LayoutFields.metadataGroupName(fieldName), DataTypes.NullType);
  }

  /**
   * Returns whether a stored value and its metadata group make an element: where the value is
   * present, and, where a primitive's id and extensions are read, where either is present beside a
   * value that is not (decision 141, D14), as FHIRPath takes an element with no value.
   */
  @Nonnull
  private static Column isElement(
      @Nonnull final Column value, @Nonnull final Column group, final boolean withMetadata) {
    if (!withMetadata) {
      return value.isNotNull();
    }
    return value
        .isNotNull()
        .or(
            ColumnFunctions.resolveOrNull(group, PrimitiveMetadata.ID, DataTypes.StringType)
                .isNotNull())
        .or(
            ColumnFunctions.resolveOrNull(group, PrimitiveMetadata.EXTENSION, DataTypes.NullType)
                .isNotNull());
  }

  /** Returns whether one primitive, as {@link #storedValues} pairs it, is an element. */
  @Nonnull
  private static Column isElement(@Nonnull final Column primitive, final boolean withMetadata) {
    return isElement(primitive.getField(StoredPrimitive.VALUE), primitive, withMetadata);
  }

  /**
   * Returns the values of a primitive as the engine holds them, read from the stored values and the
   * metadata groups beside them (FR-004, FR-022, FR-023): a structure of the stored value and the
   * annotations the schema carries, with no annotation computed (design §4.1, decision 152). A
   * decimal whose number does not decode is null.
   *
   * <p>Without its id and extensions, only a decimal or a date is read in this way, and a value
   * that is absent is no element. With them, any primitive is, its {@code id} and {@code extension}
   * are read too, where the schema carries them, and a value that is absent beside an id or
   * extensions is an element without a value, as FHIRPath takes it (decision 141, D14). Every field
   * is read from the group by name, never the group itself.
   *
   * <p>A repeating field's values are paired with its metadata by position. A position that is no
   * element is left out, or, where asked, kept as a null item.
   *
   * @param values the stored values, an array of them where the field repeats
   * @param groups the metadata groups beside them, aligned with them by position
   * @param fhirType the FHIR type of the field
   * @param repeating whether the field repeats
   * @param withMetadata whether the primitive's id and extensions are read
   * @param keepAbsent whether a position of a repeating field that is no element is kept as a null
   *     item rather than left out
   * @return the value, or an array of the values where the field repeats
   */
  @Nonnull
  static Column storedValues(
      @Nonnull final Column values,
      @Nonnull final Column groups,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      final boolean repeating,
      final boolean withMetadata,
      final boolean keepAbsent) {
    final List<String> metadata =
        withMetadata ? List.of(PrimitiveMetadata.ID, PrimitiveMetadata.EXTENSION) : List.of();
    final Column paired;
    if (isDate(fhirType)) {
      paired = SystemDateTime.paired(values, groups, metadata, repeating);
    } else if (isDecimal(fhirType)) {
      paired = StoredDecimal.paired(values, groups, metadata, repeating);
    } else {
      // A base64Binary is held as its base64 text, as it is without its metadata.
      final Column decoded =
          fhirType.filter(FHIRDefinedType.BASE64BINARY::equals).isPresent()
              ? (repeating
                  ? functions.transform(values, functions::base64)
                  : functions.base64(values))
              : values;
      paired = StoredPrimitive.paired(decoded, groups, metadata, repeating);
    }
    final UnaryOperator<Column> decode =
        isDecimal(fhirType) ? StoredDecimal::decodable : UnaryOperator.identity();
    if (!repeating) {
      return when(isElement(values, groups, withMetadata), decode.apply(paired));
    }
    final Column elements =
        keepAbsent
            ? functions.transform(
                paired, primitive -> when(isElement(primitive, withMetadata), primitive))
            : functions.filter(paired, primitive -> isElement(primitive, withMetadata));
    return isDecimal(fhirType) ? functions.transform(elements, decode::apply) : elements;
  }

  /**
   * Returns the numeric annotation of one singular decimal: the stored one where the schema carries
   * it, and the decoding of its text otherwise.
   *
   * <p>The value is not tested for presence. A stored annotation is taken as given (ticket 31), and
   * the annotation step computes it as null wherever the text is null, so it is null wherever the
   * value is absent. The annotation is read as the kind reads it, at the engine's System.Decimal
   * type, whatever type it was stored at.
   *
   * @param value the stored text of the decimal
   * @param group the metadata group beside it
   * @return the decimal, typed as {@link SystemDecimal#TYPE}
   */
  @Nonnull
  public static Column numericValue(@Nonnull final Column value, @Nonnull final Column group) {
    return resolveAnnotation(
        DecimalNumericKind.INSTANCE, LayoutFields.NUMERIC_ANNOTATION, value, group);
  }

  /**
   * Resolves one field of an annotation of one value: the stored one where the schema carries it,
   * and the computed one otherwise, decided once for the schema by the Catalyst resolver, and read
   * as the kind reads it.
   */
  @Nonnull
  private static Column resolveAnnotation(
      @Nonnull final AnnotationKind kind,
      @Nonnull final String annotationField,
      @Nonnull final Column value,
      @Nonnull final Column container) {
    return kind.read(kind.apply(annotationField, value, container, ColumnFunctions::resolveOrElse));
  }

  /**
   * Returns whether a field is a decimal, which is decoded from its numeric annotation.
   *
   * @param fhirType the FHIR type of the field
   * @return true where the field is a decimal
   */
  static boolean isDecimal(@Nonnull final Optional<FHIRDefinedType> fhirType) {
    return fhirType.filter(FHIRDefinedType.DECIMAL::equals).isPresent();
  }

  /**
   * Returns whether a field is a date, a dateTime or an instant, which is read with its stored
   * range annotations.
   *
   * @param fhirType the FHIR type of the field
   * @return true where the field is a date, a dateTime or an instant
   */
  static boolean isDate(@Nonnull final Optional<FHIRDefinedType> fhirType) {
    return fhirType
        .filter(
            type ->
                type == FHIRDefinedType.DATE
                    || type == FHIRDefinedType.DATETIME
                    || type == FHIRDefinedType.INSTANT)
        .isPresent();
  }

  /**
   * Decodes a traversed field for computation, by its FHIR type. The field has been read in the
   * shape it is stored in.
   *
   * <p>A decimal is not decoded here. Its traversal reads it as its stored text and its numeric
   * annotation, and it is decoded where it is computed with.
   *
   * <p>A quantity is not decoded here. It stays in the stored shape, so that it keeps its
   * extensions, and the quantity collection decodes it where it is computed with.
   *
   * @param field the traversed field
   * @param fhirType the FHIR type of the field
   * @return the decoded field
   */
  @Nonnull
  static ColumnRepresentation decodeField(
      @Nonnull final ColumnRepresentation field,
      @Nonnull final Optional<FHIRDefinedType> fhirType) {
    @Nullable final FHIRDefinedType resolvedFhirType = fhirType.orElse(null);
    if (FHIRDefinedType.BASE64BINARY.equals(resolvedFhirType)) {
      // If the field is a base64Binary, represent it using a BinaryRepresentation.
      return DefaultRepresentation.fromBinaryColumn(field.getValue());
    } else {
      // Otherwise, use the field as it was traversed.
      return field;
    }
  }

  /**
   * Gets a field of the structure, or of every structure in the array, that this representation
   * holds, without flattening. The field is referenced through the tolerant traversal expression,
   * so the result is a null of the given type where the input schema does not carry the field.
   *
   * @param fieldName the name of the field to get
   * @param fallback the type of the null that stands for the field where it is absent
   * @return the field, unflattened
   */
  @Override
  @Nonnull
  public ColumnRepresentation getField(
      @Nonnull final String fieldName, @Nonnull final DataType fallback) {
    return copyOf(ColumnFunctions.resolveOrNull(value, fieldName, fallback));
  }
}
