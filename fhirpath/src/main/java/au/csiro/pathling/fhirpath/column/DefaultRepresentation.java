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

import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.encoders.ColumnFunctions;
import au.csiro.pathling.encoders.ValueFunctions;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.annotation.AnnotationKind;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
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

  /** The name of the value in each item {@link #traverseAnnotated} returns. */
  @Nonnull public static final String ANNOTATED_VALUE = "value";

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
   * Traverses to a primitive field, as {@link #traverse} does, pairing each value with one field of
   * an annotation of it (decision 135, part 4).
   *
   * <p>A primitive's annotations are fields of its metadata group {@code _x}, aligned with the
   * values of {@code x} by position. They are paired here before any null is removed, because the
   * traversal's removal of nulls would lose the positions: a value whose group is absent would hand
   * its place to the next value's annotation. The annotation is read for every value as one field
   * of the groups, so that the read prunes to it, and is the stored one where the schema carries it
   * and the one the kind computes otherwise, decided once for the schema by the Catalyst resolver.
   *
   * <p>Each item of the result is a structure of the value, named {@link #ANNOTATED_VALUE}, and the
   * annotation, named for the annotation's field. An item is present wherever its value is.
   *
   * @param fieldName the name of the primitive field
   * @param fhirType the FHIR type of the field, by which each value is decoded
   * @param fallback the type of a value of the field, taken where the input does not carry it
   * @param repeating whether the field repeats
   * @param kind the kind of the annotation
   * @param annotationField the field of the kind to pair each value with
   * @return the values, each paired with its annotation
   */
  @Nonnull
  public ColumnRepresentation traverseAnnotated(
      @Nonnull final String fieldName,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final DataType fallback,
      final boolean repeating,
      @Nonnull final AnnotationKind kind,
      @Nonnull final String annotationField) {
    final UnaryOperator<Column> annotated =
        parent ->
            annotatedValues(
                parent, fieldName, fhirType, fallback, repeating, kind, annotationField);
    return vectorize(array -> functions.transform(array, annotated::apply), annotated)
        .removeNulls()
        .flatten()
        .removeNulls();
  }

  @Nonnull
  private static Column annotatedValues(
      @Nonnull final Column parent,
      @Nonnull final String fieldName,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final DataType fallback,
      final boolean repeating,
      @Nonnull final AnnotationKind kind,
      @Nonnull final String annotationField) {
    final Column values =
        ColumnFunctions.resolveOrNull(
            parent, fieldName, repeating ? DataTypes.createArrayType(fallback) : fallback);
    final Column groups =
        ColumnFunctions.resolveOrNull(
            parent, LayoutFields.metadataGroupName(fieldName), DataTypes.NullType);
    if (!repeating) {
      return annotatedValue(
          values,
          kind.apply(annotationField, values, groups, ColumnFunctions::resolveOrElse),
          fhirType,
          annotationField);
    }
    // The annotations are read as one field of all the groups, so that the read prunes to it, and
    // zipped with the values, so that each array is evaluated once rather than at every position.
    final Column annotations =
        ColumnFunctions.resolveOrElse(
            groups,
            annotationField,
            functions.transform(values, each -> kind.compute(each).getField(annotationField)));
    return functions.transform(
        functions.arrays_zip(values.alias(ANNOTATED_VALUE), annotations.alias(annotationField)),
        pair ->
            annotatedValue(
                pair.getField(ANNOTATED_VALUE),
                pair.getField(annotationField),
                fhirType,
                annotationField));
  }

  /**
   * Pairs one value with its annotation, which is computed from the value as it is stored, and then
   * decodes the value.
   */
  @Nonnull
  private static Column annotatedValue(
      @Nonnull final Column value,
      @Nonnull final Column annotation,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final String annotationField) {
    final Column decoded = decodeField(new DefaultRepresentation(value), fhirType).getValue();
    return functions.when(
        value.isNotNull(),
        functions.struct(decoded.alias(ANNOTATED_VALUE), annotation.alias(annotationField)));
  }

  /**
   * Decodes a traversed field for computation, by its FHIR type. The field has been read in the
   * shape it is stored in.
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
    } else if (FHIRDefinedType.DECIMAL.equals(resolvedFhirType)) {
      // A decimal is read as text, and decoded for computation.
      return DecimalCollection.decode(field);
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
