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
import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.annotation.AnnotationKind;
import au.csiro.pathling.schema.annotation.kinds.DecimalNumericKind;
import au.csiro.pathling.utilities.SparkTypes;
import au.csiro.pathling.utilities.SystemDecimal;
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
   * <p>A decimal is decoded from its numeric annotation where the schema carries it, and from its
   * stored text otherwise (T095a).
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
    if (isDecimal(fhirType)) {
      return traverseDecimal(fieldName, fallback);
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
   * Traverses to a decimal, decoded from its numeric annotation where the schema carries it and
   * from its stored text otherwise (T095a, FR-023).
   *
   * <p>A singular decimal is read as the annotation alone, which is null wherever the text is, so
   * where the schema does not carry the annotation the plan decodes the text as it did before the
   * annotation existed. Under a repeating parent the parents without a value are left out first, so
   * that a text that does not decode still gives a null, as decoding the text alone gives. A
   * repeating decimal is paired with its annotations by position, as {@link #traverseAnnotated}
   * pairs them.
   */
  @Nonnull
  private ColumnRepresentation traverseDecimal(
      @Nonnull final String fieldName, @Nonnull final DataType fallback) {
    if (fallback instanceof ArrayType) {
      return numericValues(
          traverseAnnotated(
              fieldName,
              Optional.empty(),
              SparkTypes.elementTypeOf(fallback),
              true,
              DecimalNumericKind.INSTANCE,
              LayoutFields.NUMERIC_ANNOTATION));
    }
    final UnaryOperator<Column> numeric =
        parent ->
            numericValue(
                ColumnFunctions.resolveOrNull(parent, fieldName, fallback),
                ColumnFunctions.resolveOrNull(
                    parent, LayoutFields.metadataGroupName(fieldName), DataTypes.NullType));
    return vectorize(
            parents ->
                functions.transform(
                    functions.filter(
                        parents,
                        parent ->
                            ColumnFunctions.resolveOrNull(parent, fieldName, fallback).isNotNull()),
                    numeric::apply),
            numeric)
        .flatten();
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
  static Column numericValue(@Nonnull final Column value, @Nonnull final Column group) {
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
   * annotation, named for the annotation's field, read as the kind reads it. An item is present
   * wherever its value is.
   *
   * <p>A decimal is not paired here: its value is its numeric annotation, which {@link #traverse}
   * reads.
   *
   * @param fieldName the name of the primitive field
   * @param fhirType the FHIR type of the field, by which each value is decoded, which is not a
   *     decimal
   * @param fallback the type of a value of the field, taken where the input does not carry it
   * @param repeating whether the field repeats
   * @param kind the kind of the annotation
   * @param annotationField the field of the kind to pair each value with
   * @return the values, each paired with its annotation
   * @throws IllegalArgumentException if the field is a decimal
   */
  @Nonnull
  public ColumnRepresentation traverseAnnotated(
      @Nonnull final String fieldName,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final DataType fallback,
      final boolean repeating,
      @Nonnull final AnnotationKind kind,
      @Nonnull final String annotationField) {
    if (isDecimal(fhirType)) {
      throw new IllegalArgumentException(
          "A decimal is not paired with an annotation: its value is its numeric annotation, which"
              + " traverse reads: "
              + fieldName);
    }
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
    return pairWithAnnotation(
        ColumnFunctions.resolveOrNull(
            parent, fieldName, repeating ? DataTypes.createArrayType(fallback) : fallback),
        ColumnFunctions.resolveOrNull(
            parent, LayoutFields.metadataGroupName(fieldName), DataTypes.NullType),
        fhirType,
        repeating,
        kind,
        annotationField);
  }

  /**
   * Pairs the values of a primitive with one field of an annotation of them, as {@link
   * #traverseAnnotated} does for the values and metadata groups of one parent.
   *
   * @param values the values of the primitive, an array of them where it repeats
   * @param groups the metadata groups beside the values, aligned with them by position
   * @param fhirType the FHIR type of the primitive, by which each value is decoded
   * @param repeating whether the primitive repeats
   * @param kind the kind of the annotation
   * @param annotationField the field of the kind to pair each value with
   * @return the paired value, null where the value is, or an array of them where it repeats
   */
  @Nonnull
  static Column pairWithAnnotation(
      @Nonnull final Column values,
      @Nonnull final Column groups,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      final boolean repeating,
      @Nonnull final AnnotationKind kind,
      @Nonnull final String annotationField) {
    if (!repeating) {
      return annotatedValue(
          values,
          resolveAnnotation(kind, annotationField, values, groups),
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
                kind.read(pair.getField(annotationField)),
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
   * Takes repeating decimals paired with their numeric annotations as the decoded decimals (FR-022,
   * FR-023).
   *
   * <p>The annotation is the stored one where the schema carries it, and the decoding of the text
   * otherwise, decided once for the schema by the resolver that paired them. The stored annotation
   * is computed as the text is decoded, so either gives the same value. Each value present gives
   * one decimal, which is null where the text does not decode, as decoding the text alone gives.
   * The pairing reads each annotation as the kind reads it, at the engine's System.Decimal type.
   *
   * @param pairs decimals paired with their numeric annotations, as {@link #pairWithAnnotation}
   *     pairs them
   * @return the decimals, typed as {@link SystemDecimal#TYPE}
   */
  @Nonnull
  static ColumnRepresentation numericValues(@Nonnull final ColumnRepresentation pairs) {
    return pairs.transform(pair -> pair.getField(LayoutFields.NUMERIC_ANNOTATION));
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
   * Decodes a traversed field for computation, by its FHIR type. The field has been read in the
   * shape it is stored in.
   *
   * <p>A decimal is not decoded here. Its traversal decodes it from its numeric annotation, or from
   * its text where the schema does not carry the annotation.
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
