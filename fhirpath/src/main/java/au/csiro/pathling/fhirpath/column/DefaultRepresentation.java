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

import au.csiro.pathling.encoders.ColumnFunctions;
import au.csiro.pathling.encoders.ValueFunctions;
import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.annotation.AnnotationKind;
import au.csiro.pathling.schema.annotation.kinds.DecimalNumericKind;
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

  /** The name of a stored value where it is paired with its annotation. */
  @Nonnull private static final String VALUE = "value";

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
    if (isDecimal(fhirType) && !(fallback instanceof ArrayType)) {
      return traverseSingularDecimal(fieldName, fallback);
    }
    if (isDecimal(fhirType) || isDate(fhirType)) {
      return traverseStored(fieldName, fhirType, fallback);
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
   * Traverses to a decimal or a date, read as the engine holds it from the values and the metadata
   * groups beside them, as {@link #storedValues} reads them.
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
      @Nonnull final DataType fallback) {
    final UnaryOperator<Column> stored =
        parent ->
            let(
                parent,
                p ->
                    storedValues(
                        ColumnFunctions.resolveOrNull(p, fieldName, fallback),
                        ColumnFunctions.resolveOrNull(
                            p, LayoutFields.metadataGroupName(fieldName), DataTypes.NullType),
                        fhirType,
                        fallback instanceof ArrayType));
    // Only the parents without a value are left out, as a decimal that does not decode stays null.
    return vectorize(
            array -> functions.filter(functions.transform(array, stored::apply), Column::isNotNull),
            stored)
        .flatten();
  }

  /**
   * Returns the values of a decimal or a date as the engine holds them, read from the stored values
   * and the metadata groups beside them (FR-004, FR-022, FR-023): a decimal decoded from its
   * numeric annotation, and a date converted to a System date with its range. Each annotation is
   * the stored one where the schema carries it, and the computed one otherwise, decided once for
   * the schema.
   *
   * <p>A repeating field's values are paired with its annotations by position, and the values that
   * are absent are left out. A decimal whose text does not decode stays as a null, as decoding the
   * text alone gives.
   *
   * @param values the stored values, an array of them where the field repeats
   * @param groups the metadata groups beside them, aligned with them by position
   * @param fhirType the FHIR type of the field, a decimal or a date
   * @param repeating whether the field repeats
   * @return the value, or an array of the values present where the field repeats
   */
  @Nonnull
  static Column storedValues(
      @Nonnull final Column values,
      @Nonnull final Column groups,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      final boolean repeating) {
    if (isDate(fhirType)) {
      return repeating
          ? functions.filter(SystemDateTime.fromStoredArray(values, groups), Column::isNotNull)
          : SystemDateTime.fromStored(values, groups);
    }
    if (!repeating) {
      return numericValue(values, groups);
    }
    // The annotations are read as one field of all the groups, so that the read prunes to it, and
    // are paired with the texts by position, so that the absent values can be left out.
    final DecimalNumericKind kind = DecimalNumericKind.INSTANCE;
    final String numeric = LayoutFields.NUMERIC_ANNOTATION;
    final Column annotations =
        ColumnFunctions.resolveOrElse(
            groups,
            numeric,
            functions.transform(values, each -> kind.compute(each).getField(numeric)));
    return functions.transform(
        functions.filter(
            functions.arrays_zip(values.alias(VALUE), annotations.alias(numeric)),
            pair -> pair.getField(VALUE).isNotNull()),
        pair -> kind.read(pair.getField(numeric)));
  }

  /**
   * Traverses to a singular decimal, decoded from its numeric annotation where the schema carries
   * it and from its stored text otherwise (T095a, FR-023).
   *
   * <p>A singular decimal is read as the annotation alone, which is null wherever the text is, so
   * where the schema does not carry the annotation the plan decodes the text as it did before the
   * annotation existed. Under a repeating parent the parents without a value are left out first, so
   * that a text that does not decode still gives a null, as decoding the text alone gives.
   */
  @Nonnull
  private ColumnRepresentation traverseSingularDecimal(
      @Nonnull final String fieldName, @Nonnull final DataType fallback) {
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
   * Returns whether a field is a date, a dateTime or an instant, which is read with its range.
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
