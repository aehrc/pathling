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

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * The representation of stored elements that the engine decodes, element by element, into a
 * structure of its own, retaining the elements as they were stored.
 *
 * <p>Its value is the decoded elements. The stored elements are kept for what the decoded structure
 * cannot carry, which is the extensions: the decoded structure has one fixed type, so that it
 * combines with the structures the engine builds itself, while the stored element's extensions have
 * the type the data gives them. So extension traversal reads the stored elements (T097).
 *
 * <p>An operation that only selects among the elements, such as a filter, taking the first or the
 * last, or indexing, commutes with decoding. Those operations are applied to the stored elements
 * and keep them. So do the union and combination of two such representations, which merge the
 * stored elements and decode the result (T113b, decision 79). Every other operation, such as a
 * union with a value the engine built, acts on the decoded value and yields an ordinary
 * representation, which reaches extensions through the decoded structure's {@code _fid}. That finds
 * them on the previous layout. On the new layout the {@code _fid} is null and the table has no
 * {@code _extension} column, so such a value has no extensions there, as a System value should not.
 *
 * @author Piotr Szul
 */
@Getter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class DecodedRepresentation extends DefaultRepresentation {

  /** The elements as they were stored. */
  @Nonnull private final ColumnRepresentation stored;

  /** The decoding of one stored element. */
  @Nonnull @EqualsAndHashCode.Exclude @ToString.Exclude private final UnaryOperator<Column> decoder;

  /**
   * Creates a representation of stored elements, decoded element by element.
   *
   * @param stored the elements as they were stored
   * @param decoder the decoding of one stored element, which maps a null to a null
   */
  public DecodedRepresentation(
      @Nonnull final ColumnRepresentation stored, @Nonnull final UnaryOperator<Column> decoder) {
    super(stored.transform(decoder).getValue());
    this.stored = stored;
    this.decoder = decoder;
  }

  @Override
  @Nonnull
  public ColumnRepresentation traverseExtension() {
    return stored.traverseExtension();
  }

  /**
   * Traverses to a field of the stored elements, which commutes with decoding them.
   *
   * <p>The decoded structure is not traversed. It has the same type as a previous-layout quantity,
   * canonical form and value scale included, so the traversal expression, which chooses its
   * normalisation from the type alone, would take it for stored data and normalise it again, at
   * every step. Traversing the stored elements normalises them once, and decodes the field as any
   * stored field of its type is decoded.
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
    return stored.traverse(fieldName, fhirType, fallback);
  }

  /**
   * Gets a field of the stored elements, without flattening, for the reason {@link
   * #traverse(String, Optional, DataType)} gives.
   *
   * @param fieldName the name of the field to get
   * @param fallback the type of the null that stands for the field where it is absent
   * @return the field, unflattened
   */
  @Override
  @Nonnull
  public ColumnRepresentation getField(
      @Nonnull final String fieldName, @Nonnull final DataType fallback) {
    return stored.getField(fieldName, fallback);
  }

  @Override
  @Nonnull
  public ColumnRepresentation filterElements(
      @Nonnull final Function<ColumnRepresentation, Column> predicate) {
    return new DecodedRepresentation(
        stored.filterElements(
            element -> predicate.apply(new DecodedRepresentation(element, decoder))),
        decoder);
  }

  @Override
  @Nonnull
  public ColumnRepresentation first() {
    return new DecodedRepresentation(stored.first(), decoder);
  }

  @Override
  @Nonnull
  public ColumnRepresentation last() {
    return new DecodedRepresentation(stored.last(), decoder);
  }

  @Override
  @Nonnull
  public ColumnRepresentation elementAt(@Nonnull final Column index) {
    return new DecodedRepresentation(stored.elementAt(index), decoder);
  }
}
