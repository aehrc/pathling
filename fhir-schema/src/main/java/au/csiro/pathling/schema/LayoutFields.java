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

package au.csiro.pathling.schema;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * The names of the fields the storage layout owns, and the rule that positions them.
 *
 * <p>These fields have no definition element to take a position from, so their positions are
 * declared here. Without a stated rule two implementations could both order by the definitions and
 * still produce structures that compare positionally wrong, which is the failure FR-057 exists to
 * prevent. The rule is:
 *
 * <ul>
 *   <li>{@code resourceType} is the first field of a resource.
 *   <li>The metadata group of a primitive sits immediately after the element it accompanies.
 *   <li>The annotations follow the metadata group, in the order declared here. Where a type carries
 *       more than one — the two bounds of a date range, or the two canonical forms of a quantity —
 *       that order is fixed here too: the specification's canonical annotation precedes the
 *       magnitude-preserving one, so a consumer reading the specification's layout meets the
 *       specification's annotation first.
 * </ul>
 *
 * <p>Whether an annotation is actually written is decided elsewhere: annotations are optional by
 * definition and individually disableable, so this class says where one goes rather than whether
 * there is one.
 */
public final class LayoutFields {

  /** The field naming the type of the resource, which every conformant file carries. */
  @Nonnull public static final String RESOURCE_TYPE = "resourceType";

  /** The element carrying the resources a resource contains, which this layout does not store. */
  @Nonnull public static final String CONTAINED = "contained";

  /** The prefix that marks the metadata group beside a primitive. */
  @Nonnull public static final String METADATA_GROUP_PREFIX = "_";

  /** The prefix that marks an annotation. */
  @Nonnull public static final String ANNOTATION_PREFIX = "__";

  /** The field carrying the id of a primitive, within its metadata group. */
  @Nonnull public static final String METADATA_GROUP_ID = "id";

  /** The field carrying the extensions of a primitive, within its metadata group. */
  @Nonnull public static final String METADATA_GROUP_EXTENSION = "extension";

  /** The fields of a metadata group, in the order they appear. */
  @Nonnull
  public static final List<String> METADATA_GROUP_FIELDS =
      List.of(METADATA_GROUP_ID, METADATA_GROUP_EXTENSION);

  /** The suffix of the annotation carrying the numeric value of a decimal. */
  @Nonnull public static final String NUMERIC_SUFFIX = "_numeric";

  /** The suffix of the annotation carrying the lower bound implied by a stated precision. */
  @Nonnull public static final String START_SUFFIX = "_start";

  /** The suffix of the annotation carrying the upper bound implied by a stated precision. */
  @Nonnull public static final String END_SUFFIX = "_end";

  /**
   * The suffix of the annotation the Parquet on FHIR specification gives the canonical form of a
   * quantity. It is emitted under the specification's own name, because a consumer reading the
   * specification's layout must find the specification's annotation where the specification says it
   * is.
   */
  @Nonnull public static final String CANONICAL_SUFFIX = "_canonical";

  /**
   * The suffix of the annotation carrying the canonical form of a quantity at a precision that
   * preserves magnitude.
   *
   * <p>It accompanies the specification's annotation rather than replacing it. The specification
   * types its value as a fixed-point decimal whose absolute precision is constant regardless of
   * magnitude, so quantities differing by orders of magnitude compare equal and no engine can
   * compare on it. That argues for a second annotation, not against the first: dropping the
   * specification's form would make the files unreadable to every other implementation of the
   * layout, for a saving of one column beside a quantity.
   */
  @Nonnull public static final String CANONICAL_EXACT_SUFFIX = "_canonical_exact";

  /** The types whose stated precision implies a range, and which therefore carry range bounds. */
  @Nonnull private static final Set<String> RANGE_ANNOTATED = Set.of("date", "dateTime", "instant");

  /**
   * The types that carry a canonical annotation. It is the quantity family rather than the {@code
   * Quantity} type alone, matching the rule the previous implementation applied through
   * assignability from the quantity class.
   */
  @Nonnull
  private static final Set<String> CANONICAL_ANNOTATED =
      Set.of("Quantity", "SimpleQuantity", "MoneyQuantity", "Age", "Count", "Distance", "Duration");

  private LayoutFields() {}

  /**
   * Returns the name of the metadata group that accompanies an element.
   *
   * @param elementName the name of the element the group accompanies
   * @return the name of the metadata group
   */
  @Nonnull
  public static String metadataGroupName(@Nonnull final String elementName) {
    return METADATA_GROUP_PREFIX + elementName;
  }

  /**
   * Returns the name of the numeric annotation of a field.
   *
   * @param fieldName the name of the field the annotation accompanies
   * @return the name of the annotation
   */
  @Nonnull
  public static String numericAnnotationName(@Nonnull final String fieldName) {
    return ANNOTATION_PREFIX + fieldName + NUMERIC_SUFFIX;
  }

  /**
   * Returns the name of the range start annotation of a field.
   *
   * @param fieldName the name of the field the annotation accompanies
   * @return the name of the annotation
   */
  @Nonnull
  public static String startAnnotationName(@Nonnull final String fieldName) {
    return ANNOTATION_PREFIX + fieldName + START_SUFFIX;
  }

  /**
   * Returns the name of the range end annotation of a field.
   *
   * @param fieldName the name of the field the annotation accompanies
   * @return the name of the annotation
   */
  @Nonnull
  public static String endAnnotationName(@Nonnull final String fieldName) {
    return ANNOTATION_PREFIX + fieldName + END_SUFFIX;
  }

  /**
   * Returns the name of the specification's canonical annotation of a field.
   *
   * @param fieldName the name of the field the annotation accompanies
   * @return the name of the annotation
   */
  @Nonnull
  public static String canonicalAnnotationName(@Nonnull final String fieldName) {
    return ANNOTATION_PREFIX + fieldName + CANONICAL_SUFFIX;
  }

  /**
   * Returns the name of the magnitude-preserving canonical annotation of a field.
   *
   * @param fieldName the name of the field the annotation accompanies
   * @return the name of the annotation
   */
  @Nonnull
  public static String canonicalExactAnnotationName(@Nonnull final String fieldName) {
    return ANNOTATION_PREFIX + fieldName + CANONICAL_EXACT_SUFFIX;
  }

  /**
   * Returns the names of the annotations that accompany a field of the given type, in the order
   * they appear beside it.
   *
   * @param fieldName the name of the field the annotations accompany
   * @param type the FHIR type of that field
   * @return the annotation names, in order
   */
  @Nonnull
  public static List<String> annotationNames(
      @Nonnull final String fieldName, @Nonnull final FHIRDefinedType type) {
    final String code = type.toCode();
    if (FHIRDefinedType.DECIMAL.toCode().equals(code)) {
      return List.of(numericAnnotationName(fieldName));
    }
    if (RANGE_ANNOTATED.contains(code)) {
      return List.of(startAnnotationName(fieldName), endAnnotationName(fieldName));
    }
    if (CANONICAL_ANNOTATED.contains(code)) {
      return List.of(canonicalAnnotationName(fieldName), canonicalExactAnnotationName(fieldName));
    }
    return List.of();
  }
}
