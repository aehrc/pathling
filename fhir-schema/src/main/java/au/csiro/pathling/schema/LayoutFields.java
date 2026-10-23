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
 *   <li>An annotation lives in the container its element already owns (decision 135, part 4). A
 *       primitive's annotations are the last fields of its metadata group, after {@code id} and
 *       {@code extension}, and a complex element's are the last fields of its own structure, after
 *       its elements. Where a type carries more than one — the two bounds of a date range, or the
 *       two canonical forms of a quantity — their order is fixed here too: the specification's
 *       canonical annotation precedes the magnitude-preserving one, so a consumer reading the
 *       specification's layout meets the specification's annotation first.
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

  /** The annotation carrying the numeric value of a decimal. */
  @Nonnull public static final String NUMERIC_ANNOTATION = ANNOTATION_PREFIX + "numeric";

  /** The annotation carrying the lower bound implied by a stated precision. */
  @Nonnull public static final String START_ANNOTATION = ANNOTATION_PREFIX + "start";

  /** The annotation carrying the upper bound implied by a stated precision. */
  @Nonnull public static final String END_ANNOTATION = ANNOTATION_PREFIX + "end";

  /**
   * The annotation the Parquet on FHIR specification gives the canonical form of a quantity. It is
   * emitted under the specification's own name, because a consumer reading the specification's
   * layout must find the specification's annotation where the specification says it is.
   */
  @Nonnull public static final String CANONICAL_ANNOTATION = ANNOTATION_PREFIX + "canonical";

  /**
   * The annotation carrying the canonical form of a quantity at a precision that preserves
   * magnitude.
   *
   * <p>It accompanies the specification's annotation rather than replacing it. The specification
   * types its value as a fixed-point decimal whose absolute precision is constant regardless of
   * magnitude, so quantities differing by orders of magnitude compare equal and no engine can
   * compare on it. That argues for a second annotation, not against the first: dropping the
   * specification's form would make the files unreadable to every other implementation of the
   * layout, for a saving of one field in a quantity.
   */
  @Nonnull
  public static final String CANONICAL_EXACT_ANNOTATION = ANNOTATION_PREFIX + "canonical_exact";

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
   * Returns whether a field's name marks it as an annotation, whether or not Pathling defines it.
   *
   * @param name the name of the field
   * @return true where the field is an annotation
   */
  public static boolean isAnnotation(@Nonnull final String name) {
    return name.startsWith(ANNOTATION_PREFIX);
  }

  /**
   * Returns the names of the annotations that an element of the given type carries, in the order
   * they appear in the container the element owns: its metadata group where it is a primitive, and
   * its own structure where it is complex.
   *
   * @param type the FHIR type of the element
   * @return the annotation names, in order
   */
  @Nonnull
  public static List<String> annotationNames(@Nonnull final FHIRDefinedType type) {
    final String code = type.toCode();
    if (FHIRDefinedType.DECIMAL.toCode().equals(code)) {
      return List.of(NUMERIC_ANNOTATION);
    }
    if (RANGE_ANNOTATED.contains(code)) {
      return List.of(START_ANNOTATION, END_ANNOTATION);
    }
    if (CANONICAL_ANNOTATED.contains(code)) {
      return List.of(CANONICAL_ANNOTATION, CANONICAL_EXACT_ANNOTATION);
    }
    return List.of();
  }
}
