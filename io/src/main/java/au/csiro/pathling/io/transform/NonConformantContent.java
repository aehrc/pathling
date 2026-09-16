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

package au.csiro.pathling.io.transform;

import jakarta.annotation.Nonnull;

/**
 * One piece of content the source carries and this layout does not store, named by the path it was
 * found at.
 *
 * <p>Every kind of it is detected whether or not the strictness switch is set to fail, because
 * ignoring content is permitted and losing it silently is not (FR-018). The switch decides whether
 * a finding raises or is logged, and nothing else.
 *
 * <p>The kind is carried as a string with a predicate for each value, rather than as an
 * enumeration, matching the way the layout entries distinguish their own kinds.
 */
public final class NonConformantContent {

  private static final String CONTAINED_RESOURCE = "containedResource";
  private static final String UNDESCRIBED_CONTENT = "undescribedContent";
  private static final String SHAPE_MISMATCH = "shapeMismatch";
  private static final String PRIMITIVE_METADATA = "primitiveMetadata";
  private static final String OUTSIDE_DENSE_BOUNDS = "outsideDenseBounds";

  @Nonnull private final String path;

  @Nonnull private final String kind;

  @Nonnull private final String detail;

  private NonConformantContent(
      @Nonnull final String path, @Nonnull final String kind, @Nonnull final String detail) {
    this.path = path;
    this.kind = kind;
    this.detail = detail;
  }

  /**
   * Returns a finding for the resources a resource contains, which this layout does not represent
   * (FR-006).
   *
   * @param path the path the content was found at
   * @return the finding
   */
  @Nonnull
  public static NonConformantContent containedResource(@Nonnull final String path) {
    return new NonConformantContent(
        path, CONTAINED_RESOURCE, "contained resources are not represented in this layout");
  }

  /**
   * Returns a finding for content the definition set does not describe, which includes a name the
   * definition library resolves but the layout carries no column for.
   *
   * @param path the path the content was found at
   * @return the finding
   */
  @Nonnull
  public static NonConformantContent undescribedContent(@Nonnull final String path) {
    return new NonConformantContent(
        path, UNDESCRIBED_CONTENT, "the definitions describe no element of this name");
  }

  /**
   * Returns a finding for content whose shape contradicts the definitions, which is a repeating
   * element supplied as a single value or a singular element supplied as an array.
   *
   * @param path the path the content was found at
   * @param detail what was observed, against what the definitions declare
   * @return the finding
   */
  @Nonnull
  public static NonConformantContent shapeMismatch(
      @Nonnull final String path, @Nonnull final String detail) {
    return new NonConformantContent(path, SHAPE_MISMATCH, detail);
  }

  /**
   * Returns a finding for the id or extensions of a primitive element, which the transform does not
   * yet populate.
   *
   * @param path the path the content was found at
   * @return the finding
   */
  @Nonnull
  public static NonConformantContent primitiveMetadata(@Nonnull final String path) {
    return new NonConformantContent(
        path,
        PRIMITIVE_METADATA,
        "the id and extensions of a primitive element are not yet stored");
  }

  /**
   * Returns a finding for content the definitions describe but the bounds configured for the dense
   * mode drop (FR-017).
   *
   * <p>It is a different finding from undescribed content, because the remedy is different: the
   * caller raises the bound rather than correcting the data.
   *
   * @param path the path the content was found at
   * @param detail which bound drops it
   * @return the finding
   */
  @Nonnull
  public static NonConformantContent outsideDenseBounds(
      @Nonnull final String path, @Nonnull final String detail) {
    return new NonConformantContent(path, OUTSIDE_DENSE_BOUNDS, detail);
  }

  /**
   * Returns the path the content was found at, as a dotted path from the resource type.
   *
   * @return the path
   */
  @Nonnull
  public String getPath() {
    return path;
  }

  /**
   * Returns whether this finding names the resources a resource contains.
   *
   * @return true where it does
   */
  public boolean isContainedResource() {
    return CONTAINED_RESOURCE.equals(kind);
  }

  /**
   * Returns whether this finding names content the definition set does not describe.
   *
   * @return true where it does
   */
  public boolean isUndescribedContent() {
    return UNDESCRIBED_CONTENT.equals(kind);
  }

  /**
   * Returns whether this finding names content whose shape contradicts the definitions.
   *
   * @return true where it does
   */
  public boolean isShapeMismatch() {
    return SHAPE_MISMATCH.equals(kind);
  }

  /**
   * Returns whether this finding names the id or extensions of a primitive element.
   *
   * @return true where it does
   */
  public boolean isPrimitiveMetadata() {
    return PRIMITIVE_METADATA.equals(kind);
  }

  /**
   * Returns whether this finding names content the bounds configured for the dense mode drop.
   *
   * @return true where it does
   */
  public boolean isOutsideDenseBounds() {
    return OUTSIDE_DENSE_BOUNDS.equals(kind);
  }

  @Override
  @Nonnull
  public String toString() {
    return path + ": " + detail;
  }
}
