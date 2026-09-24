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
 * <p>Content of every kind is ignored and never fails the read, but it is always detected, because
 * ignoring content is permitted and losing it silently is not (decision 68). Each finding is
 * reported as a warning naming what was dropped.
 *
 * <p>The kind is carried as a string with a predicate for each value, rather than as an
 * enumeration, matching the way the layout entries distinguish their own kinds.
 */
public final class NonConformantContent {

  private static final String CONTAINED_RESOURCE = "containedResource";
  private static final String UNDESCRIBED_CONTENT = "undescribedContent";
  private static final String SHAPE_MISMATCH = "shapeMismatch";
  private static final String PRIMITIVE_METADATA = "primitiveMetadata";
  private static final String ENCODING_MISMATCH = "encodingMismatch";

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
   * definition library resolves but the layout carries no column for, and a name the layout gives
   * one of its own annotations.
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
   * element supplied as a single value or as an array of arrays, a singular element supplied as an
   * array, a structure and a leaf supplied one for the other, or a primitive's metadata group in a
   * shape FHIR does not give it.
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
   * Returns a finding for the id or extensions of a primitive element, carried in the shape FHIR
   * gives them, which the transform does not yet populate.
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
   * Returns a finding for a primitive element whose JSON encoding contradicts the type the
   * definitions give it, such as text where a boolean is declared or a fraction where an integer
   * is. Inference types a column from every value in a file, so the finding is for the column:
   * every value of the element in that file is dropped, not only the offending one.
   *
   * @param path the path the content was found at
   * @param detail what was observed, against what the definitions declare
   * @return the finding
   */
  @Nonnull
  public static NonConformantContent encodingMismatch(
      @Nonnull final String path, @Nonnull final String detail) {
    return new NonConformantContent(path, ENCODING_MISMATCH, detail);
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
   * Returns whether this finding names a primitive element whose JSON encoding contradicts its
   * type.
   *
   * @return true where it does
   */
  public boolean isEncodingMismatch() {
    return ENCODING_MISMATCH.equals(kind);
  }

  @Override
  @Nonnull
  public String toString() {
    return path + ": " + detail;
  }
}
