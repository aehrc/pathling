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

package au.csiro.pathling.schema.annotation;

import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.Column;

/**
 * A kind of annotation: the function from the stored value of the element it annotates to the
 * annotation's value, defined once for the engine and for ingest alike (decision 135).
 *
 * <p>A kind is carried in one or more fields, whose names and positions {@link
 * au.csiro.pathling.schema.LayoutFields} fixes, and is governed by one switch whichever field it is
 * carried in. Ingest computes it and stores it. The engine reads the stored annotation where the
 * schema carries it and computes it otherwise. Which of the two is decided by the {@link
 * AnnotationResolver} that applying the kind takes.
 *
 * <p>The set of kinds is closed. This class is public so that the modules that apply kinds can
 * reach it, not as an extension point.
 */
public abstract class AnnotationKind {

  @Nonnull private final AnnotationSwitch annotationSwitch;

  @Nonnull private final List<String> fields;

  /**
   * Creates a kind.
   *
   * @param annotationSwitch the switch that governs whether the kind is stored
   * @param fields the names of the fields the kind is carried in, in their stored order
   */
  protected AnnotationKind(
      @Nonnull final AnnotationSwitch annotationSwitch, @Nonnull final List<String> fields) {
    this.annotationSwitch = annotationSwitch;
    this.fields = List.copyOf(fields);
  }

  /**
   * Returns the switch that governs whether this kind is stored.
   *
   * @return the switch
   */
  @Nonnull
  public AnnotationSwitch getSwitch() {
    return annotationSwitch;
  }

  /**
   * Returns the names of the fields this kind is carried in.
   *
   * @return the field names, in their stored order
   */
  @Nonnull
  public List<String> getFields() {
    return fields;
  }

  /**
   * Computes this annotation from one stored value of the element it annotates.
   *
   * <p>Every kind is a function of the value alone. A kind derived from a primitive's extensions
   * would also take its metadata group, and, where the primitive repeats, take positions from it.
   *
   * @param value the stored value: a primitive's value, or a complex element's structure
   * @return a structure with one field for each of {@link #getFields()}, named as they are, and
   *     null where the value is null
   */
  @Nonnull
  public abstract Column compute(@Nonnull Column value);

  /**
   * Applies this kind for one of its fields to one value: the stored annotation where the schema
   * carries it, and the computed one otherwise.
   *
   * <p>The container is where the annotation is stored, which is not always where its value is. A
   * primitive's annotation is a field of its metadata group, so the container is the group and the
   * value is the primitive beside it. A complex element's annotation is a field of its own
   * structure, so the container and the value are that structure.
   *
   * <p>Where the element repeats, the caller pairs the values with the containers by position,
   * either applying the kind at each position or resolving the field over all the containers at
   * once, so that a read of the containers can prune to it.
   *
   * @param field the field of this kind to apply
   * @param value the stored value of the element
   * @param container the structure that carries the annotation
   * @param resolver resolves the field to the stored one or to the computed one
   * @return the annotation
   */
  @Nonnull
  public Column apply(
      @Nonnull final String field,
      @Nonnull final Column value,
      @Nonnull final Column container,
      @Nonnull final AnnotationResolver resolver) {
    return resolver.resolve(container, field, compute(value).getField(field));
  }

  @Override
  @Nonnull
  public String toString() {
    return getClass().getSimpleName() + fields;
  }
}
