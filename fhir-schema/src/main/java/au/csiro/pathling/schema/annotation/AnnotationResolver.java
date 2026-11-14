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
import org.apache.spark.sql.Column;

/**
 * Resolves a field of a structure to the stored field where the schema carries it, and to a
 * fallback where it does not.
 *
 * <p>Applying an annotation kind takes one, because its two clients learn the schema at different
 * times (decision 135). The annotation step knows the schema of the dataset it rebuilds, and
 * resolves in Java. The engine builds its columns without a schema and learns presence only when
 * Spark analyses the plan, so it resolves with a Catalyst expression, which this module may not
 * hold (FR-048). Either way presence is decided once for the schema, never per row (FR-023).
 */
@FunctionalInterface
public interface AnnotationResolver {

  /**
   * Resolves a field of a structure, or of every structure in an array.
   *
   * @param structure the structure, or array of structures, that may carry the field
   * @param field the name of the field
   * @param fallback the value to take where the schema does not carry the field, in the shape the
   *     field would have
   * @return the stored field, or the fallback
   */
  @Nonnull
  Column resolve(@Nonnull Column structure, @Nonnull String field, @Nonnull Column fallback);
}
