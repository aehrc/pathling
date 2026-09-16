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
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataType;

/**
 * Maps a source column onto a target structure, so that the transform of a particular kind of
 * content can hand the ordinary structural mapping back to the transform that owns it.
 */
interface StructureMapping {

  /**
   * Maps a source column onto a target structure.
   *
   * @param source the column the source was read into
   * @param target the type the content is stored as
   * @param observed the type the source was read as
   * @return the stored value
   */
  @Nonnull
  Column map(@Nonnull Column source, @Nonnull DataType target, @Nonnull DataType observed);
}
