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

package au.csiro.pathling.utilities;

import jakarta.annotation.Nonnull;

/**
 * The marker that names a field of the storage layout as an annotation (decision 152), shared by
 * every module that has to recognise one, including those that cannot see the layout's own
 * definitions.
 */
public final class AnnotationNames {

  /** The prefix that marks an annotation. */
  @Nonnull public static final String PREFIX = "__";

  private AnnotationNames() {}

  /**
   * Returns whether a field's name marks it as an annotation, whether or not Pathling defines it.
   *
   * @param name the name of the field
   * @return true where the field is an annotation
   */
  public static boolean isAnnotation(@Nonnull final String name) {
    return name.startsWith(PREFIX);
  }
}
