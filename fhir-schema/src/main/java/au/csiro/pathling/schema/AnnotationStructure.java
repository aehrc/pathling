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

import au.csiro.pathling.utilities.CanonicalStructure;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.types.StructType;

/**
 * The canonical structure of an annotation that is a structure, such as the exact canonical value
 * of a quantity: the order of the fields of its type, which {@link LayoutFields} declares.
 *
 * <p>A reader may take such an annotation's fields by position, so a merge must keep the order of
 * its type rather than impose one of its own. An annotation's type is flat, so there is no
 * structure beneath its fields.
 */
public final class AnnotationStructure implements CanonicalStructure {

  @Nonnull private final List<String> fieldOrder;

  AnnotationStructure(@Nonnull final StructType type) {
    this.fieldOrder = List.of(type.fieldNames());
  }

  @Override
  @Nonnull
  public List<String> fieldOrder() {
    return fieldOrder;
  }

  @Override
  @Nonnull
  public Optional<CanonicalStructure> field(@Nonnull final String name) {
    return Optional.empty();
  }
}
