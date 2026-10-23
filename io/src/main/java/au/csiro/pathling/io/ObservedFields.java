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

package au.csiro.pathling.io;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Finds the fields of a structure the input carries, by name. */
final class ObservedFields {

  private ObservedFields() {}

  /**
   * Returns the field of a structure with a name, matched exactly, because FHIR names are
   * case-sensitive.
   *
   * @param observed the structure the input carries
   * @param name the name of the field
   * @return the field, or empty where the structure does not carry it
   */
  @Nonnull
  static Optional<StructField> find(
      @Nonnull final StructType observed, @Nonnull final String name) {
    return Stream.of(observed.fields()).filter(field -> field.name().equals(name)).findFirst();
  }
}
