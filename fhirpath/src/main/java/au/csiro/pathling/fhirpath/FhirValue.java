/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath;

import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Type;

/**
 * Designates an expression that is capable of being extracted into a FHIR value.
 *
 * @param <T> the {@link Type} of FHIR value that can be extracted
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/R4/datatypes.html">Data Types</a>
 */
public interface FhirValue<T extends Type> {

  /**
   * Extracts a FHIR value from a {@link Row} within a {@link org.apache.spark.sql.Dataset}.
   * <p>
   * Implementations of this method should use {@link Row#isNullAt} to check for a null value.
   *
   * @param row the {@link Row} to get the value from
   * @param columnNumber the index of the column within the row
   * @return the value, which may be missing
   */
  @Nonnull
  Optional<T> getFhirValueFromRow(@Nonnull Row row, int columnNumber);

}
