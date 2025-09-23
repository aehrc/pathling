/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.fhirpath.comparison.ColumnComparator;
import au.csiro.pathling.fhirpath.comparison.Comparable;
import au.csiro.pathling.fhirpath.comparison.TemporalComparator;
import jakarta.annotation.Nonnull;

/**
 * An interface indicating that a class can be compared as a DateTime.
 * <p>
 * This includes both DateTime and Date, as per the FHIRPath specification. Also includes Instant,
 * which has to be specified at least up to seconds precision so should be comparable with DateTime
 * of the same precision.
 *
 * @author Piotr Szul
 */
public interface DateTimeComparable extends Comparable {
  
  @Override
  @Nonnull
  default ColumnComparator getComparator() {
    return TemporalComparator.forDateTime();
  }

  @Override
  default boolean isComparableTo(@Nonnull final Collection target) {
    return target instanceof DateTimeComparable;
  }

}
