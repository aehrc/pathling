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
package au.csiro.pathling.fhirpath.comparison;

import jakarta.annotation.Nonnull;

/**
 * A comparator for decimal values, implementing standard comparison operations. It uses
 * element-wise equality for arrays of decimals to allow for comparions between differenct decimal
 * precisions.
 *
 * @author Piotr Szul
 */
public class DecimalComparator extends DefaultComparator implements ElementWiseEquality {

  private static final DecimalComparator INSTANCE = new DecimalComparator();

  private DecimalComparator() {
    super();
  }

  @Nonnull
  public static DecimalComparator getInstance() {
    return INSTANCE;
  }
}
