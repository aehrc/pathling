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

package au.csiro.pathling.test.helpers;

import jakarta.annotation.Nonnull;
import org.hl7.fhir.r4.model.Coding;

public final class TerminologyHelpers {

  private TerminologyHelpers() {
  }

  public static final String SNOMED_URI = "http://snomed.info/sct";


  public static final Coding CD_SNOMED_284551006 = snomedCoding("284551006",
      "Laceration of foot");

  @SuppressWarnings("unused")
  public static Coding CD_SNOMED_63816008 = snomedCoding("63816008",
      "Left hepatectomy");

  // http://snomed.info/sct|444814009 -- subsumes --> http://snomed.info/sct|40055000

  // LOINC

  public static final String LOINC_URI = "http://loinc.org";

  // Others

  @Nonnull
  public static Coding snomedCoding(@Nonnull final String code,
      @Nonnull final String displayName) {
    return new Coding(SNOMED_URI, code, displayName);
  }


}
