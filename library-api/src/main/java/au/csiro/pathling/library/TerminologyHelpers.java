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

package au.csiro.pathling.library;

import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.fhirpath.encoding.CodingSchema;
import au.csiro.pathling.sql.Terminology;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.spark.sql.Column;

/**
 * Utility class for working with FHIR terminology concepts.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
public class TerminologyHelpers {

  /**
   * The URI for the SNOMED CT code system.
   */
  public static final String SNOMED_URI = "http://snomed.info/sct";

  /**
   * The URI for the LOINC code system.
   */
  public static final String LOINC_URI = "http://loinc.org";

  private TerminologyHelpers() {
  }

  /**
   * Converts a Column containing codes into a Column that contains a Coding struct. The Coding
   * struct Column can be used as an input to terminology functions such as
   * {@link Terminology#member_of} and {@link Terminology#translate}. A null value passed in to the
   * {@code codingColumn} parameter will result in a null value being returned.
   *
   * @param codingColumn the Column containing codes
   * @param system the URI of the system for the codes
   * @param version the version of the system for the codes
   * @return a Column containing a Coding struct
   */
  @Nonnull
  public static Column toCoding(@Nonnull final Column codingColumn, @Nullable final String system,
      @Nullable final String version) {
    return CodingSchema.toStruct(
        lit(null),
        lit(system),
        lit(version),
        codingColumn,
        lit(null),
        lit(null)
    );
  }

  /**
   * Converts a SNOMED CT ECL expression into a FHIR ValueSet URI. Can be used with the
   * {@link Terminology#member_of} function.
   *
   * @param ecl the ECL expression
   * @return a FHIR ValueSet URI
   */
  @Nonnull
  public static String toEclValueSet(@Nonnull final String ecl) {
    final String encodedEcl = URLEncoder.encode(ecl, StandardCharsets.UTF_8)
        .replace("+", "%20");
    return SNOMED_URI + "?fhir_vs=ecl/" + encodedEcl;
  }

}
