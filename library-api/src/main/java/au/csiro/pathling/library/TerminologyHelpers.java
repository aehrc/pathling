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

package au.csiro.pathling.library;

import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import jakarta.annotation.Nonnull;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;

public class TerminologyHelpers {

  public static final String SNOMED_URI = "http://snomed.info/sct";

  /**
   * Converts a Column containing codes into a Column that contains a Coding struct. The Coding
   * struct Column can be used as an input to terminology functions such as
   * {@link PathlingContext#memberOf} and {@link PathlingContext#translate}. A null value passed in
   * to the {@code codingColumn} parameter will result in a null value being returned.
   *
   * @param codingColumn the Column containing codes
   * @param system the URI of the system for the codes
   * @param version the version of the system for the codes
   * @return a Column containing a Coding struct
   */
  @Nonnull
  public static Column toCoding(@Nonnull final Column codingColumn, @Nullable final String system,
      @Nullable final String version) {
    return CodingEncoding.toStruct(
        lit(null),
        lit(system),
        lit(version),
        codingColumn,
        lit(null),
        lit(null)
    );
  }

  /**
   * Converts a Column containing codes into a Column that contains a Coding struct. The Coding
   * struct Column can be used as an input to terminology functions such as
   * {@link PathlingContext#memberOf} and {@link PathlingContext#translate}. A null value passed in
   * to the {@code codingColumn} parameter will result in a null value being returned.
   *
   * @param codingColumn the Column containing codes
   * @param system the URI of the system for the codes
   * @return a Column containing a Coding struct
   */
  @Nonnull
  public static Column toCoding(@Nonnull final Column codingColumn, @Nullable final String system) {
    return toCoding(codingColumn, system, null);
  }

  /**
   * Converts a Column containing SNOMED CT codes into a Column that contains a Coding struct.
   *
   * @param codingColumn the Column containing codes
   * @param version the version of the system for the codes
   * @return a Column containing a Coding struct
   */
  @Nonnull
  public static Column toSnomedCoding(@Nonnull final Column codingColumn,
      @Nullable final String version) {
    return toCoding(codingColumn, SNOMED_URI, version);
  }

  /**
   * Converts a Column containing SNOMED CT codes into a Column that contains a Coding struct.
   *
   * @param codingColumn the Column containing codes
   * @return a Column containing a Coding struct
   */
  @Nonnull
  public static Column toSnomedCoding(@Nonnull final Column codingColumn) {
    return toCoding(codingColumn, SNOMED_URI);
  }

  /**
   * Converts a SNOMED CT ECL expression into a FHIR ValueSet URI. Can be used with the
   * {@link PathlingContext#memberOf} function.
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
