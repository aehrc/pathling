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

package au.csiro.pathling.test;

import jakarta.annotation.Nonnull;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Constants and helpers for the {@code rf2-mini} synthetic SNOMED CT fixture. The well-known codes
 * mirror those documented in {@code terminology/src/test/resources/rf2-mini/README.md} and are
 * stable across regeneration of the fixture.
 *
 * @author John Grimes
 */
public final class Rf2Mini {

  private Rf2Mini() {
    // Constants holder.
  }

  /** The SNOMED CT system URI. */
  public static final String SNOMED_URI = "http://snomed.info/sct";

  /** The detected edition/version URI of the base ({@code 20230601}) release. */
  public static final String VERSION_20230601 =
      "http://snomed.info/sct/900000000000207008/version/20230601";

  /** The detected edition/version URI of the later ({@code 20240601}) release. */
  public static final String VERSION_20240601 =
      "http://snomed.info/sct/900000000000207008/version/20240601";

  /** The number of concepts in the base release. */
  public static final int CONCEPT_COUNT_20230601 = 200;

  // Well-known clinical codes (see the fixture README).
  public static final String ROOT_FINDING = "1000004";
  public static final String DISORDER = "1001000";
  public static final String DIABETES = "1002007";
  public static final String TYPE1_DIABETES = "1003002";
  public static final String TYPE2_DIABETES = "1004008";
  public static final String TYPE2_WITH_COMPLICATION = "1005009";
  public static final String GESTATIONAL_DIABETES = "1006005";
  public static final String HYPERTENSION = "1007001";
  public static final String BODY_STRUCTURE = "1008006";
  public static final String ENDOCRINE_STRUCTURE = "1009003";
  public static final String PANCREAS_STRUCTURE = "1010008";
  public static final String MORPHOLOGY_TOP = "1011007";
  public static final String DEGENERATION_MORPH = "1012000";
  public static final String DIABETES_INACTIVE = "1013005";
  public static final String SIMPLE_REFSET = "1199008";
  public static final String GESTATIONAL_SUBTYPE = "1200006";

  // Filler concepts that the SAME AS association reference set also relates to TYPE2_DIABETES, so
  // that reverse translation has several results and their order is observable. Named in ascending
  // code order, which is not the order their rows are written in.
  public static final String ASSOCIATED_FILLER_1 = "1099005";
  public static final String ASSOCIATED_FILLER_2 = "1139006";
  public static final String ASSOCIATED_FILLER_3 = "1159005";

  // Metadata codes.
  public static final String IS_A = "116680003";
  public static final String FINDING_SITE = "363698007";
  public static final String ASSOCIATED_MORPHOLOGY = "116676008";
  public static final String SAME_AS_REFSET = "900000000000527005";
  public static final String CORE_MODULE = "900000000000207008";

  /**
   * Resolves the absolute filesystem path to a release directory of the fixture on the test
   * classpath.
   *
   * @param release the release directory name (e.g. {@code international-20230601})
   * @return the absolute path to the extracted release directory
   */
  @Nonnull
  public static Path releasePath(@Nonnull final String release) {
    final URL url = Rf2Mini.class.getResource("/rf2-mini/" + release);
    if (url == null) {
      throw new IllegalStateException(
          "rf2-mini fixture release not found on classpath: " + release);
    }
    return Paths.get(url.getPath());
  }

  /**
   * Resolves the absolute filesystem path to the base ({@code 20230601}) release.
   *
   * @return the absolute path to the base release directory
   */
  @Nonnull
  public static Path baseRelease() {
    return releasePath("international-20230601");
  }
}
