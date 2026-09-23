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

package au.csiro.pathling.terminology;

import au.csiro.pathling.terminology.local.ValueSetResolver;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * The URL grammar of the implicit value sets and concept maps that a terminology source defines
 * without a resource: SNOMED CT implicit value sets ({@code fhir_vs}), VCL implicit value sets, and
 * SNOMED CT implicit concept maps ({@code fhir_cm}). A SNOMED CT base is {@code
 * http://snomed.info/sct} or an edition/version URI, exactly as the local store understands it, so
 * that the grammar here and the store's resolution agree on what is implicit.
 *
 * @author John Grimes
 */
public final class ImplicitTerminologyUrls {

  private static final String SNOMED_URI = "http://snomed.info/sct";
  private static final String VCL_PREFIX = "http://fhir.org/VCL?";
  private static final String VALUE_SET_PARAMETER = "fhir_vs";
  private static final String CONCEPT_MAP_PARAMETER = "fhir_cm=";

  private ImplicitTerminologyUrls() {
    // Static helper.
  }

  /**
   * Tests whether a URL names an implicit value set: a SNOMED CT base with a {@code fhir_vs} query,
   * or a URL starting {@code http://fhir.org/VCL?}.
   *
   * @param url the URL
   * @return true if the URL names an implicit value set
   */
  public static boolean isImplicitValueSet(@Nonnull final String url) {
    if (url.startsWith(VCL_PREFIX)) {
      return true;
    }
    final String query = snomedQuery(url);
    return query != null
        && (query.equals(VALUE_SET_PARAMETER) || query.startsWith(VALUE_SET_PARAMETER + "="));
  }

  /**
   * Tests whether a URL names an implicit concept map: a SNOMED CT base with a {@code fhir_cm=}
   * query.
   *
   * @param url the URL
   * @return true if the URL names an implicit concept map
   */
  public static boolean isImplicitConceptMap(@Nonnull final String url) {
    final String query = snomedQuery(url);
    return query != null && query.startsWith(CONCEPT_MAP_PARAMETER);
  }

  /**
   * Returns the query string of a URL whose base is a SNOMED CT base, or null where the URL has no
   * query or its base is not SNOMED CT.
   */
  @Nullable
  private static String snomedQuery(@Nonnull final String url) {
    final int query = url.indexOf('?');
    if (query < 0) {
      return null;
    }
    final String base = url.substring(0, query);
    if (!SNOMED_URI.equals(base) && !ValueSetResolver.SNOMED_VERSIONED.matcher(base).matches()) {
      return null;
    }
    return url.substring(query + 1);
  }
}
