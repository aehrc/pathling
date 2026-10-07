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

package au.csiro.pathling.io.transform;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;

/**
 * The FHIR parsers the two ingest formats without a Spark-native path use, configured so that what
 * they parse and then write as JSON keeps what the source said (decision 83).
 *
 * <p>A parser is made where it is used, inside the function Spark runs, because neither a parser
 * nor the FHIR context behind it can be serialised. The context is HAPI's cached one, so it is
 * built once per executor rather than once per call.
 */
final class FhirParsers {

  private FhirParsers() {}

  /**
   * Returns a parser of FHIR JSON, which is also the parser every resource is written out with.
   *
   * @return the parser
   */
  @Nonnull
  static IParser json() {
    return configured(context().newJsonParser());
  }

  /**
   * Returns a parser of FHIR XML.
   *
   * @return the parser
   */
  @Nonnull
  static IParser xml() {
    return configured(context().newXmlParser());
  }

  /**
   * Returns the FHIR context, which also says which resource types exist.
   *
   * @return the context
   */
  @Nonnull
  static FhirContext context() {
    return FhirContext.forR4Cached();
  }

  @Nonnull
  private static IParser configured(@Nonnull final IParser parser) {
    // A resource in a bundle keeps the identifier it carries, rather than taking one from the full
    // URL of its entry, as the previous encoder kept it.
    parser.setOverrideResourceIdWithBundleEntryFullUrl(false);
    // HAPI drops the version from a reference when it writes one by default, which would change a
    // versioned reference the source carries.
    parser.setStripVersionsFromReferences(false);
    parser.setPrettyPrint(false);
    return parser;
  }
}
