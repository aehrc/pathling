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

package au.csiro.pathling.fhirpath.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The SQL on FHIR conformance level. This allows us to map a feature within Pathling to a grouping
 * of features within the specification.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface SqlOnFhirConformance {

  /** SQL on FHIR conformance profiles. */
  enum Profile {
    /** Required features that must be supported. */
    REQUIRED,
    /**
     * Features that are required to be supported as part of the Shareable View Definition profile.
     *
     * @see <a
     *     href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ShareableViewDefinition.html">Shareable
     *     View Definition</a>
     */
    SHARABLE,
    /** Experimental features that are not yet stable. */
    EXPERIMENTAL,
    /** Terminology-related features. */
    TERMINOLOGY
  }

  /**
   * The compatibility profile associated with this feature.
   *
   * @return the profile
   */
  Profile value();
}
