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

package au.csiro.pathling.terminology.conceptmap;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.hl7.fhir.r4.model.ConceptMap;

/**
 * A ConceptMap summary returned by a terminology server search, with its version. Its string form
 * is the version alone, so that version resolution messages that list the candidates name them by
 * version rather than by the resource that carries them.
 *
 * @author John Grimes
 */
final class VersionedConceptMapSummary {

  @Nonnull private final ConceptMap summary;

  VersionedConceptMapSummary(@Nonnull final ConceptMap summary) {
    this.summary = summary;
  }

  @Nonnull
  ConceptMap getSummary() {
    return summary;
  }

  @Nullable
  String getVersion() {
    return summary.hasVersion() ? summary.getVersion() : null;
  }

  @Override
  @Nonnull
  public String toString() {
    return String.valueOf(getVersion());
  }
}
