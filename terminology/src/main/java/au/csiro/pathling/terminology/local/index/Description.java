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

package au.csiro.pathling.terminology.local.index;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;
import lombok.Value;

/**
 * One description of a concept: a SNOMED description or a FHIR designation. Carries the term, its
 * language and type, and, for SNOMED, the acceptability of the term within each language reference
 * set, so display selection and designation filtering can be driven from the stored content.
 *
 * @author John Grimes
 */
@Value
public class Description {

  /** The description term. */
  @Nonnull String term;

  /** The BCP-47 language of the term, or null if unknown. */
  @Nullable String language;

  /** The description type code (a SNOMED description type SCTID or a designation use code). */
  @Nullable String typeCode;

  /** The code system of the description type. */
  @Nullable String typeSystem;

  /**
   * The acceptability of this term within each language reference set, keyed by reference set
   * identifier and valued by the acceptability SCTID (preferred or acceptable); null for content
   * without SNOMED-style acceptability.
   */
  @Nullable Map<String, String> acceptability;
}
