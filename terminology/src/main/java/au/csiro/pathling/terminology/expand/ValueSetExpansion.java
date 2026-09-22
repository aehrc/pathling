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

package au.csiro.pathling.terminology.expand;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import lombok.Value;
import org.hl7.fhir.r4.model.ValueSet;

/**
 * The membership of one value set at one resolution, with the provenance of that resolution.
 *
 * @author John Grimes
 */
@Value
public class ValueSetExpansion implements Serializable {

  @Serial private static final long serialVersionUID = -7429066236106257364L;

  /** The canonical URL of the value set. */
  @Nonnull String url;

  /** The version of the value set that was resolved, or null where none is known. */
  @Nullable String version;

  /** The identifier of the expansion, or null where the source provides none. */
  @Nullable String identifier;

  /** The timestamp of the expansion as a FHIR dateTime string, or null where none is provided. */
  @Nullable String timestamp;

  /**
   * The code system versions the membership was computed against, as {@code system|version}
   * strings; may be empty.
   */
  @Nonnull List<String> codeSystemVersions;

  /** The members, deduplicated on identity and in first-seen order; may be empty. */
  @Nonnull List<ValueSetMember> members;

  /**
   * Flattens the expansion carried by a value set resource into a membership.
   *
   * @param valueSet the value set resource
   * @param maxMembers the largest membership the caller will accept
   * @return the membership
   * @throws ValueSetExpansionException if the resource carries no expansion, the expansion is
   *     incomplete, or an entry does not identify a member
   * @throws ExpansionLimitExceededException if the membership exceeds {@code maxMembers}
   */
  @Nonnull
  public static ValueSetExpansion fromResource(
      @Nonnull final ValueSet valueSet, final int maxMembers) {
    throw new UnsupportedOperationException("Not yet implemented");
  }
}
