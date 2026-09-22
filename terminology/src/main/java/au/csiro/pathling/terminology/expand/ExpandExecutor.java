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

import au.csiro.pathling.fhir.TerminologyClient;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import org.hl7.fhir.r4.model.ValueSet;

/**
 * Expands a value set through the FHIR {@code ValueSet/$expand} operation of a terminology server,
 * paging through the expansion until it is complete.
 *
 * @author John Grimes
 */
public class ExpandExecutor {

  /** The number of entries requested per page. */
  public static final int EXPAND_PAGE_SIZE = 10000;

  @Nonnull private final TerminologyClient terminologyClient;

  /**
   * Creates an executor over a terminology client.
   *
   * @param terminologyClient the client for communicating with the terminology server
   */
  public ExpandExecutor(@Nonnull final TerminologyClient terminologyClient) {
    this.terminologyClient = terminologyClient;
  }

  /**
   * Expands a value set identified by canonical URL.
   *
   * @param url the canonical URL of the value set, without a version suffix
   * @param version the value set version to expand, or null for the server's default
   * @param maxMembers the largest membership the caller will accept
   * @return the expansion, or empty if the server does not know the canonical URL
   * @throws ValueSetExpansionException if the server cannot expand the value set or cannot be
   *     reached
   * @throws ExpansionLimitExceededException if the membership exceeds {@code maxMembers}
   */
  @Nonnull
  public Optional<ValueSetExpansion> expand(
      @Nonnull final String url, @Nullable final String version, final int maxMembers) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Expands a value set supplied as a resource. Where the resource carries an expansion, that
   * expansion is the membership; otherwise the resource is sent to the server for expansion.
   *
   * @param valueSet the value set resource
   * @param maxMembers the largest membership the caller will accept
   * @return the expansion
   * @throws ValueSetExpansionException if the membership cannot be determined
   * @throws ExpansionLimitExceededException if the membership exceeds {@code maxMembers}
   */
  @Nonnull
  public ValueSetExpansion expand(@Nonnull final ValueSet valueSet, final int maxMembers) {
    throw new UnsupportedOperationException("Not yet implemented");
  }
}
