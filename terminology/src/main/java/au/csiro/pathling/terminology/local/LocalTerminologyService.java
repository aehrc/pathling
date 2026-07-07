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

package au.csiro.pathling.terminology.local;

import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.terminology.TerminologyService;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;

/**
 * A {@link TerminologyService} that resolves the terminology functions against a local, imported
 * terminology store with no network dependency.
 *
 * <p>This is the foundational skeleton: it wires the service behind the existing factory seam and,
 * until the evaluation engine is added, returns the same "unknown content" results that remote mode
 * produces when referenced content is absent - {@code member_of} false, {@code translate} empty,
 * {@code subsumes} not-subsumed, and {@code lookup} empty. Query evaluation against the store's
 * indexes is layered on in the user-story phases.
 *
 * @author John Grimes
 */
@Slf4j
public class LocalTerminologyService implements TerminologyService {

  @Nonnull private final TerminologyConfiguration configuration;

  @Nonnull private final Map<String, String> hadoopConfiguration;

  /**
   * Creates a local terminology service.
   *
   * @param configuration the terminology configuration, including the local store settings
   * @param hadoopConfiguration a snapshot of the Hadoop configuration used to reach the store
   */
  public LocalTerminologyService(
      @Nonnull final TerminologyConfiguration configuration,
      @Nonnull final Map<String, String> hadoopConfiguration) {
    this.configuration = configuration;
    this.hadoopConfiguration = hadoopConfiguration;
  }

  /**
   * Returns the terminology configuration backing this service.
   *
   * @return the configuration
   */
  @Nonnull
  public TerminologyConfiguration getConfiguration() {
    return configuration;
  }

  /**
   * Returns the snapshot of the Hadoop configuration used to reach the store.
   *
   * @return the Hadoop configuration snapshot
   */
  @Nonnull
  public Map<String, String> getHadoopConfiguration() {
    return hadoopConfiguration;
  }

  @Override
  public boolean validateCode(@Nonnull final String valueSetUrl, @Nonnull final Coding coding) {
    return false;
  }

  @Nonnull
  @Override
  public List<Translation> translate(
      @Nonnull final Coding coding,
      @Nonnull final String conceptMapUrl,
      final boolean reverse,
      @Nullable final String target) {
    return Collections.emptyList();
  }

  @Nonnull
  @Override
  public ConceptSubsumptionOutcome subsumes(
      @Nonnull final Coding codingA, @Nonnull final Coding codingB) {
    return ConceptSubsumptionOutcome.NOTSUBSUMED;
  }

  @Nonnull
  @Override
  public List<PropertyOrDesignation> lookup(
      @Nonnull final Coding coding,
      @Nullable final String propertyCode,
      @Nullable final String acceptLanguage) {
    return Collections.emptyList();
  }
}
