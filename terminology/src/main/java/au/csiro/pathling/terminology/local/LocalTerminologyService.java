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

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.local.index.CodeSystemIndexes;
import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;
import org.roaringbitmap.RoaringBitmap;

/**
 * A {@link TerminologyService} that resolves the terminology functions against a local, imported
 * terminology store with no network dependency.
 *
 * <p>The store, catalogue, and query engine are built lazily on first use and memoised for the life
 * of the service. {@code member_of} resolves the value set URL to a code system version and a VCL
 * expression, evaluates it once per URL and version into a cached bitmap, and tests each coding by
 * a bitmap membership check. Referenced content that is absent from the store yields the same
 * unknown-content results as remote mode. The remaining operations are layered on in the later user
 * stories.
 *
 * @author John Grimes
 */
@Slf4j
public class LocalTerminologyService implements TerminologyService, Closeable {

  @Nonnull private final TerminologyConfiguration configuration;
  @Nonnull private final Map<String, String> hadoopConfiguration;
  @Nonnull private final Map<String, CodeSystemIndexes> indexesCache = new ConcurrentHashMap<>();

  private volatile boolean initialised;
  private TerminologyStoreReader reader;
  private ValueSetResolver valueSetResolver;
  private ExpansionCache expansionCache;

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
    if (coding.getSystem() == null || coding.getCode() == null) {
      return false;
    }
    ensureInitialised();

    final Optional<ResolvedValueSet> resolved = valueSetResolver.resolve(valueSetUrl);
    if (resolved.isEmpty()) {
      // Unknown value set: the coding is not a member (unknown-content fallback).
      return false;
    }
    final ResolvedValueSet valueSet = resolved.get();
    if (!valueSet.getSystemUrl().equals(coding.getSystem())) {
      return false;
    }
    final CodeSystemIndexes indexes = indexesFor(valueSet.getSystemVersionId());
    final Integer dense = indexes.dictionary().denseId(coding.getCode());
    if (dense == null) {
      return false;
    }
    final RoaringBitmap expansion =
        expansionCache.get(
            valueSetUrl,
            valueSet.getSystemVersionId(),
            () ->
                new VclEvaluator(indexes, valueSet.getSystemUrl())
                    .evaluate(valueSet.getExpression()));
    return expansion.contains(dense);
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

  /**
   * Releases the store reader, loaded indexes, and cached expansions so that a subsequent rebuild
   * (for example after re-importing content) starts from a fresh view of the store. This is invoked
   * by {@link LocalTerminologyServiceFactory#reset()} through the {@code ObjectHolder}, which only
   * releases services that are {@link Closeable}.
   */
  @Override
  public synchronized void close() {
    indexesCache.clear();
    reader = null;
    valueSetResolver = null;
    expansionCache = null;
    initialised = false;
  }

  @Nonnull
  private CodeSystemIndexes indexesFor(@Nonnull final String systemVersionId) {
    return indexesCache.computeIfAbsent(systemVersionId, id -> CodeSystemIndexes.load(reader, id));
  }

  private void ensureInitialised() {
    if (initialised) {
      return;
    }
    synchronized (this) {
      if (initialised) {
        return;
      }
      final LocalTerminologyConfiguration local =
          Objects.requireNonNull(
              configuration.getLocal(),
              "Local terminology configuration is required in local mode");
      final String storagePath =
          Objects.requireNonNull(local.getStoragePath(), "A terminology storage path is required");
      log.debug("Opening local terminology store: {}", storagePath);
      reader = TerminologyStoreReader.open(storagePath, hadoopConfiguration);
      valueSetResolver =
          new ValueSetResolver(
              CodeSystemEntry.loadCatalogue(reader),
              new VersionResolver(local.getDefaultSnomedEdition()));
      expansionCache = new ExpansionCache(local.getExpansionCacheSize());
      initialised = true;
    }
  }
}
