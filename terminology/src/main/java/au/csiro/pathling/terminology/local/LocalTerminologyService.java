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
import au.csiro.pathling.terminology.local.index.ConceptDictionary;
import au.csiro.pathling.terminology.local.index.Description;
import au.csiro.pathling.terminology.local.index.HierarchyIndex;
import au.csiro.pathling.terminology.local.index.RelationshipIndex;
import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;
import org.roaringbitmap.IntConsumer;
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

  private static final String PROPERTY_DISPLAY = "display";
  private static final String PROPERTY_PARENT = "parent";
  private static final String PROPERTY_CHILD = "child";
  private static final String PROPERTY_INACTIVE = "inactive";
  private static final String PROPERTY_MODULE_ID = "moduleId";
  private static final String PROPERTY_EFFECTIVE_TIME = "effectiveTime";
  private static final String PROPERTY_SUFFICIENTLY_DEFINED = "sufficientlyDefined";

  /** The SNOMED CT synonym description type SCTID. */
  private static final String SYNONYM_TYPE = "900000000000013009";

  /** The SNOMED CT "preferred" acceptability SCTID within a language reference set. */
  private static final String PREFERRED_ACCEPTABILITY = "900000000000548007";

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
    if (codingA.getSystem() == null
        || codingA.getCode() == null
        || codingB.getSystem() == null
        || codingB.getCode() == null) {
      return ConceptSubsumptionOutcome.NOTSUBSUMED;
    }
    // Codings from different systems are never subsumption-related.
    if (!codingA.getSystem().equals(codingB.getSystem())) {
      return ConceptSubsumptionOutcome.NOTSUBSUMED;
    }
    // Equal codings are equivalent, matching the existing short-circuit behaviour.
    if (codingA.getCode().equals(codingB.getCode())) {
      return ConceptSubsumptionOutcome.EQUIVALENT;
    }
    ensureInitialised();
    final Optional<CodeSystemIndexes> indexes = indexesForCoding(codingA);
    if (indexes.isEmpty()) {
      return ConceptSubsumptionOutcome.NOTSUBSUMED;
    }
    final ConceptDictionary dictionary = indexes.get().dictionary();
    final Integer denseA = dictionary.denseId(codingA.getCode());
    final Integer denseB = dictionary.denseId(codingB.getCode());
    if (denseA == null || denseB == null) {
      return ConceptSubsumptionOutcome.NOTSUBSUMED;
    }
    final HierarchyIndex hierarchy = indexes.get().hierarchy();
    if (hierarchy.subsumes(denseA, denseB)) {
      return ConceptSubsumptionOutcome.SUBSUMES;
    }
    if (hierarchy.subsumes(denseB, denseA)) {
      return ConceptSubsumptionOutcome.SUBSUMEDBY;
    }
    return ConceptSubsumptionOutcome.NOTSUBSUMED;
  }

  @Nonnull
  @Override
  public List<PropertyOrDesignation> lookup(
      @Nonnull final Coding coding,
      @Nullable final String propertyCode,
      @Nullable final String acceptLanguage) {
    if (coding.getSystem() == null || coding.getCode() == null) {
      return Collections.emptyList();
    }
    ensureInitialised();
    final Optional<CodeSystemIndexes> maybeIndexes = indexesForCoding(coding);
    if (maybeIndexes.isEmpty()) {
      return Collections.emptyList();
    }
    final CodeSystemIndexes indexes = maybeIndexes.get();
    final Integer dense = indexes.dictionary().denseId(coding.getCode());
    if (dense == null) {
      // The system is known but the code is not: unknown-content fallback.
      return Collections.emptyList();
    }
    return buildLookup(coding.getSystem(), indexes, dense, propertyCode, acceptLanguage);
  }

  /**
   * Builds the property and designation list for a resolved concept, honouring an optional property
   * filter and display language.
   */
  @Nonnull
  private List<PropertyOrDesignation> buildLookup(
      @Nonnull final String systemUrl,
      @Nonnull final CodeSystemIndexes indexes,
      final int dense,
      @Nullable final String propertyCode,
      @Nullable final String acceptLanguage) {
    final List<PropertyOrDesignation> result = new ArrayList<>();
    final ConceptDictionary dictionary = indexes.dictionary();

    if (wants(propertyCode, PROPERTY_DISPLAY)) {
      final String display = selectDisplay(indexes, dense, acceptLanguage);
      if (display != null) {
        result.add(Property.of(PROPERTY_DISPLAY, new StringType(display)));
      }
    }
    if (wants(propertyCode, Designation.PROPERTY_CODE)) {
      for (final Description description : indexes.descriptions().descriptionsOf(dense)) {
        final Coding use =
            description.getTypeCode() == null
                ? null
                : new Coding()
                    .setSystem(description.getTypeSystem())
                    .setCode(description.getTypeCode());
        result.add(Designation.of(use, description.getLanguage(), description.getTerm()));
      }
    }
    if (wants(propertyCode, PROPERTY_PARENT)) {
      indexes
          .hierarchy()
          .parentsOf(dense)
          .forEach(
              (IntConsumer)
                  parent ->
                      result.add(
                          Property.of(PROPERTY_PARENT, new CodeType(dictionary.code(parent)))));
    }
    if (wants(propertyCode, PROPERTY_CHILD)) {
      indexes
          .hierarchy()
          .childrenOf(dense)
          .forEach(
              (IntConsumer)
                  child ->
                      result.add(
                          Property.of(PROPERTY_CHILD, new CodeType(dictionary.code(child)))));
    }
    if (wants(propertyCode, PROPERTY_INACTIVE)) {
      result.add(Property.of(PROPERTY_INACTIVE, new BooleanType(!dictionary.isActive(dense))));
    }
    if (wants(propertyCode, PROPERTY_MODULE_ID) && dictionary.moduleId(dense) != null) {
      result.add(Property.of(PROPERTY_MODULE_ID, new CodeType(dictionary.moduleId(dense))));
    }
    if (wants(propertyCode, PROPERTY_EFFECTIVE_TIME) && dictionary.effectiveTime(dense) != null) {
      result.add(
          Property.of(
              PROPERTY_EFFECTIVE_TIME,
              new DateTimeType(formatEffectiveTime(dictionary.effectiveTime(dense)))));
    }
    if (wants(propertyCode, PROPERTY_SUFFICIENTLY_DEFINED)) {
      result.add(
          Property.of(PROPERTY_SUFFICIENTLY_DEFINED, new BooleanType(dictionary.isDefined(dense))));
    }
    addAttributeProperties(systemUrl, indexes, dense, propertyCode, result);
    return result;
  }

  /** Adds each defining-relationship attribute of the concept as a Coding-valued property. */
  private void addAttributeProperties(
      @Nonnull final String systemUrl,
      @Nonnull final CodeSystemIndexes indexes,
      final int dense,
      @Nullable final String propertyCode,
      @Nonnull final List<PropertyOrDesignation> result) {
    final RelationshipIndex relationships = indexes.relationships();
    final ConceptDictionary dictionary = indexes.dictionary();
    final RoaringBitmap source = RoaringBitmap.bitmapOf(dense);
    for (final String type : relationships.typeCodes()) {
      if (!wants(propertyCode, type)) {
        continue;
      }
      relationships
          .targetsOf(type, source)
          .forEach(
              (IntConsumer)
                  target ->
                      result.add(
                          Property.of(
                              type,
                              new Coding()
                                  .setSystem(systemUrl)
                                  .setCode(dictionary.code(target))
                                  .setDisplay(dictionary.display(target)))));
    }
  }

  /**
   * Selects the display term for a concept, preferring the language-reference-set preferred synonym
   * for the requested language and falling back to the stored default display.
   */
  @Nullable
  private String selectDisplay(
      @Nonnull final CodeSystemIndexes indexes,
      final int dense,
      @Nullable final String acceptLanguage) {
    if (acceptLanguage != null && !acceptLanguage.isBlank()) {
      final String preferred = preferredSynonym(indexes, dense, acceptLanguage);
      if (preferred != null) {
        return preferred;
      }
    }
    return indexes.dictionary().display(dense);
  }

  /** Finds the preferred synonym of a concept in the given language, or null if there is none. */
  @Nullable
  private String preferredSynonym(
      @Nonnull final CodeSystemIndexes indexes, final int dense, @Nonnull final String language) {
    final String primary = primaryLanguageTag(language);
    for (final Description description : indexes.descriptions().descriptionsOf(dense)) {
      final boolean languageMatches =
          description.getLanguage() != null
              && primary.equals(primaryLanguageTag(description.getLanguage()));
      final boolean isPreferredSynonym =
          SYNONYM_TYPE.equals(description.getTypeCode())
              && description.getAcceptability() != null
              && description.getAcceptability().containsValue(PREFERRED_ACCEPTABILITY);
      if (languageMatches && isPreferredSynonym) {
        return description.getTerm();
      }
    }
    return null;
  }

  @Nonnull
  private Optional<CodeSystemIndexes> indexesForCoding(@Nonnull final Coding coding) {
    return valueSetResolver
        .resolveCodeSystemVersion(coding.getSystem(), emptyToNull(coding.getVersion()))
        .map(this::indexesFor);
  }

  /** Returns whether a lookup should emit a property, given an optional property-code filter. */
  private static boolean wants(
      @Nullable final String requestedPropertyCode, @Nonnull final String candidate) {
    return requestedPropertyCode == null || requestedPropertyCode.equals(candidate);
  }

  @Nullable
  private static String emptyToNull(@Nullable final String value) {
    return value == null || value.isEmpty() ? null : value;
  }

  /**
   * Formats an RF2 {@code YYYYMMDD} effectiveTime as an ISO {@code YYYY-MM-DD} date, leaving any
   * other value unchanged.
   */
  @Nonnull
  private static String formatEffectiveTime(@Nonnull final String effectiveTime) {
    if (effectiveTime.length() == 8 && effectiveTime.chars().allMatch(Character::isDigit)) {
      return effectiveTime.substring(0, 4)
          + "-"
          + effectiveTime.substring(4, 6)
          + "-"
          + effectiveTime.substring(6, 8);
    }
    return effectiveTime;
  }

  /** Returns the primary subtag of a BCP-47 language tag (e.g. {@code en} for {@code en-US}). */
  @Nonnull
  private static String primaryLanguageTag(@Nonnull final String language) {
    final int dash = language.indexOf('-');
    return (dash < 0 ? language : language.substring(0, dash)).toLowerCase();
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
