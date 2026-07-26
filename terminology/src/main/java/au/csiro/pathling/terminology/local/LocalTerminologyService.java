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
import au.csiro.pathling.terminology.local.index.ConceptMapIndex;
import au.csiro.pathling.terminology.local.index.Description;
import au.csiro.pathling.terminology.local.index.HierarchyIndex;
import au.csiro.pathling.terminology.local.index.RelationshipIndex;
import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;
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

  /** The designation use code identifying a display designation. */
  private static final String DISPLAY_DESIGNATION_USE = "display";

  /** The system of the {@code preferredForLanguage} designation use coding. */
  private static final String PREFERRED_FOR_LANGUAGE_SYSTEM =
      "http://terminology.hl7.org/CodeSystem/hl7TermMaintInfra";

  /** The designation use code for a language reference set's preferred term. */
  private static final String PREFERRED_FOR_LANGUAGE_CODE = "preferredForLanguage";

  @Nonnull private final TerminologyConfiguration configuration;
  @Nonnull private final Map<String, String> hadoopConfiguration;
  @Nonnull private final Map<String, CodeSystemIndexes> indexesCache = new ConcurrentHashMap<>();

  private static final String SNOMED_URI = "http://snomed.info/sct";

  private volatile boolean initialised;
  private TerminologyStoreReader reader;
  private ValueSetResolver valueSetResolver;
  private ExpansionCache expansionCache;
  private ConceptMapIndex conceptMapIndex;

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
    if (coding.getSystem() == null || coding.getCode() == null) {
      return Collections.emptyList();
    }
    ensureInitialised();
    final String refsetId = snomedImplicitConceptMap(conceptMapUrl);
    final List<Translation> translations =
        refsetId != null
            ? translateSnomedAssociation(conceptMapUrl, coding, refsetId, reverse)
            : conceptMapIndex.translate(
                conceptMapUrl, coding.getSystem(), coding.getCode(), reverse);
    if (target == null || target.isEmpty()) {
      return translations;
    }
    // The target names the value set in which a translation is sought: keep only translations
    // whose concept is a member of it, matching remote-mode behaviour.
    return translations.stream()
        .filter(translation -> validateCode(target, translation.getConcept()))
        .toList();
  }

  /**
   * Returns the reference set identifier of a SNOMED implicit concept map URL ({@code
   * ?fhir_cm=[refsetId]}), or null if the URL is not a SNOMED implicit concept map.
   */
  @Nullable
  private static String snomedImplicitConceptMap(@Nonnull final String conceptMapUrl) {
    final int query = conceptMapUrl.indexOf('?');
    if (query < 0 || !conceptMapUrl.startsWith(SNOMED_URI)) {
      return null;
    }
    final String queryString = conceptMapUrl.substring(query + 1);
    return queryString.startsWith("fhir_cm=") ? queryString.substring("fhir_cm=".length()) : null;
  }

  /** Translates through a SNOMED association reference set, forward or reversed. */
  @Nonnull
  private List<Translation> translateSnomedAssociation(
      @Nonnull final String conceptMapUrl,
      @Nonnull final Coding coding,
      @Nonnull final String refsetId,
      final boolean reverse) {
    if (!SNOMED_URI.equals(coding.getSystem())) {
      return Collections.emptyList();
    }
    final int base = conceptMapUrl.indexOf('?');
    final String baseUri = base < 0 ? conceptMapUrl : conceptMapUrl.substring(0, base);
    final String requestedVersion = SNOMED_URI.equals(baseUri) ? null : baseUri;
    final Optional<String> systemVersionId =
        valueSetResolver.resolveCodeSystemVersion(SNOMED_URI, requestedVersion);
    if (systemVersionId.isEmpty()) {
      return Collections.emptyList();
    }
    final CodeSystemIndexes indexes = indexesFor(systemVersionId.get());
    final Map<Integer, String> associations = indexes.refsets().associationTargets(refsetId);
    final ConceptDictionary dictionary = indexes.dictionary();
    final List<Translation> translations = new ArrayList<>();
    if (reverse) {
      // Find the referenced concepts whose association target is the requested code.
      for (final Map.Entry<Integer, String> entry : associations.entrySet()) {
        if (coding.getCode().equals(entry.getValue())) {
          translations.add(snomedTranslation(dictionary.code(entry.getKey())));
        }
      }
    } else {
      final Integer dense = dictionary.denseId(coding.getCode());
      if (dense != null && associations.containsKey(dense)) {
        translations.add(snomedTranslation(associations.get(dense)));
      }
    }
    return translations;
  }

  @Nonnull
  private static Translation snomedTranslation(@Nonnull final String code) {
    return Translation.of(
        ConceptMapEquivalence.EQUAL, new Coding().setSystem(SNOMED_URI).setCode(code));
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
   * Emits one property per related concept, in ascending code order.
   *
   * <p>The order is taken from the codes rather than from the bitmap, whose order is that of the
   * store's internal dense identifiers. Those identifiers are an implementation detail whose
   * assignment depends on how the store was imported, so ordering by them would let an internal
   * choice show through in a lookup result.
   *
   * @param propertyCode the property to emit, {@code parent} or {@code child}
   * @param related the related concepts, by dense identifier
   * @param dictionary the concept dictionary, for translating identifiers to codes
   * @param result the list to append to
   */
  private static void addRelatedCodes(
      @Nonnull final String propertyCode,
      @Nonnull final RoaringBitmap related,
      @Nonnull final ConceptDictionary dictionary,
      @Nonnull final List<PropertyOrDesignation> result) {
    final List<String> codes = new ArrayList<>(related.getCardinality());
    related.forEach((IntConsumer) dense -> codes.add(dictionary.code(dense)));
    Collections.sort(codes);
    codes.forEach(code -> result.add(Property.of(propertyCode, new CodeType(code))));
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
      final String display = selectDisplay(systemUrl, indexes, dense, acceptLanguage);
      if (display != null) {
        result.add(Property.of(PROPERTY_DISPLAY, new StringType(display)));
      }
    }
    if (wants(propertyCode, Designation.PROPERTY_CODE)) {
      addDesignations(systemUrl, indexes, dense, result);
    }
    if (wants(propertyCode, PROPERTY_PARENT)) {
      addRelatedCodes(PROPERTY_PARENT, indexes.hierarchy().parentsOf(dense), dictionary, result);
    }
    if (wants(propertyCode, PROPERTY_CHILD)) {
      addRelatedCodes(PROPERTY_CHILD, indexes.hierarchy().childrenOf(dense), dictionary, result);
    }
    if (wants(propertyCode, PROPERTY_INACTIVE)) {
      result.add(Property.of(PROPERTY_INACTIVE, new BooleanType(!dictionary.isActive(dense))));
    }
    // The following properties are specific to SNOMED CT.
    if (SNOMED_URI.equals(systemUrl)) {
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
            Property.of(
                PROPERTY_SUFFICIENTLY_DEFINED, new BooleanType(dictionary.isDefined(dense))));
      }
    } else {
      // Declared scalar properties of a FHIR CodeSystem concept.
      addScalarProperties(indexes, dense, propertyCode, result);
    }
    addAttributeProperties(systemUrl, indexes, dense, propertyCode, result);
    return result;
  }

  /**
   * Adds each declared scalar FHIR CodeSystem property of the concept, typed per its value type.
   */
  private void addScalarProperties(
      @Nonnull final CodeSystemIndexes indexes,
      final int dense,
      @Nullable final String propertyCode,
      @Nonnull final List<PropertyOrDesignation> result) {
    for (final au.csiro.pathling.terminology.local.index.PropertyValue value :
        indexes.properties().propertiesOf(dense)) {
      if (wants(propertyCode, value.getCode())) {
        result.add(
            Property.of(value.getCode(), scalarValue(value.getValueType(), value.getValue())));
      }
    }
  }

  /** Reconstructs a FHIR value from a stored scalar property type and string encoding. */
  @Nonnull
  private static org.hl7.fhir.r4.model.Type scalarValue(
      @Nonnull final String valueType, @Nonnull final String value) {
    return switch (valueType) {
      case "integer" -> new org.hl7.fhir.r4.model.IntegerType(value);
      case "boolean" -> new BooleanType(value);
      case "decimal" -> new org.hl7.fhir.r4.model.DecimalType(value);
      case "dateTime" -> new DateTimeType(value);
      case "code" -> new CodeType(value);
      default -> new StringType(value);
    };
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
   * Builds the designation list for a concept, replicating the reference server's presentation of
   * SNOMED descriptions. Within each language reference set, the preferred synonym is designated
   * {@code preferredForLanguage} with a dialect language code, while acceptable terms keep their
   * description type as the use; descriptions outside every language reference set carry no use.
   * The stored display also surfaces as a {@code preferredForLanguage} designation in its plain
   * language. FHIR CodeSystem designations are passed through with their declared use.
   */
  private void addDesignations(
      @Nonnull final String systemUrl,
      @Nonnull final CodeSystemIndexes indexes,
      final int dense,
      @Nonnull final List<PropertyOrDesignation> result) {
    final boolean snomed = SNOMED_URI.equals(systemUrl);
    final Set<String> seen = new HashSet<>();
    for (final Description description : indexes.descriptions().descriptionsOf(dense)) {
      final Map<String, String> acceptability = description.getAcceptability();
      if (!snomed) {
        addDesignation(
            result, seen, typeUse(description), description.getLanguage(), description.getTerm());
      } else if (acceptability == null || acceptability.isEmpty()) {
        // A description outside every language reference set carries no use.
        addDesignation(result, seen, null, description.getLanguage(), description.getTerm());
      } else {
        for (final Map.Entry<String, String> entry : acceptability.entrySet()) {
          final boolean preferredSynonym =
              SYNONYM_TYPE.equals(description.getTypeCode())
                  && PREFERRED_ACCEPTABILITY.equals(entry.getValue());
          if (preferredSynonym) {
            addDesignation(
                result,
                seen,
                preferredForLanguageUse(),
                dialectLanguage(description.getLanguage(), entry.getKey()),
                description.getTerm());
          } else {
            addDesignation(
                result,
                seen,
                typeUse(description),
                description.getLanguage(),
                description.getTerm());
          }
        }
      }
    }
    if (snomed) {
      // The stored display surfaces as a preferredForLanguage designation in its plain language.
      final String display = indexes.dictionary().display(dense);
      if (display != null) {
        addDesignation(
            result,
            seen,
            preferredForLanguageUse(),
            languageOfTerm(indexes, dense, display),
            display);
      }
    }
  }

  /** Adds a designation to the result unless an identical one has already been added. */
  private static void addDesignation(
      @Nonnull final List<PropertyOrDesignation> result,
      @Nonnull final Set<String> seen,
      @Nullable final Coding use,
      @Nullable final String language,
      @Nonnull final String term) {
    final String key =
        (use == null ? "" : use.getSystem() + "|" + use.getCode()) + "|" + language + "|" + term;
    if (seen.add(key)) {
      result.add(Designation.of(use, language, term));
    }
  }

  /** Builds the use coding for a description's declared type, or null when it has none. */
  @Nullable
  private static Coding typeUse(@Nonnull final Description description) {
    return description.getTypeCode() == null
        ? null
        : new Coding().setSystem(description.getTypeSystem()).setCode(description.getTypeCode());
  }

  /** Builds the {@code preferredForLanguage} designation use coding. */
  @Nonnull
  private static Coding preferredForLanguageUse() {
    return new Coding()
        .setSystem(PREFERRED_FOR_LANGUAGE_SYSTEM)
        .setCode(PREFERRED_FOR_LANGUAGE_CODE)
        .setDisplay("Preferred For Language");
  }

  /**
   * Builds the dialect language code for a term preferred within a language reference set, in the
   * form the reference server uses (for example {@code en-x-sctlang-90000000-00005090-07}).
   */
  @Nullable
  private static String dialectLanguage(
      @Nullable final String language, @Nonnull final String refsetId) {
    if (language == null) {
      return null;
    }
    final StringBuilder dialect = new StringBuilder(language).append("-x-sctlang");
    for (int start = 0; start < refsetId.length(); start += 8) {
      dialect.append('-').append(refsetId, start, Math.min(start + 8, refsetId.length()));
    }
    return dialect.toString();
  }

  /** Returns the language of the first description carrying the given term, or null. */
  @Nullable
  private static String languageOfTerm(
      @Nonnull final CodeSystemIndexes indexes, final int dense, @Nonnull final String term) {
    for (final Description description : indexes.descriptions().descriptionsOf(dense)) {
      if (term.equals(description.getTerm())) {
        return description.getLanguage();
      }
    }
    return null;
  }

  /**
   * Selects the display term for a concept in the requested language, falling back to the stored
   * default display. SNOMED CT uses the language reference set's preferred synonym; FHIR code
   * systems use a matching-language designation.
   */
  @Nullable
  private String selectDisplay(
      @Nonnull final String systemUrl,
      @Nonnull final CodeSystemIndexes indexes,
      final int dense,
      @Nullable final String acceptLanguage) {
    if (acceptLanguage != null && !acceptLanguage.isBlank()) {
      final String preferred =
          SNOMED_URI.equals(systemUrl)
              ? preferredSynonym(indexes, dense, acceptLanguage)
              : fhirDisplayForLanguage(indexes, dense, acceptLanguage);
      if (preferred != null) {
        return preferred;
      }
    }
    return indexes.dictionary().display(dense);
  }

  /** Finds the preferred synonym of a SNOMED concept in the given language, or null if none. */
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

  /**
   * Finds the display term of a FHIR CodeSystem concept in the given language from its
   * designations, preferring a designation whose use is {@code display} over any other
   * matching-language designation. Returns null when no designation matches the language.
   */
  @Nullable
  private String fhirDisplayForLanguage(
      @Nonnull final CodeSystemIndexes indexes, final int dense, @Nonnull final String language) {
    final String primary = primaryLanguageTag(language);
    String match = null;
    for (final Description description : indexes.descriptions().descriptionsOf(dense)) {
      if (description.getLanguage() == null
          || !primary.equals(primaryLanguageTag(description.getLanguage()))) {
        continue;
      }
      if (DISPLAY_DESIGNATION_USE.equals(description.getTypeCode())) {
        return description.getTerm();
      }
      if (match == null) {
        match = description.getTerm();
      }
    }
    return match;
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
    conceptMapIndex = null;
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
      final VersionResolver versionResolver = new VersionResolver(local.getDefaultSnomedEdition());
      valueSetResolver =
          new ValueSetResolver(
              CodeSystemEntry.loadCatalogue(reader),
              versionResolver,
              ValueSetStore.load(reader, versionResolver));
      expansionCache = new ExpansionCache(local.getExpansionCacheSize());
      conceptMapIndex = ConceptMapIndex.load(reader);
      initialised = true;
    }
  }
}
