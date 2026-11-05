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

package au.csiro.pathling.library.terminology;

import static java.util.Objects.requireNonNullElse;

import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.terminology.conceptmap.ConceptMapContent;
import au.csiro.pathling.terminology.conceptmap.ConceptMapContentException;
import au.csiro.pathling.terminology.conceptmap.ConceptMapLookupException;
import au.csiro.pathling.terminology.conceptmap.ConceptMapVersionException;
import au.csiro.pathling.terminology.conceptmap.ConceptMapping;
import au.csiro.pathling.terminology.expand.ValueSetExpansion;
import au.csiro.pathling.terminology.expand.ValueSetExpansionException;
import au.csiro.pathling.terminology.expand.ValueSetMember;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.DataFormatException;
import ca.uhn.fhir.parser.StrictErrorHandler;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ValueSet;

/**
 * Builds the value set and concept map relations of the SQL on FHIR <a
 * href="https://build.fhir.org/ig/HL7/sql-on-fhir/en/terminology-in-sql.html">Terminology in
 * SQL</a> page as Spark datasets, so that a query can join to the content of a ValueSet or a
 * ConceptMap like any other table.
 *
 * <p>Each relation is resolved when it is built and held as a local relation, so every use of it
 * sees the same content. The content is bounded by a caller-supplied cap, which keeps it small
 * enough to hold on the driver. The provenance of each resolution is logged at INFO.
 *
 * @author John Grimes
 */
@Slf4j
public class TerminologyRelations {

  /** The member cap applied when the caller gives none. */
  public static final int DEFAULT_MAX_MEMBERS = 100_000;

  /** The mapping cap applied when the caller gives none. */
  public static final int DEFAULT_MAX_MAPPINGS = 100_000;

  /**
   * The schema of a value set relation: {@code system} and {@code code} identify a member and are
   * never null. These are the default type mappings for the FHIR {@code uri}, {@code string},
   * {@code code} and {@code boolean} types.
   */
  public static final StructType VALUE_SET_SCHEMA =
      DataTypes.createStructType(
          new StructField[] {
            DataTypes.createStructField("system", DataTypes.StringType, false),
            DataTypes.createStructField("version", DataTypes.StringType, true),
            DataTypes.createStructField("code", DataTypes.StringType, false),
            DataTypes.createStructField("display", DataTypes.StringType, true),
            DataTypes.createStructField("inactive", DataTypes.BooleanType, true)
          });

  /**
   * The schema of a concept map relation: {@code source_system} and {@code source_code} identify
   * the source of a mapping and are never null; the target code, display and relationship are null
   * on a row stating that the source code has no mapping.
   */
  public static final StructType CONCEPT_MAP_SCHEMA =
      DataTypes.createStructType(
          new StructField[] {
            DataTypes.createStructField("source_system", DataTypes.StringType, false),
            DataTypes.createStructField("source_version", DataTypes.StringType, true),
            DataTypes.createStructField("source_code", DataTypes.StringType, false),
            DataTypes.createStructField("source_display", DataTypes.StringType, true),
            DataTypes.createStructField("target_system", DataTypes.StringType, true),
            DataTypes.createStructField("target_version", DataTypes.StringType, true),
            DataTypes.createStructField("target_code", DataTypes.StringType, true),
            DataTypes.createStructField("target_display", DataTypes.StringType, true),
            DataTypes.createStructField("relationship", DataTypes.StringType, true)
          });

  private static final String VALUE_SET = "Value set";
  private static final String CONCEPT_MAP = "Concept map";
  private static final String SUPPLIED_VALUE_SET = "Supplied value set";
  private static final String SUPPLIED_CONCEPT_MAP = "Supplied concept map";
  private static final String SUPPLIED_SOURCE = "supplied resource";
  private static final String NONE = "none";

  @Nonnull private final SparkSession spark;

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final TerminologyServiceFactory terminologyServiceFactory;

  /**
   * Creates a builder of terminology relations.
   *
   * @param spark the Spark session the relations are created in
   * @param fhirContext the FHIR context used to parse supplied resources
   * @param terminologyServiceFactory the factory of the terminology service that resolves canonical
   *     URLs and expands supplied composes
   */
  public TerminologyRelations(
      @Nonnull final SparkSession spark,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    this.spark = spark;
    this.fhirContext = fhirContext;
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  /**
   * Resolves a value set by canonical URL through the terminology service and returns its
   * membership as a relation under {@link #VALUE_SET_SCHEMA}.
   *
   * @param canonical the canonical URL, optionally with a {@code |version} suffix
   * @param maxMembers the largest membership accepted
   * @return the value set relation
   * @throws IllegalArgumentException if the canonical has no URL or the cap is not positive
   * @throws TerminologyRelationException if the value set cannot be resolved, or its membership
   *     cannot be determined or exceeds the cap
   */
  @Nonnull
  public Dataset<Row> valueSet(@Nonnull final String canonical, final int maxMembers) {
    requirePositive(maxMembers);
    final Canonical parsed = Canonical.parse(canonical);
    final Optional<ValueSetExpansion> expansion =
        resolving(
            VALUE_SET,
            canonical,
            () ->
                terminologyServiceFactory
                    .build()
                    .expand(parsed.url(), parsed.version(), maxMembers));
    return valueSetRelation(
        expansion.orElseThrow(() -> notResolved(VALUE_SET, canonical)), terminologySource());
  }

  /**
   * Returns the membership of a ValueSet supplied as FHIR R4 JSON as a relation under {@link
   * #VALUE_SET_SCHEMA}. A resource carrying an {@code expansion} supplies the membership as-is; one
   * carrying only a {@code compose} is expanded by the terminology service. The resource must carry
   * a {@code url}.
   *
   * @param json the ValueSet resource as FHIR R4 JSON
   * @param maxMembers the largest membership accepted
   * @return the value set relation
   * @throws IllegalArgumentException if the cap is not positive
   * @throws TerminologyRelationException if the JSON is not a ValueSet, the resource defines no
   *     membership, or its membership cannot be determined or exceeds the cap
   */
  @Nonnull
  public Dataset<Row> valueSetFromJson(@Nonnull final String json, final int maxMembers) {
    requirePositive(maxMembers);
    final ValueSet valueSet = parse(json, ValueSet.class, SUPPLIED_VALUE_SET);
    final String name = valueSet.hasUrl() ? valueSet.getUrl() : "(no url)";
    if (valueSet.hasExpansion()) {
      return valueSetRelation(
          resolving(
              SUPPLIED_VALUE_SET, name, () -> ValueSetExpansion.fromResource(valueSet, maxMembers)),
          SUPPLIED_SOURCE);
    }
    if (!valueSet.hasCompose()) {
      throw new TerminologyRelationException(
          SUPPLIED_VALUE_SET
              + " '"
              + name
              + "' could not be resolved: it carries neither an expansion nor a compose");
    }
    return valueSetRelation(
        resolving(
            SUPPLIED_VALUE_SET,
            name,
            () -> terminologyServiceFactory.build().expand(valueSet, maxMembers)),
        SUPPLIED_SOURCE + ", expanded by " + terminologySource());
  }

  /**
   * Resolves a concept map by canonical URL through the terminology service and returns its
   * mappings as a relation under {@link #CONCEPT_MAP_SCHEMA}.
   *
   * @param canonical the canonical URL, optionally with a {@code |version} suffix
   * @param maxMappings the largest number of mappings accepted
   * @return the concept map relation
   * @throws IllegalArgumentException if the canonical has no URL or the cap is not positive
   * @throws TerminologyRelationException if the concept map cannot be resolved, the version to use
   *     cannot be determined, or its content cannot be represented or exceeds the cap
   */
  @Nonnull
  public Dataset<Row> conceptMap(@Nonnull final String canonical, final int maxMappings) {
    requirePositive(maxMappings);
    final Canonical parsed = Canonical.parse(canonical);
    final Optional<ConceptMapContent> content =
        resolving(
            CONCEPT_MAP,
            canonical,
            () ->
                terminologyServiceFactory
                    .build()
                    .readConceptMap(parsed.url(), parsed.version(), maxMappings));
    return conceptMapRelation(
        content.orElseThrow(() -> notResolved(CONCEPT_MAP, canonical)), terminologySource());
  }

  /**
   * Returns the mappings of a ConceptMap supplied as FHIR R4 JSON as a relation under {@link
   * #CONCEPT_MAP_SCHEMA}, without consulting the terminology service.
   *
   * @param json the ConceptMap resource as FHIR R4 JSON
   * @param maxMappings the largest number of mappings accepted
   * @return the concept map relation
   * @throws IllegalArgumentException if the cap is not positive
   * @throws TerminologyRelationException if the JSON is not a ConceptMap with a url, or its content
   *     cannot be represented or exceeds the cap
   */
  @Nonnull
  public Dataset<Row> conceptMapFromJson(@Nonnull final String json, final int maxMappings) {
    requirePositive(maxMappings);
    final ConceptMap conceptMap = parse(json, ConceptMap.class, SUPPLIED_CONCEPT_MAP);
    if (!conceptMap.hasUrl()) {
      throw new TerminologyRelationException(
          SUPPLIED_CONCEPT_MAP + " could not be resolved: it carries no url");
    }
    return conceptMapRelation(
        resolving(
            SUPPLIED_CONCEPT_MAP,
            conceptMap.getUrl(),
            () -> ConceptMapContent.fromResource(conceptMap, maxMappings)),
        SUPPLIED_SOURCE);
  }

  /**
   * Runs a resolution, turning each failure the terminology layer reports into a {@link
   * TerminologyRelationException} that names the artefact and carries the reason.
   *
   * @param kind the kind of artefact, for the message
   * @param name the canonical or url of the artefact, for the message
   * @param resolution the resolution to run
   * @param <T> the type of the resolved content
   * @return the resolved content
   */
  @Nonnull
  private static <T> T resolving(
      @Nonnull final String kind,
      @Nonnull final String name,
      @Nonnull final Supplier<T> resolution) {
    try {
      return resolution.get();
    } catch (final ValueSetExpansionException
        | ConceptMapContentException
        | ConceptMapVersionException
        | ConceptMapLookupException e) {
      throw new TerminologyRelationException(
          kind + " '" + name + "' could not be resolved: " + e.getMessage(), e);
    }
  }

  @Nonnull
  private static TerminologyRelationException notResolved(
      @Nonnull final String kind, @Nonnull final String canonical) {
    return new TerminologyRelationException(kind + " '" + canonical + "' could not be resolved");
  }

  /**
   * Parses a supplied resource as FHIR R4 JSON, rejecting elements that R4 does not define. A
   * lenient parse would drop them silently, so that an R5 ConceptMap whose elements carry only
   * {@code noMap} would read as a map with no mappings.
   */
  @Nonnull
  private <T extends IBaseResource> T parse(
      @Nonnull final String json, @Nonnull final Class<T> type, @Nonnull final String kind) {
    try {
      return fhirContext
          .newJsonParser()
          .setParserErrorHandler(new StrictErrorHandler())
          .parseResource(type, json);
    } catch (final DataFormatException e) {
      throw new TerminologyRelationException(
          kind + " could not be parsed as a FHIR " + type.getSimpleName() + ": " + e.getMessage(),
          e);
    }
  }

  @Nonnull
  private Dataset<Row> valueSetRelation(
      @Nonnull final ValueSetExpansion expansion, @Nonnull final String source) {
    log.info(
        "Resolved value set '{}' (version {}) from {}: {} members; expansion {} at {}; code"
            + " systems {}",
        expansion.getUrl(),
        requireNonNullElse(expansion.getVersion(), NONE),
        source,
        expansion.getMembers().size(),
        requireNonNullElse(expansion.getIdentifier(), NONE),
        requireNonNullElse(expansion.getTimestamp(), NONE),
        expansion.getCodeSystemVersions());
    final List<Row> rows =
        expansion.getMembers().stream().map(TerminologyRelations::memberRow).toList();
    return spark.createDataFrame(rows, VALUE_SET_SCHEMA);
  }

  @Nonnull
  private Dataset<Row> conceptMapRelation(
      @Nonnull final ConceptMapContent content, @Nonnull final String source) {
    log.info(
        "Resolved concept map '{}' (version {}) from {}: {} mappings",
        content.getUrl(),
        requireNonNullElse(content.getVersion(), NONE),
        source,
        content.getMappings().size());
    final List<Row> rows =
        content.getMappings().stream().map(TerminologyRelations::mappingRow).toList();
    return spark.createDataFrame(rows, CONCEPT_MAP_SCHEMA);
  }

  @Nonnull
  private static Row memberRow(@Nonnull final ValueSetMember member) {
    return RowFactory.create(
        member.getSystem(),
        member.getVersion(),
        member.getCode(),
        member.getDisplay(),
        member.getInactive());
  }

  @Nonnull
  private static Row mappingRow(@Nonnull final ConceptMapping mapping) {
    return RowFactory.create(
        mapping.getSourceSystem(),
        mapping.getSourceVersion(),
        mapping.getSourceCode(),
        mapping.getSourceDisplay(),
        mapping.getTargetSystem(),
        mapping.getTargetVersion(),
        mapping.getTargetCode(),
        mapping.getTargetDisplay(),
        mapping.getRelationship());
  }

  /**
   * Names the terminology source for the provenance log: the local store or the terminology server
   * URL. A factory that exposes no configuration, such as a test double, is named as unknown.
   *
   * @return the name of the terminology source
   */
  @Nonnull
  private String terminologySource() {
    final TerminologyConfiguration configuration;
    try {
      configuration = terminologyServiceFactory.getConfiguration();
    } catch (final IllegalStateException e) {
      return "an unknown terminology source";
    }
    return TerminologyMode.LOCAL.equals(configuration.getMode())
        ? "the local terminology store"
        : configuration.getServerUrl();
  }

  private static void requirePositive(final int cap) {
    if (cap < 1) {
      throw new IllegalArgumentException("The cap must be positive, but was " + cap);
    }
  }

  /**
   * A canonical reference split into its URL and optional version at the first {@code |}, as the
   * {@code resource} of a {@code relatedArtifact} dependency is written. An empty version is no
   * version.
   *
   * @param url the URL
   * @param version the version, or null
   */
  private record Canonical(@Nonnull String url, @Nullable String version) {

    @Nonnull
    static Canonical parse(@Nonnull final String canonical) {
      final int pipe = canonical.indexOf('|');
      final String url = pipe >= 0 ? canonical.substring(0, pipe) : canonical;
      final String version = pipe >= 0 ? canonical.substring(pipe + 1) : "";
      if (url.isBlank()) {
        throw new IllegalArgumentException(
            "Canonical reference '" + canonical + "' is missing the url");
      }
      return new Canonical(url, version.isBlank() ? null : version);
    }
  }
}
