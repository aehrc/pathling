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

package au.csiro.pathling.library;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.terminology.TerminologyRelationException;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.terminology.conceptmap.ConceptMapContent;
import au.csiro.pathling.terminology.conceptmap.ConceptMapContentException;
import au.csiro.pathling.terminology.conceptmap.ConceptMapLookupException;
import au.csiro.pathling.terminology.conceptmap.ConceptMapVersionException;
import au.csiro.pathling.terminology.conceptmap.ConceptMapping;
import au.csiro.pathling.terminology.expand.ExpansionLimitExceededException;
import au.csiro.pathling.terminology.expand.ValueSetExpansion;
import au.csiro.pathling.terminology.expand.ValueSetExpansionException;
import au.csiro.pathling.terminology.expand.ValueSetMember;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.ValueSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the value set and concept map relations exposed by {@link PathlingContext}, which make
 * the content of a ValueSet or ConceptMap available to Spark SQL as described in the SQL on FHIR
 * "Terminology in SQL" page. The terminology service is mocked, so these tests pin down the shape
 * of the relations, the parsing of canonical references, the handling of supplied resources and the
 * failure behaviour; resolution through a real terminology store is covered by {@link
 * LocalTerminologyRelationsTest}.
 *
 * @author John Grimes
 */
class PathlingContextTerminologyRelationsTest {

  private static final String CVD_URL = "http://example.org/ValueSet/cardiovascular-disease";
  private static final String CVD_CANONICAL = CVD_URL + "|2026";
  private static final String MAP_URL = "http://example.org/ConceptMap/sct-to-icd10";
  private static final String MAP_CANONICAL = MAP_URL + "|2026";
  private static final String SNOMED = "http://snomed.info/sct";
  private static final String ICD10 = "http://hl7.org/fhir/sid/icd-10";
  private static final String SNOMED_VERSION =
      "http://snomed.info/sct/900000000000207008/version/20260201";
  private static final int DEFAULT_CAP = 100_000;

  private static SparkSession spark;
  private static FhirEncoders encoders;

  private TerminologyService terminologyService;
  private PathlingContext pathling;

  @BeforeAll
  static void setUpAll() {
    spark = TestHelpers.spark();
    encoders = FhirEncoders.forR4().getOrCreate();
  }

  @AfterAll
  static void tearDownAll() {
    spark.stop();
  }

  @BeforeEach
  void setUp() {
    final TerminologyServiceFactory factory =
        mock(TerminologyServiceFactory.class, withSettings().serializable());
    terminologyService = mock(TerminologyService.class, withSettings().serializable());
    when(factory.build()).thenReturn(terminologyService);
    when(factory.getConfiguration()).thenReturn(TerminologyConfiguration.builder().build());
    pathling = PathlingContext.createInternal(spark, encoders, factory);
  }

  // ---------------------------------------------------------------------------------------------
  // Value set relation from a canonical reference.
  // ---------------------------------------------------------------------------------------------

  @Test
  void valueSetHasTheFiveSpecifiedColumnsWithSystemAndCodeNonNullable() {
    // The relation must carry system, version, code, display and inactive, with only system and
    // code declared non-nullable, using the default type mappings for uri, string, code, boolean.
    stubExpansion(CVD_URL, "2026", DEFAULT_CAP, List.of());

    final StructType schema = pathling.valueSet(CVD_CANONICAL).schema();

    assertEquals(
        List.of(
            field("system", DataTypes.StringType, false),
            field("version", DataTypes.StringType, true),
            field("code", DataTypes.StringType, false),
            field("display", DataTypes.StringType, true),
            field("inactive", DataTypes.BooleanType, true)),
        Arrays.asList(schema.fields()));
  }

  @Test
  void valueSetSplitsTheVersionFromTheCanonicalAndReturnsOneRowPerMember() {
    // The canonical's |version suffix is passed to the terminology layer as the value set version,
    // and every member becomes a row, with null carried through for the optional columns.
    stubExpansion(
        CVD_URL,
        "2026",
        DEFAULT_CAP,
        List.of(
            new ValueSetMember(SNOMED, SNOMED_VERSION, "22298006", "Myocardial infarction", null),
            new ValueSetMember(ICD10, null, "I21", null, true)));

    final List<Row> rows = pathling.valueSet(CVD_CANONICAL).collectAsList();

    assertEquals(
        List.of(
            RowFactory.create(SNOMED, SNOMED_VERSION, "22298006", "Myocardial infarction", null),
            RowFactory.create(ICD10, null, "I21", null, true)),
        rows);
  }

  @Test
  void valueSetWithoutVersionResolvesTheUnversionedUrl() {
    // A canonical without a version suffix, or with an empty one, leaves the version to the
    // terminology layer.
    stubExpansion(CVD_URL, null, DEFAULT_CAP, List.of());

    pathling.valueSet(CVD_URL);
    pathling.valueSet(CVD_URL + "|");

    verify(terminologyService, times(2)).expand(eq(CVD_URL), isNull(), eq(DEFAULT_CAP));
  }

  @Test
  void valueSetPassesAnExplicitMemberCap() {
    // The cap defaults to 100,000 and can be overridden per call.
    stubExpansion(CVD_URL, "2026", 5, List.of());

    pathling.valueSet(CVD_CANONICAL, 5);

    verify(terminologyService).expand(CVD_URL, "2026", 5);
  }

  @Test
  void valueSetWithNoMembersIsAnEmptyRelation() {
    // An empty membership is not an error.
    stubExpansion(CVD_URL, "2026", DEFAULT_CAP, List.of());

    assertEquals(0, pathling.valueSet(CVD_CANONICAL).count());
  }

  @Test
  void valueSetIsResolvedOnceAndEveryUseSeesTheSameContent() {
    // Resolution happens when the relation is created, so evaluating it repeatedly does not consult
    // the terminology layer again.
    stubExpansion(
        CVD_URL,
        "2026",
        DEFAULT_CAP,
        List.of(new ValueSetMember(SNOMED, null, "22298006", null, null)));

    final Dataset<Row> relation = pathling.valueSet(CVD_CANONICAL);
    relation.count();
    relation.collectAsList();

    verify(terminologyService, times(1)).expand(anyUrl(), any(), anyInt());
  }

  @Test
  void unresolvableValueSetFailsRatherThanReturningNoRows() {
    // A canonical the terminology layer cannot resolve is an error, never an empty relation.
    when(terminologyService.expand(CVD_URL, "2026", DEFAULT_CAP)).thenReturn(Optional.empty());

    final TerminologyRelationException error =
        assertThrows(TerminologyRelationException.class, () -> pathling.valueSet(CVD_CANONICAL));

    assertEquals("Value set '" + CVD_CANONICAL + "' could not be resolved", error.getMessage());
  }

  @Test
  void valueSetWhoseMembershipCannotBeDeterminedFailsWithTheReason() {
    final ValueSetExpansionException cause = new ValueSetExpansionException("the server failed");
    when(terminologyService.expand(CVD_URL, "2026", DEFAULT_CAP)).thenThrow(cause);

    final TerminologyRelationException error =
        assertThrows(TerminologyRelationException.class, () -> pathling.valueSet(CVD_CANONICAL));

    assertEquals(
        "Value set '" + CVD_CANONICAL + "' could not be resolved: the server failed",
        error.getMessage());
    assertSame(cause, error.getCause());
  }

  @Test
  void valueSetOverTheCapFailsNamingTheCap() {
    when(terminologyService.expand(CVD_URL, "2026", 5))
        .thenThrow(new ExpansionLimitExceededException(5));

    final TerminologyRelationException error =
        assertThrows(TerminologyRelationException.class, () -> pathling.valueSet(CVD_CANONICAL, 5));

    assertTrue(error.getMessage().contains("more than the maximum of 5 members"));
  }

  @Test
  void valueSetRejectsACanonicalWithoutAUrl() {
    assertThrows(IllegalArgumentException.class, () -> pathling.valueSet("|2026"));
    assertThrows(IllegalArgumentException.class, () -> pathling.valueSet(" "));
    verify(terminologyService, never()).expand(anyUrl(), any(), anyInt());
  }

  @Test
  void valueSetRejectsANonPositiveCap() {
    assertThrows(IllegalArgumentException.class, () -> pathling.valueSet(CVD_CANONICAL, 0));
  }

  // ---------------------------------------------------------------------------------------------
  // Value set relation from a supplied resource.
  // ---------------------------------------------------------------------------------------------

  @Test
  void suppliedExpansionIsUsedAsIsWithAbstractEntriesExcluded() throws IOException {
    // The worked example: an abstract grouping entry contributes no row, its two nested members do,
    // and the terminology layer is not consulted.
    final List<Row> rows =
        pathling.valueSetFromJson(fixture("cardiovascular-disease.ValueSet.json")).collectAsList();

    assertEquals(
        List.of(
            RowFactory.create(SNOMED, SNOMED_VERSION, "22298006", "Myocardial infarction", null),
            RowFactory.create(ICD10, "2019", "I21", "Acute myocardial infarction", null)),
        rows);
    verify(terminologyService, never()).expand(any(ValueSet.class), anyInt());
  }

  @Test
  void suppliedIncompleteExpansionIsRejected() {
    // An expansion whose total exceeds the entries it carries does not determine membership.
    final String json =
        """
        {"resourceType": "ValueSet", "url": "http://example.org/ValueSet/partial",
         "expansion": {"timestamp": "2026-01-01T00:00:00Z", "total": 2,
           "contains": [{"system": "http://snomed.info/sct", "code": "22298006"}]}}
        """;

    final TerminologyRelationException error =
        assertThrows(TerminologyRelationException.class, () -> pathling.valueSetFromJson(json));

    assertTrue(
        error
            .getMessage()
            .startsWith("Supplied value set 'http://example.org/ValueSet/partial' could not"));
    assertTrue(error.getMessage().contains("incomplete"));
  }

  @Test
  void suppliedComposeIsExpandedByTheTerminologyLayerUnderTheCap() {
    final String json =
        """
        {"resourceType": "ValueSet", "url": "http://example.org/ValueSet/composed",
         "compose": {"include": [{"system": "http://snomed.info/sct",
           "concept": [{"code": "22298006"}]}]}}
        """;
    when(terminologyService.expand(any(ValueSet.class), eq(7)))
        .thenReturn(
            expansion(
                "http://example.org/ValueSet/composed",
                null,
                List.of(new ValueSetMember(SNOMED, null, "22298006", null, null))));

    final List<Row> rows = pathling.valueSetFromJson(json, 7).collectAsList();

    assertEquals(List.of(RowFactory.create(SNOMED, null, "22298006", null, null)), rows);
  }

  @Test
  void suppliedValueSetWithNeitherExpansionNorComposeIsRejected() {
    final String json =
        "{\"resourceType\": \"ValueSet\", \"url\": \"http://example.org/ValueSet/empty\"}";

    final TerminologyRelationException error =
        assertThrows(TerminologyRelationException.class, () -> pathling.valueSetFromJson(json));

    assertTrue(error.getMessage().contains("neither an expansion nor a compose"));
    verify(terminologyService, never()).expand(any(ValueSet.class), anyInt());
  }

  @Test
  void suppliedJsonThatIsNotAValueSetIsRejected() throws IOException {
    // A ConceptMap passed where a ValueSet is expected, and JSON that does not parse, both fail.
    final String conceptMap = fixture("sct-to-icd10.ConceptMap.json");

    assertThrows(TerminologyRelationException.class, () -> pathling.valueSetFromJson(conceptMap));
    assertThrows(TerminologyRelationException.class, () -> pathling.valueSetFromJson("{"));
  }

  // ---------------------------------------------------------------------------------------------
  // Concept map relation from a canonical reference.
  // ---------------------------------------------------------------------------------------------

  @Test
  void conceptMapHasTheNineSpecifiedColumnsWithSourceSystemAndCodeNonNullable() {
    stubConceptMap(MAP_URL, "2026", DEFAULT_CAP, List.of());

    final StructType schema = pathling.conceptMap(MAP_CANONICAL).schema();

    assertEquals(
        List.of(
            field("source_system", DataTypes.StringType, false),
            field("source_version", DataTypes.StringType, true),
            field("source_code", DataTypes.StringType, false),
            field("source_display", DataTypes.StringType, true),
            field("target_system", DataTypes.StringType, true),
            field("target_version", DataTypes.StringType, true),
            field("target_code", DataTypes.StringType, true),
            field("target_display", DataTypes.StringType, true),
            field("relationship", DataTypes.StringType, true)),
        Arrays.asList(schema.fields()));
  }

  @Test
  void conceptMapSplitsTheVersionAndReturnsOneRowPerMapping() {
    // A mapping and a no-mapping row both come through, the latter with a null target code,
    // display and relationship but its target system kept.
    stubConceptMap(
        MAP_URL,
        "2026",
        DEFAULT_CAP,
        List.of(
            new ConceptMapping(
                SNOMED, null, "22298006", "MI", ICD10, "2019", "I21", "AMI", "equivalent"),
            new ConceptMapping(SNOMED, null, "102499006", "Fit", ICD10, "2019", null, null, null)));

    final List<Row> rows = pathling.conceptMap(MAP_CANONICAL).collectAsList();

    assertEquals(
        List.of(
            RowFactory.create(
                SNOMED, null, "22298006", "MI", ICD10, "2019", "I21", "AMI", "equivalent"),
            RowFactory.create(SNOMED, null, "102499006", "Fit", ICD10, "2019", null, null, null)),
        rows);
  }

  @Test
  void conceptMapPassesAnExplicitMappingCap() {
    stubConceptMap(MAP_URL, null, 3, List.of());

    pathling.conceptMap(MAP_URL, 3);

    verify(terminologyService).readConceptMap(MAP_URL, null, 3);
  }

  @Test
  void unresolvableConceptMapFailsRatherThanReturningNoRows() {
    when(terminologyService.readConceptMap(MAP_URL, "2026", DEFAULT_CAP))
        .thenReturn(Optional.empty());

    final TerminologyRelationException error =
        assertThrows(TerminologyRelationException.class, () -> pathling.conceptMap(MAP_CANONICAL));

    assertEquals("Concept map '" + MAP_CANONICAL + "' could not be resolved", error.getMessage());
  }

  @Test
  void conceptMapFailuresOfTheTerminologyLayerCarryTheReason() {
    // Unrepresentable content, an undeterminable version and a failed lookup all fail the request.
    final List<RuntimeException> causes =
        List.of(
            new ConceptMapContentException("a target carries dependsOn"),
            new ConceptMapVersionException("two versions tie"),
            new ConceptMapLookupException("the server failed"));
    for (final RuntimeException cause : causes) {
      doThrow(cause).when(terminologyService).readConceptMap(MAP_URL, "2026", DEFAULT_CAP);

      final TerminologyRelationException error =
          assertThrows(
              TerminologyRelationException.class, () -> pathling.conceptMap(MAP_CANONICAL));

      assertEquals(
          "Concept map '" + MAP_CANONICAL + "' could not be resolved: " + cause.getMessage(),
          error.getMessage());
      assertSame(cause, error.getCause());
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Concept map relation from a supplied resource.
  // ---------------------------------------------------------------------------------------------

  @Test
  void suppliedConceptMapYieldsTheWorkedExampleRows() throws IOException {
    // The worked example: the group's target canonical is split at '|', the narrower R4
    // equivalence becomes source-is-broader-than-target, and the unmatched element is a no-mapping
    // row that keeps the target system and version.
    final List<Row> rows =
        pathling.conceptMapFromJson(fixture("sct-to-icd10.ConceptMap.json")).collectAsList();

    assertEquals(
        List.of(
            RowFactory.create(
                SNOMED,
                null,
                "22298006",
                "Myocardial infarction",
                ICD10,
                "2019",
                "I21",
                "Acute myocardial infarction",
                "equivalent"),
            RowFactory.create(
                SNOMED,
                null,
                "73211009",
                "Diabetes mellitus",
                ICD10,
                "2019",
                "E14",
                "Unspecified diabetes mellitus",
                "source-is-broader-than-target"),
            RowFactory.create(
                SNOMED, null, "102499006", "Fit and well", ICD10, "2019", null, null, null)),
        rows);
    verify(terminologyService, never()).readConceptMap(anyUrl(), any(), anyInt());
  }

  @Test
  void suppliedConceptMapWithDependsOnIsRejected() {
    // A dependent mapping cannot be evaluated by a flat relation, so the map is rejected outright.
    final String json =
        """
        {"resourceType": "ConceptMap", "url": "http://example.org/ConceptMap/dependent",
         "group": [{"source": "http://snomed.info/sct", "target": "http://hl7.org/fhir/sid/icd-10",
           "element": [{"code": "22298006", "target": [{"code": "I21",
             "equivalence": "equivalent",
             "dependsOn": [{"property": "http://example.org/age", "value": "adult"}]}]}]}]}
        """;

    final TerminologyRelationException error =
        assertThrows(TerminologyRelationException.class, () -> pathling.conceptMapFromJson(json));

    assertTrue(
        error
            .getMessage()
            .startsWith(
                "Supplied concept map 'http://example.org/ConceptMap/dependent' could not be"
                    + " resolved: "));
  }

  @Test
  void suppliedConceptMapOverTheCapIsRejected() throws IOException {
    final String json = fixture("sct-to-icd10.ConceptMap.json");

    final TerminologyRelationException error =
        assertThrows(
            TerminologyRelationException.class, () -> pathling.conceptMapFromJson(json, 2));

    assertTrue(error.getMessage().contains("more than the maximum of 2 mappings"));
  }

  @Test
  void suppliedJsonThatIsNotAConceptMapIsRejected() throws IOException {
    final String valueSet = fixture("cardiovascular-disease.ValueSet.json");

    assertThrows(TerminologyRelationException.class, () -> pathling.conceptMapFromJson(valueSet));
    assertThrows(
        TerminologyRelationException.class,
        () -> pathling.conceptMapFromJson("{\"resourceType\": \"ConceptMap\"}"));
  }

  // ---------------------------------------------------------------------------------------------
  // The relations in Spark SQL, using the worked examples of the specification.
  // ---------------------------------------------------------------------------------------------

  @Test
  void semiJoinToTheValueSetReturnsTheMemberConditions() throws IOException {
    registerWorkedExample();

    final List<Row> rows =
        spark
            .sql(
                """
                SELECT conditions.patient_id, conditions.code
                FROM conditions
                WHERE EXISTS (
                  SELECT 1
                  FROM cvd_codes
                  WHERE cvd_codes.system = conditions.system
                    AND cvd_codes.code = conditions.code
                )
                ORDER BY conditions.patient_id
                """)
            .collectAsList();

    assertEquals(
        List.of(RowFactory.create("p1", "22298006"), RowFactory.create("p2", "I21")), rows);
  }

  @Test
  void leftJoinToTheConceptMapTellsTheThreeCasesApart() throws IOException {
    // p1 and p3 are translated, p4 is in the map with no mapping, and p2's system is absent from
    // the map altogether.
    registerWorkedExample();

    final List<Row> rows =
        spark
            .sql(
                """
                SELECT conditions.patient_id,
                       conditions.code,
                       sct_to_icd10.source_code,
                       sct_to_icd10.target_code,
                       sct_to_icd10.relationship
                FROM conditions
                LEFT JOIN sct_to_icd10
                  ON sct_to_icd10.source_system = conditions.system
                 AND sct_to_icd10.source_code = conditions.code
                 AND (sct_to_icd10.relationship IS NULL
                      OR sct_to_icd10.relationship <> 'not-related-to')
                ORDER BY conditions.patient_id
                """)
            .collectAsList();

    assertEquals(
        List.of(
            RowFactory.create("p1", "22298006", "22298006", "I21", "equivalent"),
            RowFactory.create("p2", "I21", null, null, null),
            RowFactory.create("p3", "73211009", "73211009", "E14", "source-is-broader-than-target"),
            RowFactory.create("p4", "102499006", "102499006", null, null)),
        rows);
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------------------------

  private void registerWorkedExample() throws IOException {
    final StructType conditionsSchema =
        new StructType()
            .add("patient_id", DataTypes.StringType)
            .add("system", DataTypes.StringType)
            .add("version", DataTypes.StringType)
            .add("code", DataTypes.StringType);
    spark
        .createDataFrame(
            List.of(
                RowFactory.create("p1", SNOMED, null, "22298006"),
                RowFactory.create("p2", ICD10, null, "I21"),
                RowFactory.create("p3", SNOMED, null, "73211009"),
                RowFactory.create("p4", SNOMED, null, "102499006")),
            conditionsSchema)
        .createOrReplaceTempView("conditions");
    pathling
        .valueSetFromJson(fixture("cardiovascular-disease.ValueSet.json"))
        .createOrReplaceTempView("cvd_codes");
    pathling
        .conceptMapFromJson(fixture("sct-to-icd10.ConceptMap.json"))
        .createOrReplaceTempView("sct_to_icd10");
  }

  private void stubExpansion(
      @Nonnull final String url,
      final String version,
      final int cap,
      @Nonnull final List<ValueSetMember> members) {
    when(terminologyService.expand(eq(url), version == null ? isNull() : eq(version), eq(cap)))
        .thenReturn(Optional.of(expansion(url, version, members)));
  }

  private void stubConceptMap(
      @Nonnull final String url,
      final String version,
      final int cap,
      @Nonnull final List<ConceptMapping> mappings) {
    when(terminologyService.readConceptMap(
            eq(url), version == null ? isNull() : eq(version), eq(cap)))
        .thenReturn(Optional.of(ConceptMapContent.fromMappings(url, version, mappings, cap)));
  }

  @Nonnull
  private static ValueSetExpansion expansion(
      @Nonnull final String url,
      final String version,
      @Nonnull final List<ValueSetMember> members) {
    return new ValueSetExpansion(url, version, null, null, List.of(), members);
  }

  @Nonnull
  private static String anyUrl() {
    return any(String.class);
  }

  @Nonnull
  private static StructField field(
      @Nonnull final String name,
      @Nonnull final org.apache.spark.sql.types.DataType type,
      final boolean nullable) {
    return DataTypes.createStructField(name, type, nullable);
  }

  @Nonnull
  private static String fixture(@Nonnull final String name) throws IOException {
    return Files.readString(Path.of("src", "test", "resources", "terminology-relations", name));
  }
}
