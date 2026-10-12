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

package au.csiro.pathling.library.io.source;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.FhirReader;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests the layout detector on schemas alone (US7, FR-037 to FR-039).
 *
 * <p>The full schemas come from the two real writers, the previous encoder and the Parquet on FHIR
 * transform, so that a marker the detector relies on cannot drift away from what is actually
 * stored. The sparse, mixed and nested cases are built by hand, because no writer produces them on
 * request.
 *
 * <p>Reading through a source and writing through a sink are the wiring step's tests, not these.
 *
 * @author Piotr Szul
 */
class LayoutDetectorTest {

  private static SparkSession spark;
  private static PathlingContext context;

  /** Set up Spark and the Pathling context, which the new-layout transform needs. */
  @BeforeAll
  static void setUp() {
    spark = TestHelpers.spark();
    context = PathlingContext.create(spark);
  }

  /** Stop Spark. */
  @AfterAll
  static void tearDown() {
    spark.stop();
  }

  // T039: the four schemas.

  /** A full previous-layout schema is classified as the previous layout, citing its markers. */
  @Test
  void fullPreviousLayoutSchemaIsThePreviousLayout() {
    final StructType schema = previousLayout("Observation", true);

    final LayoutDetection detection = LayoutDetector.detect(schema);

    assertEquals(StorageLayout.PREVIOUS, detection.getLayout());
    assertFalse(detection.isSupported());
    final List<String> markers = detection.getPreviousLayoutMarkers();
    assertTrue(markers.contains("_fid"), markers::toString);
    assertTrue(markers.contains("_extension"), markers::toString);
    assertTrue(markers.contains("id_versioned"), markers::toString);
    assertTrue(markers.contains("valueQuantity._fid"), markers::toString);
    assertTrue(markers.contains("valueQuantity.value_scale"), markers::toString);
    assertTrue(markers.contains("valueQuantity._value_canonicalized"), markers::toString);
    assertTrue(markers.contains("valueQuantity._code_canonicalized"), markers::toString);
    assertEquals(List.of(), detection.getNewLayoutMarkers());
  }

  /**
   * A previous-layout schema encoded with extensions disabled carries neither {@code _fid} nor
   * {@code _extension}, and a resource without decimals or quantities carries neither of their
   * companions, so the versioned id is all that is left to identify it. It must still be enough.
   */
  @Test
  void previousLayoutWithoutExtensionsIsIdentifiedByTheVersionedId() {
    final StructType schema = previousLayout("Patient", false);

    final LayoutDetection detection = LayoutDetector.detect(schema);

    assertEquals(StorageLayout.PREVIOUS, detection.getLayout());
    assertEquals(List.of("id_versioned"), detection.getPreviousLayoutMarkers());
  }

  /** A sparse previous-layout schema is still the previous layout, on its root versioned id. */
  @Test
  void sparsePreviousLayoutSchemaIsThePreviousLayout() {
    final StructType versionedId = struct(string("id"), string("id_versioned"), string("gender"));

    final LayoutDetection detection = LayoutDetector.detect(versionedId);

    assertEquals(StorageLayout.PREVIOUS, detection.getLayout());
    assertEquals(List.of("id_versioned"), detection.getPreviousLayoutMarkers());
  }

  /**
   * Previous-layout evidence without the root versioned id does not classify a schema (decision
   * 100): it is cited, but the schema is unclassifiable, as it would be without the evidence, and
   * is accepted.
   */
  @Test
  void previousLayoutEvidenceWithoutTheVersionedIdIsNotDecisive() {
    final StructType nestedFid =
        struct(string("id"), field("name", array(struct(string("family"), integer("_fid")))));
    final StructType scale =
        struct(string("id"), field("value", new DecimalType(32, 6)), integer("value_scale"));
    final StructType nestedVersionedId =
        struct(
            string("id"), field("contained", array(struct(string("id"), string("id_versioned")))));

    for (final StructType schema : List.of(nestedFid, scale, nestedVersionedId)) {
      final LayoutDetection detection = LayoutDetector.detect(schema);
      assertEquals(StorageLayout.UNCLASSIFIABLE, detection.getLayout(), schema::treeString);
      assertDoesNotThrow(() -> LayoutDetector.check("Patient", schema));
    }
    assertEquals(List.of("name._fid"), previousMarkers(nestedFid));
    assertEquals(List.of("value_scale"), previousMarkers(scale));
    assertEquals(List.of("contained.id_versioned"), previousMarkers(nestedVersionedId));
  }

  /**
   * A full schema written by the new layout is classified as the new layout and carries no
   * previous-layout marker. It includes a decimal and a quantity, which are where the previous
   * layout's companions lived, so that a false positive there would show.
   */
  @Test
  void fullNewLayoutSchemaIsTheNewLayout() {
    final StructType observation =
        newLayout(
            "Observation",
            """
            {"resourceType":"Observation","id":"o1","status":"final",
             "code":{"coding":[{"system":"http://loinc.org","code":"1234-5"}]},
             "valueQuantity":{"value":1.50,"unit":"mg","system":"http://unitsofmeasure.org",
                              "code":"mg"},
             "extension":[{"url":"http://example.org/e","valueDecimal":2.5}]}\
            """);
    final StructType patient =
        newLayout(
            "Patient",
            """
            {"resourceType":"Patient","id":"p1","gender":"male",
             "name":[{"family":"Alpha","given":["Beta"]}],
             "contact":[{"name":{"family":"Gamma"}}]}\
            """);

    for (final StructType schema : List.of(observation, patient)) {
      final LayoutDetection detection = LayoutDetector.detect(schema);
      assertEquals(StorageLayout.PARQUET_ON_FHIR, detection.getLayout(), schema::treeString);
      assertTrue(detection.isSupported());
      assertEquals(List.of(), detection.getPreviousLayoutMarkers());
      assertTrue(detection.getNewLayoutMarkers().contains("resourceType"));
    }
  }

  /** A sparse new-layout schema is the new layout on its resource type alone. */
  @Test
  void sparseNewLayoutSchemaIsTheNewLayout() {
    final LayoutDetection detection =
        LayoutDetector.detect(struct(string("resourceType"), string("id")));

    assertEquals(StorageLayout.PARQUET_ON_FHIR, detection.getLayout());
    assertEquals(List.of("resourceType"), detection.getNewLayoutMarkers());
  }

  /**
   * A schema carrying no marker of either layout is unclassifiable, and accepted, because marker
   * absence is not evidence of an unsupported layout (FR-039).
   */
  @Test
  void schemaWithNoMarkersIsUnclassifiableAndAccepted() {
    final StructType schema =
        struct(
            string("id"),
            string("gender"),
            field("name", array(struct(string("family"), array(DataTypes.StringType, "given")))));

    final LayoutDetection detection = LayoutDetector.detect(schema);

    assertEquals(StorageLayout.UNCLASSIFIABLE, detection.getLayout());
    assertTrue(detection.isSupported());
    assertEquals(List.of(), detection.getPreviousLayoutMarkers());
    assertEquals(List.of(), detection.getNewLayoutMarkers());
    assertDoesNotThrow(() -> LayoutDetector.check("Patient", schema));
  }

  /**
   * Fields that resemble a previous-layout marker without being one are not taken as one.
   * New-layout metadata groups and annotations begin with an underscore too, and a decimal can
   * appear without a scale companion, so neither a leading underscore nor a decimal type is
   * evidence on its own.
   */
  @Test
  void lookalikesAreNotPreviousLayoutMarkers() {
    final StructType schema =
        struct(
            string("resourceType"),
            string("id"),
            field("_id", struct(string("id"))),
            field("value", new DecimalType(32, 6)),
            string("count_scale"),
            string("_fid_note"),
            field("__value_numeric", new DecimalType(38, 18)),
            field(
                "name",
                array(
                    struct(
                        array(DataTypes.StringType, "given"),
                        field("_given", array(struct(string("id"))))))));

    final LayoutDetection detection = LayoutDetector.detect(schema);

    assertEquals(List.of(), detection.getPreviousLayoutMarkers());
    assertEquals(StorageLayout.PARQUET_ON_FHIR, detection.getLayout());
    assertEquals(
        List.of("resourceType", "_id", "__value_numeric", "name._given"),
        detection.getNewLayoutMarkers());
  }

  /** A field of a marker's name but not its type is not a marker. */
  @Test
  void markersMustHaveTheirTypeAsWellAsTheirName() {
    final StructType schema =
        struct(
            string("id"),
            string("_fid"),
            string("_extension"),
            integer("id_versioned"),
            string("value"),
            integer("value_scale"));

    assertEquals(List.of(), previousMarkers(schema));
  }

  // T039a: the mixed schema.

  /**
   * A table carrying markers of both layouts, as an append of new-layout data into a
   * previous-layout table produces, is rejected as the previous layout, with the evidence of both
   * layouts kept. Decision 47 assumes the two layouts never meet inside one query, so the root
   * versioned id the earlier table keeps is decisive whatever else the table carries.
   */
  @Test
  void mixedSchemaIsRejectedWithTheEvidenceOfBothLayouts() {
    final StructType previous = previousLayout("Patient", true);
    final StructType mixed =
        previous.add(
            StructField.apply("resourceType", DataTypes.StringType, true, Metadata.empty()));

    final LayoutDetection detection = LayoutDetector.detect(mixed);

    assertEquals(StorageLayout.PREVIOUS, detection.getLayout());
    assertFalse(detection.isSupported());
    assertTrue(detection.isMixed());
    assertTrue(detection.getPreviousLayoutMarkers().contains("_fid"));
    assertEquals(List.of("resourceType"), detection.getNewLayoutMarkers());
  }

  /**
   * A new-layout table carrying a nested {@code _fid}, as a coding a terminology function returned
   * does, is the new layout and accepted, with the field still cited as evidence (decision 100).
   */
  @Test
  void newLayoutSchemaWithOneNestedPreviousLayoutFieldIsAccepted() {
    final StructType schema =
        struct(
            string("resourceType"),
            string("id"),
            field("name", array(struct(string("family"), integer("_fid")))));

    final LayoutDetection detection = LayoutDetector.detect(schema);

    assertEquals(StorageLayout.PARQUET_ON_FHIR, detection.getLayout());
    assertFalse(detection.isMixed());
    assertEquals(List.of("name._fid"), detection.getPreviousLayoutMarkers());
    assertEquals(List.of("resourceType"), detection.getNewLayoutMarkers());
    assertDoesNotThrow(() -> LayoutDetector.check("Patient", schema));
  }

  // T040: the nested walk.

  /** Detection reaches markers in structures, arrays of structures and deeper nestings. */
  @Test
  void detectionWalksNestedStructuresAndArrays() {
    final StructType quantity =
        struct(
            field("value", new DecimalType(32, 6)),
            integer("value_scale"),
            field("_value_canonicalized", struct(string("value"))),
            string("_code_canonicalized"));
    final StructType schema =
        struct(
            string("id"),
            field("meta", struct(string("versionId"), integer("_fid"))),
            field(
                "contact", array(struct(field("name", struct(string("family"), integer("_fid")))))),
            field("component", array(struct(field("valueQuantity", quantity)))),
            field("grid", DataTypes.createArrayType(array(struct(integer("_fid"))))),
            field(
                "repeating",
                struct(
                    field("values", array(new DecimalType(32, 6))),
                    array(DataTypes.IntegerType, "values_scale"))));

    assertEquals(
        List.of(
            "meta._fid",
            "contact.name._fid",
            "component.valueQuantity.value_scale",
            "component.valueQuantity._value_canonicalized",
            "component.valueQuantity._code_canonicalized",
            "grid._fid",
            "repeating.values_scale"),
        previousMarkers(schema));
  }

  /** Detection reaches into the values of a map, which is where the extension table keeps them. */
  @Test
  void detectionWalksMapValues() {
    final StructType schema =
        struct(
            string("id"),
            field(
                "lookup",
                DataTypes.createMapType(
                    DataTypes.StringType, array(struct(string("url"), integer("_fid"))))));

    assertEquals(List.of("lookup._fid"), previousMarkers(schema));
  }

  /** New-layout markers are found at depth too. */
  @Test
  void newLayoutMarkersAreFoundAtDepth() {
    final StructType schema =
        struct(
            string("id"),
            field(
                "contact",
                array(
                    struct(
                        field(
                            "name",
                            struct(
                                array(DataTypes.StringType, "given"),
                                field("_given", array(struct(string("id")))),
                                string("__family_note")))))));

    final LayoutDetection detection = LayoutDetector.detect(schema);

    assertEquals(StorageLayout.PARQUET_ON_FHIR, detection.getLayout());
    assertEquals(
        List.of("contact.name._given", "contact.name.__family_note"),
        detection.getNewLayoutMarkers());
  }

  // The actionable message, at the detector (T042's content; T042 asserts it at the source).

  /**
   * A previous-layout schema is refused with a message naming the resource type, the detected
   * layout, the expected layout, the evidence and the remedy, which is to re-import from source.
   */
  @Test
  void previousLayoutIsRefusedWithAnActionableMessage() {
    final StructType schema =
        struct(
            string("id"),
            string("id_versioned"),
            field("name", array(struct(string("family"), integer("_fid")))));

    final UnsupportedLayoutException error =
        assertThrows(
            UnsupportedLayoutException.class, () -> LayoutDetector.check("Patient", schema));

    assertEquals(
        "The Patient data is stored in the layout written by earlier releases of Pathling, "
            + "not the Parquet on FHIR layout this release expects. "
            + "Fields identifying the earlier layout: id_versioned, name._fid. "
            + "Re-import the data from its source with this release; "
            + "data in the earlier layout cannot be converted in place.",
        error.getMessage());
    assertEquals("Patient", error.getResourceType());
    assertSame(StorageLayout.PREVIOUS, error.getDetection().getLayout());
  }

  /** A mixed schema's message says the data mixes both layouts, citing each layout's fields. */
  @Test
  void mixedLayoutIsRefusedWithAMessageCitingBothLayouts() {
    final StructType schema = struct(string("resourceType"), string("id"), string("id_versioned"));

    final UnsupportedLayoutException error =
        assertThrows(
            UnsupportedLayoutException.class, () -> LayoutDetector.check("Condition", schema));

    assertEquals(
        "The Condition data is stored in the layout written by earlier releases of Pathling, "
            + "not the Parquet on FHIR layout this release expects. "
            + "Fields identifying the earlier layout: id_versioned. "
            + "It also carries fields of the Parquet on FHIR layout (resourceType), "
            + "so it mixes the two layouts, as appending new data to earlier data does. "
            + "Re-import the data from its source into a new location with this release, "
            + "because appending to or merging into this data is refused, and data in the "
            + "earlier layout cannot be converted in place. Where the data is a directory of "
            + "files, the directory holds files of the earlier layout: remove them, or "
            + "re-import their data from its source.",
        error.getMessage());
  }

  /** A long list of evidence is cut short, so that the message stays readable. */
  @Test
  void longEvidenceIsAbbreviated() {
    final StructType schema = previousLayout("Patient", true);
    final List<String> markers = LayoutDetector.detect(schema).getPreviousLayoutMarkers();
    assertTrue(markers.size() > LayoutDetector.MAX_CITED_MARKERS, markers::toString);

    final UnsupportedLayoutException error =
        assertThrows(
            UnsupportedLayoutException.class, () -> LayoutDetector.check("Patient", schema));

    final String cited =
        String.join(", ", markers.subList(0, LayoutDetector.MAX_CITED_MARKERS))
            + " and "
            + (markers.size() - LayoutDetector.MAX_CITED_MARKERS)
            + " more";
    assertTrue(
        error.getMessage().contains("Fields identifying the earlier layout: " + cited + ". "),
        error.getMessage());
  }

  /** Accepted layouts pass the check. */
  @Test
  void acceptedLayoutsPassTheCheck() {
    assertDoesNotThrow(
        () -> LayoutDetector.check("Patient", struct(string("resourceType"), string("id"))));
    assertDoesNotThrow(() -> LayoutDetector.check("Patient", struct(string("id"))));
    assertDoesNotThrow(() -> LayoutDetector.check("Patient", new StructType()));
  }

  // Helpers.

  @Nonnull
  private static List<String> previousMarkers(@Nonnull final StructType schema) {
    return LayoutDetector.detect(schema).getPreviousLayoutMarkers();
  }

  @Nonnull
  private static StructType previousLayout(
      @Nonnull final String resourceType, final boolean extensions) {
    return FhirEncoders.forR4()
        .withExtensionsEnabled(extensions)
        .getOrCreate()
        .of(resourceType)
        .schema();
  }

  @Nonnull
  private static StructType newLayout(
      @Nonnull final String resourceType, @Nonnull final String json) {
    return FhirReader.of(spark, FhirDefinitionContext.of(context.getFhirContext()))
        .json()
        .read(resourceType, spark.createDataset(List.of(json), Encoders.STRING()))
        .schema();
  }

  @Nonnull
  private static StructType struct(@Nonnull final StructField... fields) {
    return new StructType(fields);
  }

  @Nonnull
  private static StructField field(@Nonnull final String name, @Nonnull final DataType type) {
    return StructField.apply(name, type, true, Metadata.empty());
  }

  @Nonnull
  private static StructField string(@Nonnull final String name) {
    return field(name, DataTypes.StringType);
  }

  @Nonnull
  private static StructField integer(@Nonnull final String name) {
    return field(name, DataTypes.IntegerType);
  }

  @Nonnull
  private static StructField array(
      @Nonnull final DataType elementType, @Nonnull final String name) {
    return field(name, DataTypes.createArrayType(elementType));
  }

  @Nonnull
  private static ArrayType array(@Nonnull final DataType elementType) {
    return DataTypes.createArrayType(elementType);
  }
}
