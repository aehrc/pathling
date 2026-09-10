/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2026 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
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
package au.csiro.pathling.encoders;

import static au.csiro.pathling.encoders.utils.SchemaMisalignment.conformTo;
import static au.csiro.pathling.encoders.utils.SchemaMisalignment.elementStruct;
import static au.csiro.pathling.encoders.utils.SchemaMisalignment.mapElementStruct;
import static au.csiro.pathling.encoders.utils.SchemaMisalignment.migratedSchema;
import static au.csiro.pathling.encoders.utils.SchemaMisalignment.narrowEncoders;
import static au.csiro.pathling.encoders.utils.SchemaMisalignment.wideEncoders;
import static au.csiro.pathling.encoders.utils.SchemaMisalignment.withFieldOrder;
import static au.csiro.pathling.encoders.utils.SchemaMisalignment.withReversedFields;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ca.uhn.fhir.context.FhirVersionEnum;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.objects.MapObjects;
import org.apache.spark.sql.catalyst.expressions.objects.UnresolvedMapObjects;
import org.apache.spark.sql.catalyst.types.DataTypeUtils;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Tests that decoding resolves the fields of a struct nested inside a collection by name against
 * the schema of the data, rather than by position against the encoder's own schema.
 *
 * <p>The state under test is what a {@code mergeSchema} migration leaves behind: the fields the
 * table already held stay where they were, and the fields only the new encoder emits are appended
 * after them. The encoder that performed the migration then reads its own table at the wrong
 * offsets.
 *
 * <p>The opposite direction - data carrying fields the encoder has no place for - crashed the JVM
 * before the fix, so it is kept apart in {@link NarrowEncoderDecodingTest}.
 *
 * @author John Grimes
 */
class MisalignedSchemaDecodingTest {

  private static final String STRING_EXTENSION_URL = "http://example.org/string-extension";
  private static final String INTEGER_EXTENSION_URL = "http://example.org/integer-extension";
  private static final String IDENTIFIER_SYSTEM = "http://example.org/identifiers";
  private static final String IDENTIFIER_TYPE_SYSTEM = "http://example.org/identifier-types";

  private static SparkSession spark;

  /** Set up Spark. */
  @BeforeAll
  static void setUp() {
    spark =
        AnsiTestSupport.configureAnsiMode(
            SparkSession.builder()
                .master("local[*]")
                .appName("testing")
                .config("spark.driver.bindAddress", "localhost")
                .config("spark.driver.host", "localhost")
                .config("spark.ui.enabled", "false")
                .getOrCreate());
  }

  /** Tear down Spark. */
  @AfterAll
  static void tearDown() {
    spark.stop();
  }

  // Migrated field order.

  /**
   * A dataset whose extension struct carries the encoder's fields with two of them appended after
   * the trailing internal field, rather than in the encoder's order, decodes to the values that
   * were written (US1.1, US1.2, SC-001).
   *
   * <p>Before the fix this raised {@code AssertionError: sizeInBytes (17) should be a multiple of
   * 8} from {@code UnsafeRow.pointTo}, surfaced as an {@code INTERNAL_ERROR} SparkException. The
   * size in the message is a function of the row's contents, so it varies with the fixture; the
   * assertion is the same one recorded against #2698.
   */
  @Test
  void migratedFieldOrderDecodesToTheValuesWritten() {
    // Arrange: encode a Patient carrying extensions with the narrow encoder, then present it under
    // the schema a mergeSchema migration by the wide encoder produces.
    final Dataset<Row> narrowlyEncoded = encode(narrowEncoders(), patientWithNestedCollections());
    final Dataset<Row> migrated =
        conformTo(
            narrowlyEncoded, migratedSchema(narrowlyEncoded.schema(), schemaOf(wideEncoders())));

    // Act: decode with the wide encoder, which is the encoder that performed the migration.
    final Patient decoded = decode(migrated, wideEncoders());

    // Assert: the extensions hold the values that were written.
    assertExtensionsSurvived(decoded);
  }

  // Pure reordering, with no field added or removed.

  /**
   * A struct nested in an array within a map value - which is the shape of the extension container
   * - decodes correctly when its fields are purely reordered, with none added and none removed
   * (FR-004, FR-005, and the reordering and nesting edge cases).
   */
  @Test
  void reorderedExtensionStructDecodesCorrectly() {
    // Arrange: reverse the field order of the extension element struct alone, leaving every other
    // struct in the schema as the encoder emits it.
    final Dataset<Row> encoded = encode(wideEncoders(), patientWithNestedCollections());
    final StructType reordered =
        mapElementStruct(
            encoded.schema(),
            ExtensionSupport.EXTENSIONS_FIELD_NAME(),
            struct -> withFieldOrder(struct, reversedNames(struct)));
    // Guard: the reordering must actually have changed the order, or the test proves nothing.
    assertFalse(
        Arrays.equals(
            elementStruct(encoded.schema(), ExtensionSupport.EXTENSIONS_FIELD_NAME()).fieldNames(),
            elementStruct(reordered, ExtensionSupport.EXTENSIONS_FIELD_NAME()).fieldNames()));

    // Act.
    final Patient decoded = decode(conformTo(encoded, reordered), wideEncoders());

    // Assert.
    assertExtensionsSurvived(decoded);
  }

  /**
   * A struct nested in an array - an ordinary repeating element - decodes correctly when its fields
   * are purely reordered (FR-004, FR-005).
   */
  @Test
  void reorderedRepeatingElementStructDecodesCorrectly() {
    // Arrange: reverse the field order of the Identifier element struct alone.
    final Dataset<Row> encoded = encode(wideEncoders(), patientWithNestedCollections());
    final StructType reordered =
        mapElementStruct(
            encoded.schema(),
            "identifier",
            struct -> withFieldOrder(struct, reversedNames(struct)));

    // Act.
    final Patient decoded = decode(conformTo(encoded, reordered), wideEncoders());

    // Assert: both identifiers, and the CodeableConcept nested inside the first one, survive.
    assertIdentifiersSurvived(decoded);
  }

  /**
   * Reversing the field order of every struct at every level of nesting - inside arrays, inside map
   * values, and inside arrays within map values, all at once - does not affect any decoded value
   * (FR-004, FR-005).
   */
  @Test
  void reorderingEveryStructAtEveryDepthDecodesCorrectly() {
    // Arrange.
    final Dataset<Row> encoded = encode(wideEncoders(), patientWithNestedCollections());
    final StructType reordered = withReversedFields(encoded.schema());

    // Act.
    final Patient decoded = decode(conformTo(encoded, reordered), wideEncoders());

    // Assert.
    assertExtensionsSurvived(decoded);
    assertIdentifiersSurvived(decoded);
    assertScalarsSurvived(decoded);
  }

  /**
   * A collection of primitives, where field order cannot differ because there are no fields, must
   * not regress when the struct that contains it is reordered (the primitive collection edge case).
   */
  @Test
  void primitiveCollectionSurvivesReordering() {
    // Arrange: HumanName.given is an array of strings, held inside the HumanName element struct,
    // which the reversal reorders.
    final Dataset<Row> encoded = encode(wideEncoders(), patientWithNestedCollections());
    final StructType reordered = withReversedFields(encoded.schema());

    // Act.
    final Patient decoded = decode(conformTo(encoded, reordered), wideEncoders());

    // Assert: every given name survives, in the order written.
    final HumanName name = decoded.getName().getFirst();
    assertEquals("Wilson", name.getFamily());
    assertEquals(
        List.of("Marie", "Jane"), name.getGiven().stream().map(StringType::getValue).toList());
  }

  /**
   * A resource with no extensions and no repeating elements takes the same decode path it does
   * today, whether its schema is aligned or reordered (the no-collections edge case).
   */
  @Test
  void resourceWithNoCollectionsDecodesUnchanged() {
    // Arrange: a Patient carrying only scalar elements.
    final Patient scalarOnly = new Patient();
    scalarOnly.setId("scalar-only");
    scalarOnly.setGender(AdministrativeGender.FEMALE);
    scalarOnly.setActive(true);
    final Dataset<Row> encoded = encode(wideEncoders(), scalarOnly);

    // Act: decode the aligned dataset, and the same dataset with every struct reordered.
    final Patient aligned = decode(encoded, wideEncoders());
    final Patient reordered =
        decode(conformTo(encoded, withReversedFields(encoded.schema())), wideEncoders());

    // Assert: both agree, and both carry what was written.
    assertEquals(AdministrativeGender.FEMALE, aligned.getGender());
    assertEquals(AdministrativeGender.FEMALE, reordered.getGender());
    assertTrue(aligned.getActive());
    assertTrue(reordered.getActive());
    assertTrue(aligned.getExtension().isEmpty());
    assertTrue(reordered.getExtension().isEmpty());
  }

  /**
   * A dataset whose field order matches the encoder exactly decodes to the same result as it does
   * today (US1.3, FR-007).
   */
  @Test
  void alignedSchemaDecodesUnchanged() {
    // Arrange: the same dataset decoded directly, and after a projection onto its own schema.
    final Dataset<Row> encoded = encode(wideEncoders(), patientWithNestedCollections());

    // Act.
    final Patient direct = decode(encoded, wideEncoders());
    final Patient viaProjection = decode(conformTo(encoded, encoded.schema()), wideEncoders());

    // Assert: identical, field for field, as serialised by the FHIR parser.
    assertEquals(asJson(direct), asJson(viaProjection));
    assertExtensionsSurvived(direct);
    assertIdentifiersSurvived(direct);
  }

  /**
   * A stored field whose name differs from the encoder's only in case is resolved consistently with
   * Spark's configured resolver, which is case-insensitive by default (the case-difference edge
   * case).
   */
  @Test
  void caseDifferenceInFieldNameIsResolvedByTheSparkResolver() {
    // Arrange: store the extension's string value field under an upper-cased name.
    final Dataset<Row> encoded = encode(wideEncoders(), patientWithNestedCollections());
    final StructType upperCased =
        mapElementStruct(
            encoded.schema(),
            ExtensionSupport.EXTENSIONS_FIELD_NAME(),
            struct -> renameField(struct, "valueString", "VALUESTRING"));

    // Act.
    final Patient decoded = decode(conformTo(encoded, upperCased), wideEncoders());

    // Assert: the value is found under the differently-cased name.
    assertEquals(
        "the written value",
        ((StringType) decoded.getExtensionByUrl(STRING_EXTENSION_URL).getValue()).getValue());
  }

  // The resolved decode plan.

  /**
   * An aligned schema resolves to a decode plan of the same shape as a misaligned one, so decoding
   * an aligned dataset adds no per-row work (SC-005, FR-007).
   *
   * <p>The resolution that replaced the encoder-derived element type happens once per query plan,
   * during {@code resolveAndBind}. This is the check that it leaves nothing behind to be done per
   * row: the aligned plan retains no unresolved node, and it has exactly the same node-by-node
   * makeup as the plan for a migrated schema, which differs from it only in the ordinals the field
   * accesses read. Nothing is conformed, cast or rebuilt on the way through.
   */
  @Test
  void alignedSchemaResolvesToTheSameDecodePlanAsAMigratedOne() {
    // Arrange.
    final StructType alignedSchema = schemaOf(wideEncoders());
    final StructType narrowSchema = schemaOf(narrowEncoders());
    final StructType migratedSchema = migratedSchema(narrowSchema, alignedSchema);

    // Act: resolve the same encoder against its own schema and against the migrated one.
    final Expression alignedPlan = resolvedDeserializer(wideEncoders(), alignedSchema);
    final Expression migratedPlan = resolvedDeserializer(wideEncoders(), migratedSchema);

    // Assert: the aligned plan is fully resolved, with no element type left to derive per row.
    final List<Expression> alignedNodes = nodes(alignedPlan);
    assertTrue(
        alignedNodes.stream().noneMatch(node -> node instanceof UnresolvedMapObjects),
        "the aligned plan should carry no unresolved collection mapping");
    assertTrue(
        alignedNodes.stream().noneMatch(node -> node instanceof UnresolvedExtractValue),
        "the aligned plan should carry no unresolved field access");

    // Assert: the two plans are made of exactly the same nodes, so the misalignment is absorbed by
    // the ordinals the plan reads rather than by extra work.
    assertEquals(nodeHistogram(alignedPlan), nodeHistogram(migratedPlan));

    // Assert: the collection mappings in the aligned plan take their element type from the data,
    // which for an aligned schema is the encoder's own type - the type the plan used to be built
    // with directly. The count guards against the assertion being vacuous.
    final List<MapObjects> mappings =
        alignedNodes.stream()
            .filter(MapObjects.class::isInstance)
            .map(MapObjects.class::cast)
            .toList();
    assertFalse(mappings.isEmpty(), "the plan should map over collections");
    for (final MapObjects mapping : mappings) {
      if (mapping.inputData().dataType() instanceof final ArrayType arrayType) {
        assertEquals(arrayType.elementType(), mapping.loopVar().dataType());
      }
    }
  }

  // Fixtures and helpers.

  /**
   * Returns a Patient carrying extensions, repeating elements with structs nested inside them, a
   * collection of primitives, and scalar elements.
   */
  private static Patient patientWithNestedCollections() {
    final Patient patient = new Patient();
    patient.setId("misaligned-patient");
    patient.setGender(AdministrativeGender.FEMALE);
    patient.setActive(true);

    final Identifier withType = new Identifier();
    withType.setSystem(IDENTIFIER_SYSTEM);
    withType.setValue("first-identifier");
    withType.setType(
        new CodeableConcept()
            .addCoding(new Coding(IDENTIFIER_TYPE_SYSTEM, "MR", "Medical record number")));
    patient.addIdentifier(withType);
    patient.addIdentifier(new Identifier().setSystem(IDENTIFIER_SYSTEM).setValue("second"));

    patient.addName(new HumanName().setFamily("Wilson").addGiven("Marie").addGiven("Jane"));

    patient.addExtension(new Extension(STRING_EXTENSION_URL, new StringType("the written value")));
    patient.addExtension(new Extension(INTEGER_EXTENSION_URL, new IntegerType(42)));
    return patient;
  }

  private static void assertExtensionsSurvived(final Patient decoded) {
    assertEquals(
        "the written value",
        ((StringType) decoded.getExtensionByUrl(STRING_EXTENSION_URL).getValue()).getValue());
    assertEquals(
        42, ((IntegerType) decoded.getExtensionByUrl(INTEGER_EXTENSION_URL).getValue()).getValue());
  }

  private static void assertIdentifiersSurvived(final Patient decoded) {
    assertEquals(2, decoded.getIdentifier().size());
    final Identifier first = decoded.getIdentifier().getFirst();
    assertEquals(IDENTIFIER_SYSTEM, first.getSystem());
    assertEquals("first-identifier", first.getValue());
    final Coding coding = first.getType().getCoding().getFirst();
    assertEquals(IDENTIFIER_TYPE_SYSTEM, coding.getSystem());
    assertEquals("MR", coding.getCode());
    assertEquals("Medical record number", coding.getDisplay());
    assertEquals("second", decoded.getIdentifier().get(1).getValue());
  }

  private static void assertScalarsSurvived(final Patient decoded) {
    assertEquals(AdministrativeGender.FEMALE, decoded.getGender());
    assertTrue(decoded.getActive());
  }

  private static Dataset<Row> encode(final FhirEncoders encoders, final Patient patient) {
    return spark.createDataset(List.of(patient), encoders.of(Patient.class)).toDF();
  }

  private static Patient decode(final Dataset<Row> dataset, final FhirEncoders encoders) {
    return dataset.as(encoders.of(Patient.class)).head();
  }

  private static StructType schemaOf(final FhirEncoders encoders) {
    return encoders.of(Patient.class).schema();
  }

  private static String asJson(final Patient patient) {
    return FhirEncoders.contextFor(FhirVersionEnum.R4)
        .newJsonParser()
        .encodeResourceToString(patient);
  }

  private static List<String> reversedNames(final StructType struct) {
    final List<String> names = new ArrayList<>(Arrays.asList(struct.fieldNames()));
    Collections.reverse(names);
    return names;
  }

  private static StructType renameField(
      final StructType struct, final String from, final String to) {
    return new StructType(
        Arrays.stream(struct.fields())
            .map(
                field ->
                    field.name().equals(from)
                        ? new StructField(to, field.dataType(), field.nullable(), field.metadata())
                        : field)
            .toArray(StructField[]::new));
  }

  /** Resolves the encoder's deserializer against the given data schema, as {@code as()} does. */
  @SuppressWarnings("unchecked")
  private static Expression resolvedDeserializer(
      final FhirEncoders encoders, final StructType dataSchema) {
    final ExpressionEncoder<Patient> encoder = encoders.of(Patient.class);
    // Seq is covariant in Scala but invariant to Java's generics, so the element type of the
    // attribute list has to be widened by hand.
    final Seq<Attribute> attributes =
        (Seq<Attribute>) (Seq<?>) DataTypeUtils.toAttributes(dataSchema);
    return encoder.resolveAndBind(attributes, SimpleAnalyzer$.MODULE$).objDeserializer();
  }

  /** Returns every node of the expression tree, parents before children. */
  private static List<Expression> nodes(final Expression root) {
    final List<Expression> collected = new ArrayList<>();
    final Deque<Expression> pending = new ArrayDeque<>();
    pending.push(root);
    while (!pending.isEmpty()) {
      final Expression current = pending.pop();
      collected.add(current);
      CollectionConverters.asJava(current.children()).forEach(pending::push);
    }
    return collected;
  }

  /** Returns how many nodes of each expression class the tree holds. */
  private static Map<String, Integer> nodeHistogram(final Expression root) {
    final Map<String, Integer> histogram = new HashMap<>();
    for (final Expression node : nodes(root)) {
      histogram.merge(node.getClass().getName(), 1, Integer::sum);
    }
    return histogram;
  }
}
