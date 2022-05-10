/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2022, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 *
 */

package au.csiro.pathling.encoders;

import static au.csiro.pathling.encoders.AllResourcesEncodingTest.EXCLUDED_RESOURCES;
import static au.csiro.pathling.encoders.SchemaConverterTest.OPEN_TYPES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import au.csiro.pathling.encoders.terminology.ucum.Ucum;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.hl7.fhir.r4.model.BaseResource;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.MolecularSequence;
import org.hl7.fhir.r4.model.MolecularSequence.MolecularSequenceQualityRocComponent;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.PlanDefinition;
import org.hl7.fhir.r4.model.PlanDefinition.PlanDefinitionActionComponent;
import org.json4s.jackson.JsonMethods;
import org.junit.Test;
import scala.collection.mutable.WrappedArray;

public class LightweightFhirEncodersTest implements JsonMethods {

  private static final FhirContext fhirContext = FhirContext.forR4();
  private static final FhirEncoders fhirEncoders = FhirEncoders.forR4()
      .withOpenTypes(OPEN_TYPES)
      .withExtensionsEnabled(true)
      .getOrCreate();
  private static final IParser jsonParser = fhirContext.newJsonParser().setPrettyPrint(true);

  public static <T extends BaseResource> void assertSerDeIsIdentity(
      final ExpressionEncoder<T> encoder, final T obj) {
    final ExpressionEncoder<T> resolvedEncoder = EncoderUtils
        .defaultResolveAndBind(encoder);
    final InternalRow serializedRow = resolvedEncoder.createSerializer()
        .apply(obj);
    final T deserializedObj = resolvedEncoder.createDeserializer()
        .apply(serializedRow);
    assertResourceEquals(obj, deserializedObj);
  }

  public static void assertResourceEquals(final BaseResource expected, final BaseResource actual) {
    assertEquals(jsonParser.encodeResourceToString(expected),
        jsonParser.encodeResourceToString(actual));
  }

  public void assertStringExtension(final String expectedUrl, final String expectedValue,
      final Row actualExtensionRow) {
    assertEquals(expectedUrl, actualExtensionRow.getString(actualExtensionRow.fieldIndex("url")));
    assertEquals(expectedValue,
        actualExtensionRow.getString(actualExtensionRow.fieldIndex("valueString")));
  }

  public void assertIntExtension(final String expectedUrl, final int expectedValue,
      final Row actualExtensionRow) {
    assertEquals(expectedUrl, actualExtensionRow.getString(actualExtensionRow.fieldIndex("url")));
    assertEquals(expectedValue,
        actualExtensionRow.getInt(actualExtensionRow.fieldIndex("valueInteger")));
  }

  public void assertSingletNestedExtension(final String expectedUrl,
      final Row actualExtensionRow, final Map<Object, Object> extensionMap,
      final Consumer<Row> nestedConsumer) {
    assertEquals(expectedUrl, actualExtensionRow.getString(actualExtensionRow.fieldIndex("url")));
    assertTrue(actualExtensionRow.isNullAt(actualExtensionRow.fieldIndex("valueString")));
    assertTrue(actualExtensionRow.isNullAt(actualExtensionRow.fieldIndex("valueInteger")));

    final Object nestedFid = actualExtensionRow.get(actualExtensionRow.fieldIndex("_fid"));
    final WrappedArray<?> nestedExtensions = (WrappedArray<?>) extensionMap.get(nestedFid);
    assertNotNull(nestedExtensions);
    assertEquals(1, nestedExtensions.length());

    nestedConsumer.accept((Row) nestedExtensions.apply(0));
  }

  @Test
  public void testDecimalCollection() {
    final ExpressionEncoder<MolecularSequence> encoder = fhirEncoders
        .of(MolecularSequence.class);

    final MolecularSequence molecularSequence = new MolecularSequence();
    molecularSequence.setId("someId");
    final MolecularSequenceQualityRocComponent rocComponent = molecularSequence
        .getQualityFirstRep().getRoc();

    rocComponent.addSensitivity(new BigDecimal("0.100").setScale(6, RoundingMode.UNNECESSARY));
    rocComponent.addSensitivity(new BigDecimal("1.23").setScale(3, RoundingMode.UNNECESSARY));
    assertSerDeIsIdentity(encoder, molecularSequence);
  }

  @Test
  public void testIdCollection() {
    final ExpressionEncoder<PlanDefinition> encoder = fhirEncoders
        .of(PlanDefinition.class);

    final PlanDefinition planDefinition = new PlanDefinition();

    final PlanDefinitionActionComponent actionComponent = planDefinition
        .getActionFirstRep();
    actionComponent.addGoalId(new IdType("Condition", "goal-1", "1").getValue());
    actionComponent.addGoalId(new IdType("Condition", "goal-2", "2").getValue());
    actionComponent.addGoalId(new IdType("Patient", "goal-3", "3").getValue());

    assertSerDeIsIdentity(encoder, planDefinition);
  }

  @Test
  public void testHtmlNarrative() {
    final ExpressionEncoder<Condition> encoder = fhirEncoders
        .of(Condition.class);
    final Condition conditionWithNarrative = new Condition();
    conditionWithNarrative.setId("someId");
    conditionWithNarrative.getText().getDiv().setValueAsString("Some Narrative Value");

    assertSerDeIsIdentity(encoder, conditionWithNarrative);
  }

  @Test
  public void testThrowsExceptionWhenUnsupportedResource() {
    for (final String resourceName : EXCLUDED_RESOURCES) {
      assertThrows(UnsupportedResourceError.class, () -> fhirEncoders.of(resourceName));
    }
  }

  @Test
  public void testEncodeDecodeExtensionOnResourceAndComposite() {

    final ExpressionEncoder<Condition> encoder = fhirEncoders
        .of(Condition.class);
    final Condition conditionWithExtension = TestData.newConditionWithExtensions();
    assertSerDeIsIdentity(encoder, conditionWithExtension);
  }

  @Test
  public void testEncodesExtensions() {
    final ExpressionEncoder<Condition> encoder = fhirEncoders
        .of(Condition.class);
    final Condition conditionWithExtension = TestData.newConditionWithExtensions();

    final ExpressionEncoder<Condition> resolvedEncoder = EncoderUtils
        .defaultResolveAndBind(encoder);
    final InternalRow serializedRow = resolvedEncoder.createSerializer()
        .apply(conditionWithExtension);

    // Deserialize the InternalRow to a Row with explicit schema.
    final ExpressionEncoder<Row> rowEncoder = EncoderUtils
        .defaultResolveAndBind(RowEncoder.apply(encoder.schema()));
    final Row conditionRow = rowEncoder.createDeserializer().apply(serializedRow);

    // Get the extensionContainer.
    final Map<Object, Object> extensionMap = conditionRow
        .getJavaMap(conditionRow.fieldIndex("_extension"));

    // Get resource extensions.
    final WrappedArray<?> resourceExtensions = (WrappedArray<?>) extensionMap
        .get(conditionRow.get(conditionRow.fieldIndex("_fid")));

    assertNotNull(resourceExtensions);
    assertStringExtension("uuid:ext1", "ext1", (Row) resourceExtensions.apply(0));
    assertIntExtension("uuid:ext2", 2, (Row) resourceExtensions.apply(1));
    assertSingletNestedExtension("uuid:ext4", (Row) resourceExtensions.apply(2), extensionMap,
        ext -> assertStringExtension("uuid:nested", "nested", ext));

    // Get Identifier extensions.
    final WrappedArray<?> identifiers = (WrappedArray<?>) conditionRow
        .get(conditionRow.fieldIndex("identifier"));
    final Row identifierRow = (Row) identifiers.apply(0);
    final WrappedArray<?> identifierExtensions = (WrappedArray<?>) extensionMap
        .get(identifierRow.get(identifierRow.fieldIndex("_fid")));

    assertNotNull(identifierExtensions);
    assertStringExtension("uuid:ext10", "ext10", (Row) identifierExtensions.apply(0));
    assertIntExtension("uuid:ext11", 11, (Row) identifierExtensions.apply(1));

    // Get Stage/Type extensions.
    final WrappedArray<?> stages = (WrappedArray<?>) conditionRow
        .get(conditionRow.fieldIndex("stage"));
    final Row stageRow = (Row) stages.apply(0);
    final Row stageTypeRow = (Row) stageRow.get(stageRow.fieldIndex("type"));

    final WrappedArray<?> stageTypeExtensions = (WrappedArray<?>) extensionMap
        .get(stageTypeRow.get(stageTypeRow.fieldIndex("_fid")));
    assertNotNull(stageTypeExtensions);
    assertStringExtension("uuid:ext12", "ext12", (Row) stageTypeExtensions.apply(0));
  }

  @Test
  public void testQuantityCanonicalization() {
    final ExpressionEncoder<Observation> encoder = fhirEncoders.of(Observation.class);
    final Observation observation = TestData.newUcumObservation();

    final ExpressionEncoder<Observation> resolvedEncoder = EncoderUtils.defaultResolveAndBind(
        encoder);
    final InternalRow serializedRow = resolvedEncoder.createSerializer().apply(observation);

    final ExpressionEncoder<Row> rowEncoder = EncoderUtils.defaultResolveAndBind(
        RowEncoder.apply(encoder.schema()));
    final Row observationRow = rowEncoder.createDeserializer().apply(serializedRow);

    final Row quantityRow = observationRow.getStruct(observationRow.fieldIndex("valueQuantity"));
    final BigDecimal canonicalizedValue = quantityRow.getDecimal(
        quantityRow.fieldIndex("value_canonicalized"));
    final String canonicalizedCode = quantityRow.getString(
        quantityRow.fieldIndex("code_canonicalized"));

    assertEquals(new BigDecimal("76000.000000"), canonicalizedValue);
    assertEquals("g", canonicalizedCode);
  }

}
