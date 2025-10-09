/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific
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

import static au.csiro.pathling.encoders.AllResourcesEncodingTest.EXCLUDED_RESOURCES;
import static au.csiro.pathling.encoders.SchemaConverterTest.OPEN_TYPES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.sql.types.FlexiDecimal;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.r4.model.BaseResource;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Device;
import org.hl7.fhir.r4.model.Expression;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.Identifier.IdentifierUse;
import org.hl7.fhir.r4.model.MolecularSequence;
import org.hl7.fhir.r4.model.MolecularSequence.MolecularSequenceQualityRocComponent;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.PlanDefinition;
import org.hl7.fhir.r4.model.PlanDefinition.PlanDefinitionActionComponent;
import org.hl7.fhir.r4.model.Reference;
import org.json4s.jackson.JsonMethods;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.ArraySeq;

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
    final ArraySeq<?> nestedExtensions = (ArraySeq<?>) extensionMap.get(nestedFid);
    assertNotNull(nestedExtensions);
    assertEquals(1, nestedExtensions.length());

    nestedConsumer.accept((Row) nestedExtensions.apply(0));
  }

  private static void assertQuantity(final Row quantityRow, final String canonicalizedValue,
      final String canonicalizedCode) {
    final BigDecimal actualCanonicalizedValue = FlexiDecimal.fromValue(quantityRow.getStruct(
        quantityRow.fieldIndex(QuantitySupport.VALUE_CANONICALIZED_FIELD_NAME())));
    final String actualCanonicalizedCode = quantityRow.getString(
        quantityRow.fieldIndex(QuantitySupport.CODE_CANONICALIZED_FIELD_NAME()));

    assertEquals(new BigDecimal(canonicalizedValue), actualCanonicalizedValue);
    assertEquals(canonicalizedCode, actualCanonicalizedCode);
  }

  @Test
  void testDecimalCollection() {
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
  void testIdCollection() {
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
  void testHtmlNarrative() {
    final ExpressionEncoder<Condition> encoder = fhirEncoders
        .of(Condition.class);
    final Condition conditionWithNarrative = new Condition();
    conditionWithNarrative.setId("someId");
    conditionWithNarrative.getText().getDiv().setValueAsString("Some Narrative Value");

    assertSerDeIsIdentity(encoder, conditionWithNarrative);
  }

  @Test
  void testReference() {
    final ExpressionEncoder<Condition> encoder = fhirEncoders
        .of(Condition.class);
    final Condition conditionWithFullReference = new Condition();
    final Identifier identifier = new Identifier()
        .setSystem("urn:id-system")
        .setValue("id-value")
        .setUse(IdentifierUse.OFFICIAL)
        .setType(new CodeableConcept().addCoding(new Coding().setCode("code").setSystem("system"))
            .setText("text"));
    final Reference referenceWithAllFields = new Reference("Patient/1234")
        .setDisplay("Some Display Name")
        .setType("Patient")
        .setIdentifier(identifier);
    // Set also the Element inherited fields
    referenceWithAllFields.setId("some-id");
    conditionWithFullReference.setSubject(referenceWithAllFields);
    assertSerDeIsIdentity(encoder, conditionWithFullReference);
  }

  @Test
  void testIdentifier() {
    final ExpressionEncoder<Condition> encoder = fhirEncoders
        .of(Condition.class);
    final Condition conditionWithIdentifierWithAssigner = new Condition();

    final Reference assignerReference = new Reference("Organization/1234")
        .setDisplay("Some Display Name")
        .setType("Organization");

    final Identifier identifier = new Identifier()
        .setSystem("urn:id-system")
        .setValue("id-value")
        .setUse(IdentifierUse.OFFICIAL)
        .setAssigner(assignerReference)
        .setType(new CodeableConcept().addCoding(new Coding().setCode("code").setSystem("system"))
            .setText("text"));
    conditionWithIdentifierWithAssigner.addIdentifier(identifier);
    assertSerDeIsIdentity(encoder, conditionWithIdentifierWithAssigner);
  }

  @Test
  void testExpression() {

    // Expression contains 'reference' field 
    // We are checking that it is encoded in generic way not and not the subject to special case for Reference 'reference' field.
    final ExpressionEncoder<PlanDefinition> encoder = fhirEncoders
        .of(PlanDefinition.class);

    final PlanDefinition planDefinition = new PlanDefinition();

    final PlanDefinitionActionComponent actionComponent = planDefinition
        .getActionFirstRep();
    actionComponent.getConditionFirstRep().setExpression(new Expression().setLanguage("language")
        .setExpression("expression").setDescription("description"));
    assertSerDeIsIdentity(encoder, planDefinition);
  }

  @Test
  void testThrowsExceptionWhenUnsupportedResource() {
    for (final String resourceName : EXCLUDED_RESOURCES) {
      assertThrows(UnsupportedResourceError.class, () -> fhirEncoders.of(resourceName));
    }
  }

  @Test
  void testEncodeDecodeExtensionOnResourceAndComposite() {

    final ExpressionEncoder<Condition> encoder = fhirEncoders
        .of(Condition.class);
    final Condition conditionWithExtension = TestData.newConditionWithExtensions();
    assertSerDeIsIdentity(encoder, conditionWithExtension);
  }

  @Test
  void testEncodesExtensions() {
    final ExpressionEncoder<Condition> encoder = fhirEncoders
        .of(Condition.class);
    final Condition conditionWithExtension = TestData.newConditionWithExtensions();

    final ExpressionEncoder<Condition> resolvedEncoder = EncoderUtils
        .defaultResolveAndBind(encoder);
    final InternalRow serializedRow = resolvedEncoder.createSerializer()
        .apply(conditionWithExtension);

    // Deserialize the InternalRow to a Row with explicit schema.
    final ExpressionEncoder<Row> rowEncoder = EncoderUtils
        .defaultResolveAndBind(ExpressionEncoder.apply(encoder.schema()));
    final Row conditionRow = rowEncoder.createDeserializer().apply(serializedRow);

    // Get the extensionContainer.
    final Map<Object, Object> extensionMap = conditionRow
        .getJavaMap(conditionRow.fieldIndex("_extension"));

    // Get resource extensions.
    final ArraySeq<?> resourceExtensions = (ArraySeq<?>) extensionMap
        .get(conditionRow.get(conditionRow.fieldIndex("_fid")));

    assertNotNull(resourceExtensions);
    assertStringExtension("uuid:ext1", "ext1", (Row) resourceExtensions.apply(0));
    assertIntExtension("uuid:ext2", 2, (Row) resourceExtensions.apply(1));
    assertSingletNestedExtension("uuid:ext4", (Row) resourceExtensions.apply(2), extensionMap,
        ext -> assertStringExtension("uuid:nested", "nested", ext));

    // Get Identifier extensions.
    final ArraySeq<?> identifiers = (ArraySeq<?>) conditionRow
        .get(conditionRow.fieldIndex("identifier"));
    final Row identifierRow = (Row) identifiers.apply(0);
    final ArraySeq<?> identifierExtensions = (ArraySeq<?>) extensionMap
        .get(identifierRow.get(identifierRow.fieldIndex("_fid")));

    assertNotNull(identifierExtensions);
    assertStringExtension("uuid:ext10", "ext10", (Row) identifierExtensions.apply(0));
    assertIntExtension("uuid:ext11", 11, (Row) identifierExtensions.apply(1));

    // Get Stage/Type extensions.
    final ArraySeq<?> stages = (ArraySeq<?>) conditionRow
        .get(conditionRow.fieldIndex("stage"));
    final Row stageRow = (Row) stages.apply(0);
    final Row stageTypeRow = (Row) stageRow.get(stageRow.fieldIndex("type"));

    final ArraySeq<?> stageTypeExtensions = (ArraySeq<?>) extensionMap
        .get(stageTypeRow.get(stageTypeRow.fieldIndex("_fid")));
    assertNotNull(stageTypeExtensions);
    assertStringExtension("uuid:ext12", "ext12", (Row) stageTypeExtensions.apply(0));
  }

  @Test
  void testQuantityCanonicalization() {
    final ExpressionEncoder<Observation> encoder = fhirEncoders.of(Observation.class);
    final Observation observation = TestData.newUcumObservation();

    final ExpressionEncoder<Observation> resolvedEncoder = EncoderUtils.defaultResolveAndBind(
        encoder);
    final InternalRow serializedRow = resolvedEncoder.createSerializer().apply(observation);

    final ExpressionEncoder<Row> rowEncoder = EncoderUtils.defaultResolveAndBind(
        ExpressionEncoder.apply(encoder.schema()));
    final Row observationRow = rowEncoder.createDeserializer().apply(serializedRow);

    final Row quantityRow = observationRow.getStruct(observationRow.fieldIndex("valueQuantity"));
    assertQuantity(quantityRow, "76000", "g");
  }

  @Test
  void testQuantityArrayCanonicalization() {
    final ExpressionEncoder<Device> encoder = fhirEncoders.of(Device.class);
    final Device device = TestData.newDevice();

    final ExpressionEncoder<Device> resolvedEncoder = EncoderUtils.defaultResolveAndBind(
        encoder);
    final InternalRow serializedRow = resolvedEncoder.createSerializer().apply(device);

    final ExpressionEncoder<Row> rowEncoder = EncoderUtils.defaultResolveAndBind(
        ExpressionEncoder.apply(encoder.schema()));
    final Row deviceRow = rowEncoder.createDeserializer().apply(serializedRow);

    final List<Row> properties = deviceRow.getList(deviceRow.fieldIndex("property"));
    final Row propertyRow = properties.getFirst();
    final List<Row> quantityArray = propertyRow.getList(propertyRow.fieldIndex("valueQuantity"));

    final Row quantity1 = quantityArray.getFirst();
    assertQuantity(quantity1, "0.0010", "m");

    final Row quantity2 = quantityArray.get(1);
    assertQuantity(quantity2, "0.0020", "m");
  }

}
