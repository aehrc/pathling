package au.csiro.pathling.encoders2;

import static au.csiro.pathling.encoders2.ResourceEncoding2Test.EXCLUDED_RESOURCES;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import au.csiro.pathling.encoders.EncoderUtils;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.UnsupportedResourceError;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.r4.model.BaseResource;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.MolecularSequence;
import org.hl7.fhir.r4.model.MolecularSequence.MolecularSequenceQualityRocComponent;
import org.hl7.fhir.r4.model.PlanDefinition;
import org.hl7.fhir.r4.model.PlanDefinition.PlanDefinitionActionComponent;
import org.json4s.jackson.JsonMethods;
import org.junit.Test;

public class FhirEncoders2Test implements JsonMethods {

  private final FhirEncoders fhirEncoders = FhirEncoders.forR4().getOrCreate();


  public static <T extends BaseResource> void assertSerDeIsIdentity(ExpressionEncoder<T> encoder,
      T obj) {
    final ExpressionEncoder<T> resolvedEncoder = EncoderUtils
        .defaultResolveAndBind(encoder);
    final InternalRow serializedRow = resolvedEncoder.createSerializer()
        .apply(obj);
    final T deserializedObj = resolvedEncoder.createDeserializer()
        .apply(serializedRow);
    assertTrue(obj.equalsDeep(deserializedObj));
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
}
