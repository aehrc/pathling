package au.csiro.pathling.encoders2;

import static org.junit.Assert.assertTrue;

import au.csiro.pathling.encoders.EncoderUtils;
import au.csiro.pathling.encoders.FhirEncoders;
import java.math.BigDecimal;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.MolecularSequence;
import org.hl7.fhir.r4.model.MolecularSequence.MolecularSequenceQualityRocComponent;
import org.hl7.fhir.r4.model.PlanDefinition;
import org.hl7.fhir.r4.model.PlanDefinition.PlanDefinitionActionComponent;
import org.json4s.jackson.JsonMethods;
import org.junit.Test;

public class EncoderTest2 implements JsonMethods {

  private final FhirEncoders fhirEncoders = FhirEncoders.forR4().getOrCreate();

  @Test
  public void testDecimalCollection() {
    final ExpressionEncoder<MolecularSequence> patientEncoder = fhirEncoders
        .of(MolecularSequence.class);
    ExpressionEncoder<MolecularSequence> resolvedPatientEncoder = EncoderUtils
        .defaultResolveAndBind(patientEncoder);

    final MolecularSequence molecularSequence = new MolecularSequence();
    molecularSequence.setId("someId");
    final MolecularSequenceQualityRocComponent rocComponent = molecularSequence
        .getQualityFirstRep().getRoc();

    rocComponent.addSensitivity(new BigDecimal("0.100").setScale(6));
    rocComponent.addSensitivity(new BigDecimal("1.23").setScale(3));

    final InternalRow serializedRow = resolvedPatientEncoder.createSerializer()
        .apply(molecularSequence);
    MolecularSequence deserializedMolecularSequence = resolvedPatientEncoder.createDeserializer()
        .apply(serializedRow);
    assertTrue(molecularSequence.equalsDeep(deserializedMolecularSequence));
  }


  @Test
  public void testIdCollection() {
    final ExpressionEncoder<PlanDefinition> encoder = fhirEncoders
        .of(PlanDefinition.class);
    final ExpressionEncoder<PlanDefinition> resolvedEncoder = EncoderUtils
        .defaultResolveAndBind(encoder);

    final PlanDefinition planDefinition = new PlanDefinition();

    final PlanDefinitionActionComponent actionComponent = planDefinition
        .getActionFirstRep();
    actionComponent.addGoalId(new IdType("Condition", "goal-1", "1").getValue());
    actionComponent.addGoalId(new IdType("Condition", "goal-2", "2").getValue());
    actionComponent.addGoalId(new IdType("Patient", "goal-3", "3").getValue());

    final InternalRow serializedRow = resolvedEncoder.createSerializer()
        .apply(planDefinition);
    final PlanDefinition deserializedPlanDefinition = resolvedEncoder.createDeserializer()
        .apply(serializedRow);
    assertTrue(planDefinition.equalsDeep(deserializedPlanDefinition));
  }
}
