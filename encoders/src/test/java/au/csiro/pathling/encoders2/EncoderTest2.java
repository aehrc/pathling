package au.csiro.pathling.encoders2;

import static org.junit.Assert.assertTrue;

import au.csiro.pathling.encoders.EncoderUtils;
import au.csiro.pathling.encoders.FhirEncoders;
import ca.uhn.fhir.context.FhirContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.r4.model.Observation;
import org.json4s.jackson.JsonMethods;
import org.junit.Test;

public class EncoderTest2 implements JsonMethods {

  private final FhirContext fhirContext = FhirContext.forR4();
  private final FhirEncoders fhirEncoders = FhirEncoders.forR4().getOrCreate();

  // @Test
  // public void testCanEncodeDecodeAllR4Resources() {
  //
  //   // TODO: Remove when the corresponding issues are fixed (#375)
  //   final Set<String> excludeResources = ImmutableSet.of(
  //       "Parameters",
  //       // Collections are not supported for custom encoders for: condition-> RuntimeIdDatatypeDefinition[id, IdType]
  //       "Task",
  //       // Collections are not supported for custom encoders for: condition-> RuntimeIdDatatypeDefinition[id, IdType]
  //       "StructureDefinition",
  //       // Collections are not supported for custom encoders for: condition-> RuntimeIdDatatypeDefinition[id, IdType]
  //       "MolecularSequence",
  //       // Collections are not supported for custom encoders for: precision-> RuntimePrimitiveDatatypeDefinition[decimal, DecimalType]
  //       "StructureMap",
  //       // Collections are not supported for custom encoders for: condition-> RuntimeIdDatatypeDefinition[id, IdType]
  //       "Bundle",
  //       // scala.MatchError: RuntimeElementDirectResource[DirectChildResource, IBaseResource] (of class ca.uhn.fhir.context.RuntimeElementDirectResource)
  //       "PlanDefinition"
  //       // Collections are not supported for custom encoders for: goalId-> RuntimeIdDatatypeDefinition[id, IdType]
  //   );
  //
  //   for (String resourceType : fhirContext.getResourceTypes()) {
  //     RuntimeResourceDefinition rd = fhirContext
  //         .getResourceDefinition(resourceType);
  //
  //     if (!excludeResources.contains(rd.getName())) {
  //       final StructType schema = converter
  //           .resourceSchema(rd.getImplementingClass());
  //
  //       final StructType schema2 = converter2.resourceSchema(rd.getImplementingClass());
  //       //schema2.printTreeString();
  //       assertEquals("Failed on: " + rd, schema.treeString(), schema2.treeString());
  //     }
  //   }
  // }

  @Test
  public void testSingleEncoder() {

    final ExpressionEncoder<Observation> patientEncoder = fhirEncoders
        .of(Observation.class);
    ExpressionEncoder<Observation> resolvedPatientEncoder = EncoderUtils
        .defaultResolveAndBind(patientEncoder);

    final Observation patient = new Observation();
    patient.setId("someId");
    final InternalRow serializedRow = resolvedPatientEncoder.createSerializer()
        .apply(patient);
    Observation deserializedPatient = resolvedPatientEncoder.createDeserializer()
        .apply(serializedRow);
    assertTrue(patient.equalsDeep(deserializedPatient));
  }

}
