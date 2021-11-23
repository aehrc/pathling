package au.csiro.pathling.encoders2;

import static org.junit.Assert.assertTrue;

import au.csiro.pathling.encoders.FhirEncoders;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder.Serializer;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Base;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ResourceEncodingTest2 {

  private final static FhirContext fhirContext = FhirContext.forR4();
  private final static FhirEncoders fhirEncoders = FhirEncoders.forR4().getOrCreate();


  // TODO: Remove when the corresponding issues are fixed (#375)
  private final static Set<String> EXCLUDED_RESOURCES = ImmutableSet.of(
      "Parameters",
      // Collections are not supported for custom encoders for: condition-> RuntimeIdDatatypeDefinition[id, IdType]
      "Task",
      // Collections are not supported for custom encoders for: condition-> RuntimeIdDatatypeDefinition[id, IdType]
      "StructureDefinition",
      // Collections are not supported for custom encoders for: condition-> RuntimeIdDatatypeDefinition[id, IdType]
      "MolecularSequence",
      // Collections are not supported for custom encoders for: precision-> RuntimePrimitiveDatatypeDefinition[decimal, DecimalType]
      "StructureMap",
      // Collections are not supported for custom encoders for: condition-> RuntimeIdDatatypeDefinition[id, IdType]
      "Bundle",
      // scala.MatchError: RuntimeElementDirectResource[DirectChildResource, IBaseResource] (of class ca.uhn.fhir.context.RuntimeElementDirectResource)
      "PlanDefinition"
      // Collections are not supported for custom encoders for: goalId-> RuntimeIdDatatypeDefinition[id, IdType]
  );

  private final Class<? extends IBaseResource> resourceClass;

  public ResourceEncodingTest2(Class<? extends IBaseResource> resourceClass) {
    this.resourceClass = resourceClass;
  }


  @Parameters(name = "{index}: class = {0}")
  public static Collection<?> input() {
    return fhirContext.getResourceTypes().stream()
        .filter(rn -> !EXCLUDED_RESOURCES.contains(rn))
        .map(fhirContext::getResourceDefinition)
        .map(RuntimeResourceDefinition::getImplementingClass)
        .map(cls -> new Object[]{cls})
        .collect(Collectors.toList());
  }

  @Test
  public void testCanEncodeDecodeAllR4Resources() throws Exception {

    ExpressionEncoder<? extends IBaseResource> encoder = fhirEncoders
        .of(resourceClass);
    ExpressionEncoder<? extends IBaseResource> resolvedEncoder = EncoderUtils
        .defaultResolveAndBind(encoder);

    IBaseResource resourceInstance = resourceClass.getDeclaredConstructor().newInstance();
    resourceInstance.setId("someId");

    Serializer<? extends IBaseResource> serializer = resolvedEncoder
        .createSerializer();

    //noinspection unchecked
    final InternalRow serializedRow = ((Serializer<IBaseResource>) serializer)
        .apply(resourceInstance);

    IBaseResource deserializedResource = resolvedEncoder.createDeserializer().apply(serializedRow);
    assertTrue(((Base) resourceInstance).equalsDeep((Base) deserializedResource));
  }
}
