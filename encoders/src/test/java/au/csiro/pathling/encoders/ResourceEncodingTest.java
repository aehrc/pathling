package au.csiro.pathling.encoders;

import static org.junit.Assert.assertEquals;

import au.csiro.pathling.encoders.datatypes.R4DataTypeMappings;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.junit.Test;

public class ResourceEncodingTest {

  private final FhirContext fhirContext = FhirContext.forR4();
  private final SchemaConverter converter = new SchemaConverter(fhirContext,
      new R4DataTypeMappings(), 0);

  private final FhirEncoders fhirEncoders = FhirEncoders.forR4().getOrCreate();

  @Test
  public void testCanEncodeDecodeAllR4Resources() {

    // TODO: Remove when the corresponding issues are fixed (#375)
    final Set<String> excludeResources = ImmutableSet.of(
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

    for (String resourceType : fhirContext.getResourceTypes()) {
      RuntimeResourceDefinition rd = fhirContext
          .getResourceDefinition(resourceType);

      if (!excludeResources.contains(rd.getName())) {

        Class<? extends IBaseResource> implementingClass = rd.getImplementingClass();
        final StructType schema = converter
            .resourceSchema(implementingClass);
        final ExpressionEncoder<? extends IBaseResource> encoder = fhirEncoders
            .of(rd.getImplementingClass());
        assertEquals(schema, encoder.schema());
      }
    }
  }

}
