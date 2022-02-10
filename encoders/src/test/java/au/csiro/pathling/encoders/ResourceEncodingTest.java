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

import static org.junit.Assert.assertEquals;

import au.csiro.pathling.encoders.datatypes.R4DataTypeMappings;
import au.csiro.pathling.encoders2.SchemaConverter2;
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
  private final SchemaConverter2 converter = new SchemaConverter2(fhirContext,
      new R4DataTypeMappings(), EncoderConfig.apply(0, true));

  private final FhirEncoders fhirEncoders = FhirEncoders.forR4()
      .enableExtensions(true).getOrCreate();

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

    for (final String resourceType : fhirContext.getResourceTypes()) {
      final RuntimeResourceDefinition rd = fhirContext
          .getResourceDefinition(resourceType);

      if (!excludeResources.contains(rd.getName())) {

        Class<? extends IBaseResource> implementingClass = rd.getImplementingClass();
        final StructType schema = converter
            .resourceSchema(implementingClass);
        final ExpressionEncoder<? extends IBaseResource> encoder = fhirEncoders
            .of(rd.getImplementingClass());
        assertEquals(schema.treeString(), encoder.schema().treeString());
      }
    }
  }

}
