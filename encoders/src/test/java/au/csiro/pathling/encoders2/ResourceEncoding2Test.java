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

package au.csiro.pathling.encoders2;

import static org.junit.Assert.assertTrue;

import au.csiro.pathling.encoders.EncoderUtils;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.datatypes.R4DataTypeMappings;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder.Serializer;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Base;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ResourceEncoding2Test {

  private final static FhirContext fhirContext = FhirContext.forR4();
  private final static FhirEncoders fhirEncoders = FhirEncoders.forR4().getOrCreate();


  // TODO: Remove when the corresponding issues are fixed (#375)
  final static Set<String> EXCLUDED_RESOURCES = ImmutableSet.of(
      "Parameters",
      // Collections are not supported for custom encoders for: condition-> RuntimeIdDatatypeDefinition[id, IdType]
      "Task",
      // Collections are not supported for custom encoders for: condition-> RuntimeIdDatatypeDefinition[id, IdType]
      "StructureDefinition",
      // Collections are not supported for custom encoders for: condition-> RuntimeIdDatatypeDefinition[id, IdType]
      "StructureMap",
      // Collections are not supported for custom encoders for: condition-> RuntimeIdDatatypeDefinition[id, IdType]
      "Bundle"
      // scala.MatchError: RuntimeElementDirectResource[DirectChildResource, IBaseResource] (of class ca.uhn.fhir.context.RuntimeElementDirectResource)
  );

  private final Class<? extends IBaseResource> resourceClass;

  public ResourceEncoding2Test(Class<? extends IBaseResource> resourceClass) {
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
  public void testCanProduceSchema() {
    final SchemaConverter2 schemaConverter = new SchemaConverter2(fhirContext,
        new R4DataTypeMappings(),
        0);

    final StructType schema = schemaConverter.resourceSchema(resourceClass);
    schema.printTreeString();
  }


  @Test
  public void testCanEncodeDecodeResource() throws Exception {

    final ExpressionEncoder<? extends IBaseResource> encoder = fhirEncoders
        .of(resourceClass);
    final ExpressionEncoder<? extends IBaseResource> resolvedEncoder = EncoderUtils
        .defaultResolveAndBind(encoder);

    final IBaseResource resourceInstance = resourceClass.getDeclaredConstructor().newInstance();
    resourceInstance.setId("someId");

    Serializer<? extends IBaseResource> serializer = resolvedEncoder
        .createSerializer();

    //noinspection unchecked
    final InternalRow serializedRow = ((Serializer<IBaseResource>) serializer)
        .apply(resourceInstance);

    final IBaseResource deserializedResource = resolvedEncoder.createDeserializer()
        .apply(serializedRow);
    assertTrue(((Base) resourceInstance).equalsDeep((Base) deserializedResource));
  }

}
