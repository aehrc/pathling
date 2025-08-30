/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
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
 *
 */

package au.csiro.pathling.encoders;

import static au.csiro.pathling.encoders.SchemaConverterTest.OPEN_TYPES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.encoders.datatypes.R4DataTypeMappings;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import com.google.common.collect.ImmutableSet;
import jakarta.annotation.Nonnull;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder.Serializer;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Base;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import scala.collection.JavaConverters;

public class AllResourcesEncodingTest {

  private static final FhirContext FHIR_CONTEXT = FhirContext.forR4();
  private static final FhirEncoders FHIR_ENCODERS = FhirEncoders.forR4()
      .withMaxNestingLevel(2)
      .withOpenTypes(OPEN_TYPES)
      .withExtensionsEnabled(true)
      .getOrCreate();


  private static final SchemaConverter SCHEMA_CONVERTER_L2 = new SchemaConverter(FHIR_CONTEXT,
      new R4DataTypeMappings(),
      EncoderConfig.apply(2, JavaConverters.asScalaSet(OPEN_TYPES).toSet(), true));


  static final Set<String> EXCLUDED_RESOURCES = ImmutableSet.of(
      "Parameters",
      // Collections are not supported for custom encoders for: condition-> RuntimeIdDatatypeDefinition[id, IdType]
      "StructureDefinition",
      // Collections are not supported for custom encoders for: condition-> RuntimeIdDatatypeDefinition[id, IdType]
      "StructureMap",
      // Collections are not supported for custom encoders for: condition-> RuntimeIdDatatypeDefinition[id, IdType]
      "Bundle"
      // scala.MatchError: RuntimeElementDirectResource[DirectChildResource, IBaseResource] (of class ca.uhn.fhir.context.RuntimeElementDirectResource)
  );

  public static Stream<Class<? extends IBaseResource>> input() {
    return FHIR_CONTEXT.getResourceTypes().stream()
        .filter(rn -> !EXCLUDED_RESOURCES.contains(rn))
        .map(FHIR_CONTEXT::getResourceDefinition)
        .map(RuntimeResourceDefinition::getImplementingClass);
  }


  @ParameterizedTest
  @MethodSource("input")
  void testConverterSchemaMatchesEncoder(
      @Nonnull final Class<? extends IBaseResource> resourceClass) {
    final StructType schema = SCHEMA_CONVERTER_L2.resourceSchema(resourceClass);
    final ExpressionEncoder<? extends IBaseResource> encoder = FHIR_ENCODERS
        .of(resourceClass);
    assertEquals(schema.treeString(), encoder.schema().treeString());
  }

  @ParameterizedTest
  @MethodSource("input")
  <T extends IBaseResource> void testCanEncodeDecodeResource(
      @Nonnull final Class<T> resourceClass) throws Exception {
    final ExpressionEncoder<T> encoder = FHIR_ENCODERS.of(resourceClass);
    final ExpressionEncoder<T> resolvedEncoder = EncoderUtils.defaultResolveAndBind(encoder);
    final T resourceInstance = resourceClass.getDeclaredConstructor().newInstance();
    resourceInstance.setId("someId");

    final Serializer<T> serializer = resolvedEncoder.createSerializer();
    final InternalRow serializedRow = serializer.apply(resourceInstance);
    final IBaseResource deserializedResource = resolvedEncoder.createDeserializer()
        .apply(serializedRow);

    assertTrue(((Base) resourceInstance).equalsDeep((Base) deserializedResource));
  }

}
