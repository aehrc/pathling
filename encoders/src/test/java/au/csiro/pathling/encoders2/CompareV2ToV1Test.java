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

import static org.junit.Assert.assertEquals;

import au.csiro.pathling.encoders.EncoderConfig;
import au.csiro.pathling.encoders.datatypes.R4DataTypeMappings;
import au.csiro.pathling.encoders1.EncoderBuilder1;
import au.csiro.pathling.encoders1.SchemaConverter1;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.*;
import org.json4s.StringInput;
import org.json4s.jackson.JsonMethods;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import scala.collection.JavaConverters;

@RunWith(Parameterized.class)
public class CompareV2ToV1Test implements JsonMethods {

  private final FhirContext fhirContext = FhirContext.forR4();
  private final R4DataTypeMappings dataTypeMappings = new R4DataTypeMappings();

  private final Class<? extends IBaseResource> resourceClass;
  private final int nestingLevel;


  @Parameters(name = "{index}: class = {0}, level = {1}")
  public static Collection<?> input() {
    return Arrays.asList(new Object[][]{
        {Patient.class, 0},
        {Condition.class, 0},
        {Encounter.class, 0},
        {Observation.class, 0},
        {QuestionnaireResponse.class, 0},
        {Patient.class, 2},
        {Condition.class, 2},
        {Encounter.class, 2},
        {Observation.class, 2},
        {QuestionnaireResponse.class, 2}
    });
  }

  public CompareV2ToV1Test(Class<? extends IBaseResource> resourceClass, int nestingLevel) {
    this.resourceClass = resourceClass;
    this.nestingLevel = nestingLevel;
  }

  String toPrettyJson(String json) {
    return pretty(parse(new StringInput(json), false, false));
  }

  String toComparableJSON(final Expression expression) {

    // Expressions  internally use lambda variables, ids of which are assigned from a global counter.
    // E.g:
    // {
    //   "class" : "org.apache.spark.sql.catalyst.expressions.objects.LambdaVariable",
    //     "num-children" : 0,
    //     "name" : "MapObject",
    //     "dataType" : "object",
    //     "nullable" : true,
    //     "id" : 45
    // }
    // Before two expression can be compared these IDs need to be reconciled.
    // The best way would be to make them 0-based starting form the lowest id found in the expression.
    // But setting them all to 0 should give enough power to detect most differences in expressions.

    final String prettyJson = toPrettyJson(expression.toJSON());
    return prettyJson.replaceAll("\"id\" : \\d+", "\"id\" : 0");
  }

  @Test
  public void testCompareSchemaConverters() {
    final SchemaConverter1 converter = new SchemaConverter1(FhirContext.forR4(),
        dataTypeMappings, nestingLevel);
    final SchemaConverter2 schemaTraversal2 = new SchemaConverter2(FhirContext.forR4(),
        dataTypeMappings, EncoderConfig.apply(nestingLevel, false, false));
    final StructType schema = converter.resourceSchema(resourceClass);
    final StructType schema2 = schemaTraversal2.resourceSchema(resourceClass);
    assertEquals(schema.treeString(), schema2.treeString());
  }

  @Test
  public void testCompareSerializers() {
    final SchemaConverter1 converter_0 = new SchemaConverter1(FhirContext.forR4(),
        dataTypeMappings, nestingLevel);

    final ExpressionEncoder<?> encoder = EncoderBuilder1
        .of(fhirContext.getResourceDefinition(resourceClass), fhirContext, dataTypeMappings,
            converter_0, JavaConverters.asScalaBuffer(Collections.emptyList()));

    final Expression objSerializer_v1 = encoder.objSerializer();

    final SerializerBuilder2 serializerBuilder = new SerializerBuilder2(fhirContext,
        dataTypeMappings,
        EncoderConfig.apply(nestingLevel, false, false));

    final Expression objSerializer_v2 = serializerBuilder.buildSerializer(resourceClass);
    // NOTE: Two serializers cannot be compared directly, because of global state used
    // to generate ids of lambda variables use in the expression.
    // See: toComparableJSON()

    assertEquals(toComparableJSON(objSerializer_v1),
        toComparableJSON(objSerializer_v2));
  }

  @Test
  public void testCompareDeserializers() {

    final SchemaConverter1 converter_0 = new SchemaConverter1(FhirContext.forR4(),
        dataTypeMappings, nestingLevel);
    final ExpressionEncoder<?> encoder = EncoderBuilder1
        .of(fhirContext.getResourceDefinition(resourceClass), fhirContext, dataTypeMappings,
            converter_0, JavaConverters.asScalaBuffer(Collections.emptyList()));

    final Expression objDeserializer_v1 = encoder.objDeserializer();

    final SchemaConverter2 schemaTraversal2 = new SchemaConverter2(FhirContext.forR4(),
        dataTypeMappings, EncoderConfig.apply(nestingLevel, false, false));
    final DeserializerBuilder2 deserializerBuilder = DeserializerBuilder2.apply(schemaTraversal2);

    final Expression objDeserializer_v2 = deserializerBuilder.buildDeserializer(resourceClass);
    // NOTE: Two serializers cannot be compared directly, because of global state used
    // to generate ids of lambda variables use in the expression.
    // See: toComparableJSON()

    assertEquals(toComparableJSON(objDeserializer_v1),
        toComparableJSON(objDeserializer_v2));
  }
}
