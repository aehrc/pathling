package au.csiro.pathling.encoders2;

import static org.junit.Assert.assertEquals;

import au.csiro.pathling.encoders.EncoderBuilder;
import au.csiro.pathling.encoders.SchemaConverter;
import au.csiro.pathling.encoders.datatypes.R4DataTypeMappings;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Condition;
import org.json4s.StringInput;
import org.json4s.jackson.JsonMethods;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import scala.collection.JavaConversions;

@RunWith(Parameterized.class)
public class CompareV2ToV1Test implements JsonMethods {

  private final FhirContext fhirContext = FhirContext.forR4();
  private final R4DataTypeMappings dataTypeMappings = new R4DataTypeMappings();
  private final SchemaConverter converter_0 = new SchemaConverter(FhirContext.forR4(),
      dataTypeMappings, 0);

  private final Class<? extends IBaseResource> resourceClass;


  @Parameters(name = "{index}: class = {0}")
  public static Collection<?> input() {
    return Arrays.asList(new Object[][]{
        // {Patient.class},
        {Condition.class}
    });
  }

  public CompareV2ToV1Test(Class<? extends IBaseResource> resourceClass) {
    this.resourceClass = resourceClass;
  }

  String toPrettyJson(String json) {
    return pretty(parse(new StringInput(json), false, false));
  }

  String toComparableJSON(final Expression serializer) {

    // These are the sections when id needs fixing
    // {
    //   "class" : "org.apache.spark.sql.catalyst.expressions.objects.LambdaVariable",
    //     "num-children" : 0,
    //     "name" : "MapObject",
    //     "dataType" : "object",
    //     "nullable" : true,
    //     "id" : 45
    // }

    final String prettyJson = toPrettyJson(serializer.toJSON());
    return prettyJson.replaceAll("(?:\"id\" \\: )\\d+", "\"id\" : 0");
  }

  @Test
  public void testCompareSchemaConverters() {
    final SchemaConverter converter = new SchemaConverter(FhirContext.forR4(),
        dataTypeMappings, 0);
    final SchemaConverter2 schemaTraversal2 = new SchemaConverter2(FhirContext.forR4(),
        dataTypeMappings, 0);
    final StructType schema = converter.resourceSchema(resourceClass);
    final StructType schema2 = schemaTraversal2.resourceSchema(resourceClass);
    assertEquals(schema.treeString(), schema2.treeString());
  }

  @Test
  public void testCompareSerializers() {

    final ExpressionEncoder<?> encoder = EncoderBuilder
        .of(fhirContext.getResourceDefinition(resourceClass), fhirContext, dataTypeMappings,
            converter_0, JavaConversions.asScalaBuffer(Collections.emptyList()));

    final Expression objSerializer_v1 = encoder.objSerializer();

    final SerializerBuilder2 serializerBuilder = new SerializerBuilder2(dataTypeMappings,
        fhirContext, 0);

    final Expression objSerializer_v2 = serializerBuilder.buildSerializer(resourceClass);
    // NOTE: Cannot be compared direclty because lambda variables ids for Map expression are generated from
    // a global counter so the second serializer has different ids (offseted).
    // assertEquals(encoder.objSerializer().canonicalized(), otherEncoder.objSerializer().canonicalized());

    assertEquals(toComparableJSON(objSerializer_v1),
        toComparableJSON(objSerializer_v2));
    JSONAssert.assertEquals(toComparableJSON(objSerializer_v1),
        toComparableJSON(objSerializer_v2), JSONCompareMode.STRICT);
  }

  @Test
  public void testCompareDeserializers() {

    final ExpressionEncoder<?> encoder = EncoderBuilder
        .of(fhirContext.getResourceDefinition(resourceClass), fhirContext, dataTypeMappings,
            converter_0, JavaConversions.asScalaBuffer(Collections.emptyList()));

    final Expression objDeserializer_v1 = encoder.objDeserializer();

    final DeserializerBuilder2 deserializerBuilder = new DeserializerBuilder2(dataTypeMappings,
        fhirContext, 0);

    final Expression objDeserializer_v2 = deserializerBuilder.buildDeserializer(resourceClass);
    // NOTE: Cannot be compared direclty because lambda variables ids for Map expression are generated from
    // a global counter so the second serializer has different ids (offseted).
    // assertEquals(encoder.objSerializer().canonicalized(), otherEncoder.objSerializer().canonicalized());
    assertEquals(toPrettyJson(objDeserializer_v1.toJSON()),
        toPrettyJson(objDeserializer_v2.toJSON()));
  }

}
