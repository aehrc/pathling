package au.csiro.pathling.encoders2;

import static org.junit.Assert.assertEquals;

import au.csiro.pathling.encoders.SchemaConverter;
import au.csiro.pathling.encoders.datatypes.R4DataTypeMappings;
import ca.uhn.fhir.context.FhirContext;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Patient;
import org.junit.Test;

public class SchemaTraversalTest {

  private final FhirContext fhirContext = FhirContext.forR4();
  private final R4DataTypeMappings dataTypeMappings = new R4DataTypeMappings();


  @Test
  public void testCompareOldAndNew() {
    final SchemaConverter converter = new SchemaConverter(FhirContext.forR4(),
        dataTypeMappings, 0);
    final SchemaConverter2 schemaTraversal2 = new SchemaConverter2(FhirContext.forR4(),
        dataTypeMappings, 0);
    final StructType schema = converter.resourceSchema(Patient.class);
    final StructType schema2 = schemaTraversal2.resourceSchema(Patient.class);

    assertEquals(schema.treeString(), schema2.treeString());
  }

  @Test
  public void testComparSerializerSchemaForPatient() {
    final SerializerBuilder2 serializerBuilder = new SerializerBuilder2(dataTypeMappings,
        fhirContext, 0);
    final SchemaConverter2 schemaTraversal2 = new SchemaConverter2(fhirContext,
        dataTypeMappings, 0);

    final ExpressionEncoder<?> encoder = EncoderBuilder2
        .of(fhirContext.getResourceDefinition(Patient.class), fhirContext, dataTypeMappings, null,
            null);

    final StructType encoderSchema = encoder.schema();
    final StructType schema2 = schemaTraversal2.resourceSchema(Patient.class);
    assertEquals(schema2.treeString(), encoderSchema.treeString());
  }

  @Test
  public void testComparSerializerSchemaForCondition() {
    final SerializerBuilder2 serializerBuilder = new SerializerBuilder2(dataTypeMappings,
        fhirContext, 0);
    final SchemaConverter2 schemaTraversal2 = new SchemaConverter2(fhirContext,
        dataTypeMappings, 0);

    final ExpressionEncoder<?> encoder = EncoderBuilder2
        .of(fhirContext.getResourceDefinition(Condition.class), fhirContext, dataTypeMappings, null,
            null);

    final StructType encoderSchema = encoder.schema();
    final StructType schema2 = schemaTraversal2.resourceSchema(Condition.class);
    assertEquals(schema2.treeString(), encoderSchema.treeString());
  }


  @Test
  public void testSchemaConverter() {

    final SchemaConverter2 schemaTraversal = new SchemaConverter2(FhirContext.forR4(),
        dataTypeMappings, 0);
    final StructType schema = schemaTraversal.resourceSchema(Patient.class);
    schema.printTreeString();
  }

  @Test
  public void testSerializerBuilder() {

    final SerializerBuilder2 serializerBuilder = new SerializerBuilder2(new R4DataTypeMappings(),
        FhirContext.forR4(), 0);

    final Expression serializer = serializerBuilder.buildSerializer(Patient.class);
    System.out.println(serializer);
  }


}
