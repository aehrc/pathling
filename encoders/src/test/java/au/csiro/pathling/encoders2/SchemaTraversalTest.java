package au.csiro.pathling.encoders2;

import au.csiro.pathling.encoders.datatypes.R4DataTypeMappings;
import ca.uhn.fhir.context.FhirContext;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Patient;
import org.junit.Test;

public class SchemaTraversalTest {

  @Test
  public void testSchemaConverter() {

    final SchemaTraversal schemaTraversal = new SchemaConverter2(FhirContext.forR4(), 0);
    final StructType schema = (StructType) schemaTraversal.visitResource(null, Patient.class);
    schema.printTreeString();
  }

  @Test
  public void testSerializerBuilder() {

    final SerializerBuilder serializerBuilder = new SerializerBuilder(new R4DataTypeMappings(),
        FhirContext.forR4(), 0);

    final Expression serializer = serializerBuilder.buildSerializer(Patient.class);
    System.out.println(serializer);
  }


}
