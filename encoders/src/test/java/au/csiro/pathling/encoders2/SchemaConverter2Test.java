package au.csiro.pathling.encoders2;

import au.csiro.pathling.encoders.AbstractSchemaConverterTest;
import au.csiro.pathling.encoders.SchemaConverter;

public class SchemaConverter2Test extends AbstractSchemaConverterTest {

  @Override
  protected SchemaConverter createSchemaConverter(int maxNestingLevel) {
    return new SchemaConverter2(FHIR_CONTEXT, DATA_TYPE_MAPPINGS, maxNestingLevel);
  }
}
