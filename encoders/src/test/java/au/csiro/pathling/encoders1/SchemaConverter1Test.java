package au.csiro.pathling.encoders1;

import au.csiro.pathling.encoders.AbstractSchemaConverterTest;
import au.csiro.pathling.encoders.SchemaConverter;

public class SchemaConverter1Test extends AbstractSchemaConverterTest {

  @Override
  protected SchemaConverter createSchemaConverter(int maxNestingLevel) {
    return new SchemaConverter1(FHIR_CONTEXT, DATA_TYPE_MAPPINGS, maxNestingLevel);
  }
}
