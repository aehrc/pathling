package au.csiro.pathling.encoders2;

import au.csiro.pathling.encoders.AbstractSchemaConverterTest;
import au.csiro.pathling.encoders.SchemaConverter;
import org.junit.Ignore;
import org.junit.Test;

public class SchemaConverter2Test extends AbstractSchemaConverterTest {

  @Override
  protected SchemaConverter createSchemaConverter(int maxNestingLevel) {
    return new SchemaConverter2(FHIR_CONTEXT, DATA_TYPE_MAPPINGS, maxNestingLevel);
  }

  // TODO: Complete when "#375 - Collections are not supported for custom encoders is fixed"
  @Test
  @Ignore
  public void testNestedTypeInChoice() {

    //  Parameters
    //  ElementDefinition: parameter.valueElementDefinition-> parameter.valueElementDefinition.fixedElementDefinition
    //  ElementDefinition: parameter.valueElementDefinition-> parameter.valueElementDefinition.example.valueElementDefinition (indirect)
  }
}
