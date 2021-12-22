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

  // TODO: [#414] This is to check if nested types work correctly in choices.
  // So far the only instances I could find are ElementDefinition values in value[*] choices
  // But this may be HAPI artefact because according to FHIR spec ElementDefinition os not a valid
  // type for value[*] but it is returned by HAPI getChildTypes().
  // So that can either be tested when Extensions are implemented (as have value[*] field) or
  // we may need to correct what is returned from HAPI as valid types for value[*].
  @Test
  @Ignore
  public void testNestedTypeInChoice() {

    //  Extension.value
    //  ElementDefinition: extension.valueElementDefinition-> extension.valueElementDefinition.fixedElementDefinition
    //  ElementDefinition: extension.valueElementDefinition-> extension.valueElementDefinition.example.valueElementDefinition (indirect)
  }
}
