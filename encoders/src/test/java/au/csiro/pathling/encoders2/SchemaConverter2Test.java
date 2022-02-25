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
import static org.junit.Assert.assertTrue;

import au.csiro.pathling.encoders.AbstractSchemaConverterTest;
import au.csiro.pathling.encoders.EncoderConfig;
import au.csiro.pathling.encoders.SchemaConverter;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Condition;
import org.junit.Ignore;
import org.junit.Test;
import scala.collection.JavaConverters;

public class SchemaConverter2Test extends AbstractSchemaConverterTest {

  public static final Set<String> OPEN_TYPES = Set.of(
      "boolean",
      "canonical",
      "code",
      "date",
      "dateTime",
      "decimal",
      "instant",
      "integer",
      "oid",
      "positiveInt",
      "string",
      "time",
      "unsignedInt",
      "uri",
      "url",
      "Coding",
      "Identifier"
  );

  /**
   * Traverses a DataType recursively passing all encountered StructTypes to the provided consumer.
   *
   * @param type the DataType to traverse.
   * @param consumer the consumer that receives all StructTypes.
   */
  protected void traverseSchema(final DataType type, final Consumer<StructType> consumer) {
    if (type instanceof StructType) {
      final StructType structType = (StructType) type;
      consumer.accept(structType);
      Arrays.stream(structType.fields()).forEach(f -> traverseSchema(f.dataType(), consumer));
    } else if (type instanceof ArrayType) {
      traverseSchema(((ArrayType) type).elementType(), consumer);
    } else if (type instanceof MapType) {
      traverseSchema(((MapType) type).keyType(), consumer);
      traverseSchema(((MapType) type).valueType(), consumer);
    }
  }

  @Override
  protected SchemaConverter createSchemaConverter(final int maxNestingLevel) {
    return new SchemaConverter2(FHIR_CONTEXT, DATA_TYPE_MAPPINGS,
        EncoderConfig.apply(maxNestingLevel, JavaConverters.asScalaSet(OPEN_TYPES).toSet(), true));
  }

  // TODO: [#414] This is to check if nested types work correctly in choices.
  //       So far the only instances I could find are ElementDefinition values in value[*] choices
  //       But this may be HAPI artefact because according to FHIR spec ElementDefinition os not a 
  //       valid type for value[*] but it is returned by HAPI getChildTypes().
  //       So that can either be tested when Extensions are implemented (as have value[*] field) or
  //       we may need to correct what is returned from HAPI as valid types for value[*].
  @Test
  @Ignore
  public void testNestedTypeInChoice() {

    //  Extension.value
    //  ElementDefinition: extension.valueElementDefinition-> extension.valueElementDefinition.fixedElementDefinition
    //  ElementDefinition: extension.valueElementDefinition-> extension.valueElementDefinition.example.valueElementDefinition (indirect)
  }

  @Test
  public void testExtensions() {
    final StructType extensionSchema = converter_L2
        .resourceSchema(Condition.class);

    // We need to test that:
    // - That there is a global '_extension' of map type field
    // - There is not 'extension' field in any of the structure types
    // - That each struct type has a '_fid' field of INTEGER type

    final MapType extensionsContainerType = (MapType) getField(extensionSchema, true,
        "_extension");
    assertEquals(DataTypes.IntegerType, extensionsContainerType.keyType());
    assertTrue(extensionsContainerType.valueType() instanceof ArrayType);

    traverseSchema(extensionSchema, t -> {
      assertEquals(DataTypes.IntegerType, t.fields()[t.fieldIndex("_fid")].dataType());
      assertFieldNotPresent("extension", t);
    });
  }
}
