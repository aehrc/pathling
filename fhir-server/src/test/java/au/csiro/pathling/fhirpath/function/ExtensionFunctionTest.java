/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.IntegerLiteralPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.builders.ResourceDatasetBuilder;
import ca.uhn.fhir.context.FhirContext;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author John Grimes
 */
@SpringBootTest
@Tag("UnitTest")
class ExtensionFunctionTest {

  private static final StructType SIMPLE_EXTENSION_TYPE = DataTypes
      .createStructType(new StructField[]{
          DataTypes.createStructField("id", DataTypes.StringType, false),
          DataTypes.createStructField("url", DataTypes.StringType, true),
          DataTypes.createStructField("valueString", DataTypes.StringType, true),
          DataTypes.createStructField("_fid", DataTypes.IntegerType, false)
      });


  @Autowired
  private SparkSession spark;

  @Autowired
  private FhirContext fhirContext;
  private ResourceReader mockReader;

  @BeforeEach
  void setUp() {
    mockReader = mock(ResourceReader.class);
  }

  @Test
  public void testExtensionOnResources() {

    final Dataset<Row> patientDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withColumn("_fid", DataTypes.IntegerType)
        .withColumn("_extension", DataTypes
            .createMapType(DataTypes.IntegerType, DataTypes.createArrayType(SIMPLE_EXTENSION_TYPE)))
        .withRow("patient-1", "female", true, 1, ImmutableMap.builder()
            .put(1, Arrays.asList(
                RowFactory.create("ext1", "uuid:someExtension", "string1", 100),
                RowFactory.create("ext2", "uuid:someExtension", "string2", 101),
                RowFactory.create("ext3", "uuid:myExtension", "string3", 102),
                RowFactory.create("ext4", "uuid:myExtension", "string4", 103)
            ))
            .build())
        .withRow("patient-2", "female", false, 1, ImmutableMap.builder()
            .put(1, Arrays.asList(
                RowFactory.create("ext1", "uuid:otherExtension", "string1", 200),
                RowFactory.create("ext2", "uuid:myExtension", "string2", 201)
            ))
            .build())
        .withRow("patient-3", "male", false, 1, ImmutableMap.builder()
            .put(1, Arrays.asList(
                RowFactory.create("ext1", "uuid:otherExtension", "string1", 300),
                RowFactory.create("ext2", "uuid:someExtension", "string2", 301)
            ))
            .build())
        .withRow("patient-4", "male", false, 1, ImmutableMap.builder()
            .put(2, Collections.singletonList(
                RowFactory.create("ext1", "uuid:myExtension", "string1", 400)
            ))
            .build())
        .withRow("patient-5", "male", true, 1, null)
        .build();
    when(mockReader.read(ResourceType.PATIENT))
        .thenReturn(patientDataset);
    final ResourcePath inputPath = ResourcePath
        .build(fhirContext, mockReader, ResourceType.PATIENT, "Patient", false);

    final StringLiteralPath argumentExpression = StringLiteralPath
        .fromString("'" + "uuid:myExtension" + "'", inputPath);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .idColumn(inputPath.getIdColumn())
        .groupingColumns(Collections.singletonList(inputPath.getIdColumn()))
        .inputExpression("Patient")
        .build();

    final NamedFunctionInput countInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.singletonList(argumentExpression));

    final NamedFunction count = NamedFunction.getInstance("extension");
    final FhirPath result = count.invoke(countInput);

    assertThat(result)
        .hasExpression("extension('uuid:myExtension')")
        .isNotSingular()
        .isElementPath(ElementPath.class)
        .hasFhirType(FHIRDefinedType.EXTENSION)
        .selectOrderedResult()
        .hasRows(
            // Multiple extensions of requireds type present on the resource
            RowFactory.create("patient-1", null),
            RowFactory.create("patient-1", null),
            RowFactory
                .create("patient-1", RowFactory.create("ext3", "uuid:myExtension", "string3", 102)),
            RowFactory
                .create("patient-1", RowFactory.create("ext4", "uuid:myExtension", "string4", 103)),
            // A single extension of the required type present on the resource
            RowFactory.create("patient-2", null),
            RowFactory
                .create("patient-2", RowFactory.create("ext2", "uuid:myExtension", "string2", 201)),
            // Not extensions of required type present
            RowFactory.create("patient-3", null),
            RowFactory.create("patient-3", null),
            // Extension of required type present but not on the resource
            RowFactory.create("patient-4", null),
            // No extensions present all together
            RowFactory.create("patient-5", null)
        );
  }


  @Test
  public void throwsErrorIfArgumentIsNotString() {
    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .build();
    final IntegerLiteralPath argument = IntegerLiteralPath.fromString("4", input);

    final ParserContext context = new ParserContextBuilder(spark, fhirContext)
        .build();

    final NamedFunctionInput extensionInput = new NamedFunctionInput(context, input,
        Collections.singletonList(argument));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new ExtensionFunction().invoke(extensionInput));
    assertEquals("extension function must have argument of type String literal: .extension(4)",
        error.getMessage());
  }

  @Test
  public void throwsErrorIfMoreThanOneArgument() {
    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .build();
    final StringLiteralPath argument1 = StringLiteralPath.fromString("'foo'", input),
        argument2 = StringLiteralPath.fromString("'bar'", input);

    final ParserContext context = new ParserContextBuilder(spark, fhirContext)
        .terminologyClientFactory(mock(TerminologyServiceFactory.class))
        .build();

    final NamedFunctionInput extensionInput = new NamedFunctionInput(context, input,
        Arrays.asList(argument1, argument2));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new ExtensionFunction().invoke(extensionInput));
    assertEquals("extension function must have one argument: .extension('foo', 'bar')",
        error.getMessage());
  }
}