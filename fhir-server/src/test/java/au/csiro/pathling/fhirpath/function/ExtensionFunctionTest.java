/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
import static au.csiro.pathling.test.fixtures.ExtensionFixture.MANY_EXT_ROW_1;
import static au.csiro.pathling.test.fixtures.ExtensionFixture.MANY_EXT_ROW_2;
import static au.csiro.pathling.test.fixtures.ExtensionFixture.MANY_MY_EXTENSIONS;
import static au.csiro.pathling.test.fixtures.ExtensionFixture.NO_MY_EXTENSIONS;
import static au.csiro.pathling.test.fixtures.ExtensionFixture.ONE_EXT_ROW_1;
import static au.csiro.pathling.test.fixtures.ExtensionFixture.ONE_MY_EXTENSION;
import static au.csiro.pathling.test.fixtures.ExtensionFixture.oneEntryMap;
import static au.csiro.pathling.test.fixtures.ExtensionFixture.toElementDataset;
import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.IntegerLiteralPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.builders.ResourceDatasetBuilder;
import au.csiro.pathling.test.helpers.FhirHelpers;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import java.util.Collections;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
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

  @Autowired
  private SparkSession spark;

  @Autowired
  private FhirContext fhirContext;
  private Database database;

  @BeforeEach
  void setUp() {
    database = mock(Database.class);
  }

  @Test
  public void testExtensionOnResources() {

    final Dataset<Row> patientDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withFidColumn()
        .withExtensionColumn()
        .withRow("patient-1", "female", true, 1, oneEntryMap(1, MANY_MY_EXTENSIONS))
        .withRow("patient-2", "female", false, 1, oneEntryMap(1, ONE_MY_EXTENSION))
        .withRow("patient-3", "male", false, 1, oneEntryMap(1, NO_MY_EXTENSIONS))
        .withRow("patient-4", "male", false, 1, oneEntryMap(2, ONE_MY_EXTENSION))
        .withRow("patient-5", "male", true, 1, null)
        .build();
    when(database.read(ResourceType.PATIENT))
        .thenReturn(patientDataset);
    final ResourcePath inputPath = ResourcePath
        .build(fhirContext, database, ResourceType.PATIENT, "Patient", false);

    final StringLiteralPath argumentExpression = StringLiteralPath
        .fromString("'" + "uuid:myExtension" + "'", inputPath);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();

    final NamedFunctionInput extensionInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.singletonList(argumentExpression));

    final NamedFunction extension = NamedFunction.getInstance("extension");
    final FhirPath result = extension.invoke(extensionInput);

    assertThat(result)
        .hasExpression("Patient.extension('uuid:myExtension')")
        .isNotSingular()
        .isElementPath(ElementPath.class)
        .hasFhirType(FHIRDefinedType.EXTENSION)
        .selectOrderedResult()
        .hasRows(
            // Multiple extensions of required type present on the resource
            RowFactory.create("patient-1", null),
            RowFactory.create("patient-1", null),
            RowFactory.create("patient-1", MANY_EXT_ROW_1),
            RowFactory.create("patient-1", MANY_EXT_ROW_2),
            // A single extension of the required type present on the resource
            RowFactory.create("patient-2", null),
            RowFactory.create("patient-2", ONE_EXT_ROW_1),
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
  public void testExtensionOnElements() {

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();

    // Construct element dataset from the resource dataset so that the resource path
    // can be used as the current resource for this element path
    // Note: this resource path is not singular as this will be a base for elements.

    final Dataset<Row> resourceLikeDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withStructColumn("name", DataTypes.StringType)
        .withStructColumn("_fid", DataTypes.IntegerType)
        .withStructValueColumn()
        .withExtensionColumn()
        .withRow("observation-1", makeEid(0), RowFactory.create("name1", 0),
            oneEntryMap(0, MANY_MY_EXTENSIONS))
        .withRow("observation-2", makeEid(0), RowFactory.create("name2", 1),
            oneEntryMap(1, ONE_MY_EXTENSION))
        .withRow("observation-3", makeEid(0), RowFactory.create("name3", 2),
            oneEntryMap(2, NO_MY_EXTENSIONS))
        .withRow("observation-4", makeEid(0), RowFactory.create("name4", 3),
            oneEntryMap(3, ONE_MY_EXTENSION))
        .withRow("observation-4", makeEid(1), RowFactory.create("name5", 4),
            oneEntryMap(3, ONE_MY_EXTENSION))
        .withRow("observation-5", makeEid(0), null, null)
        .withRow("observation-5", makeEid(1), null, null)
        .build();

    when(database.read(ResourceType.OBSERVATION))
        .thenReturn(resourceLikeDataset);
    final ResourcePath baseResourcePath = ResourcePath
        .build(fhirContext, database, ResourceType.OBSERVATION, "Observation", false);

    final Dataset<Row> elementDataset = toElementDataset(resourceLikeDataset, baseResourcePath);

    final ElementDefinition codeDefinition = checkPresent(FhirHelpers
        .getChildOfResource(fhirContext, "Observation", "code"));

    final ElementPath inputPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .definition(codeDefinition)
        .dataset(elementDataset)
        .idAndEidAndValueColumns()
        .expression("code")
        .singular(false)
        .currentResource(baseResourcePath)
        .buildDefined();

    final StringLiteralPath argumentExpression = StringLiteralPath
        .fromString("'" + "uuid:myExtension" + "'", inputPath);

    final NamedFunctionInput extensionInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.singletonList(argumentExpression));

    final NamedFunction extension = NamedFunction.getInstance("extension");
    final FhirPath result = extension.invoke(extensionInput);

    final Dataset<Row> expectedResult = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withStructTypeColumns(DatasetBuilder.SIMPLE_EXTENSION_TYPE)
        .withRow("observation-1", makeEid(0, 0), null)
        .withRow("observation-1", makeEid(0, 1), null)
        .withRow("observation-1", makeEid(0, 2), MANY_EXT_ROW_1)
        .withRow("observation-1", makeEid(0, 3), MANY_EXT_ROW_2)
        .withRow("observation-2", makeEid(0, 0), null)
        .withRow("observation-2", makeEid(0, 1), ONE_EXT_ROW_1)
        .withRow("observation-3", makeEid(0, 0), null)
        .withRow("observation-3", makeEid(0, 1), null)
        .withRow("observation-4", makeEid(0, 0), null)
        .withRow("observation-4", makeEid(0, 1), ONE_EXT_ROW_1)
        .withRow("observation-4", makeEid(1, 0), null)
        .withRow("observation-5", makeEid(0, 0), null)
        .withRow("observation-5", makeEid(1, 0), null)
        .buildWithStructValue();

    assertThat(result)
        .hasExpression("code.extension('uuid:myExtension')")
        .isNotSingular()
        .isElementPath(ElementPath.class)
        .hasFhirType(FHIRDefinedType.EXTENSION)
        .selectOrderedResultWithEid()
        .hasRows(expectedResult);
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
