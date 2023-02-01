/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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
import static au.csiro.pathling.test.helpers.SparkHelpers.referenceStructType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.UntypedResourcePath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.query.DataSource;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.builders.ResourceDatasetBuilder;
import au.csiro.pathling.test.builders.ResourcePathBuilder;
import au.csiro.pathling.test.builders.UntypedResourcePathBuilder;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import java.util.Collections;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

/**
 * @author John Grimes
 */
@SpringBootUnitTest
class OfTypeFunctionTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @MockBean
  DataSource database;
  
  @Test
  void resolvesPolymorphicReference() {
    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withStructTypeColumns(referenceStructType())
        .withRow("encounter-1", makeEid(1), RowFactory.create(null, "Patient/patient-1", null))
        .withRow("encounter-1", makeEid(0), RowFactory.create(null, "Patient/patient-2", null))
        .withRow("encounter-2", makeEid(0), RowFactory.create(null, "Patient/patient-3", null))
        .withRow("encounter-2", makeEid(1), RowFactory.create(null, "Group/group-1", null))
        .withRow("encounter-3", makeEid(0), RowFactory.create(null, "Patient/patient-2", null))
        .withRow("encounter-4", makeEid(0), RowFactory.create(null, "Patient/patient-2", null))
        .withRow("encounter-5", makeEid(0), RowFactory.create(null, "Group/group-1", null))
        .withRow("encounter-6", null, null)
        .buildWithStructValue();
    final UntypedResourcePath inputPath = new UntypedResourcePathBuilder(spark)
        .expression("subject.resolve()")
        .dataset(inputDataset)
        .idEidAndValueColumns()
        .singular(false)
        .build();

    final Dataset<Row> argumentDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.StringType)
        .withColumn(DataTypes.BooleanType)
        .withRow("patient-1", "female", true)
        .withRow("patient-2", "female", false)
        .withRow("patient-3", "male", true)
        .build();
    when(database.read(eq(ResourceType.PATIENT)))
        .thenReturn(argumentDataset);
    final ResourcePath argumentPath = new ResourcePathBuilder(spark)
        .database(database)
        .resourceType(ResourceType.PATIENT)
        .expression("Patient")
        .build();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .idColumn(inputPath.getIdColumn())
        .database(database)
        .build();
    final NamedFunctionInput ofTypeInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.singletonList(argumentPath));
    final NamedFunction ofType = NamedFunction.getInstance("ofType");
    final FhirPath result = ofType.invoke(ofTypeInput);

    assertTrue(result instanceof ResourcePath);
    assertThat((ResourcePath) result)
        .hasExpression("subject.resolve().ofType(Patient)")
        .isNotSingular()
        .hasResourceType(ResourceType.PATIENT);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withIdColumn() // this represents value for a resource
        .withRow("encounter-1", makeEid(0), "patient-2")
        .withRow("encounter-1", makeEid(1), "patient-1")
        .withRow("encounter-2", makeEid(0), "patient-3")
        .withRow("encounter-2", makeEid(1), null)
        .withRow("encounter-3", makeEid(0), "patient-2")
        .withRow("encounter-4", makeEid(0), "patient-2")
        .withRow("encounter-5", makeEid(0), null)
        .withRow("encounter-6", null, null)
        .build();
    assertThat(result)
        .selectOrderedResultWithEid()
        .hasRows(expectedDataset);
  }

  @Test
  void throwsErrorIfInputNotPolymorphic() {
    final ResourcePath input = new ResourcePathBuilder(spark)
        .expression("Patient")
        .build();
    final ResourcePath argument = new ResourcePathBuilder(spark).build();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final NamedFunctionInput ofTypeInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final NamedFunction ofTypeFunction = NamedFunction.getInstance("ofType");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> ofTypeFunction.invoke(ofTypeInput));
    assertEquals(
        "Input to ofType function must be a polymorphic resource type: Patient",
        error.getMessage());
  }

  @Test
  void throwsErrorIfMoreThanOneArgument() {
    final UntypedResourcePath input = new UntypedResourcePathBuilder(spark)
        .expression("subject")
        .build();
    final ResourcePath argument1 = new ResourcePathBuilder(spark)
        .expression("Patient")
        .build();
    final ResourcePath argument2 = new ResourcePathBuilder(spark)
        .expression("Condition")
        .build();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final NamedFunctionInput ofTypeInput = new NamedFunctionInput(parserContext, input,
        Arrays.asList(argument1, argument2));

    final NamedFunction ofTypeFunction = NamedFunction.getInstance("ofType");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> ofTypeFunction.invoke(ofTypeInput));
    assertEquals(
        "ofType function must have one argument: subject.ofType(Patient, Condition)",
        error.getMessage());
  }

  @Test
  void throwsErrorIfArgumentNotResource() {
    final UntypedResourcePath input = new UntypedResourcePathBuilder(spark)
        .expression("subject")
        .build();
    final StringLiteralPath argument = StringLiteralPath
        .fromString("'some string'", input);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final NamedFunctionInput ofTypeInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final NamedFunction ofTypeFunction = NamedFunction.getInstance("ofType");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> ofTypeFunction.invoke(ofTypeInput));
    assertEquals("Argument to ofType function must be a resource type: 'some string'",
        error.getMessage());
  }

}
