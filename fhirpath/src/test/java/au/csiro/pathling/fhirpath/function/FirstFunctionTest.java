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
import static au.csiro.pathling.utilities.Strings.randomAlias;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.element.StringPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.builders.ResourceDatasetBuilder;
import au.csiro.pathling.test.builders.ResourcePathBuilder;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

/**
 * @author Piotr Szul
 */
@SpringBootUnitTest
class FirstFunctionTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @MockBean
  DataSource dataSource;

  @Test
  void firstOfRootResources() {

    final Dataset<Row> patientDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withRow("patient-1", "female", true)
        .withRow("patient-2", "female", false)
        .withRow("patient-3", "male", true)
        .build();
    when(dataSource.read(ResourceType.PATIENT))
        .thenReturn(patientDataset);
    final ResourcePath inputPath = ResourcePath
        .build(fhirContext, dataSource, ResourceType.PATIENT, "Patient", true);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .groupingColumns(Collections.singletonList(inputPath.getIdColumn()))
        .build();

    final NamedFunctionInput firstInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.emptyList());
    final NamedFunction firstFunction = NamedFunction.getInstance("first");
    final FhirPath result = firstFunction.invoke(firstInput);

    assertTrue(result instanceof ResourcePath);
    assertThat((ResourcePath) result)
        .hasExpression("Patient.first()")
        .isSingular()
        .hasResourceType(ResourceType.PATIENT);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", "patient-1")
        .withRow("patient-2", "patient-2")
        .withRow("patient-3", "patient-3")
        .build();

    assertThat(result)
        .selectOrderedResult()
        .hasRows(expectedDataset);
  }

  @Test
  void firstOfUngroupedSubResources() {

    final String subresourceId = randomAlias();
    final String statusColumn = randomAlias();
    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(subresourceId, DataTypes.StringType)
        .withColumn(statusColumn, DataTypes.StringType)
        .withRow("patient-1", makeEid(2), "Encounter/5", "in-progress")
        .withRow("patient-1", makeEid(1), "Encounter/1", "in-progress")
        .withRow("patient-1", makeEid(0), "Encounter/2", "finished")
        .withRow("patient-2", makeEid(0), "Encounter/3", "in-progress")
        .withRow("patient-3", null, null, null)
        .build();
    final ResourcePath inputPath = new ResourcePathBuilder(spark)
        .expression("reverseResolve(Encounter.subject)")
        .dataset(inputDataset)
        .idEidAndValueColumns()
        .valueColumn(inputDataset.col(subresourceId))
        .resourceType(ResourceType.ENCOUNTER)
        .buildCustom();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .groupingColumns(Collections.singletonList(inputPath.getIdColumn()))
        .build();

    final NamedFunctionInput firstInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.emptyList());
    final NamedFunction firstFunction = NamedFunction.getInstance("first");
    final FhirPath result = firstFunction.invoke(firstInput);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withIdColumn()
        .withRow("patient-1", null, "Encounter/2")
        .withRow("patient-2", null, "Encounter/3")
        .withRow("patient-3", null, null)
        .build();

    assertThat(result)
        .isResourcePath()
        .hasExpression("reverseResolve(Encounter.subject).first()")
        .isSingular()
        .hasResourceType(ResourceType.ENCOUNTER)
        .selectOrderedResultWithEid()
        .hasRows(expectedDataset);
  }

  @Test
  void firstOfUngroupedElements() {

    // Check the result.
    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", makeEid(0, 3), "Jude")   // when: "two values"  expect: "Jude"
        .withRow("patient-1", makeEid(0, 2), "Mark")
        .withRow("patient-1", makeEid(0, 1), "Mark")
        .withRow("patient-1", makeEid(0, 0), "Zaak")
        .withRow("patient-2", makeEid(0, 0), "Samuel") // when: "single value" expect: "Samuel"
        .withRow("patient-3", makeEid(0, 1), "Adam") // when: "leading null" expect: "Adam"
        .withRow("patient-3", makeEid(0, 0), null)
        .withRow("patient-4", makeEid(0, 1), null) // when: "trailing null" expect: "John
        .withRow("patient-4", makeEid(0, 0), "John")
        .withRow("patient-5", null, null)    // when: "single null" expect: null
        .withRow("patient-6", null, null)    // when: "many nulls" expect: null
        .withRow("patient-6", null, null)
        .build();

    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.STRING)
        .dataset(inputDataset)
        .idAndEidAndValueColumns()
        .expression("name")
        .build();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .groupingColumns(Collections.singletonList(input.getIdColumn()))
        .build();

    final NamedFunctionInput firstInput = new NamedFunctionInput(parserContext, input,
        Collections.emptyList());

    final NamedFunction firstFunction = NamedFunction.getInstance("first");
    final FhirPath result = firstFunction.invoke(firstInput);

    assertTrue(result instanceof StringPath);
    assertThat((ElementPath) result)
        .hasExpression("name.first()")
        .isSingular()
        .hasFhirType(FHIRDefinedType.STRING);

    // expected result dataset
    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", null, "Zaak")
        .withRow("patient-2", null, "Samuel")
        .withRow("patient-3", null, "Adam")
        .withRow("patient-4", null, "John")
        .withRow("patient-5", null, null)
        .withRow("patient-6", null, null)
        .build();

    assertThat(result)
        .selectOrderedResultWithEid()
        .hasRows(expectedDataset);
  }

  @Test
  void illegalToCallFirstOnGrouping() {
    final Dataset<Row> inputDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withRow("patient-1", "female", true)
        .withRow("patient-2", "female", false)
        .withRow("patient-2", "male", true)
        .build();
    when(dataSource.read(ResourceType.PATIENT)).thenReturn(inputDataset);
    final ResourcePath inputPath = new ResourcePathBuilder(spark)
        .database(dataSource)
        .resourceType(ResourceType.PATIENT)
        .expression("Patient")
        .build();

    final Column groupingColumn = inputPath.getElementColumn("gender").orElseThrow();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .groupingColumns(Collections.singletonList(groupingColumn))
        .inputExpression("Patient")
        .build();
    final NamedFunctionInput firstInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.emptyList());
    final NamedFunction firstFunction = NamedFunction.getInstance("first");

    final IllegalStateException error = assertThrows(
        IllegalStateException.class,
        () -> firstFunction.invoke(firstInput));
    assertEquals(
        "Orderable path expected",
        error.getMessage());
  }

  @Test
  void inputMustNotContainArguments() {
    final ElementPath inputPath = new ElementPathBuilder(spark).build();
    final ElementPath argumentPath = new ElementPathBuilder(spark).build();
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();

    final NamedFunctionInput firstInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.singletonList(argumentPath));

    final NamedFunction firstFunction = NamedFunction.getInstance("first");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> firstFunction.invoke(firstInput));
    assertEquals(
        "Arguments can not be passed to first function",
        error.getMessage());
  }
}
