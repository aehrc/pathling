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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.PrimitivePath;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
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
 * @author John Grimes
 */
@SpringBootUnitTest
class CountFunctionTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @MockBean
  DataSource dataSource;

  @Test
  void countsByResourceIdentity() {
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
    final ResourceCollection inputPath = ResourceCollection
        .build(fhirContext, dataSource, ResourceType.PATIENT, "Patient");

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .idColumn(inputPath.getIdColumn())
        .groupingColumns(Collections.singletonList(inputPath.getIdColumn()))
        .inputExpression("Patient")
        .build();
    final NamedFunctionInput countInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.emptyList());
    final NamedFunction count = NamedFunction.getInstance("count");
    final Collection result = count.invoke(countInput);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.LongType)
        .withRow("patient-1", 1L)
        .withRow("patient-2", 1L)
        .withRow("patient-3", 1L)
        .build();

    assertThat(result)
        .hasExpression("count()")
        .isSingular()
        .isElementPath(IntegerCollection.class)
        .hasFhirType(FHIRDefinedType.UNSIGNEDINT)
        .selectOrderedResult()
        .hasRows(expectedDataset);
  }

  @Test
  void countsByGrouping() {
    final Dataset<Row> inputDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withRow("patient-1", "female", true)
        .withRow("patient-2", "female", false)
        .withRow("patient-2", "male", true)
        .build();
    when(dataSource.read(ResourceType.PATIENT)).thenReturn(inputDataset);
    final ResourceCollection inputPath = new ResourcePathBuilder(spark)
        .database(dataSource)
        .resourceType(ResourceType.PATIENT)
        .expression("Patient")
        .build();
    final Column groupingColumn = inputPath.getElementColumn("gender").orElseThrow();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .groupingColumns(Collections.singletonList(groupingColumn))
        .inputExpression("Patient")
        .build();
    final NamedFunctionInput countInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.emptyList());
    final NamedFunction count = NamedFunction.getInstance("count");
    final Collection result = count.invoke(countInput);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withColumn(DataTypes.StringType)
        .withColumn(DataTypes.LongType)
        .withRow("female", 2L)
        .withRow("male", 1L)
        .build();

    assertThat(result)
        .hasExpression("count()")
        .isSingular()
        .isElementPath(IntegerCollection.class)
        .hasFhirType(FHIRDefinedType.UNSIGNEDINT)
        .selectGroupingResult(Collections.singletonList(groupingColumn))
        .hasRows(expectedDataset);

  }

  @Test
  void doesNotCountNullElements() {
    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withRow("patient-1", "female")
        .withRow("patient-2", null)
        .withRow("patient-3", "male")
        .build();
    final PrimitivePath inputPath = new ElementPathBuilder(spark)
        .expression("gender")
        .fhirType(FHIRDefinedType.CODE)
        .dataset(dataset)
        .idAndValueColumns()
        .build();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .idColumn(inputPath.getIdColumn())
        .groupingColumns(Collections.emptyList())
        .build();
    final NamedFunctionInput countInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.emptyList());
    final NamedFunction count = NamedFunction.getInstance("count");
    final Collection result = count.invoke(countInput);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.LongType)
        .withRow("patient-1", 2L)
        .build();

    assertThat(result)
        .hasExpression("gender.count()")
        .isSingular()
        .isElementPath(IntegerCollection.class)
        .hasFhirType(FHIRDefinedType.UNSIGNEDINT)
        .selectOrderedResult()
        .hasRows(expectedDataset);
  }

  @Test
  void inputMustNotContainArguments() {
    final PrimitivePath inputPath = new ElementPathBuilder(spark).build();
    final PrimitivePath argumentPath = new ElementPathBuilder(spark).build();
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();

    final NamedFunctionInput countInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.singletonList(argumentPath));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> NamedFunction.getInstance("count").invoke(countInput));
    assertEquals("Arguments can not be passed to count function", error.getMessage());
  }
}
