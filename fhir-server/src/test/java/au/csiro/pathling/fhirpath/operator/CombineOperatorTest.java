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

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
import static au.csiro.pathling.test.helpers.SparkHelpers.codingStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowFromCoding;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.CodingPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.element.IntegerPath;
import au.csiro.pathling.fhirpath.literal.CodingLiteralPath;
import au.csiro.pathling.fhirpath.literal.IntegerLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.builders.ResourcePathBuilder;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
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
class CombineOperatorTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  ParserContext parserContext;
  String idColumnName;

  @BeforeEach
  void setUp() {
    final Dataset<Row> input = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.BooleanType)
        .withIdsAndValue(null, Arrays.asList("observation-1", "observation-2", "observation-3"))
        .build();
    final ResourcePath inputContext = new ResourcePathBuilder(spark)
        .resourceType(ResourceType.OBSERVATION)
        .dataset(input)
        .idAndValueColumns()
        .buildCustom();
    parserContext = new ParserContextBuilder(spark, fhirContext)
        .inputContext(inputContext)
        .build();
    idColumnName = parserContext.getInputContext().getIdColumn().toString();
  }

  @Test
  void returnsCorrectResult() {
    final Dataset<Row> leftDataset = new DatasetBuilder(spark)
        .withIdColumn(idColumnName)
        .withEidColumn()
        .withColumn(DataTypes.IntegerType)
        .withRow("observation-1", makeEid(0), 3)
        .withRow("observation-1", makeEid(1), 5)
        .withRow("observation-1", makeEid(2), 7)
        .withRow("observation-2", null, null)
        .withRow("observation-3", makeEid(0), -1)
        .build();
    final ElementPath left = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.INTEGER)
        .dataset(leftDataset)
        .idAndEidAndValueColumns()
        .expression("valueInteger")
        .singular(false)
        .build();
    final Dataset<Row> rightDataset = new DatasetBuilder(spark)
        .withIdColumn(idColumnName)
        .withEidColumn()
        .withColumn(DataTypes.IntegerType)
        .withRow("observation-1", makeEid(0), 2)
        .withRow("observation-1", makeEid(1), 4)
        .withRow("observation-2", null, null)
        .withRow("observation-3", makeEid(0), 14)
        .build();
    final ElementPath right = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.INTEGER)
        .dataset(rightDataset)
        .idAndEidAndValueColumns()
        .expression("valueInteger")
        .singular(false)
        .build();

    final OperatorInput combineInput = new OperatorInput(parserContext, left, right);
    final FhirPath result = Operator.getInstance("combine").invoke(combineInput);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.IntegerType)
        .withRow("observation-1", 3)
        .withRow("observation-1", 5)
        .withRow("observation-1", 7)
        .withRow("observation-1", 2)
        .withRow("observation-1", 4)
        .withRow("observation-2", null)
        .withRow("observation-2", null)
        .withRow("observation-3", -1)
        .withRow("observation-3", 14)
        .build();
    assertThat(result)
        .hasExpression("valueInteger combine valueInteger")
        .isNotSingular()
        .isElementPath(IntegerPath.class)
        .selectResult()
        .hasRowsUnordered(expectedDataset);
  }

  @Test
  void worksWithDifferentCombinableTypes() {
    final Dataset<Row> leftDataset = new DatasetBuilder(spark)
        .withIdColumn(idColumnName)
        .withEidColumn()
        .withColumn(DataTypes.IntegerType)
        .withRow("observation-1", makeEid(0), 3)
        .withRow("observation-1", makeEid(1), 5)
        .withRow("observation-1", makeEid(2), 7)
        .withRow("observation-2", null, null)
        .withRow("observation-3", makeEid(0), -1)
        .build();
    final ElementPath left = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.INTEGER)
        .dataset(leftDataset)
        .idAndEidAndValueColumns()
        .expression("valueInteger")
        .singular(false)
        .build();
    final IntegerLiteralPath right = IntegerLiteralPath
        .fromString("99", parserContext.getInputContext());

    final OperatorInput combineInput = new OperatorInput(parserContext, left, right);
    final FhirPath result = Operator.getInstance("combine").invoke(combineInput);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.IntegerType)
        .withRow("observation-1", 3)
        .withRow("observation-1", 5)
        .withRow("observation-1", 7)
        .withRow("observation-1", 99)
        .withRow("observation-2", null)
        .withRow("observation-2", 99)
        .withRow("observation-3", -1)
        .withRow("observation-3", 99)
        .build();
    assertThat(result)
        .hasExpression("valueInteger combine 99")
        .isNotSingular()
        .isElementPath(IntegerPath.class)
        .selectResult()
        .hasRowsUnordered(expectedDataset);
  }

  @Test
  void worksWithLiteralAndNonLiteralCodingValues() {
    final Dataset<Row> leftDataset = new DatasetBuilder(spark)
        .withIdColumn(idColumnName)
        .withEidColumn()
        .withStructTypeColumns(codingStructType())
        .withRow("observation-1", makeEid(0),
            rowFromCoding(new Coding("http://snomed.info/sct", "18001011000036104", null)))
        .buildWithStructValue();
    final ElementPath left = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODING)
        .dataset(leftDataset)
        .idAndEidAndValueColumns()
        .expression("valueCoding")
        .singular(false)
        .build();
    final CodingLiteralPath right = CodingLiteralPath
        .fromString("http://snomed.info/sct|373882004", parserContext.getInputContext());

    final OperatorInput combineInput = new OperatorInput(parserContext, left, right);
    final FhirPath result = Operator.getInstance("combine").invoke(combineInput);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn(idColumnName)
        .withStructTypeColumns(codingStructType())
        .withRow("observation-1",
            rowFromCoding(new Coding("http://snomed.info/sct", "18001011000036104", null)))
        .withRow("observation-1",
            rowFromCoding(new Coding("http://snomed.info/sct", "373882004", null)))
        .withRow("observation-2",
            rowFromCoding(new Coding("http://snomed.info/sct", "373882004", null)))
        .withRow("observation-3",
            rowFromCoding(new Coding("http://snomed.info/sct", "373882004", null)))
        .buildWithStructValue();
    assertThat(result)
        .hasExpression("valueCoding combine http://snomed.info/sct|373882004")
        .isNotSingular()
        .isElementPath(CodingPath.class)
        .selectResult()
        .hasRowsUnordered(expectedDataset);
  }

  @Test
  void throwsErrorIfInputsAreNotCombinable() {
    final ElementPath left = new ElementPathBuilder(spark)
        .expression("valueInteger")
        .fhirType(FHIRDefinedType.INTEGER)
        .build();
    final ElementPath right = new ElementPathBuilder(spark)
        .expression("valueString")
        .fhirType(FHIRDefinedType.STRING)
        .build();

    final OperatorInput combineInput = new OperatorInput(parserContext, left, right);
    final Operator combineOperator = Operator.getInstance("combine");

    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> combineOperator.invoke(combineInput));
    assertEquals(
        "Paths cannot be merged into a collection together: valueInteger, valueString",
        error.getMessage());
  }

}
