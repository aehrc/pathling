/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.DecimalPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.element.IntegerPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import ca.uhn.fhir.context.FhirContext;
import java.math.BigDecimal;
import java.util.Collections;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
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
class SumFunctionTest {

  @Autowired
  private SparkSession spark;

  @Autowired
  private FhirContext fhirContext;

  private ParserContext parserContext;

  @BeforeEach
  void setUp() {
    parserContext = new ParserContextBuilder(spark, fhirContext).build();
  }

  @Test
  void returnsCorrectIntegerResult() {
    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.IntegerType)
        .withRow("observation-1", makeEid(0), 3)
        .withRow("observation-1", makeEid(1), 5)
        .withRow("observation-1", makeEid(2), 7)
        .withRow("observation-2", null, null)
        .withRow("observation-3", makeEid(0), -1)
        .withRow("observation-3", makeEid(1), null)
        .build();
    final ElementPath inputPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.INTEGER)
        .dataset(inputDataset)
        .idAndEidAndValueColumns()
        .expression("valueInteger")
        .singular(false)
        .build();

    final NamedFunctionInput sumInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.emptyList());
    final FhirPath result = NamedFunction.getInstance("sum").invoke(sumInput);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.IntegerType)
        .withRow("observation-1", 15)
        .withRow("observation-2", null)
        .withRow("observation-3", -1)
        .build();
    assertThat(result)
        .hasExpression("valueInteger.sum()")
        .isSingular()
        .isElementPath(IntegerPath.class)
        .selectResult()
        .hasRows(expectedDataset);
  }

  @Test
  void returnsCorrectDecimalResult() {
    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.createDecimalType())
        .withRow("observation-1", makeEid(0), new BigDecimal("3.0"))
        .withRow("observation-1", makeEid(1), new BigDecimal("5.5"))
        .withRow("observation-1", makeEid(2), new BigDecimal("7"))
        .withRow("observation-2", null, null)
        .withRow("observation-3", makeEid(0), new BigDecimal("-2.50"))
        .build();
    final ElementPath inputPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.DECIMAL)
        .dataset(inputDataset)
        .idAndEidAndValueColumns()
        .expression("valueDecimal")
        .singular(false)
        .build();

    final NamedFunctionInput sumInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.emptyList());
    final FhirPath result = NamedFunction.getInstance("sum").invoke(sumInput);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.createDecimalType())
        .withRow("observation-1", new BigDecimal("15.5"))
        .withRow("observation-2", null)
        .withRow("observation-3", new BigDecimal("-2.5"))
        .build();
    assertThat(result)
        .hasExpression("valueDecimal.sum()")
        .isSingular()
        .isElementPath(DecimalPath.class)
        .selectResult()
        .hasRows(expectedDataset);
  }

  @Test
  void throwsErrorIfInputNotNumeric() {
    final ElementPath inputPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.STRING)
        .expression("valueString")
        .build();

    final NamedFunctionInput sumInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.emptyList());
    final NamedFunction sumFunction = NamedFunction.getInstance("sum");

    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> sumFunction.invoke(sumInput));
    assertEquals(
        "Input to sum function must be numeric: valueString",
        error.getMessage());
  }

}
