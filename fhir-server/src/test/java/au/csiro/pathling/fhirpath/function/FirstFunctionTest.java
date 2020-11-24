/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.element.StringPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import java.util.Collections;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author Piotr Szul
 */
@Tag("UnitTest")
public class FirstFunctionTest {

  @Test
  public void firstOfUngroupedElements() {

    // Check the result.
    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn(DataTypes.StringType)
        .withRow("Patient/abc1", "Jude")   // when: "two values"  expect: "Jude"
        .withRow("Patient/abc1", "Mark")
        .withRow("Patient/abc2", "Samuel") // when: "single value" expect: "Samuel"
        .withRow("Patient/abc3", null)     // when: "leading null" expect: "Adam"
        .withRow("Patient/abc3", "Adam")
        .withRow("Patient/abc4", "John")  // when: "trailing null" expect: "John"
        .withRow("Patient/abc4", null)
        .withRow("Patient/abc5", null)    // when: "single null" expect: null
        .withRow("Patient/abc6", null)    // when: "many nulls" expect: null
        .withRow("Patient/abc6", null)
        .build();

    final ElementPath input = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(inputDataset)
        .idAndValueColumns()
        .expression("Patient.name")
        .build();

    final ParserContext parserContext = new ParserContextBuilder()
        .inputExpression("Patient.name")
        .build();

    final NamedFunctionInput firstInput = new NamedFunctionInput(parserContext, input,
        Collections.emptyList());

    final NamedFunction firstFunction = NamedFunction.getInstance("first");
    final FhirPath result = firstFunction.invoke(firstInput);

    assertTrue(result instanceof StringPath);
    assertThat((ElementPath) result)
        .hasExpression("first()")
        .isSingular()
        .hasFhirType(FHIRDefinedType.STRING);

    // expected result dataset
    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn(DataTypes.StringType)
        .withRow("Patient/abc1", "Jude")
        .withRow("Patient/abc2", "Samuel")
        .withRow("Patient/abc3", "Adam")
        .withRow("Patient/abc4", "John")
        .withRow("Patient/abc5", null)
        .withRow("Patient/abc6", null)
        .build();

    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void inputMustNotContainArguments() {
    final ElementPath inputPath = new ElementPathBuilder().build();
    final ElementPath argumentPath = new ElementPathBuilder().build();
    final ParserContext parserContext = new ParserContextBuilder().build();

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
