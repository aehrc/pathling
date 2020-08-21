/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.fixtures.StringPrimitiveRowFixture.*;
import static au.csiro.pathling.test.helpers.SparkHelpers.getSparkSession;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.element.StringPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.fixtures.PatientResourceRowFixture;
import au.csiro.pathling.test.fixtures.StringPrimitiveRowFixture;
import au.csiro.pathling.test.helpers.FhirHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author Piotr Szul
 */
@Tag("UnitTest")
@Disabled
public class FirstFunctionTest {

  private SparkSession spark;
  private ResourceReader mockReader;
  private FhirContext fhirContext;

  @BeforeEach
  public void setUp() {
    fhirContext = FhirHelpers.getFhirContext();
    spark = getSparkSession();
    mockReader = mock(ResourceReader.class);
  }

  @Test
  public void testGetsFirstResourceCorrectly() {
    when(mockReader.read(ResourceType.PATIENT))
        .thenReturn(PatientResourceRowFixture.createCompleteDataset(spark));
    final ResourcePath input = ResourcePath
        .build(fhirContext, mockReader, ResourceType.PATIENT, "", false);
    final ParserContext parserContext = new ParserContextBuilder().build();

    final NamedFunctionInput firstInput = new NamedFunctionInput(parserContext, input,
        Collections.emptyList());
    final NamedFunction firstFunction = NamedFunction.getInstance("first");

    final FhirPath result = firstFunction.invoke(firstInput);
    assertThat(result)
        .isResourcePath()
        .hasResourceType(ResourceType.PATIENT)
        .selectResult()
        .hasRows(PatientResourceRowFixture.PATIENT_ALL_ROWS);
  }

  @Test
  public void testSelectsFirstValuePerResourceFromListOfValues() {
    final ElementPath input = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(StringPrimitiveRowFixture.createCompleteDataset(spark))
        .build();
    final ParserContext parserContext = new ParserContextBuilder().build();

    final NamedFunctionInput firstInput = new NamedFunctionInput(parserContext, input,
        Collections.emptyList());
    final NamedFunction firstFunction = NamedFunction.getInstance("first");
    final FhirPath result = firstFunction.invoke(firstInput);

    final List<Row> expectedRows =
        Arrays.asList(STRING_1_JUDE, STRING_2_SAMUEL, STRING_3_NULL, STRING_4_ADAM, STRING_5_NULL);
    assertThat(result)
        .isElementPath(StringPath.class)
        .isSingular()
        .selectResult()
        .hasRows(expectedRows);
  }

  @Test
  public void inputMustNotContainArguments() {
    final ElementPath input = new ElementPathBuilder().build();
    final StringLiteralPath argument = StringLiteralPath
        .fromString("'some argument'", mock(FhirPath.class));

    final ParserContext parserContext = new ParserContextBuilder().build();
    final NamedFunctionInput firstInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final NamedFunction firstFunction = NamedFunction.getInstance("first");
    final InvalidRequestException error = assertThrows(
        InvalidRequestException.class,
        () -> firstFunction.invoke(firstInput));
    assertEquals(
        "Arguments can not be passed to first function: first('some argument')",
        error.getMessage());
  }

}
