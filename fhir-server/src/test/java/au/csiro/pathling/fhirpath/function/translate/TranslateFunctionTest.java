/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.translate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.function.NamedFunctionInput;
import au.csiro.pathling.fhirpath.literal.IntegerLiteralPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import java.util.Collections;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author John Grimes
 */
@SpringBootTest
@Tag("UnitTest")
class TranslateFunctionTest {

  @Autowired
  private SparkSession spark;

  @Autowired
  private FhirContext fhirContext;

  private static final String MY_VALUE_SET_URL = "https://csiro.au/fhir/ValueSet/my-value-set";
  private static final String TERMINOLOGY_SERVICE_URL = "https://r4.ontoserver.csiro.au/fhir";


  @Test
  public void throwsErrorIfInputTypeIsUnsupported() {
    final FhirPath mockContext = new ElementPathBuilder(spark).build();
    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.STRING)
        .expression("name.given")
        .build();
    final FhirPath argument = StringLiteralPath.fromString(MY_VALUE_SET_URL, mockContext);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .terminologyClient(mock(TerminologyClient.class))
        .terminologyClientFactory(mock(TerminologyClientFactory.class))
        .build();

    final NamedFunctionInput translateInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new TranslateFunction().invoke(translateInput));
    assertEquals("Input to translate function is of unsupported type: name.given",
        error.getMessage());
  }

  @Test
  public void throwsErrorIfArgumentIsNotString() {
    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .build();
    final IntegerLiteralPath argument = IntegerLiteralPath.fromString("4", input);

    final ParserContext context = new ParserContextBuilder(spark, fhirContext)
        .terminologyClient(mock(TerminologyClient.class))
        .terminologyClientFactory(mock(TerminologyClientFactory.class))
        .build();

    final NamedFunctionInput translateInput = new NamedFunctionInput(context, input,
        Arrays.asList(argument, argument, argument));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new TranslateFunction().invoke(translateInput));
    assertEquals("Function `translate` expects `String literal` as argument 1",
        error.getMessage());
  }

  @Test
  public void throwsErrorIfLessThanThreeArguments() {
    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .build();
    final StringLiteralPath argument1 = StringLiteralPath.fromString("'foo'", input),
        argument2 = StringLiteralPath.fromString("'bar'", input);

    final ParserContext context = new ParserContextBuilder(spark, fhirContext)
        .terminologyClient(mock(TerminologyClient.class))
        .terminologyClientFactory(mock(TerminologyClientFactory.class))
        .build();

    final NamedFunctionInput translateInput = new NamedFunctionInput(context, input,
        Arrays.asList(argument1, argument2));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new TranslateFunction().invoke(translateInput));
    assertEquals("translate function accepts 3 arguments",
        error.getMessage());
  }

  @Test
  public void throwsErrorIfTerminologyServiceNotConfigured() {
    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .build();
    final FhirPath argument = StringLiteralPath.fromString("some string", input);

    final ParserContext context = new ParserContextBuilder(spark, fhirContext)
        .build();

    final NamedFunctionInput translateInput = new NamedFunctionInput(context, input,
        Collections.singletonList(argument));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new TranslateFunction().invoke(translateInput));
    assertEquals(
        "Attempt to call terminology function translate when terminology service has not been configured",
        error.getMessage());
  }

}