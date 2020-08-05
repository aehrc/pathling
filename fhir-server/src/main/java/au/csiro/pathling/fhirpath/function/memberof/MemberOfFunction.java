/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.memberof;

import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.hash;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.NamedFunctionInput;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.slf4j.MDC;

/**
 * A function that takes a set of Codings or CodeableConcepts as inputs and returns a set of boolean
 * values, based upon whether each item is present within the ValueSet identified by the supplied
 * URL.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#memberof">memberOf</a>
 */
public class MemberOfFunction implements NamedFunction {

  private static final String NAME = "memberOf";

  @Nonnull
  private final Optional<MemberOfMapper> configuredMapper;

  /**
   * Returns a new instance of this function.
   */
  public MemberOfFunction() {
    configuredMapper = Optional.empty();
  }

  /**
   * Returns a new instance of this function, with a pre-configured {@link MemberOfMapper}.
   *
   * @param mapper An instance of {@link MemberOfMapper} for use in retrieving results from the
   * terminology service
   */
  public MemberOfFunction(@Nonnull final MemberOfMapper mapper) {
    configuredMapper = Optional.of(mapper);
  }

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    validateInput(input);
    final ElementPath inputResult = (ElementPath) input.getInput();
    final StringLiteralPath argument = (StringLiteralPath) input.getArguments().get(0);
    final ParserContext inputContext = input.getContext();

    final Dataset<Row> prevDataset = inputResult.getDataset();
    final Column prevIdColumn = inputResult.getIdColumn();
    final Column prevValueColumn = inputResult.getValueColumn();

    // Prepare the data which will be used within the map operation. All of these things must be
    // Serializable.
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    final TerminologyClientFactory terminologyClientFactory = inputContext
        .getTerminologyClientFactory().get();
    final FHIRDefinedType fhirType = inputResult.getFhirType();
    final String valueSetUri = argument.getJavaValue();

    // Perform a validate code operation on each Coding or CodeableConcept in the input dataset,
    // then create a new dataset with the boolean results.
    final MemberOfMapper mapper = configuredMapper.orElseGet(() ->
        new MemberOfMapper(MDC.get("requestId"), terminologyClientFactory, valueSetUri,
            fhirType));

    // This de-duplicates the Codings to be validated, then performs the validation on a
    // per-partition basis.
    final Column prevValueHashColumn = hash(prevValueColumn);
    final Dataset results = prevDataset
        .select(prevValueHashColumn, prevValueColumn)
        .dropDuplicates()
        .filter(prevValueColumn.isNotNull())
        .mapPartitions(mapper, Encoders.bean(MemberOfResult.class));
    Column valueColumn = results.col("result");
    final Column resultHashColumn = results.col("hash");

    // We then join the input dataset to the validated codes, and select the validation result
    // as the new value.
    final Dataset<Row> dataset = prevDataset
        .join(results, prevValueHashColumn.equalTo(resultHashColumn), "left_outer");

    // The conditional expression around the value column is required to deal with nulls. This
    // function should only ever return true or false.
    valueColumn = when(valueColumn.isNull(), false).otherwise(valueColumn);

    // Construct a new result expression.
    final String expression = expressionFromInput(input, NAME);
    return new BooleanPath(expression, dataset, prevIdColumn, valueColumn, inputResult.isSingular(),
        FHIRDefinedType.BOOLEAN);
  }

  private void validateInput(@Nonnull final NamedFunctionInput input) {
    final ParserContext context = input.getContext();
    checkUserInput(
        context.getTerminologyClient().isPresent() && context.getTerminologyClientFactory()
            .isPresent(), "Attempt to call terminology function " + NAME
            + " when terminology service has not been configured");

    final FhirPath inputPath = input.getInput();
    checkUserInput(inputPath instanceof ElementPath
            && (((ElementPath) inputPath).getFhirType().equals(FHIRDefinedType.CODING)
            || ((ElementPath) inputPath).getFhirType().equals(FHIRDefinedType.CODEABLECONCEPT)),
        "Input to memberOf function is of unsupported type: " + inputPath.getExpression());
    checkUserInput(input.getArguments().size() == 1,
        "memberOf function accepts one argument of type String: " + inputPath.getExpression());
    checkUserInput(input.getArguments().get(0) instanceof StringLiteralPath,
        "memberOf function accepts one argument of type String literal: " + inputPath
            .getExpression());
  }

}
