/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.memberof;

import static au.csiro.pathling.QueryHelpers.join;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static au.csiro.pathling.utilities.Strings.randomAlias;
import static org.apache.spark.sql.functions.hash;

import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.FhirPath;
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
    final ElementPath inputPath = (ElementPath) input.getInput();
    final StringLiteralPath argument = (StringLiteralPath) input.getArguments().get(0);
    final ParserContext inputContext = input.getContext();

    final Column idColumn = inputPath.getIdColumn();
    final Column conceptColumn = inputPath.getValueColumn();

    // Prepare the data which will be used within the map operation. All of these things must be
    // Serializable.
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    final TerminologyClientFactory terminologyClientFactory = inputContext
        .getTerminologyClientFactory().get();
    final FHIRDefinedType fhirType = inputPath.getFhirType();
    final String valueSetUri = argument.getJavaValue();

    // Perform a validate code operation on each Coding or CodeableConcept in the input dataset,
    // then create a new dataset with the boolean results.
    final MemberOfMapper mapper = configuredMapper.orElseGet(() ->
        new MemberOfMapper(MDC.get("requestId"), terminologyClientFactory, valueSetUri,
            fhirType));

    final Dataset<Row> dataset = inputPath.getDataset();
    final Column conceptHashColumn = hash(conceptColumn);
    final String resultAlias = randomAlias();
    final String hashAlias = randomAlias();

    // This de-duplicates the Codings to be validated, then performs the validation on a
    // per-partition basis.
    final Dataset<Row> results = dataset
        .select(conceptHashColumn, conceptColumn)
        .dropDuplicates()
        .filter(conceptColumn.isNotNull())
        .mapPartitions(mapper, Encoders.bean(MemberOfResult.class))
        .toDF();
    final Dataset<Row> aliasedResults = results
        .select(results.col("result").alias(resultAlias), results.col("hash").alias(hashAlias));
    final Column resultColumn = aliasedResults.col(resultAlias);
    final Column resultHashColumn = aliasedResults.col(hashAlias);

    // We then join the input dataset to the validated codes, and select the validation result as
    // the new value.
    final Dataset<Row> finalDataset = join(dataset, conceptHashColumn, aliasedResults,
        resultHashColumn, JoinType.LEFT_OUTER);

    // Construct a new result expression.
    final String expression = expressionFromInput(input, NAME);

    return ElementPath
        .build(expression, finalDataset, idColumn, inputPath.getEidColumn(), resultColumn,
            inputPath.isSingular(), inputPath.getForeignResource(), inputPath.getThisColumn(),
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
        "memberOf function accepts one argument of type String");
    final FhirPath argument = input.getArguments().get(0);
    checkUserInput(argument instanceof StringLiteralPath,
        "memberOf function accepts one argument of type String literal");
  }

}
