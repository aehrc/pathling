package au.csiro.pathling.fhirpath.function.subsumes;

import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.explode_outer;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.NamedFunctionInput;
import au.csiro.pathling.fhirpath.function.subsumes.encoding.IdAndBoolean;
import au.csiro.pathling.fhirpath.function.subsumes.encoding.IdAndCodingSets;
import au.csiro.pathling.fhirpath.literal.CodingLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Describes a function which returns a boolean value based upon whether any of the input set of
 * Codings or CodeableConcepts subsume one or more Codings or CodeableConcepts in the target set.
 *
 * @author John Grimes
 * @author Piotr Szul
 * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#functions">Additional functions</a>
 */
public class SubsumesFunction implements NamedFunction {


  private static final String COL_ID = "id";
  private static final String COL_VALUE = "value";
  private static final String COL_CODING = "coding";
  private static final String COL_CODING_SET = "codingSet";
  private static final String COL_INPUT_CODINGS = "inputCodings";
  private static final String COL_ARG_CODINGS = "argCodings";

  private static final Logger logger = LoggerFactory.getLogger(SubsumesFunction.class);

  private boolean inverted = false;
  private String functionName = "subsumes";

  public SubsumesFunction() {
  }

  public SubsumesFunction(boolean inverted) {
    this.inverted = inverted;
    if (inverted) {
      this.functionName = "subsumedBy";
    }
  }

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull NamedFunctionInput input) {
    validateInput(input);

    FhirPath inputExpression = input.getInput();
    FhirPath argExpression = input.getArguments().get(0);

    Dataset<Row> inputSystemAndCodeDataset = toInputDataset(inputExpression);
    Dataset<Row> argSystemAndCodeDataset = toArgDataset(argExpression);

    Dataset<Row> resultDataset = createSubsumesResult(input.getContext(),
        inputSystemAndCodeDataset,
        argSystemAndCodeDataset);

    Column idColumn = resultDataset.col(COL_ID);
    Column valueColumn = resultDataset.col(COL_VALUE);

    // Construct a new result expression.
    final String expression = expressionFromInput(input, functionName);
    return new BooleanPath(expression, resultDataset, Optional.of(idColumn),
        valueColumn,
        false,
        //        inputExpression.isSingular(),
        FHIRDefinedType.BOOLEAN);
  }

  private Dataset<Row> createSubsumesResult(ParserContext ctx,
      Dataset<Row> inputCodingSet, Dataset<Row> argCodingSet) {

    // JOIN the input args datasets
    Dataset<Row> joinedCodingSets = inputCodingSet.join(argCodingSet,
        inputCodingSet.col(COL_ID).equalTo(argCodingSet.col(COL_ID)), "left_outer")
        .select(inputCodingSet.col(COL_ID).alias(COL_ID),
            inputCodingSet.col(COL_CODING_SET).alias(COL_INPUT_CODINGS),
            argCodingSet.col(COL_CODING_SET).alias(COL_ARG_CODINGS));

    // apply subsumption relation per partition
    return joinedCodingSets.as(Encoders.bean(IdAndCodingSets.class))
        .mapPartitions(new SubsumptionMapper(ctx.getTerminologyClientFactory().get(), inverted),
            Encoders.bean(IdAndBoolean.class)).toDF();
  }


  /**
   * Converts extracts the input dataset from a FhirPath
   */
  @Nonnull
  private Dataset<Row> toInputDataset(@Nonnull FhirPath expression) {

    assert (isCodingOrCodeableConcept(expression));

    Dataset<Row> expressionDataset = expression.getDataset();
    Column codingArrayCol = (isCodeableConcept(expression))
                            ? expression.getValueColumn().getField("coding")
                            : array(expression.getValueColumn());

    return expressionDataset.select(expression.getIdColumn().get().alias(COL_ID),
        when(expression.getValueColumn().isNotNull(), codingArrayCol)
            .otherwise(null)
            .alias(COL_CODING_SET)
    );
  }

  @Nonnull
  private Dataset<Row> toArgDataset(@Nonnull FhirPath expression) {

    assert (isCodingOrCodeableConcept(expression));

    Dataset<Row> expressionDataset = expression.getDataset();
    Column codingCol = (isCodeableConcept(expression))
                       ? explode_outer(expression.getValueColumn().getField("coding"))
                       : expression.getValueColumn();

    Dataset<Row> systemAndCodeDataset = expressionDataset
        .select(expression.getIdColumn().get().alias(COL_ID), codingCol.alias(COL_CODING));

    return systemAndCodeDataset
        .groupBy(systemAndCodeDataset.col(COL_ID))
        .agg(collect_set(systemAndCodeDataset.col(COL_CODING)).alias(COL_CODING_SET));
  }

  private void validateInput(@Nonnull final NamedFunctionInput input) {

    final ParserContext context = input.getContext();
    checkUserInput(
        context.getTerminologyClient().isPresent() && context.getTerminologyClientFactory()
            .isPresent(), "Attempt to call terminology function " + functionName
            + " when terminology service has not been configured");

    checkUserInput(
        input.getArguments().size() == 1,
        functionName + " function accepts one argument of type Coding or CodeableConcept"
    );

    FhirPath inputExpression = input.getInput();
    FhirPath argExpression = input.getArguments().get(0);
    validateExpressionType(inputExpression, "input");
    validateExpressionType(argExpression, "argument");
  }

  private boolean isCodeableConcept(@Nonnull FhirPath fhirPath) {
    return (fhirPath instanceof ElementPath &&
        FHIRDefinedType.CODEABLECONCEPT.equals(((ElementPath) fhirPath).getFhirType()));
  }

  private boolean isCodingOrCodeableConcept(@Nonnull FhirPath fhirPath) {
    if (fhirPath instanceof CodingLiteralPath) {
      return true;
    } else if (fhirPath instanceof ElementPath) {
      FHIRDefinedType elementFhirType = ((ElementPath) fhirPath).getFhirType();
      return FHIRDefinedType.CODING.equals(elementFhirType)
          || FHIRDefinedType.CODEABLECONCEPT.equals(elementFhirType);
    } else {
      return false;
    }
  }

  private void validateExpressionType(@Nonnull FhirPath inputPath, @Nonnull String pathRole) {
    checkUserInput(
        isCodingOrCodeableConcept(inputPath),
        functionName + " function accepts " + pathRole + " of type Coding or CodeableConcept"
    );
  }
}
