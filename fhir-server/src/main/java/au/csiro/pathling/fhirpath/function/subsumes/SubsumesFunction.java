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
import au.csiro.pathling.fhirpath.encoding.BooleanResult;
import au.csiro.pathling.fhirpath.encoding.IdAndCodingSets;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.NamedFunctionInput;
import au.csiro.pathling.fhirpath.literal.CodingLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.slf4j.MDC;


/**
 * A function that takes a set of Codings or CodeableConcepts as inputs and returns a set of boolean
 * values whether based upon whether each item subsumes or is subsumedBy one or more Codings or
 * CodeableConcepts in the argument set.
 *
 * @author John Grimes
 * @author Piotr Szul
 * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#functions">Additional functions</a>
 */

@Slf4j
public class SubsumesFunction implements NamedFunction {

  private static final String COL_ID = "id";
  private static final String COL_VALUE = "value";
  private static final String COL_CODING = "coding";
  private static final String COL_CODING_SET = "codingSet";
  private static final String COL_INPUT_CODINGS = "inputCodings";
  private static final String COL_ARG_CODINGS = "argCodings";
  private static final String FIELD_CODING = "coding";


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
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    validateInput(input);

    final FhirPath inputFhirPath = input.getInput();
    final Dataset<IdAndCodingSets> idAndCodingSet = createJoinedDataset(input.getInput(),
        input.getArguments().get(0));

    // apply subsumption relation per partition
    final Dataset<Row> resultDataset = idAndCodingSet
        .mapPartitions(
            new SubsumptionMapper(MDC.get("requestId"),
                input.getContext().getTerminologyClientFactory().get(), inverted),
            Encoders.bean(BooleanResult.class)).toDF();

    final Column idColumn = resultDataset.col(COL_ID);
    final Column valueColumn = resultDataset.col(COL_VALUE);

    // Construct a new result expression.
    final String expression = expressionFromInput(input, functionName);
    return new BooleanPath(expression, resultDataset, Optional.of(idColumn),
        valueColumn,
        inputFhirPath.isSingular(),
        FHIRDefinedType.BOOLEAN);
  }

  /**
   * Creates a dataset with schema: STRING id, ARRAY(CODING) inputCoding, ARRAY(CODING) argCodings,
   * which joins for each id arrays representing input Codings or CodeableConcept with the array
   * representing all argument codings.
   *
   * @see #toInputDataset(FhirPath)
   * @see #toArgDataset(FhirPath)
   */
  @Nonnull
  private Dataset<IdAndCodingSets> createJoinedDataset(@Nonnull final FhirPath inputFhirPath,
      @Nonnull final FhirPath argFhirPath) {

    final Dataset<Row> inputCodingSet = toInputDataset(inputFhirPath);
    final Dataset<Row> argCodingSet = toArgDataset(argFhirPath);

    // JOIN the input arg args datasets
    return inputCodingSet.join(argCodingSet,
        inputCodingSet.col(COL_ID).equalTo(argCodingSet.col(COL_ID)), "left_outer")
        .select(inputCodingSet.col(COL_ID).alias(COL_ID),
            inputCodingSet.col(COL_CODING_SET).alias(COL_INPUT_CODINGS),
            argCodingSet.col(COL_CODING_SET).alias(COL_ARG_CODINGS))
        .as(Encoders.bean(IdAndCodingSets.class));
  }

  /**
   * Converts the the input fhirpath to a dataset with schema: STRING id, ARRAY(struct CODING)
   * codingSet Each CodeableConcept is converted to an array that includes all its coding. Each
   * Coding is converted to an array that only includes this coding.
   * <p>
   * NULL Codings and CodeableConcepts are represented as NULL `codingSet`.
   *
   * @param fhirPath to convert
   * @return input dataset
   */
  @Nonnull
  private Dataset<Row> toInputDataset(@Nonnull final FhirPath fhirPath) {

    assert (isCodingOrCodeableConcept(fhirPath));

    final Dataset<Row> expressionDataset = fhirPath.getDataset();
    final Column codingArrayCol = (isCodeableConcept(fhirPath))
                                  ? fhirPath.getValueColumn().getField(FIELD_CODING)
                                  : array(fhirPath.getValueColumn());

    return expressionDataset.select(fhirPath.getIdColumn().get().alias(COL_ID),
        when(fhirPath.getValueColumn().isNotNull(), codingArrayCol)
            .otherwise(null)
            .alias(COL_CODING_SET)
    );
  }

  /**
   * Converts the the argument fhirpath to a dataset with schema: STRING id, ARRAY(struct CODING)
   * codingSet
   * <p>
   * All directly provided Coding or all Codings from provided CodeableConcepts are collected in a
   * single `array` per resource
   * <p>
   * NULL Codings and CodeableConcepts are ignored. In case the resource does not have any non NULL
   * elements an empty array created.
   *
   * @param fhirPath to convert
   * @return input dataset
   */

  @Nonnull
  private Dataset<Row> toArgDataset(@Nonnull final FhirPath fhirPath) {

    assert (isCodingOrCodeableConcept(fhirPath));

    final Dataset<Row> expressionDataset = fhirPath.getDataset();
    final Column codingCol = (isCodeableConcept(fhirPath))
                             ? explode_outer(fhirPath.getValueColumn().getField(FIELD_CODING))
                             : fhirPath.getValueColumn();

    final Dataset<Row> systemAndCodeDataset = expressionDataset
        .select(fhirPath.getIdColumn().get().alias(COL_ID), codingCol.alias(COL_CODING));

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

    validateExpressionType(input.getInput(), "input");
    validateExpressionType(input.getArguments().get(0), "argument");
  }

  private boolean isCodeableConcept(@Nonnull final FhirPath fhirPath) {
    return (fhirPath instanceof ElementPath &&
        FHIRDefinedType.CODEABLECONCEPT.equals(((ElementPath) fhirPath).getFhirType()));
  }

  private boolean isCodingOrCodeableConcept(@Nonnull final FhirPath fhirPath) {
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

  private void validateExpressionType(@Nonnull final FhirPath inputPath,
      @Nonnull final String pathRole) {
    checkUserInput(
        isCodingOrCodeableConcept(inputPath),
        functionName + " function accepts " + pathRole + " of type Coding or CodeableConcept"
    );
  }
}
