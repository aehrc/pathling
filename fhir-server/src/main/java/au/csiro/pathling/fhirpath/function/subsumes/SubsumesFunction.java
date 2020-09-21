package au.csiro.pathling.fhirpath.function.subsumes;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.struct;

import au.csiro.pathling.fhir.SimpleCoding;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.NamedFunctionInput;
import au.csiro.pathling.fhirpath.function.subsumes.encoding.IdAndBoolean;
import au.csiro.pathling.fhirpath.function.subsumes.encoding.IdAndCodingSets;
import au.csiro.pathling.fhirpath.literal.CodingLiteralPath;
import au.csiro.pathling.fhirpath.operator.PathTraversalInput;
import au.csiro.pathling.fhirpath.operator.PathTraversalOperator;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;


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
  private static final String COL_CODE = "code";
  private static final String COL_SYSTEM = "system";
  private static final String COL_VERSION = "version";
  private static final String COL_CODING = "coding";
  private static final String COL_CODING_SET = "codingSet";
  private static final String COL_LEFT_CODINGS = "leftCodings";
  private static final String COL_RIGHT_CODINGS = "rightCodings";

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

    ParserContext parserContext = input.getContext();
    Dataset<Row> inputSystemAndCodeDataset = toSystemAndCodeDataset(
        normalizeToCoding(inputExpression, parserContext));
    Dataset<Row> argSystemAndCodeDataset =
        toSystemAndCodeDataset(normalizeToCoding(argExpression, parserContext));

    Dataset<Row> resultDataset = this.inverted
                                 ? createSubsumesResult(input.getContext(), argSystemAndCodeDataset,
        inputSystemAndCodeDataset)
                                 : createSubsumesResult(input.getContext(),
                                     inputSystemAndCodeDataset,
                                     argSystemAndCodeDataset);
    Column idColumn = resultDataset.col("id");
    Column valueColumn = resultDataset.col("value");

    //TODO: check what this is all about
    // // If there is a `$this` context, we need to add the value column back in to the resulting
    // // dataset so that it can be passed forward in the result from the enclosing function.
    // ParsedExpression thisContext = input.getContext().getThisContext();
    // if (thisValuePresentInDataset(inputExpression.getDataset(), thisContext)) {
    //   resultDataset = resultDataset.join(thisContext.getDataset(),
    //       idColumn.equalTo(thisContext.getIdColumn()), "inner");
    // }

    // Construct a new result expression.
    final String expression = expressionFromInput(input, functionName);
    return new BooleanPath(expression, resultDataset, Optional.of(idColumn),
        valueColumn,
        false,
//        inputExpression.isSingular(),
        FHIRDefinedType.BOOLEAN);
  }


  private Dataset<Row> createSubsumesResult(ParserContext ctx,
      Dataset<Row> inputSystemAndCodeDataset, Dataset<Row> argSystemAndCodeDataset) {
    Dataset<Row> inputCodingSet = toCodingSetsDataset(inputSystemAndCodeDataset);
    Dataset<Row> argCodingSet = toCodingSetsDataset(argSystemAndCodeDataset);

    // JOIN the input args datasets
    Dataset<Row> joinedCodingSets = inputCodingSet.join(argCodingSet,
        inputCodingSet.col(COL_ID).equalTo(argCodingSet.col(COL_ID)), "left_outer")
        .select(inputCodingSet.col(COL_ID).alias(COL_ID),
            inputCodingSet.col(COL_CODING_SET).alias(COL_LEFT_CODINGS),
            argCodingSet.col(COL_CODING_SET).alias(COL_RIGHT_CODINGS));

    // apply subsumption relation per partition
    return joinedCodingSets.as(Encoders.bean(IdAndCodingSets.class))
        .mapPartitions(new SubsumptionMapper(ctx.getTerminologyClientFactory().get()),
            Encoders.bean(IdAndBoolean.class))
        .toDF();
  }

  @Nonnull
  private Dataset<SimpleCoding> getCodes(Dataset<Row> source) {
    Column systemCol = source.col(COL_CODING).getField(COL_SYSTEM).alias(COL_SYSTEM);
    Column codeCol = source.col(COL_CODING).getField(COL_CODE).alias(COL_CODE);
    Dataset<Row> codes = source.select(codeCol, systemCol);
    return codes.where(systemCol.isNotNull().and(codeCol.isNotNull())).distinct()
        .as(Encoders.bean(SimpleCoding.class));
  }


  /**
   * Expands CodeableConcepts to a set of Codings
   */
  private FhirPath normalizeToCoding(@Nonnull FhirPath expression,
      ParserContext parserContext) {

    assert (isCodingOrCodeableConcept(expression));
    if (expression instanceof ElementPath &&
        FHIRDefinedType.CODEABLECONCEPT.equals(((ElementPath) expression).getFhirType())) {

      PathTraversalInput pathTraversalInput = new PathTraversalInput(parserContext, expression,
          "coding");
      return new PathTraversalOperator().invoke(pathTraversalInput);
    } else {
      return expression;
    }
  }

  /**
   * @return Dataframe with schema: "STRING id, STRUCT (STRING system, STRING code) coding"
   */
  @Nonnull
  private Dataset<Row> toSystemAndCodeDataset(FhirPath inputExpression) {

    assert isCodingPathOrLiteral(inputExpression) : "Expression of CODING type expected";

    // do the literal magic here
    FhirPath idExpression = inputExpression;
    Dataset<Row> codingDataset =
        idExpression.getDataset().select(idExpression.getIdColumn().get().alias(COL_ID),
            inputExpression.getValueColumn().getField(COL_SYSTEM).alias(COL_SYSTEM),
            inputExpression.getValueColumn().getField(COL_CODE).alias(COL_CODE),
            inputExpression.getValueColumn().getField(COL_VERSION).alias(COL_VERSION));
    return codingDataset.select(codingDataset.col(COL_ID), struct(codingDataset.col(COL_CODE),
        codingDataset.col(COL_SYSTEM), codingDataset.col(COL_VERSION)).alias(COL_CODING));
  }

  /**
   * Groups all coding for each into an array column.
   *
   * @return Dataframe with schema "STRING id ARRAY(STRUCT(STRING system, STRING code)) codingSet"
   */
  @Nonnull
  private Dataset<Row> toCodingSetsDataset(Dataset<Row> systemAndCodeDataset) {
        return systemAndCodeDataset.select(systemAndCodeDataset.col(COL_ID),
            array(systemAndCodeDataset.col(COL_CODING)).alias(COL_CODING_SET));

//    return systemAndCodeDataset.groupBy(systemAndCodeDataset.col(COL_ID))
//        .agg(collect_set(systemAndCodeDataset.col(COL_CODING)).alias(COL_CODING_SET));
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

    // TODO: Check if still relevant
    // // at least one expression must not be a literal
    // if (inputExpression.isLiteral() && argExpression.isLiteral()) {
    //   throw new InvalidRequestException("Input and argument cannot be both literals for "
    //       + functionName + " function: " + input.getExpression());
    // }

    // if both are not literals than they must be based on the same resource
    // otherwise the literal will inherit the resource from the non literal

    // TODO: check
    // if (!inputExpression.isLiteral() && !argExpression.isLiteral()
    // && !inputExpression.getResourceType().equals(argExpression.getResourceType())) {
    // throw new InvalidRequestException(
    // "Input and argument are based on different resources in " + functionName + " function");
    // }
  }


  private boolean isCodingPathOrLiteral(@Nonnull FhirPath fhirPath) {
    return (fhirPath instanceof CodingLiteralPath) ||
        (fhirPath instanceof ElementPath &&
            FHIRDefinedType.CODING.equals(((ElementPath) fhirPath).getFhirType()));
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
