/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.functions;

import static au.csiro.pathling.utilities.Strings.md5Short;
import static org.apache.spark.sql.functions.isnull;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.r4.model.ConceptMap.SourceElementComponent;
import org.hl7.fhir.r4.model.ConceptMap.TargetElementComponent;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.StringType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import au.csiro.pathling.encoding.Mapping;
import au.csiro.pathling.encoding.SystemAndCode;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.query.operators.PathTraversalInput;
import au.csiro.pathling.query.operators.PathTraversalOperator;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.pathling.query.parsing.parser.ExpressionParserContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;


/**
 * Represents a relation defined by a transitive closure table The closure table should have the
 * schema compliant with the Mapping bean. The relation between `from` and `to` is defined as `from`
 * -- relation --> `to` E.g. if the relation is subsumes: `from` -- subsumes --> `to` means that
 * `from` is more general.
 * 
 * @author szu004
 *
 */
class Relation {

  final Dataset<Mapping> mappingTable;
  final boolean inverted;

  public Relation(Dataset<Mapping> mappingTable, boolean inverted) {
    this.mappingTable = mappingTable;
    this.inverted = inverted;
  }

  public Relation(Dataset<Mapping> mappingTable) {
    this(mappingTable, false);
  }

  public Dataset<Row> apply(Dataset<Row> from, Dataset<Row> to) {

    Column srcCol = !inverted ? mappingTable.col("from") : mappingTable.col("to");
    Column dstCol = !inverted ? mappingTable.col("to") : mappingTable.col("from");

    Dataset<Row> joinedDataset =
        from.join(mappingTable, from.col("coding").equalTo(srcCol), "left_outer").join(to,
            to.col("id").equalTo(from.col("id")).and(to.col("coding").equalTo(dstCol)),
            "left_outer");

    return joinedDataset.groupBy(from.col("id"))
        .agg(max(not(isnull(to.col("id")))).alias("subsumes"));
  }

  public Relation union(Relation other) {
    return new Relation(mappingTable.union(other.mappingTable));
  }

  public Relation invert() {
    return new Relation(mappingTable, !inverted);
  }


  /**
   * Creates equivalence relation from given codes, that one where each code is relation with
   * itself.
   * 
   * @param codes
   */
  public static Relation createEquivalence(Dataset<SystemAndCode> codes) {

    // TODO: Maybe rewrite as select
    Dataset<Mapping> mappingDataset = codes.map(new MapFunction<SystemAndCode, Mapping>() {

      private static final long serialVersionUID = 1L;

      @Override
      public Mapping call(SystemAndCode value) throws Exception {
        return new Mapping(value, value);
      }
    }, Encoders.bean(Mapping.class));
    return new Relation(mappingDataset);
  }
}


/**
 * Helper class to encapsulate creation of subsumes Relation for a set of Codings.
 * 
 * @author szu004
 *
 */
class ClosureService {
  private static final Logger logger = LoggerFactory.getLogger(ClosureService.class);

  private final String seed;
  private final TerminologyClient terminologyClient;

  public ClosureService(String seed, TerminologyClient terminologyClient) {
    this.seed = seed;
    this.terminologyClient = terminologyClient;
  }

  public Relation getSubumesRelation(Dataset<SystemAndCode> codingsDataset) {

    Map<String, List<SystemAndCode>> codingsBySystem = codingsDataset.collectAsList().stream()
        .collect(Collectors.groupingBy(SystemAndCode::getSystem));

    Relation result = Relation.createEquivalence(codingsDataset);

    for (String codeSystem : codingsBySystem.keySet()) {
      // Get the codings for this code system.
      List<Coding> codings = codingsBySystem.get(codeSystem).stream().map(SystemAndCode::toCoding)
          .collect(Collectors.toList());

      // Create a unique name for the closure table for this code system, based upon the
      // expressions of the input, argument and the CodeSystem URI.
      String closureName = md5Short(seed + codeSystem);
      // Execute the closure operation against the terminology server.
      terminologyClient.closure(new StringType(closureName), null, null);
      logger.info("Sending $closure requests to terminology service for codings: " + codings);
      ConceptMap closure = terminologyClient.closure(new StringType(closureName), codings, null);
      result = result.union(conceptMapToSubsumesRelation(closure, codingsDataset.sparkSession()));
    }
    return result;
  }

  /**
   * According to the specification the only valid equivalences in the response are: equal,
   * specializes, subsumes and unmatched
   * 
   * @return Mapping for subsumes relation i.e from -- subsumes --> to
   */
  public static Mapping equivalenceToSubsumesMapping(SystemAndCode source, SystemAndCode target,
      ConceptMapEquivalence equivalence) {
    Mapping result = null;
    switch (equivalence) {
      case SUBSUMES:
        result = new Mapping(target, source);
        break;
      case SPECIALIZES:
        result = new Mapping(source, target);
        break;
      case EQUAL:
        result = new Mapping(source, target);
        break;
      case UNMATCHED:
        break;
      default:
        logger.warn("Ignoring unexpected equivalence: " + equivalence + " source: " + source
            + " target: " + target);
        break;
    }
    return result;
  }

  public static Relation conceptMapToSubsumesRelation(ConceptMap conceptMap, SparkSession spark) {
    List<Mapping> mappings = new ArrayList<Mapping>();
    if (conceptMap.hasGroup()) {
      List<ConceptMapGroupComponent> groups = conceptMap.getGroup();
      for (ConceptMapGroupComponent group : groups) {
        List<SourceElementComponent> elements = group.getElement();
        for (SourceElementComponent source : elements) {
          for (TargetElementComponent target : source.getTarget()) {
            Mapping subsumesMapping = equivalenceToSubsumesMapping(
                new SystemAndCode(group.getSource(), source.getCode()),
                new SystemAndCode(group.getTarget(), target.getCode()), target.getEquivalence());
            if (subsumesMapping != null) {
              mappings.add(subsumesMapping);
            }
          }
        }
      }
    }
    return new Relation(spark.createDataset(mappings, Encoders.bean(Mapping.class)));
  }
}


/**
 * Describes a function which returns a boolean value based upon whether any of the input set of
 * Codings or CodeableConcepts subsume one or more Codings or CodeableConcepts in the target set.
 *
 * @author John Grimes
 * @author Piotr Szul
 * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#functions">Additional functions</a>
 */
public class SubsumesFunction implements Function {

  private static final Logger logger = LoggerFactory.getLogger(SubsumesFunction.class);

  // private static void debugDataset(Dataset<?> dataset, String msg) {
  // System.out.println(msg);
  // dataset.printSchema();
  // dataset.show();
  // }

  private boolean inverted = false;
  private String functionName = "subsumes";

  public SubsumesFunction() {}

  public SubsumesFunction(boolean inverted) {
    this.inverted = inverted;
    if (inverted) {
      this.functionName = "subsumedBy";
    }
  }

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull FunctionInput input) {

    validateInput(input);

    ParsedExpression inputExpression = input.getInput();
    ParsedExpression argExpression = input.getArguments().get(0);

    //
    // contexExpression is a non literal expression that can be used to provide id column
    // for a liter expression.
    //
    ParsedExpression contextExpression = !inputExpression.isLiteral() ? inputExpression
        : (!argExpression.isLiteral() ? argExpression : null);
    assert (contextExpression != null) : "Context expression is not null";

    ExpressionParserContext parserContext = input.getContext();
    Dataset<Row> inputSystemAndCodeDataset = toSystemAndCodeDataset(
        normalizeToCoding(inputExpression, parserContext), contextExpression);
    Dataset<Row> argSystemAndCodeDataset =
        toSystemAndCodeDataset(normalizeToCoding(argExpression, parserContext), contextExpression);

    String seed = (inputExpression.getFhirPath() + inputExpression.getFhirPath());
    Relation relation = createTransitiveClosureTable(seed, input.getContext(),
        inputSystemAndCodeDataset, argSystemAndCodeDataset);

    Dataset<Row> resultDataset = relation.apply(inputSystemAndCodeDataset, argSystemAndCodeDataset);
    // Construct the final result
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setFhirPathType(FhirPathType.BOOLEAN);
    result.setFhirType(FHIRDefinedType.BOOLEAN);
    result.setPrimitive(true);
    result.setSingular(true);
    result.setDataset(resultDataset);
    result.setHashedValue(resultDataset.col("id"), resultDataset.col("subsumes"));
    return result;
  }


  @Nonnull
  private Relation createTransitiveClosureTable(String seed,
      ExpressionParserContext expressionParserContext, Dataset<Row> inputSystemAndCodeDataset,
      Dataset<Row> argSystemAndCodeDataset) {

    ClosureService closureService =
        new ClosureService(seed, expressionParserContext.getTerminologyClient());
    Relation subsumesRelation = closureService
        .getSubumesRelation(getCodes(inputSystemAndCodeDataset.union(argSystemAndCodeDataset)));

    return (!inverted) ? subsumesRelation : subsumesRelation.invert();
  }

  @Nonnull
  private Dataset<SystemAndCode> getCodes(Dataset<Row> source) {
    Column systemCol = source.col("coding").getField("system").alias("system");
    Column codeCol = source.col("coding").getField("code").alias("code");
    Dataset<Row> codes = source.select(codeCol, systemCol);
    return codes.where(systemCol.isNotNull().and(codeCol.isNotNull())).distinct()
        .as(Encoders.bean(SystemAndCode.class));
  }


  private ParsedExpression normalizeToCoding(ParsedExpression expression,
      ExpressionParserContext parserContext) {

    FHIRDefinedType expressionType = expression.getFhirType();
    // If this is a CodeableConcept, we need to traverse to the `coding` member first.
    if (FHIRDefinedType.CODING.equals(expressionType)) {
      return expression;
    } else if (FHIRDefinedType.CODEABLECONCEPT.equals(expressionType)) {
      PathTraversalInput pathTraversalInput = new PathTraversalInput();
      pathTraversalInput.setContext(parserContext);
      pathTraversalInput.setLeft(expression);
      pathTraversalInput.setExpression("coding");
      pathTraversalInput.setRight("coding");
      ParsedExpression codingExpresion = new PathTraversalOperator().invoke(pathTraversalInput);
      codingExpresion.setFhirPath(expression.getFhirPath() + ".coding");
      return codingExpresion;
    } else {
      throw new IllegalArgumentException("Cannot extract codings from element of type: "
          + expressionType + "in expression: " + expression);
    }
  }

  @Nonnull
  private Dataset<Row> toSystemAndCodeDataset(ParsedExpression inputExpression,
      ParsedExpression contextExpression) {

    FHIRDefinedType inputType = inputExpression.getFhirType();
    assert FHIRDefinedType.CODING.equals(inputType) : "Expression of CODING type expected";


    // do the literal magic here
    ParsedExpression idExpression =
        (!inputExpression.isLiteral()) ? inputExpression : contextExpression;
    Dataset<Row> codingDataset =
        idExpression.getDataset().select(idExpression.getIdColumn().alias("id"),
            inputExpression.getLiteralOrValueColumn().getField("system").alias("system"),
            inputExpression.getLiteralOrValueColumn().getField("code").alias("code"));
    // TODO: add filtering on non empty system and code
    // Note: the struct needs to be (code, system) as this is the order in which SystemAndCode is
    // encoded.
    return codingDataset.select(codingDataset.col("id"),
        struct(codingDataset.col("code"), codingDataset.col("system")).alias("coding"));
  }

  private void validateInput(FunctionInput input) {

    if (input.getArguments().size() != 1) {
      throw new InvalidRequestException(
          functionName + " function accepts one argument of type Coding|CodeableConcept: "
              + input.getExpression());
    }

    ParsedExpression inputExpression = input.getInput();
    ParsedExpression argExpression = input.getArguments().get(0);
    validateExpressionType(inputExpression, "input");
    validateExpressionType(argExpression, "argument");
    // at least one expression must not be a literal
    if (inputExpression.isLiteral() && argExpression.isLiteral()) {
      throw new InvalidRequestException("Input and argument cannot be both literals for "
          + functionName + " function: " + input.getExpression());
    }
    // if both are not literals than they must be based on the same resource
    // otherwise the literal will inherit the resource from the non literal

    // TODO: check
    // if (!inputExpression.isLiteral() && !argExpression.isLiteral()
    // && !inputExpression.getResourceType().equals(argExpression.getResourceType())) {
    // throw new InvalidRequestException(
    // "Input and argument are based on different resources in " + functionName + " function");
    // }
  }

  private void validateExpressionType(ParsedExpression inputResult, String expressionRole) {
    FHIRDefinedType typeCode = inputResult.getFhirType();
    if (!FHIRDefinedType.CODING.equals(typeCode)
        && !FHIRDefinedType.CODEABLECONCEPT.equals(typeCode)) {
      throw new InvalidRequestException(functionName + " function accepts " + expressionRole
          + " of type Coding or CodeableConcept: " + inputResult.getFhirPath());
    }
  }
}
