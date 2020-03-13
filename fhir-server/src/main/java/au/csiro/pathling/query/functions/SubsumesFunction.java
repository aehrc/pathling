/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.functions;

import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.struct;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.r4.model.ConceptMap.SourceElementComponent;
import org.hl7.fhir.r4.model.ConceptMap.TargetElementComponent;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.StringType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Streams;
import au.csiro.pathling.encoding.IdAndBoolean;
import au.csiro.pathling.encoding.IdAndCodingSets;
import au.csiro.pathling.encoding.Mapping;
import au.csiro.pathling.encoding.SystemAndCode;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.query.operators.PathTraversalInput;
import au.csiro.pathling.query.operators.PathTraversalOperator;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.pathling.query.parsing.parser.ExpressionParserContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;

/**
 * Represents a relation defined by a transitive closure table
 * 
 * @author szu004
 */
class Closure {
  private final Map<SystemAndCode, List<SystemAndCode>> mappings;

  private Closure(Map<SystemAndCode, List<SystemAndCode>> mappings) {
    this.mappings = mappings;
  }

  public Set<SystemAndCode> expand(Set<SystemAndCode> base, boolean includeSelf) {
    final Set<SystemAndCode> result = new HashSet<SystemAndCode>();
    if (includeSelf) {
      result.addAll(base);
    }
    base.forEach(c -> {
      List<SystemAndCode> mapping = mappings.get(c);
      if (mapping != null) {
        result.addAll(mapping);
      }
    });
    return result;
  }

  public boolean anyRelates(Collection<SystemAndCode> from, Collection<SystemAndCode> to) {
    // filter out null SystemAndCodes
    Set<SystemAndCode> fromSet =
        from.stream().filter(SystemAndCode::isNotNull).collect(Collectors.toSet());
    Set<SystemAndCode> expansion = expand(fromSet, true);
    expansion.retainAll(to);
    return !expansion.isEmpty();
  }

  @Override
  public String toString() {
    return "Closure [mappings=" + mappings + "]";
  }

  public static Closure fromMappings(List<Mapping> mappins) {
    Map<SystemAndCode, List<Mapping>> groupedMappings =
        mappins.stream().collect(Collectors.groupingBy(Mapping::getFrom));
    Map<SystemAndCode, List<SystemAndCode>> groupedCodings =
        groupedMappings.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(),
            e -> e.getValue().stream().map(m -> m.getTo()).collect(Collectors.toList())));
    return new Closure(groupedCodings);
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

  private final TerminologyClient terminologyClient;

  public ClosureService(TerminologyClient terminologyClient) {
    this.terminologyClient = terminologyClient;
  }

  public Closure getSubumesRelation(Collection<SystemAndCode> systemAndCodes) {
    List<Coding> codings =
        systemAndCodes.stream().map(SystemAndCode::toCoding).collect(Collectors.toList());
    // recreate the systemAndCodes dataset from the list not to execute the query again.
    // Create a unique name for the closure table for this code system, based upon the
    // expressions of the input, argument and the CodeSystem URI.
    String closureName = UUID.randomUUID().toString();
    logger.info("Sending $closure requests to terminology service with name '{}' and {} codinings",
        closureName, codings.size());
    // Execute the closure operation against the terminology server.
    // TODO: add validation checks for the response
    ConceptMap initialResponse = terminologyClient.closure(new StringType(closureName), null, null);
    validateConceptMap(initialResponse, closureName, "1");
    ConceptMap closureResponse =
        terminologyClient.closure(new StringType(closureName), codings, null);
    validateConceptMap(closureResponse, closureName, "2");
    return Closure.fromMappings(conceptMapToMappings(closureResponse));
  }

  private void validateConceptMap(ConceptMap conceptMap, String closureName, String version) {
    if (!PublicationStatus.ACTIVE.equals(conceptMap.getStatus())) {
      throw new RuntimeException(
          "Expected ConceptMap with status: ACTIVE, got: " + conceptMap.getStatus());
    }
    // TODO: Uncomment when testing is done
    // if (!closureName.equals(conceptMap.getName())) {
    // throw new RuntimeException("");
    // }
    // if (!version.equals(conceptMap.getVersion())) {
    // throw new RuntimeException("");
    // }
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

  public static List<Mapping> conceptMapToMappings(ConceptMap conceptMap) {
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
    return mappings;
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

  public static class SubsumptionMapper
      implements MapPartitionsFunction<IdAndCodingSets, IdAndBoolean> {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(SubsumptionMapper.class);
    private final TerminologyClientFactory terminologyClientFactory;

    public SubsumptionMapper(TerminologyClientFactory terminologyClientFactory) {
      super();
      this.terminologyClientFactory = terminologyClientFactory;
    }

    @Override
    public Iterator<IdAndBoolean> call(Iterator<IdAndCodingSets> input) throws Exception {
      List<IdAndCodingSets> entries = Streams.stream(input).collect(Collectors.toList());
      // collect distinct tokens
      Set<SystemAndCode> entrySet = entries.stream()
          .flatMap(r -> Streams.concat(r.getLeftCodings().stream(), r.getRightCodings().stream()))
          .filter(SystemAndCode::isNotNull).collect(Collectors.toSet());
      ClosureService closureService = new ClosureService(terminologyClientFactory.build(logger));
      final Closure subsumeClosure = closureService.getSubumesRelation(entrySet);
      return entries.stream().map(r -> IdAndBoolean.of(r.getId(),
          subsumeClosure.anyRelates(r.getLeftCodings(), r.getRightCodings()))).iterator();
    }
  }

  private static final String COL_ID = "id";
  private static final String COL_CODE = "code";
  private static final String COL_SYSTEM = "system";
  private static final String COL_CODING = "coding";
  private static final String COL_CODING_SET = "codingSet";
  private static final String COL_LEFT_CODINGS = "leftCodings";
  private static final String COL_RIGHT_CODINGS = "rightCodings";

  private static final Logger logger = LoggerFactory.getLogger(SubsumesFunction.class);

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

    Dataset<Row> resultDataset = this.inverted
        ? createSubsumesResult(input.getContext(), argSystemAndCodeDataset,
            inputSystemAndCodeDataset)
        : createSubsumesResult(input.getContext(), inputSystemAndCodeDataset,
            argSystemAndCodeDataset);

    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setFhirPathType(FhirPathType.BOOLEAN);
    result.setFhirType(FHIRDefinedType.BOOLEAN);
    result.setPrimitive(true);
    result.setSingular(true);
    result.setDataset(resultDataset);
    result.setHashedValue(resultDataset.col("id"), resultDataset.col("value"));
    return result;
  }

  private Dataset<Row> createSubsumesResult(ExpressionParserContext ctx,
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
    Dataset<Row> resultDataset = joinedCodingSets.as(Encoders.bean(IdAndCodingSets.class))
        .mapPartitions(new SubsumptionMapper(ctx.getTerminologyClientFactory()),
            Encoders.bean(IdAndBoolean.class))
        .toDF();
    return resultDataset;
  }

  @Nonnull
  private Dataset<SystemAndCode> getCodes(Dataset<Row> source) {
    Column systemCol = source.col(COL_CODING).getField(COL_SYSTEM).alias(COL_SYSTEM);
    Column codeCol = source.col(COL_CODING).getField(COL_CODE).alias(COL_CODE);
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

  /**
   * @param inputExpression
   * @param contextExpression
   * @return Dataframe with schema: "STRING id, STRUCT (STRING system, STRING code) coding"
   */
  @Nonnull
  private Dataset<Row> toSystemAndCodeDataset(ParsedExpression inputExpression,
      ParsedExpression contextExpression) {

    FHIRDefinedType inputType = inputExpression.getFhirType();
    assert FHIRDefinedType.CODING.equals(inputType) : "Expression of CODING type expected";

    // do the literal magic here
    ParsedExpression idExpression =
        (!inputExpression.isLiteral()) ? inputExpression : contextExpression;
    Dataset<Row> codingDataset =
        idExpression.getDataset().select(idExpression.getIdColumn().alias(COL_ID),
            inputExpression.getLiteralOrValueColumn().getField(COL_SYSTEM).alias(COL_SYSTEM),
            inputExpression.getLiteralOrValueColumn().getField(COL_CODE).alias(COL_CODE));
    return codingDataset.select(codingDataset.col(COL_ID),
        struct(codingDataset.col(COL_CODE), codingDataset.col(COL_SYSTEM)).alias(COL_CODING));
  }

  /**
   * Groups all coding for each into an array column.
   * 
   * @param systemAndCodeDataset
   * @return Dataframe with schema "STRING id ARRAY(STRUCT(STRING system, STRING code)) codingSet"
   */
  @Nonnull
  private Dataset<Row> toCodingSetsDataset(Dataset<Row> systemAndCodeDataset) {
    return systemAndCodeDataset.groupBy(systemAndCodeDataset.col(COL_ID))
        .agg(collect_set(systemAndCodeDataset.col(COL_CODING)).alias(COL_CODING_SET));
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
