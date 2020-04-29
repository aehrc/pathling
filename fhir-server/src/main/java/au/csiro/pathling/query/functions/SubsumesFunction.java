/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.functions;

import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.struct;

import au.csiro.pathling.encoding.IdAndBoolean;
import au.csiro.pathling.encoding.IdAndCodingSets;
import au.csiro.pathling.encoding.Mapping;
import au.csiro.pathling.encoding.SimpleCoding;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.query.operators.PathTraversalInput;
import au.csiro.pathling.query.operators.PathTraversalOperator;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.pathling.query.parsing.parser.ExpressionParserContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import com.google.common.collect.Streams;
import java.util.*;
import java.util.Map.Entry;
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

/**
 * Represents a relation defined by a transitive closure table.
 *
 * @author Piotr Szul
 */
class Closure {

  private final Map<SimpleCoding, List<SimpleCoding>> mappings;

  private Closure(Map<SimpleCoding, List<SimpleCoding>> mappings) {
    this.mappings = mappings;
  }

  Map<SimpleCoding, List<SimpleCoding>> getMappings() {
    return mappings;
  }

  /**
   * Set of codings with contains() following the coding's equivalence semantics.
   *
   * @author Piotr Szul
   */
  static class CodingSet {

    private final Set<SimpleCoding> allCodings;
    private final Set<SimpleCoding> unversionedCodings;

    CodingSet(Set<SimpleCoding> allCodings) {
      this.allCodings = allCodings;
      this.unversionedCodings =
          allCodings.stream().map(SimpleCoding::toNonVersioned).collect(Collectors.toSet());
    }

    /**
     * Belongs to set operation with the coding's equivalence semantics, i.e. if the set includes an
     * unversioned coding, it contains any versioned coding with the same system code, and; if a set
     * contains a versioned coding, it contains its corresponding unversioned coding as well.
     *
     * @param c coding
     */
    boolean contains(SimpleCoding c) {
      return allCodings.contains(c) || (c.isVersioned()
                                        ? allCodings.contains(c.toNonVersioned())
                                        : unversionedCodings.contains(c));
    }
  }

  /**
   * Expands given set of Codings using with the closure, that is produces a set of Codings that are
   * in the relation with the given set.
   */
  public Set<SimpleCoding> expand(Set<SimpleCoding> codings) {
    final CodingSet baseSet = new CodingSet(codings);
    return Streams
        .concat(codings.stream(), mappings.entrySet().stream()
            .filter(kv -> baseSet.contains(kv.getKey())).flatMap(kv -> kv.getValue().stream()))
        .collect(Collectors.toSet());
  }

  /**
   * Checks if any of the Codings in the right set is in the relation with any of the Codings in the
   * left set.
   */
  public boolean anyRelates(Collection<SimpleCoding> left, Collection<SimpleCoding> right) {
    // filter out null SystemAndCodes
    Set<SimpleCoding> leftSet =
        left.stream().filter(SimpleCoding::isNotNull).collect(Collectors.toSet());
    final CodingSet expansion = new CodingSet(expand(leftSet));
    return right.stream().anyMatch(expansion::contains);
  }

  @Override
  public String toString() {
    return "Closure [mappings=" + mappings + "]";
  }

  public static Closure fromMappings(List<Mapping> mappings) {
    Map<SimpleCoding, List<Mapping>> groupedMappings =
        mappings.stream().collect(Collectors.groupingBy(Mapping::getFrom));
    Map<SimpleCoding, List<SimpleCoding>> groupedCodings =
        groupedMappings.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().stream()
                .map(Mapping::getTo)
                .collect(Collectors.toList())));
    return new Closure(groupedCodings);
  }
}


/**
 * Helper class to encapsulate the creation of a subsumes relation for a set of Codings.
 *
 * @author Piotr Szul
 */
class ClosureService {

  private static final Logger logger = LoggerFactory.getLogger(ClosureService.class);

  private final TerminologyClient terminologyClient;

  public ClosureService(TerminologyClient terminologyClient) {
    this.terminologyClient = terminologyClient;
  }

  public Closure getSubsumesRelation(Collection<SimpleCoding> systemAndCodes) {
    List<Coding> codings =
        systemAndCodes.stream().map(SimpleCoding::toCoding).collect(Collectors.toList());
    // recreate the systemAndCodes dataset from the list not to execute the query again.
    // Create a unique name for the closure table for this code system, based upon the
    // expressions of the input, argument and the CodeSystem URI.
    String closureName = UUID.randomUUID().toString();
    logger.info("Sending $closure request to terminology service with name '{}' and {} codings",
        closureName, codings.size());
    // Execute the closure operation against the terminology server.
    // TODO: add validation checks for the response
    ConceptMap initialResponse = terminologyClient.closure(new StringType(closureName), null, null);
    validateConceptMap(initialResponse, closureName, "1");
    ConceptMap closureResponse =
        terminologyClient.closure(new StringType(closureName), codings, null);
    validateConceptMap(closureResponse, closureName, "2");
    return conceptMapToClosure(closureResponse);
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
  private static Mapping appendSubsumesMapping(List<Mapping> mappings, SimpleCoding source,
      SimpleCoding target, ConceptMapEquivalence equivalence) {
    Mapping result = null;
    switch (equivalence) {
      case SUBSUMES:
        mappings.add(new Mapping(target, source));
        break;
      case SPECIALIZES:
        mappings.add(new Mapping(source, target));
        break;
      case EQUAL:
        mappings.add(new Mapping(source, target));
        mappings.add(new Mapping(target, source));
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

  private static List<Mapping> conceptMapToMappings(ConceptMap conceptMap) {
    List<Mapping> mappings = new ArrayList<Mapping>();
    if (conceptMap.hasGroup()) {
      List<ConceptMapGroupComponent> groups = conceptMap.getGroup();
      for (ConceptMapGroupComponent group : groups) {
        List<SourceElementComponent> elements = group.getElement();
        for (SourceElementComponent source : elements) {
          for (TargetElementComponent target : source.getTarget()) {
            appendSubsumesMapping(mappings,
                new SimpleCoding(group.getSource(), source.getCode(), group.getSourceVersion()),
                new SimpleCoding(group.getTarget(), target.getCode(), group.getTargetVersion()),
                target.getEquivalence());
          }
        }
      }
    }
    return mappings;
  }

  static Closure conceptMapToClosure(ConceptMap conceptMap) {
    return Closure.fromMappings(conceptMapToMappings(conceptMap));
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
    public Iterator<IdAndBoolean> call(Iterator<IdAndCodingSets> input) {
      List<IdAndCodingSets> entries = Streams.stream(input).collect(Collectors.toList());
      // collect distinct tokens
      Set<SimpleCoding> entrySet = entries.stream()
          .flatMap(r -> Streams.concat(r.getLeftCodings().stream(), r.getRightCodings().stream()))
          .filter(SimpleCoding::isNotNull).collect(Collectors.toSet());
      ClosureService closureService = new ClosureService(terminologyClientFactory.build(logger));
      final Closure subsumeClosure = closureService.getSubsumesRelation(entrySet);
      return entries.stream().map(r -> IdAndBoolean.of(r.getId(),
          subsumeClosure.anyRelates(r.getLeftCodings(), r.getRightCodings()))).iterator();
    }
  }

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
  public ParsedExpression invoke(@Nonnull FunctionInput input) {

    validateInput(input);

    ParsedExpression inputExpression = input.getInput();
    ParsedExpression argExpression = input.getArguments().get(0);

    //
    // contexExpression is a non literal expression that can be used to provide id column
    // for a liter expression.
    //
    ParsedExpression contextExpression = !inputExpression.isLiteral()
                                         ? inputExpression
                                         : (!argExpression.isLiteral()
                                            ? argExpression
                                            : null);
    assert (contextExpression != null) : "Context expression is not null";

    ExpressionParserContext parserContext = input.getContext();
    Dataset<Row> inputSystemAndCodeDataset = toSystemAndCodeDataset(
        normalizeToCoding(inputExpression, parserContext), contextExpression);
    Dataset<Row> argSystemAndCodeDataset =
        toSystemAndCodeDataset(normalizeToCoding(argExpression, parserContext), contextExpression);

    Dataset<Row> resultDataset = this.inverted
                                 ? createSubsumesResult(input.getContext(), argSystemAndCodeDataset,
        inputSystemAndCodeDataset)
                                 : createSubsumesResult(input.getContext(),
                                     inputSystemAndCodeDataset,
                                     argSystemAndCodeDataset);
    Column idColumn = resultDataset.col("id");
    Column valueColumn = resultDataset.col("value");

    // If there is a `$this` context, we need to add the value column back in to the resulting
    // dataset so that it can be passed forward in the result from the enclosing function.
    ParsedExpression thisContext = input.getContext().getThisContext();
    if (thisContext != null) {
      resultDataset = resultDataset.join(thisContext.getDataset(),
          idColumn.equalTo(thisContext.getIdColumn()), "inner");
    }

    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setFhirPathType(FhirPathType.BOOLEAN);
    result.setFhirType(FHIRDefinedType.BOOLEAN);
    result.setPrimitive(true);
    result.setSingular(true);
    result.setDataset(resultDataset);
    result.setHashedValue(idColumn, valueColumn);
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
    return joinedCodingSets.as(Encoders.bean(IdAndCodingSets.class))
        .mapPartitions(new SubsumptionMapper(ctx.getTerminologyClientFactory()),
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
   * @return Dataframe with schema: "STRING id, STRUCT (STRING system, STRING code) coding"
   */
  @Nonnull
  private Dataset<Row> toSystemAndCodeDataset(ParsedExpression inputExpression,
      ParsedExpression contextExpression) {

    FHIRDefinedType inputType = inputExpression.getFhirType();
    assert FHIRDefinedType.CODING.equals(inputType) : "Expression of CODING type expected";

    // do the literal magic here
    ParsedExpression idExpression =
        (!inputExpression.isLiteral())
        ? inputExpression
        : contextExpression;
    Dataset<Row> codingDataset =
        idExpression.getDataset().select(idExpression.getIdColumn().alias(COL_ID),
            inputExpression.getLiteralOrValueColumn().getField(COL_SYSTEM).alias(COL_SYSTEM),
            inputExpression.getLiteralOrValueColumn().getField(COL_CODE).alias(COL_CODE),
            inputExpression.getLiteralOrValueColumn().getField(COL_VERSION).alias(COL_VERSION));
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
