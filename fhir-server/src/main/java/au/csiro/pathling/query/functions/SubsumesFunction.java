/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.functions;

import static au.csiro.pathling.utilities.Strings.md5Short;
import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.isnull;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.not;
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
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
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

  public Closure getSubumesRelationInt(Collection<SystemAndCode> systemAndCodes) {

    List<Coding> codings =
        systemAndCodes.stream().map(SystemAndCode::toCoding).collect(Collectors.toList());
    // recreate the systemAndCodes dataset from the list not to execute the query again.
    // Create a unique name for the closure table for this code system, based upon the
    // expressions of the input, argument and the CodeSystem URI.
    logger.info("Sending $closure requests to terminology service for codings: " + systemAndCodes);
    String closureName = md5Short(seed + codings.hashCode());
    // Execute the closure operation against the terminology server.
    // TODO: add validatoin checks for the response
    terminologyClient.closure(new StringType(closureName), null, null);
    ConceptMap closure = terminologyClient.closure(new StringType(closureName), codings, null);
    return Closure.fromMappings(conceptMapToMappings(closure));
  }


  public Relation getSubumesRelation(Dataset<SystemAndCode> codingsDataset) {

    List<SystemAndCode> systemAndCodes = codingsDataset.collectAsList();
    List<Coding> codings =
        systemAndCodes.stream().map(SystemAndCode::toCoding).collect(Collectors.toList());
    // recreate the systemAndCodes dataset from the list not to execute the query again.
    Relation result = Relation.createEquivalence(codingsDataset.sparkSession()
        .createDataset(systemAndCodes, Encoders.bean(SystemAndCode.class)));
    // Create a unique name for the closure table for this code system, based upon the
    // expressions of the input, argument and the CodeSystem URI.
    logger.debug("Sending $closure requests to terminology service for codings: " + systemAndCodes);
    String closureName = md5Short(seed + codings.hashCode());
    // Execute the closure operation against the terminology server.
    // TODO: add validatoin checks for the response
    terminologyClient.closure(new StringType(closureName), null, null);
    ConceptMap closure = terminologyClient.closure(new StringType(closureName), codings, null);
    return result.union(conceptMapToSubsumesRelation(closure, codingsDataset.sparkSession()));
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
      // the expected structure of the rows is
      // id, array<coding>, array<coding>
      // there are two parts here:
      // 1) we need to collect all the IDs for closure construction
      // 2) we need to cache the actual data also so that later we can resolve each id
      // NOTE: NULL codings is present will be represented as SystemAndCode with null values for
      // system and code.
      // in the array
      logger.info("Processing partition iterator: {}", input);
      List<IdAndCodingSets> entries = Streams.stream(input).collect(Collectors.toList());
      logger.info("Collected entries: {}", entries.size());
      // collect distinct tokens
      Set<SystemAndCode> entrySet = entries.stream()
          .flatMap(r -> Streams.concat(r.getLeftCodings().stream(), r.getRightCodings().stream()))
          .filter(SystemAndCode::isNotNull).collect(Collectors.toSet());
      logger.info("Collected left sets: {}", entrySet);
      String seed = UUID.randomUUID().toString();
      ClosureService cs = new ClosureService(seed, terminologyClientFactory.build(logger));
      final Closure cl = cs.getSubumesRelationInt(entrySet);
      logger.info("Closure mappins are: {}", cl);
      return entries.stream().map(
          r -> IdAndBoolean.of(r.getId(), cl.anyRelates(r.getLeftCodings(), r.getRightCodings())))
          .iterator();
    }
  }


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
    Dataset<Row> inputCodingSet =
        inputSystemAndCodeDataset.groupBy(inputSystemAndCodeDataset.col("id"))
            .agg(collect_set(inputSystemAndCodeDataset.col("coding")).alias("coding_set"));

    Dataset<Row> argCodingSet = argSystemAndCodeDataset.groupBy(argSystemAndCodeDataset.col("id"))
        .agg(collect_set(argSystemAndCodeDataset.col("coding")).alias("coding_set"));

    Dataset<Row> joinedCodingSets = inputCodingSet
        .join(argCodingSet, inputCodingSet.col("id").equalTo(argCodingSet.col("id")), "left_outer")
        .select(inputCodingSet.col("id").alias("id"),
            inputCodingSet.col("coding_set").alias("leftCodings"),
            argCodingSet.col("coding_set").alias("rightCodings"));

    Dataset<Row> resultDataset = joinedCodingSets.as(Encoders.bean(IdAndCodingSets.class))
        .mapPartitions(new SubsumptionMapper(ctx.getTerminologyClientFactory()),
            Encoders.bean(IdAndBoolean.class))
        .toDF();
    return resultDataset;
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
