/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.functions;

import static au.csiro.pathling.utilities.Strings.md5Short;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode_outer;
import static org.apache.spark.sql.functions.isnull;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.builder.ToStringBuilder;
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
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.StringType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import au.csiro.pathling.encoding.Mapping;
import au.csiro.pathling.encoding.SystemAndCode;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.pathling.query.parsing.parser.ExpressionParserContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;



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
    
    Column srcCol = !inverted?mappingTable.col("from"):mappingTable.col("to");
    Column dstCol = !inverted?mappingTable.col("to"):mappingTable.col("from");
    
    Dataset<Row> joinedDataset =
        from.join(mappingTable, from.col("coding").equalTo(srcCol), "left_outer")
            .join(to, to.col("id").equalTo(from.col("id"))
                .and(to.col("coding").equalTo(dstCol)), "left_outer");

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
   * Creates equivalence relation from given codes
   * 
   * @param codes
   */
  public static Relation createEquivalence(Dataset<SystemAndCode> codes) {
    
    //TODO: Maybe rewrite as select
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


class ClosureService {
  private static final Logger logger = LoggerFactory.getLogger(ClosureService.class);

  private final String seed;
  private final TerminologyClient terminologyClient;

  public ClosureService(String seed, TerminologyClient terminologyClient) {
    this.seed = seed;
    this.terminologyClient = terminologyClient;
  }

  public Relation getClosure(Dataset<SystemAndCode> codingsDataset) {
    // need

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
      ConceptMap closure = terminologyClient.closure(new StringType(closureName), codings, null);
      // TODO: try to rewrite as reduce/fold
      result = result.union(conceptMapToRelation(closure, codingsDataset.sparkSession()));
    }
    return result;
  }

  public static Relation conceptMapToRelation(ConceptMap conceptMap, SparkSession spark) {

    List<Mapping> mappings = new ArrayList<Mapping>();
    if (conceptMap.hasGroup()) {
      List<ConceptMapGroupComponent> groups = conceptMap.getGroup();
      // TODO: check with JOHN
      // how to handle this
      if (groups.size() > 1) {
        logger.warn("Encountered closure response with more than one group");
      }
      assert groups.size() <= 1 : "At most one group expected";
      // logger.warn("Encountered closure response with more than one group");
      if (groups.size() > 0) {
        ConceptMapGroupComponent group = groups.get(0);
        String sourceSystem = group.getSource();
        String targetSystem = group.getTarget();
        List<SourceElementComponent> elements = group.getElement();
        for (SourceElementComponent source : elements) {
          for (TargetElementComponent target : source.getTarget()) {
            // TODO: handle different direction returned by in eqivalence
            mappings
                .add(new Mapping(sourceSystem, source.getCode(), targetSystem, target.getCode()));
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
 * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#functions">Additional functions</a>
 */
public class SubsumesFunction implements Function {

  private static final Logger logger = LoggerFactory.getLogger(SubsumesFunction.class);

  private static void debugDataset(Dataset<?> dataset, String msg) {
    System.out.println(msg);
    dataset.printSchema();
    dataset.show();
  }

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

    // the transitive closure table though can be build globally -
    // this may however produce a very large transitive closure table that includes potentially
    // irrelevant entries

    // let's to do it this way however first
    // So the first step is to depending on the type of the input create an ID based set of
    // system/id objects

    // Find the context expression (the non literal one)

    ParsedExpression inputExpression = input.getInput();
    ParsedExpression argExpression = input.getArguments().get(0);
    ParsedExpression contextExpression = !inputExpression.isLiteral() ? inputExpression
        : (!argExpression.isLiteral() ? argExpression : null);
    assert (contextExpression != null) : "Context expression is not null";

    Dataset<Row> inputSystemAndCodeDataset =
        toSystemAndCodeDataset(inputExpression, contextExpression);
    Dataset<Row> argSystemAndCodeDataset = toSystemAndCodeDataset(argExpression, contextExpression);

    debugDataset(inputSystemAndCodeDataset, "Input SystemAndCode");
    debugDataset(argSystemAndCodeDataset, "Argument SystemAndCode");

    String seed = (inputExpression.getFhirPath() + inputExpression.getFhirPath());
    Relation relation = createTransitiveClosureTable(seed, input.getContext(),
        inputSystemAndCodeDataset, argSystemAndCodeDataset);

    debugDataset(relation.mappingTable, " RelationTable");

    Dataset<Row> resultDataset = relation.apply(inputSystemAndCodeDataset, argSystemAndCodeDataset);
    debugDataset(resultDataset, "Result dataset:");

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
    Relation subsumesRelation =  closureService
        .getClosure(getCodes(inputSystemAndCodeDataset.union(argSystemAndCodeDataset)));
    
    return (!inverted)? subsumesRelation : subsumesRelation.invert();
  }
  
  @Nonnull
  private Dataset<SystemAndCode> getCodes(Dataset<Row> source) {
    Column systemCol = source.col("coding").getField("system").alias("system");
    Column codeCol = source.col("coding").getField("code").alias("code");
    Dataset<Row> codes = source.select(codeCol, systemCol);
    return codes.where(systemCol.isNotNull().and(codeCol.isNotNull())).distinct()
        .as(Encoders.bean(SystemAndCode.class));
  }
  
  @Nonnull
  private Dataset<Row> toSystemAndCodeDataset(ParsedExpression inputExpression,
      ParsedExpression contextExpression) {

    System.out.println("Converting experession");
    System.out.println(ToStringBuilder.reflectionToString(inputExpression));

    FHIRDefinedType inputType = inputExpression.getFhirType();
    Dataset<Row> inputDataset = inputExpression.getDataset();
    Dataset<Row> codingDataset = null;

    // TODO: RECONSIDER
    // Perhaps the travelsal should be done with the follwing code
    // If this is a CodeableConcept, we need to traverse to the `coding` member first.
    // PathTraversalInput pathTraversalInput = new PathTraversalInput();
    // pathTraversalInput.setContext(input.getContext());
    // pathTraversalInput.setLeft(argument);
    // pathTraversalInput.setExpression("coding");
    // pathTraversalInput.setRight("coding");
    // argument = new PathTraversalOperator().invoke(pathTraversalInput);
    // argument.setFhirPath(argumentFhirPath + ".coding");
    // return argument;

    if (FHIRDefinedType.CODING.equals(inputType)) {
      // do the literal magic here
      ParsedExpression idExpression =
          (!inputExpression.isLiteral()) ? inputExpression : contextExpression;
      codingDataset = idExpression.getDataset().select(idExpression.getIdColumn().alias("id"),
          inputExpression.getLiteralOrValueColumn().getField("system").alias("system"),
          inputExpression.getLiteralOrValueColumn().getField("code").alias("code"));
    } else if (FHIRDefinedType.CODEABLECONCEPT.equals(inputType)) {
      codingDataset = inputDataset
          .select(inputExpression.getIdColumn(),
              explode_outer(inputExpression.getValueColumn().getField("coding")).alias("coding"))
          .select(inputExpression.getIdColumn().alias("id"),
              col("coding").getField("system").alias("system"),
              col("coding").getField("code").alias("code"));
    } else {
      throw new IllegalArgumentException("Cannot extract codings from element of type: " + inputType
          + "in expression: " + inputExpression);
    }
    // TODO: add filtering on non empty system and code
    // Note: the struct needs to be (code, system) as this is the order in which SystemAndCode is
    // encoded.
    return codingDataset.select(codingDataset.col("id"),
        struct(codingDataset.col("code"), codingDataset.col("system")).alias("coding"));
  }

  private void validateInput(FunctionInput input) {

    if (input.getArguments().size() != 1) {
      throw new InvalidRequestException(
          functionName + " function accepts one argument of type Coding|CodeableConcept: " + input.getExpression());
    }

    ParsedExpression inputExpression = input.getInput();
    ParsedExpression argExpression = input.getArguments().get(0);
    validateExpressionType(inputExpression, "Input");
    validateExpressionType(argExpression, "Argument");
    // at least one expression must not be a literal
    if (inputExpression.isLiteral() && argExpression.isLiteral()) {
      throw new InvalidRequestException(
          "Input and argument cannot be both literals for " + functionName + " function");
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
      throw new InvalidRequestException(expressionRole + " to " + functionName
          + " function must be Coding or CodeableConcept: " + inputResult.getFhirPath());
    }
  }
}
