/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.functions;

import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.CODING;
import static au.csiro.pathling.utilities.Strings.md5Short;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.isnull;
import static org.apache.spark.sql.functions.not;
import au.csiro.pathling.encoding.Mapping;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.query.operators.PathTraversalInput;
import au.csiro.pathling.query.operators.PathTraversalOperator;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.pathling.query.parsing.parser.ExpressionParserContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.CodeableConcept;
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



class TransitiveTable {

  private final List<Mapping> mappings = new ArrayList<Mapping>();

  void addMapping(Coding source, Coding target, ConceptMapEquivalence equivalence) {
    mappings.add(new Mapping(source.getSystem(), source.getCode(), target.getSystem(),
        target.getCode(), equivalence.toCode()));
  }
}


class ClosureService {
  private final String seed;
  private final TerminologyClient terminologyClient;

  public ClosureService(String seed, TerminologyClient terminologyClient) {
    this.seed = seed;
    this.terminologyClient = terminologyClient;
  }

  TransitiveTable getClosure(String system, List<String> codes) {
    String closureName = md5Short(seed + system);
    // Execute the closure operation against the terminology server.
    terminologyClient.closure(new StringType(closureName), null, null);
    ConceptMap closure =
        terminologyClient.closure(new StringType(closureName), toCodesList(system, codes), null);
    return fromConceptMap(closure);
  }

  static List<Coding> toCodesList(final String system, List<String> codes) {
    return codes.stream().map(code -> new Coding(system, code, null)).collect(Collectors.toList());
  }

  static TransitiveTable fromConceptMap(ConceptMap conceptMap) {
    // Extract the mappings from the closure result into a set of Mapping objects.

    TransitiveTable result = new TransitiveTable();

    List<ConceptMapGroupComponent> groups = conceptMap.getGroup();
    if (groups.size() == 1) {
      ConceptMapGroupComponent group = groups.get(0);
      String sourceSystem = group.getSource();
      String targetSystem = group.getTarget();
      List<SourceElementComponent> elements = group.getElement();
      for (SourceElementComponent element : elements) {
        for (TargetElementComponent target : element.getTarget()) {
          Mapping mapping = new Mapping();
          mapping.setSourceSystem(sourceSystem);
          mapping.setSourceCode(element.getCode());
          mapping.setTargetSystem(targetSystem);
          mapping.setTargetCode(target.getCode());
          mapping.setEquivalence(target.getEquivalence().toCode());
          // TODO: add actual mappings
          // mappings.add(mapping);
        }
      }
    }
    return result;
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
    if (input.getArguments().size() != 1) {
      throw new InvalidRequestException(
          "One argument must be passed to " + functionName + " function");
    }

   
    
    // TODO: check
    // both dataset and argument must be of the same type (the id must match)
    // as we only can subsume within the same resource
    
    // the transitive closure table though can be build globally -
    // this may however produce a very large transitive closure table that includes potentially irrelevant entries
    
    // let's to do it this way however first
    
    
 
    // we at least need to evaluate the argument to the unique list of code/system pairs that we
    // can
    // broadcast for the execution

    
 
    // So the first step is to depending on the type of the input create an ID based set of system/id objects
    
    
    Dataset<Row> inputSystemAndCodeDataset = toSystemAndCodeDataset(input.getInput());
    
    inputSystemAndCodeDataset.printSchema();
    inputSystemAndCodeDataset.show();
    
    
    Dataset<Row> argSystemAndCodeDataset = toSystemAndCodeDataset(input.getArguments().get(0));

    argSystemAndCodeDataset.printSchema();
    argSystemAndCodeDataset.show();
    
    // now I literally need to produce a transitive closure table for all elements in combination of these two datasets
    // and I should get 
    
    
    Dataset<Row> transitiveClosureTable = createTransitiveClosureTable(input.getContext(), inputSystemAndCodeDataset, argSystemAndCodeDataset);
    // The transitiveClosureTable should have the following schema and 
    // represent the subsumes relation from the src to dst 
    // (srcSystem STRING, srcCode STRING, dstCode STRING, dstCode STRING)
    
    
    System.out.println("Transitive closure table");
    transitiveClosureTable.printSchema();
    transitiveClosureTable.show();   
    
    // So now we can create the join condition
    // we actually need to join three tables
    // inputSystemAndCodeDataset and  argSystemAndCodeDataset by ID
    // and also    inputSystemAndCodeDataset.code = transitiveClosureTablr.srcCode and transitiveClosureTable.dstCode = argSystemAndCodeDataset.code
    
    Dataset<Row> joinedDataset = inputSystemAndCodeDataset.join(transitiveClosureTable, inputSystemAndCodeDataset.col("system")
        .equalTo(transitiveClosureTable.col("srcSystem")).and(inputSystemAndCodeDataset.col("code")
            .equalTo(transitiveClosureTable.col("srcCode"))), "left_outer")
      .join(argSystemAndCodeDataset, argSystemAndCodeDataset.col("id").equalTo(inputSystemAndCodeDataset.col("id")).and(argSystemAndCodeDataset.col("system")
          .equalTo(transitiveClosureTable.col("dstSystem")).and(argSystemAndCodeDataset.col("code")
              .equalTo(transitiveClosureTable.col("dstCode")))), "left_outer");
    
    joinedDataset.printSchema();
    joinedDataset.show();
    
    System.out.println("Result dataset");
    Dataset<Row> resultDataset = joinedDataset
        .groupBy(inputSystemAndCodeDataset.col("id"))
        .agg(max(not(isnull(argSystemAndCodeDataset.col("id")))).alias("subsumes"));
    resultDataset.printSchema();
    resultDataset.show();  
 
    
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setFhirPathType(FhirPathType.BOOLEAN);
    result.setFhirType(FHIRDefinedType.BOOLEAN);
    result.setPrimitive(true);
    result.setSingular(input.getInput().isSingular());
    result.setDataset(resultDataset);
    result.setHashedValue(resultDataset.col("id"), resultDataset.col("subsumes"));
    return result;
    
    
    
//    
//    
//    
//    
//    List<SystemAndCode> argumentCodes = collectCodes(input.getArguments().get(0));
//    System.out.println(argumentCodes);
//
//    List<SystemAndCode> inputCodes = collectCodes(input.getInput());
//    System.out.println(inputCodes);
//
//
//    Map<String, List<SystemAndCode>> codesBySystem =
//        inputCodes.stream().collect(Collectors.groupingBy(SystemAndCode::getSystem));
//
//    System.out.println(codesBySystem);
//
//
//
//
//    SparkSession spark = input.getContext().getSparkSession();
//    ParsedExpression inputResult = validateInput(input);
//    ParsedExpression argument = validateArgument(input);
//
//
//    // inputResult.getDataset().mapPartitionsInR(func, packageNames, broadcastVars, schema)
//
//    // Create a dataset to represent the input expression.
//    Dataset<SystemAndCode> inputDataset = inputResult.getDataset()
//        .select(inputResult.getValueColumn().getField("system").alias("system"),
//            inputResult.getValueColumn().getField("code").alias("code"))
//        .as(Encoders.bean(SystemAndCode.class));
//    // inputDataset = inputResult.getDataset().as(Encoders.bean(Coding.class));
//
//    // Create a dataset to represent the argument expression.
//    Dataset<SystemAndCode> argumentDataset;
//    if (argument.getLiteralValue() == null) {
//      argumentDataset = argument.getDataset().as(Encoders.bean(SystemAndCode.class));
//    } else {
//      // If the argument is a literal value, we create a dataset with a single Coding.
//      assert argument.getLiteralValue().fhirType().equals("Coding");
//      List<SystemAndCode> codings =
//          Collections.singletonList(new SystemAndCode((Coding) argument.getLiteralValue()));
//      argumentDataset = spark.createDataset(codings, Encoders.bean(SystemAndCode.class));
//    }
//
//    Column inputIdCol = inputResult.getIdColumn();
//    Column inputSystemCol = inputResult.getValueColumn().getField("system");
//    Column inputCodeCol = inputResult.getValueColumn().getField("code");
//    Column argumentSystemCol = argumentDataset.col("system");
//    Column argumentCodeCol = argumentDataset.col("code");
//
//    // Build a closure table dataset.
//    Dataset<Mapping> closureTable = buildClosureTable(input, inputResult, argument);
//    Column closureSourceSystem = closureTable.col("sourceSystem");
//    Column closureSourceCode = closureTable.col("sourceCode");
//    Column closureTargetSystem = closureTable.col("targetSystem");
//    Column closureTargetCode = closureTable.col("targetCode");
//    Column closureEquivalence = closureTable.col("equivalence");
//
//    // Create a new dataset which contains a boolean value for each input coding, which indicates
//    // whether the coding subsumes (or is subsumed by) any of the codings within the argument
//    // dataset.
//    Column inputSystemMatch = inverted ? inputSystemCol.equalTo(closureSourceSystem)
//        : inputSystemCol.equalTo(closureTargetSystem);
//    Column inputCodeMatch = inverted ? inputCodeCol.equalTo(closureSourceCode)
//        : inputCodeCol.equalTo(closureTargetCode);
//    Column argumentSystemMatch = inverted ? argumentSystemCol.equalTo(closureTargetSystem)
//        : argumentSystemCol.equalTo(closureSourceSystem);
//    Column argumentCodeMatch = inverted ? argumentCodeCol.equalTo(closureTargetCode)
//        : argumentCodeCol.equalTo(closureSourceCode);
//    Column equivalenceMatch = inverted ? closureEquivalence.equalTo("specializes")
//        : closureEquivalence.equalTo("subsumes");
//    Column joinCondition = inputSystemMatch.and(inputCodeMatch);
//    joinCondition = joinCondition.and(argumentSystemMatch.and(argumentCodeMatch));
//    Dataset<Row> dataset =
//        inputDataset.join(argumentDataset, joinCondition).select(inputIdCol, equivalenceMatch);
//    dataset = dataset.groupBy(inputIdCol).agg(max(equivalenceMatch));
//    dataset = dataset.select(inputIdCol, equivalenceMatch);

  }

  private Dataset<Row> createTransitiveClosureTable(ExpressionParserContext expressionParserContext, Dataset<Row> inputSystemAndCodeDataset,
      Dataset<Row> argSystemAndCodeDataset) {

    
    // per minium each code should subsume itself
    
    
    Dataset<Row> codes = getCodes(inputSystemAndCodeDataset.union(argSystemAndCodeDataset));
    
    return codes.select(codes.col("system").alias("srcSystem"),codes.col("code").alias("srcCode"), codes.col("system").alias("dstSystem"), codes.col("code").alias("dstCode"));
    
//    SparkSession spark = inputSystemAndCodeDataset.sparkSession();
//    Metadata metadata = new MetadataBuilder().build();
//    StructType closureTableStruct = new StructType(new StructField[] {
//        new StructField("srcSystem", DataTypes.StringType, true, metadata),
//        new StructField("srcCode", DataTypes.StringType, true, metadata),
//        new StructField("dstSystem", DataTypes.StringType, true, metadata),
//        new StructField("dstCode", DataTypes.StringType, true, metadata),
//    });  
//    return spark.createDataFrame(Collections.emptyList(), closureTableStruct);
  }

  private Dataset<Row> toSystemAndCodeDataset(ParsedExpression inputExpression) {
    
    //TODO: Add support for literal coding values
    FHIRDefinedType inputType = inputExpression.getFhirType();
    Dataset<Row> inputDst= inputExpression.getDataset();
    Dataset<Row> codingDataset = null;
    if (FHIRDefinedType.CODING.equals(inputType)) {
      codingDataset = inputDst.select(inputExpression.getIdColumn().alias("id"),
          inputExpression.getValueColumn().getField("system").alias("system"), inputExpression.getValueColumn().getField("code").alias("code"));
    } else if (FHIRDefinedType.CODEABLECONCEPT.equals(inputType)) {
      codingDataset = inputDst.select(inputExpression.getIdColumn(), explode(inputExpression.getValueColumn().getField("coding")).alias("coding")).
          select(inputExpression.getIdColumn().alias("id"),
              col("coding").getField("system").alias("system"),  col("coding").getField("code").alias("code"));
    } else {
      throw new IllegalArgumentException("Cannot extract codings from element of type: " + inputType);
    }
    return codingDataset;
  }

  private List<SystemAndCode> collectCodes(ParsedExpression parsedExpression) {
    List<SystemAndCode> codings = null;
    if (parsedExpression.getLiteralValue() != null) {
      SystemAndCode literalValue = new SystemAndCode((Coding) parsedExpression.getLiteralValue());
      codings = Collections.singletonList(literalValue);
    } else {
      Dataset<SystemAndCode> inputDataset = parsedExpression.getDataset()
          .select(parsedExpression.getValueColumn().getField("system").alias("system"),
              parsedExpression.getValueColumn().getField("code").alias("code"))
          .distinct().as(Encoders.bean(SystemAndCode.class));
      codings = inputDataset.collectAsList();
    }
    return codings;
  }

  /**
   * Executes a closure operation including the codes from the input and argument to this function,
   * then store the result in a Spark Dataset accessible via a temporary view.
   */
  private Dataset<Mapping> buildClosureTable(FunctionInput input, ParsedExpression inputResult,
      ParsedExpression argument) {
    SparkSession spark = input.getContext().getSparkSession();
    Map<String, List<Coding>> codingsBySystem = new HashMap<>();

    // Get the set of codes from both the input and the argument.
    Dataset<Row> inputCodes = null;
    Dataset<Row> argumentCodes = null;
    // If the input is a literal, harvest the code - otherwise we need to query for the codes.
    if (inputResult.getLiteralValue() != null) {
      Coding literalValue = (Coding) inputResult.getLiteralValue();
      List<Coding> codings = Collections.singletonList(literalValue);
      codingsBySystem.put(literalValue.getSystem(), codings);
    } else {
      inputCodes = getCodes(inputResult.getDataset());
    }
    // If the argument is a literal, harvest the code - otherwise we need to query for the codes.
    if (argument.getLiteralValue() != null) {
      Coding literalValue = (Coding) argument.getLiteralValue();
      List<Coding> codings = Collections.singletonList(literalValue);
      if (codingsBySystem.get(literalValue.getSystem()) == null) {
        codingsBySystem.put(literalValue.getSystem(), codings);
      } else {
        codingsBySystem.get(literalValue.getSystem()).addAll(codings);
      }
    } else {
      argumentCodes = getCodes(argument.getDataset());
    }
    // The query will be a union if we need to query for both the input and argument codes.
    Dataset<Row> allCodes =
        inputCodes != null && argumentCodes != null ? inputCodes.union(argumentCodes)
            : inputCodes != null ? inputCodes : argumentCodes;

    // If needed, execute the query to get the codes.
    if (inputCodes != null || argumentCodes != null) {
      // Get the list of distinct code systems.
      List<Row> distinctCodeSystemRows =
          allCodes.select(allCodes.col("system")).distinct().collectAsList();
      List<String> distinctCodeSystems =
          distinctCodeSystemRows.stream().map(row -> row.getString(0)).collect(Collectors.toList());

      // Query for the codings from each distinct code system within the result, and add the
      // codings
      // to the map.
      for (String codeSystem : distinctCodeSystems) {
        Column systemCol = allCodes.col("system");
        List<Row> codeResults = allCodes.filter(systemCol.equalTo(codeSystem)).collectAsList();
        List<Coding> codings =
            codeResults.stream().map(row -> new Coding(row.getString(0), row.getString(1), null))
                .collect(Collectors.toList());
        if (codingsBySystem.get(codeSystem) == null) {
          codingsBySystem.put(codeSystem, codings);
        } else {
          codingsBySystem.get(codeSystem).addAll(codings);
        }
      }
    }

    // Execute a closure operation for each set of codings within each distinct code system.
    TerminologyClient terminologyClient = input.getContext().getTerminologyClient();
    List<Mapping> mappings = new ArrayList<>();
    for (String codeSystem : codingsBySystem.keySet()) {
      // Get the codings for this code system.
      List<Coding> codings = codingsBySystem.get(codeSystem);

      // Create a unique name for the closure table for this code system, based upon the
      // expressions
      // of the input, argument and the CodeSystem URI.
      String closureName =
          md5Short(inputResult.getFhirPath() + argument.getFhirPath() + codeSystem);

      // Execute the closure operation against the terminology server.
      terminologyClient.closure(new StringType(closureName), null, null);
      ConceptMap closure = terminologyClient.closure(new StringType(closureName), codings, null);

      // Extract the mappings from the closure result into a set of Mapping objects.
      List<ConceptMapGroupComponent> groups = closure.getGroup();
      if (groups.size() == 1) {
        ConceptMapGroupComponent group = groups.get(0);
        String sourceSystem = group.getSource();
        String targetSystem = group.getTarget();
        List<SourceElementComponent> elements = group.getElement();
        for (SourceElementComponent element : elements) {
          for (TargetElementComponent target : element.getTarget()) {
            Mapping mapping = new Mapping();
            mapping.setSourceSystem(sourceSystem);
            mapping.setSourceCode(element.getCode());
            mapping.setTargetSystem(targetSystem);
            mapping.setTargetCode(target.getCode());
            mapping.setEquivalence(target.getEquivalence().toCode());
            mappings.add(mapping);
          }
        }
      } else if (groups.size() > 1) {
        logger.warn("Encountered closure response with more than one group");
      }
    }

    // Return a Spark Dataset containing the mappings.
    return spark.createDataset(mappings, Encoders.bean(Mapping.class));
  }

  private ParsedExpression validateInput(FunctionInput input) {
    ParsedExpression inputResult = input.getInput();
    String inputFhirPath = inputResult.getFhirPath();
    if (inputResult.getLiteralValue() != null) {
      throw new InvalidRequestException("Input to " + functionName
          + " function cannot be a literal value: " + inputResult.getFhirPath());
    }
    if (inputResult.getFhirPathType() == CODING) {
      return inputResult;
    }
    FHIRDefinedType typeCode = inputResult.getFhirType();
    if (!typeCode.equals(FHIRDefinedType.CODEABLECONCEPT)) {
      throw new InvalidRequestException("Input to " + functionName
          + " function must be Coding or CodeableConcept: " + inputResult.getFhirPath());
    } else {
      // If this is a CodeableConcept, we need to traverse to the `coding` member first.
      PathTraversalInput pathTraversalInput = new PathTraversalInput();
      pathTraversalInput.setContext(input.getContext());
      pathTraversalInput.setLeft(inputResult);
      pathTraversalInput.setExpression("coding");
      pathTraversalInput.setRight("coding");
      inputResult = new PathTraversalOperator().invoke(pathTraversalInput);
      inputResult.setFhirPath(inputFhirPath + ".coding");
      return inputResult;
    }
  }

  private ParsedExpression validateArgument(FunctionInput input) {
    ParsedExpression argument = input.getArguments().get(0);
    String argumentFhirPath = argument.getFhirPath();
    if (argument.getFhirPathType() == CODING) {
      return argument;
    }
    FHIRDefinedType typeCode = argument.getFhirType();
    if (!typeCode.equals(FHIRDefinedType.CODEABLECONCEPT)) {
      throw new InvalidRequestException("Argument to " + functionName
          + " function must be Coding or CodeableConcept: " + argument.getFhirPath());
    } else {
      // If this is a CodeableConcept, we need to traverse to the `coding` member first.
      PathTraversalInput pathTraversalInput = new PathTraversalInput();
      pathTraversalInput.setContext(input.getContext());
      pathTraversalInput.setLeft(argument);
      pathTraversalInput.setExpression("coding");
      pathTraversalInput.setRight("coding");
      argument = new PathTraversalOperator().invoke(pathTraversalInput);
      argument.setFhirPath(argumentFhirPath + ".coding");
      return argument;
    }
  }

  @Nonnull
  private Dataset<Row> getCodes(Dataset<Row> source) {
    Column systemCol = source.col("system");
    Column codeCol = source.col("code");

    Dataset<Row> codes = source.select(systemCol, codeCol);
    codes = codes.filter(systemCol.isNotNull().and(codeCol.isNotNull()));
    codes.distinct();

    return codes;
  }
}
