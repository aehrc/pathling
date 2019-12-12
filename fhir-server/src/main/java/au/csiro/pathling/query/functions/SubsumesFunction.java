/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.functions;

import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.CODING;
import static au.csiro.pathling.utilities.Strings.md5Short;
import static org.apache.spark.sql.functions.max;

import au.csiro.pathling.encoding.Mapping;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.query.operators.PathTraversalInput;
import au.csiro.pathling.query.operators.PathTraversalOperator;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.spark.sql.*;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.r4.model.ConceptMap.SourceElementComponent;
import org.hl7.fhir.r4.model.ConceptMap.TargetElementComponent;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.StringType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Describes a function which returns a boolean value based upon whether any of the input set of
 * Codings or CodeableConcepts subsume one or more Codings or CodeableConcepts in the target set.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/fhirpath.html#functions">https://hl7.org/fhir/fhirpath.html#functions</a>
 */
public class SubsumesFunction implements Function {

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
    if (input.getArguments().size() != 1) {
      throw new InvalidRequestException(
          "One argument must be passed to " + functionName + " function");
    }

    SparkSession spark = input.getContext().getSparkSession();
    ParsedExpression inputResult = validateInput(input);
    ParsedExpression argument = validateArgument(input);

    // Create a dataset to represent the input expression.
    Dataset<Coding> inputDataset;
    inputDataset = inputResult.getDataset().as(Encoders.bean(Coding.class));

    // Create a dataset to represent the argument expression.
    Dataset<Coding> argumentDataset;
    if (argument.getLiteralValue() == null) {
      argumentDataset = argument.getDataset().as(Encoders.bean(Coding.class));
    } else {
      // If the argument is a literal value, we create a dataset with a single Coding.
      assert argument.getLiteralValue().fhirType().equals("Coding");
      List<Coding> codings = Collections.singletonList((Coding) argument.getLiteralValue());
      argumentDataset = spark.createDataset(codings, Encoders.bean(Coding.class));
    }

    Column inputIdCol = inputResult.getIdColumn();
    Column inputSystemCol = inputResult.getValueColumn().getField("system");
    Column inputCodeCol = inputResult.getValueColumn().getField("code");
    Column argumentSystemCol = argument.getValueColumn().getField("system");
    Column argumentCodeCol = argument.getValueColumn().getField("code");

    // Build a closure table dataset.
    Dataset<Mapping> closureTable = buildClosureTable(input, inputResult, argument);
    Column closureSourceSystem = closureTable.col("sourceSystem");
    Column closureSourceCode = closureTable.col("sourceCode");
    Column closureTargetSystem = closureTable.col("targetSystem");
    Column closureTargetCode = closureTable.col("targetCode");
    Column closureEquivalence = closureTable.col("equivalence");

    // Create a new dataset which contains a boolean value for each input coding, which indicates
    // whether the coding subsumes (or is subsumed by) any of the codings within the argument
    // dataset.
    Column inputSystemMatch = inverted
        ? inputSystemCol.equalTo(closureSourceSystem)
        : inputSystemCol.equalTo(closureTargetSystem);
    Column inputCodeMatch = inverted
        ? inputCodeCol.equalTo(closureSourceCode)
        : inputCodeCol.equalTo(closureTargetCode);
    Column argumentSystemMatch = inverted
        ? argumentSystemCol.equalTo(closureTargetSystem)
        : argumentSystemCol.equalTo(closureSourceSystem);
    Column argumentCodeMatch = inverted
        ? argumentCodeCol.equalTo(closureTargetCode)
        : argumentCodeCol.equalTo(closureSourceCode);
    Column equivalenceMatch = inverted
        ? closureEquivalence.equalTo("specializes")
        : closureEquivalence.equalTo("subsumes");
    Column joinCondition = inputSystemMatch.and(inputCodeMatch);
    joinCondition = joinCondition.and(argumentSystemMatch.and(argumentCodeMatch));
    Dataset<Row> dataset = inputDataset
        .join(argumentDataset, joinCondition)
        .select(inputIdCol, equivalenceMatch);
    dataset = dataset
        .groupBy(inputIdCol)
        .agg(max(equivalenceMatch));
    dataset = dataset.select(inputIdCol, equivalenceMatch);

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setFhirPathType(FhirPathType.BOOLEAN);
    result.setFhirType(FHIRDefinedType.BOOLEAN);
    result.setPrimitive(true);
    result.setSingular(inputResult.isSingular());
    result.setDataset(dataset);
    result.setIdColumn(inputIdCol);
    result.setValueColumn(equivalenceMatch);
    return result;
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
      inputCodes = getCodes(inputResult);
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
      argumentCodes = getCodes(argument);
    }
    // The query will be a union if we need to query for both the input and argument codes.
    Dataset<Row> allCodes =
        inputCodes != null && argumentCodes != null
            ? inputCodes.union(argumentCodes)
            : inputCodes != null
                ? inputCodes : argumentCodes;

    // If needed, execute the query to get the codes.
    if (inputCodes != null || argumentCodes != null) {
      // Get the list of distinct code systems.
      List<Row> distinctCodeSystemRows = allCodes.select(allCodes.col("system")).distinct()
          .collectAsList();
      List<String> distinctCodeSystems = distinctCodeSystemRows.stream()
          .map(row -> row.getString(0))
          .collect(Collectors.toList());

      // Query for the codings from each distinct code system within the result, and add the codings
      // to the map.
      for (String codeSystem : distinctCodeSystems) {
        Column systemCol = allCodes.col("system");
        List<Row> codeResults = allCodes.filter(systemCol.equalTo(codeSystem)).collectAsList();
        List<Coding> codings = codeResults.stream()
            .map(row -> new Coding(row.getString(0), row.getString(1), null))
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

      // Create a unique name for the closure table for this code system, based upon the expressions
      // of the input, argument and the CodeSystem URI.
      String closureName = md5Short(
          inputResult.getFhirPath() + argument.getFhirPath() + codeSystem);

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
      throw new InvalidRequestException(
          "Input to " + functionName + " function cannot be a literal value: " + inputResult
              .getFhirPath());
    }
    if (inputResult.getFhirPathType() == CODING) {
      return inputResult;
    }
    FHIRDefinedType typeCode = inputResult.getFhirType();
    if (!typeCode.equals(FHIRDefinedType.CODEABLECONCEPT)) {
      throw new InvalidRequestException(
          "Input to " + functionName + " function must be Coding or CodeableConcept: "
              + inputResult
              .getFhirPath());
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
      throw new InvalidRequestException(
          "Argument to " + functionName + " function must be Coding or CodeableConcept: " + argument
              .getFhirPath());
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
  private Dataset<Row> getCodes(ParsedExpression result) {
    Dataset<Row> source = result.getDataset();
    Column systemCol = source.col("system");
    Column codeCol = source.col("code");

    Dataset<Row> codes = source.select(systemCol, codeCol);
    codes = codes.filter(systemCol.isNotNull().and(codeCol.isNotNull()));
    codes.distinct();

    return codes;
  }

}
