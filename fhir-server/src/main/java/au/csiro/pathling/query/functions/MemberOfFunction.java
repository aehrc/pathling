/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.functions;

import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.CODING;
import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.STRING;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.encoding.ValidateCodeResult;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.pathling.query.parsing.parser.ExpressionParserContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleEntryRequestComponent;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.Bundle.HTTPVerb;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * A function that takes a set of Codings or CodeableConcepts as inputs and returns a set of boolean
 * values, based upon whether each item is present within the ValueSet identified by the supplied
 * URL.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#memberof">memberOf</a>
 */
public class MemberOfFunction implements Function {

  private ValidateCodeMapper configuredMapper;

  public MemberOfFunction() {
  }

  /**
   * This is used for substituting an alternate mapper for testing purposes.
   */
  public MemberOfFunction(ValidateCodeMapper configuredMapper) {
    this.configuredMapper = configuredMapper;
  }

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull FunctionInput input) {
    validateInput(input);
    ParsedExpression inputResult = input.getInput();
    ParsedExpression argument = input.getArguments().get(0);
    Dataset<Row> prevDataset = input.getInput().getDataset();
    Column prevIdColumn = inputResult.getIdColumn(),
        prevValueColumn = inputResult.getValueColumn();

    // Prepare the data which will be used within the map operation. All of these things must be
    // Serializable.
    TerminologyClientFactory terminologyClientFactory = input.getContext()
        .getTerminologyClientFactory();
    FHIRDefinedType fhirType = inputResult.getFhirType();
    String valueSetUri = argument.getLiteralValue().toString();

    // Perform a validate code operation on each Coding or CodeableConcept in the input dataset,
    // then create a new dataset with the boolean results.
    Dataset<Row> dataset;
    Dataset validateResults;
    ValidateCodeMapper validateCodeMapper = configuredMapper == null
                                            ? new ValidateCodeMapper(MDC.get("requestId"),
        terminologyClientFactory, valueSetUri,
        fhirType)
                                            : configuredMapper;

    // This de-duplicates the Codings to be validated, then performs the validation on a
    // per-partition basis.
    Column prevValueHashColumn = functions.hash(prevValueColumn);
    validateResults = prevDataset
        .select(prevValueHashColumn, prevValueColumn)
        .dropDuplicates()
        .filter(prevValueColumn.isNotNull())
        .mapPartitions(validateCodeMapper, Encoders.bean(ValidateCodeResult.class));
    Column valueColumn = validateResults.col("result");
    Column resultHashColumn = validateResults.col("hash");

    // We then join the input dataset to the validated codes, and select the validation result
    // as the new value.
    dataset = prevDataset
        .join(validateResults, prevValueHashColumn.equalTo(resultHashColumn), "left_outer");
    
    // The conditional expression around the value column is required to deal with nulls. This
    // function should only ever return true or false.
    valueColumn = when(valueColumn.isNull(), false).otherwise(valueColumn);

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setFhirPathType(FhirPathType.BOOLEAN);
    result.setFhirType(FHIRDefinedType.BOOLEAN);
    result.setPrimitive(true);
    result.setSingular(inputResult.isSingular());
    result.setDataset(dataset);
    result.setHashedValue(prevIdColumn, valueColumn);
    return result;
  }

  private void validateInput(FunctionInput input) {
    ExpressionParserContext context = input.getContext();
    if (context.getTerminologyClientFactory() == null) {
      throw new InvalidRequestException(
          "Attempt to call terminology function memberOf when no terminology service is configured");
    }
    ParsedExpression inputResult = input.getInput();
    if (!(inputResult.getFhirPathType() == CODING
        || inputResult.getFhirType() == FHIRDefinedType.CODEABLECONCEPT)) {
      throw new InvalidRequestException(
          "Input to memberOf function is of unsupported type: " + inputResult.getFhirPath());
    }

    if (input.getArguments().size() != 1
        || input.getArguments().get(0).getFhirPathType() != STRING) {
      throw new InvalidRequestException(
          "memberOf function accepts one argument of type String: " + input.getExpression());
    }
  }

  /**
   * Takes a set of Rows with two columns: (1) a correlation identifier, and; (2) a Coding or
   * CodeableConcept to validate. Returns a set of ValidateCodeResults, which contain the
   * correlation identifier and a boolean result.
   */
  public static class ValidateCodeMapper implements MapPartitionsFunction<Row, ValidateCodeResult> {

    private static final Logger logger = LoggerFactory.getLogger(ValidateCodeMapper.class);
    private final String requestId;
    private final TerminologyClientFactory terminologyClientFactory;
    private final String valueSetUri;
    private final FHIRDefinedType fhirType;

    public ValidateCodeMapper(String requestId,
        TerminologyClientFactory terminologyClientFactory,
        String valueSetUri, FHIRDefinedType fhirType) {
      this.requestId = requestId;
      this.terminologyClientFactory = terminologyClientFactory;
      this.valueSetUri = valueSetUri;
      this.fhirType = fhirType;
    }

    @Override
    public Iterator<ValidateCodeResult> call(Iterator<Row> inputRows) {
      if (!inputRows.hasNext()) {
        return Collections.emptyIterator();
      }

      // Add the request ID to the logging context, so that we can track the logging for this
      // request across all workers.
      MDC.put("requestId", requestId);

      // Create a terminology client.
      TerminologyClient terminologyClient = terminologyClientFactory.build(logger);

      // Create a Bundle to represent the batch of validate code requests.
      Bundle validateCodeBatch = new Bundle();
      validateCodeBatch.setType(BundleType.BATCH);

      // Create a list to store the details of the Codings requested - this will be used to
      // correlate requested Codings with responses later on.
      List<ValidateCodeResult> results = new ArrayList<>();

      inputRows.forEachRemaining(inputRow -> {
        // Get the Coding struct out of the Row. If it is null, skip this Row.
        Row inputCoding = inputRow.getStruct(1);
        if (inputCoding == null) {
          return;
        }
        // Add the hash to an ordered list - we will later update these objects based on index
        // from the ordered results of the $validate-code operation.
        results.add(new ValidateCodeResult(inputRow.getInt(0)));

        // Extract the Coding or CodeableConcept from the Row.
        Type concept;
        assert fhirType == FHIRDefinedType.CODING || fhirType == FHIRDefinedType.CODEABLECONCEPT;
        if (fhirType == FHIRDefinedType.CODING) {
          concept = getCodingFromRow(inputCoding);
        } else {
          CodeableConcept codeableConcept = new CodeableConcept();
          codeableConcept.setId(inputCoding.getString(inputCoding.fieldIndex("id")));
          List<Row> codingRows = inputCoding.getList(inputCoding.fieldIndex("coding"));
          List<Coding> codings = codingRows.stream()
              .map(ValidateCodeMapper::getCodingFromRow)
              .collect(Collectors.toList());
          codeableConcept.setCoding(codings);
          codeableConcept.setText(inputCoding.getString(inputCoding.fieldIndex("text")));
          concept = codeableConcept;
        }

        // Construct a Bundle entry containing a validate code request using the Coding or
        // CodeableConcept.
        BundleEntryComponent entry = new BundleEntryComponent();
        BundleEntryRequestComponent request = new BundleEntryRequestComponent();
        request.setMethod(HTTPVerb.POST);
        request.setUrl("ValueSet/$validate-code");
        entry.setRequest(request);
        Parameters parameters = new Parameters();
        ParametersParameterComponent systemParam = new ParametersParameterComponent();
        systemParam.setName("url");
        systemParam.setValue(new UriType(valueSetUri));
        ParametersParameterComponent conceptParam = new ParametersParameterComponent();
        conceptParam.setName(fhirType == FHIRDefinedType.CODING
                             ? "coding"
                             : "codeableConcept");
        conceptParam.setValue(concept);
        parameters.addParameter(systemParam);
        parameters.addParameter(conceptParam);
        entry.setResource(parameters);

        // Add the entry to the Bundle.
        validateCodeBatch.addEntry(entry);
      });

      // Execute the operation on the terminology server.
      logger.info(
          "Sending batch of $validate-code requests to terminology service, " + validateCodeBatch
              .getEntry().size() + " concepts");
      Bundle validateCodeResult = terminologyClient.batch(validateCodeBatch);

      // Convert each result into a Row.
      for (int i = 0; i < validateCodeResult.getEntry().size(); i++) {
        BundleEntryComponent entry = validateCodeResult.getEntry().get(i);
        ValidateCodeResult result = results.get(i);
        if (entry.getResponse().getStatus().startsWith("2")) {
          // If the response was successful, check that it has a `result` parameter with a
          // value of false.
          Type resultValue = ((Parameters) entry.getResource()).getParameter("result");
          boolean validated =
              resultValue.hasType("boolean") && ((BooleanType) resultValue).booleanValue();
          result.setResult(validated);
        } else {
          result.setResult(false);
        }
      }

      return results.iterator();
    }

    private static Coding getCodingFromRow(Row inputCoding) {
      Coding coding;
      String id = inputCoding.getString(inputCoding.fieldIndex("id"));
      String system = inputCoding.getString(inputCoding.fieldIndex("system"));
      String version = inputCoding.getString(inputCoding.fieldIndex("version"));
      String code = inputCoding.getString(inputCoding.fieldIndex("code"));
      String display = inputCoding.getString(inputCoding.fieldIndex("display"));
      coding = new Coding(system, code, display);
      coding.setId(id);
      coding.setVersion(version);
      // Conditionally set the `userSelected` field based on whether it is null in the data -
      // missingness is significant in FHIR.
      Boolean userSelected = (Boolean) inputCoding.get(inputCoding.fieldIndex("userSelected"));
      if (userSelected != null) {
        coding.setUserSelected(userSelected);
      }
      return coding;
    }
  }

}
