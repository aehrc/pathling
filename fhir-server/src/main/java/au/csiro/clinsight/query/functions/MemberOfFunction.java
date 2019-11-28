/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType.CODING;
import static au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType.STRING;

import au.csiro.clinsight.encoding.Coding;
import au.csiro.clinsight.encoding.ValidateCodingResult;
import au.csiro.clinsight.fhir.FhirContextFactory;
import au.csiro.clinsight.fhir.TerminologyClient;
import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleEntryRequestComponent;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.Bundle.HTTPVerb;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;

/**
 * A function that takes a set of Codings or CodeableConcepts as inputs and returns a set of boolean
 * values, based upon whether each item is present within the ValueSet identified by the supplied
 * URL.
 *
 * @author John Grimes
 * @see <a href="https://build.fhir.org/fhirpath.html#functions">https://build.fhir.org/fhirpath.html#functions</a>
 */
public class MemberOfFunction implements Function {

  private ValidateCodingMapper validateCodingMapper;

  public MemberOfFunction() {
  }

  /**
   * This is used for substituting an alternate mapper for testing purposes.
   */
  public MemberOfFunction(ValidateCodingMapper validateCodingMapper) {
    this.validateCodingMapper = validateCodingMapper;
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
    FhirContextFactory fhirContextFactory = input.getContext().getFhirContextFactory();
    String terminologyServerUrl = input.getContext().getTerminologyClient().getServerBase();
    FhirPathType fhirPathType = inputResult.getFhirPathType();
    String valueSetUri = argument.getLiteralValue().toString();

    // Perform a validate code operation on each Coding or CodeableConcept in the input dataset,
    // then create a new dataset with the boolean results.
    Dataset<Row> dataset;
    Dataset<ValidateCodingResult> validateResults;
    Column conceptColumn;
    if (fhirPathType == CODING) {
      if (validateCodingMapper == null) {
        validateCodingMapper = new ValidateCodingMapper(fhirContextFactory, terminologyServerUrl,
            fhirPathType, valueSetUri);
      }
      // This de-duplicates the Codings to be validated, then performs the validation on a
      // per-partition basis.
      validateResults = prevDataset
          .select(prevValueColumn)
          .dropDuplicates()
          .mapPartitions(validateCodingMapper, Encoders.bean(ValidateCodingResult.class));
      conceptColumn = Coding.reorderStructFields(validateResults.col("value"));
    } else {
      throw new RuntimeException("This should not happen");
    }

    // We then join the input dataset to the validated codes, and select the validation result
    // as the new value.
    dataset = prevDataset
        .join(validateResults, prevValueColumn.equalTo(conceptColumn));
    Column valueColumn = validateResults.col("result");
    dataset = dataset.select(prevIdColumn, valueColumn);

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setFhirPathType(FhirPathType.BOOLEAN);
    result.setFhirType(FHIRDefinedType.BOOLEAN);
    result.setPrimitive(true);
    result.setSingular(inputResult.isSingular());
    result.setDataset(dataset);
    result.setIdColumn(prevIdColumn);
    result.setValueColumn(valueColumn);
    return result;
  }

  private void validateInput(FunctionInput input) {
    ParsedExpression inputResult = input.getInput();
    if (!(inputResult.getFhirPathType() == CODING
        || inputResult.getFhirType() == FHIRDefinedType.CODEABLECONCEPT)) {
      throw new InvalidRequestException(
          "Input to memberOf function is of unsupported type: " + inputResult.getFhirPath());
    }

    if (input.getArguments().isEmpty()
        || input.getArguments().get(0).getFhirPathType() != STRING) {
      throw new InvalidRequestException(
          "memberOf function accepts one argument of type String: " + input.getExpression());
    }
  }

  public static class ValidateCodingMapper implements
      MapPartitionsFunction<Row, ValidateCodingResult> {

    private final FhirContextFactory fhirContextFactory;
    private final String terminologyServerUrl;
    private final FhirPathType fhirPathType;
    private final String valueSetUri;

    public ValidateCodingMapper(FhirContextFactory fhirContextFactory, String terminologyServerUrl,
        FhirPathType fhirPathType, String valueSetUri) {
      this.fhirContextFactory = fhirContextFactory;
      this.terminologyServerUrl = terminologyServerUrl;
      this.fhirPathType = fhirPathType;
      this.valueSetUri = valueSetUri;
    }

    @Override
    public Iterator<ValidateCodingResult> call(Iterator<Row> inputRows) throws Exception {
      // Create a terminology client.
      TerminologyClient terminologyClient = fhirContextFactory
          .getFhirContext(FhirVersionEnum.R4)
          .newRestfulClient(TerminologyClient.class, terminologyServerUrl);

      // Create a Bundle to represent the batch of validate code requests.
      Bundle validateCodeBatch = new Bundle();
      validateCodeBatch.setType(BundleType.BATCH);

      // Create a list to store the details of the Codings requested - this will be used to
      // correlate requested Codings with responses later on.
      List<ValidateCodingResult> codings = new ArrayList<>();

      inputRows.forEachRemaining(inputRow -> {
        // Get the Coding struct out of the Row. If it is null, skip this Row.
        Row inputCoding = inputRow.getStruct(0);
        if (inputCoding == null) {
          return;
        }

        // Get the Coding information from the row.
        String system = inputCoding.getString(inputCoding.fieldIndex("system"));
        String version = inputCoding.getString(inputCoding.fieldIndex("version"));
        String code = inputCoding.getString(inputCoding.fieldIndex("code"));
        String display = inputCoding.getString(inputCoding.fieldIndex("display"));
        boolean userSelected =
            inputCoding.get(inputCoding.fieldIndex("userSelected")) != null && inputCoding
                .getBoolean(inputCoding.fieldIndex("userSelected"));
        Coding coding = new Coding();
        coding.setSystem(system);
        coding.setVersion(version);
        coding.setCode(code);
        coding.setDisplay(display);
        coding.setUserSelected(userSelected);
        codings.add(new ValidateCodingResult(coding));

        // Construct a Bundle entry containing a validate code request using the Coding.
        BundleEntryComponent entry = new BundleEntryComponent();
        BundleEntryRequestComponent request = new BundleEntryRequestComponent();
        request.setMethod(HTTPVerb.POST);
        request.setUrl("ValueSet/$validate-code");
        entry.setRequest(request);
        Parameters parameters = new Parameters();
        ParametersParameterComponent systemParam = new ParametersParameterComponent();
        systemParam.setName("url");
        systemParam.setValue(new UriType(valueSetUri));
        ParametersParameterComponent codingParam = new ParametersParameterComponent();
        codingParam.setName("coding");
        codingParam.setValue(coding.toHapiCoding());
        parameters.addParameter(systemParam);
        parameters.addParameter(codingParam);
        entry.setResource(parameters);

        // Add the entry to the Bundle.
        validateCodeBatch.addEntry(entry);
      });

      // Execute the operation on the terminology server.
      Bundle validateCodeResult = terminologyClient.batch(validateCodeBatch);

      // Convert each result into a Row.
      List<ValidateCodingResult> results = new ArrayList<>();
      for (int i = 0; i < validateCodeResult.getEntry().size(); i++) {
        BundleEntryComponent entry = validateCodeResult.getEntry().get(i);
        ValidateCodingResult result = codings.get(i);
        if (entry.getResponse().getStatus().startsWith("2")) {
          // If the response was successful, check that it has a `result` parameter with a
          // value of false.
          Type resultValue = ((Parameters) entry.getResource()).getParameter("result");
          boolean validated =
              resultValue.hasType("boolean") && ((BooleanType) resultValue).booleanValue();
          result.setResult(validated);
        } else {
          // TODO: Investigate whether an unsuccessful response will raise a HAPI server error.
          result.setResult(false);
        }
        results.add(result);
      }

      return results.iterator();
    }

  }

}
