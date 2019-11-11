/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType.CODING;
import static au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType.STRING;

import au.csiro.clinsight.fhir.FhirContextFactory;
import au.csiro.clinsight.fhir.TerminologyClient;
import au.csiro.clinsight.query.IdAndBoolean;
import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A function that takes a set of Codings or CodeableConcepts as inputs and returns a set of boolean
 * values, based upon whether each item is present within the ValueSet identified by the supplied
 * URL.
 *
 * @author John Grimes
 * @see <a href="https://build.fhir.org/fhirpath.html#functions">https://build.fhir.org/fhirpath.html#functions</a>
 */
public class MemberOfFunction implements Function {

  private ValidateCodeMapper validateCodeMapper;

  public MemberOfFunction() {
  }

  /**
   * This is used for substituting an alternate mapper for testing purposes.
   */
  public MemberOfFunction(ValidateCodeMapper validateCodeMapper) {
    this.validateCodeMapper = validateCodeMapper;
  }

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull FunctionInput input) {
    validateInput(input);
    ParsedExpression inputResult = input.getInput();
    ParsedExpression argument = input.getArguments().get(0);
    Dataset<Row> prevDataset = input.getInput().getDataset();

    // Prepare the data which will be used within the map operation. All of these things must be
    // Serializable.
    FhirContextFactory fhirContextFactory = input.getContext().getFhirContextFactory();
    String terminologyServerUrl = input.getContext().getTerminologyClient().getServerBase();
    FhirPathType fhirPathType = inputResult.getFhirPathType();
    String valueSetUri = argument.getLiteralValue().toString();

    // Perform a validate code operation on each Coding or CodeableConcept in the input dataset,
    // then create a new dataset with the boolean results.
    if (validateCodeMapper == null) {
      validateCodeMapper = new ValidateCodeMapper(fhirContextFactory, terminologyServerUrl,
          fhirPathType, valueSetUri);
    }
    Dataset<IdAndBoolean> validateResults = prevDataset
        .select(inputResult.getIdColumn(), inputResult.getValueColumn())
        .map(validateCodeMapper, Encoders.bean(IdAndBoolean.class));
    Column idColumn = validateResults.col(validateResults.columns()[0]);
    Column valueColumn = validateResults.col(validateResults.columns()[1]);
    Dataset<Row> dataset = validateResults.select(idColumn, valueColumn);

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setFhirPathType(FhirPathType.BOOLEAN);
    result.setFhirType(FHIRDefinedType.BOOLEAN);
    result.setPrimitive(true);
    result.setSingular(inputResult.isSingular());
    result.setDataset(dataset);
    result.setIdColumn(idColumn);
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

  public static class ValidateCodeMapper implements MapFunction<Row, IdAndBoolean> {

    private static final Logger logger = LoggerFactory.getLogger(ValidateCodeMapper.class);
    private final FhirContextFactory fhirContextFactory;
    private final String terminologyServerUrl;
    private final FhirPathType fhirPathType;
    private final String valueSetUri;

    public ValidateCodeMapper(FhirContextFactory fhirContextFactory, String terminologyServerUrl,
        FhirPathType fhirPathType, String valueSetUri) {
      this.fhirContextFactory = fhirContextFactory;
      this.terminologyServerUrl = terminologyServerUrl;
      this.fhirPathType = fhirPathType;
      this.valueSetUri = valueSetUri;
    }

    @Override
    public IdAndBoolean call(Row row) {
      TerminologyClient terminologyClient = fhirContextFactory
          .getFhirContext(FhirVersionEnum.R4)
          .newRestfulClient(TerminologyClient.class, terminologyServerUrl);
      IdAndBoolean result = new IdAndBoolean();
      result.setId(row.getString(0));
      Row concept = row.getStruct(1);
      if (concept == null) {
        result.setValue(false);
      } else if (fhirPathType == CODING) {
        result.setValue(validateCoding(terminologyClient, valueSetUri, concept));
      } else {
        result.setValue(
            validateCodeableConcept(terminologyClient, valueSetUri, concept));
      }
      return result;
    }

    private static boolean validateCoding(TerminologyClient terminologyClient, String valueSetUri,
        Row row) {
      String system = row.getString(row.fieldIndex("system"));
      String version = row.getString(row.fieldIndex("version"));
      String code = row.getString(row.fieldIndex("code"));
      String display = row.getString(row.fieldIndex("display"));
      Coding coding = new Coding(system, code, display);
      coding.setVersion(version);
      Parameters validateCodeResult = terminologyClient
          .validateCode(new UriType(valueSetUri), coding);
      ParametersParameterComponent resultParam = validateCodeResult.getParameter().stream()
          .filter(param -> param.getName().equals("result"))
          .findFirst()
          .orElse(null);
      return resultParam != null
          && resultParam.hasValue()
          && resultParam.getValue().isBooleanPrimitive()
          && ((BooleanType) resultParam.getValue()).booleanValue();
    }

    private static boolean validateCodeableConcept(TerminologyClient terminologyClient,
        String valueSetUri,
        Row row) {
      List<Row> codingRows = row.getList(row.fieldIndex("coding"));
      CodeableConcept codeableConcept = new CodeableConcept();
      for (Row codingRow : codingRows) {
        String system = codingRow.getString(codingRow.fieldIndex("system"));
        String version = codingRow.getString(codingRow.fieldIndex("version"));
        String code = codingRow.getString(codingRow.fieldIndex("code"));
        String display = codingRow.getString(codingRow.fieldIndex("display"));
        Coding coding = new Coding(system, code, display);
        coding.setVersion(version);
        codeableConcept.getCoding().add(coding);
      }
      Parameters validateCodeResult = terminologyClient
          .validateCode(new UriType(valueSetUri), codeableConcept);
      ParametersParameterComponent resultParam = validateCodeResult.getParameter().stream()
          .filter(param -> param.getName().equals("result"))
          .findFirst()
          .orElse(null);
      return resultParam != null
          && resultParam.hasValue()
          && resultParam.getValue().isBooleanPrimitive()
          && ((BooleanType) resultParam.getValue()).booleanValue();
    }
  }

}
