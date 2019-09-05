/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType.CODING;
import static au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType.STRING;
import static au.csiro.clinsight.utilities.Strings.md5Short;

import au.csiro.clinsight.fhir.TerminologyClient;
import au.csiro.clinsight.query.IdAndBoolean;
import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.hl7.fhir.dstu3.model.*;
import org.hl7.fhir.dstu3.model.Parameters.ParametersParameterComponent;

/**
 * A function that takes a set of Codings as inputs and returns a set of boolean values, based upon
 * whether each Coding is present within the ValueSet identified by the supplied URL.
 *
 * @author John Grimes
 * @see <a href="https://build.fhir.org/fhirpath.html#functions">https://build.fhir.org/fhirpath.html#functions</a>
 */
public class MemberOfFunction implements Function {

  private static final Set<FhirPathType> supportedTypes = new HashSet<FhirPathType>() {{
    add(CODING);
  }};

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull FunctionInput input) {
    validateInput(input);
    ParsedExpression inputResult = input.getInput();
    ParsedExpression argument = input.getArguments().get(0);
    String valueSetUri = argument.getLiteralValue().toString();
    Dataset<Row> prevDataset = input.getInput().getDataset();
    TerminologyClient terminologyClient = input.getContext().getTerminologyClient();
    String hash = md5Short(input.getExpression());

    // Perform a validate code operation on each Coding or CodeableConcept in the input dataset,
    // then create a new dataset with the boolean results.
    Dataset<IdAndBoolean> validateResults = prevDataset
        .map((MapFunction<Row, IdAndBoolean>) row -> {
          IdAndBoolean result = new IdAndBoolean();
          result.setId(row.getString(0));
          if (inputResult.getFhirPathType() == CODING) {
            result.setValue(validateCoding(terminologyClient, valueSetUri, row));
          } else {
            result.setValue(validateCodeableConcept(terminologyClient, valueSetUri, row));
          }
          return result;
        }, Encoders.bean(IdAndBoolean.class));
    Column idColumn = validateResults.col(validateResults.columns()[0]).alias(hash + "_id");
    Column column = validateResults.col(validateResults.columns()[1]).alias(hash);
    Dataset<Row> dataset = validateResults.select(idColumn, column);

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setFhirPathType(FhirPathType.BOOLEAN);
    result.setFhirType(FhirType.BOOLEAN);
    result.setPrimitive(true);
    result.setSingular(inputResult.isSingular());
    result.setDataset(dataset);
    result.setDatasetColumn(hash);
    return result;
  }

  private boolean validateCoding(TerminologyClient terminologyClient, String valueSetUri, Row row) {
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

  private boolean validateCodeableConcept(TerminologyClient terminologyClient, String valueSetUri,
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

  private void validateInput(FunctionInput input) {
    ParsedExpression inputResult = input.getInput();
    if (!(supportedTypes.contains(inputResult.getFhirPathType()) || inputResult.getPathTraversal()
        .getElementDefinition().getTypeCode().equals("CodeableConcept"))) {
      throw new InvalidRequestException(
          "Input to memberOf function is of unsupported type: " + inputResult.getFhirPath());
    }

    if (!input.getArguments().isEmpty()
        || input.getArguments().get(0).getFhirPathType() != STRING) {
      throw new InvalidRequestException(
          "memberOf function accepts one argument of type String: " + input.getExpression());
    }
  }

}
