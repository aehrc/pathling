/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.parsing.parser;

import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.CODING;
import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.DATE_TIME;
import static au.csiro.pathling.utilities.Strings.unSingleQuote;
import static au.csiro.pathling.utilities.Strings.unescapeFhirPathString;

import au.csiro.pathling.encoding.IdAndBoolean;
import au.csiro.pathling.fhir.FhirPathBaseVisitor;
import au.csiro.pathling.fhir.FhirPathParser.*;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * This class deals with terms that are literal expressions.
 *
 * @author John Grimes
 */
class ExpressionLiteralTermVisitor extends FhirPathBaseVisitor<ParsedExpression> {

  final ExpressionParserContext context;

  ExpressionLiteralTermVisitor(ExpressionParserContext context) {
    this.context = context;
  }

  @Override
  public ParsedExpression visitCodingLiteral(CodingLiteralContext ctx) {
    ParsedExpression result = new ParsedExpression();
    result.setFhirPathType(CODING);
    result.setFhirType(FHIRDefinedType.CODING);
    result.setFhirPath(ctx.getText());
    LinkedList<String> codingTokens = new LinkedList<>(Arrays.asList(ctx.getText().split("\\|")));
    Coding literalValue;
    if (codingTokens.size() == 2) {
      literalValue = new Coding(codingTokens.get(0), codingTokens.get(1), null);
    } else if (codingTokens.size() == 3) {
      literalValue = new Coding(codingTokens.get(0), codingTokens.get(2), null);
      literalValue.setVersion(codingTokens.get(1));
    } else {
      throw new InvalidRequestException(
          "Coding literal must be of form [system]|[code] or [system]|[version]|[code]");
    }
    result.setLiteralValue(literalValue);
    result.setSingular(true);
    return result;
  }

  @Override
  public ParsedExpression visitStringLiteral(StringLiteralContext ctx) {
    // Remove the surrounding single quotes and unescape the string according to the rules within
    // the FHIRPath specification.
    String value = unSingleQuote(ctx.getText());
    value = unescapeFhirPathString(value);

    ParsedExpression result = new ParsedExpression();
    result.setFhirPathType(FhirPathType.STRING);
    result.setFhirType(FHIRDefinedType.STRING);
    result.setFhirPath(ctx.getText());
    result.setLiteralValue(new StringType(value));
    result.setPrimitive(true);
    result.setSingular(true);
    return result;
  }

  @Override
  public ParsedExpression visitDateTimeLiteral(DateTimeLiteralContext ctx) {
    String dateTimeString = ctx.getText().replace("@", "");
    ParsedExpression result = new ParsedExpression();
    result.setFhirPathType(DATE_TIME);
    result.setFhirType(FHIRDefinedType.DATETIME);
    result.setFhirPath(ctx.getText());
    result.setLiteralValue(new StringType(dateTimeString));
    result.setPrimitive(true);
    result.setSingular(true);
    return result;
  }

  @Override
  public ParsedExpression visitTimeLiteral(TimeLiteralContext ctx) {
    String timeString = ctx.getText().replace("@T", "");
    ParsedExpression result = new ParsedExpression();
    result.setFhirPathType(FhirPathType.TIME);
    result.setFhirType(FHIRDefinedType.TIME);
    result.setFhirPath(ctx.getText());
    result.setLiteralValue(new StringType(timeString));
    result.setPrimitive(true);
    result.setSingular(true);
    return result;
  }

  @Override
  public ParsedExpression visitNumberLiteral(NumberLiteralContext ctx) {
    // Try to parse an integer out of the string. If this fails, try and parse a decimal value out
    // of the string.
    Type value;
    try {
      value = new IntegerType(Integer.parseInt(ctx.getText()));
    } catch (NumberFormatException e) {
      value = new DecimalType(new BigDecimal(ctx.getText()));
    }

    ParsedExpression result = new ParsedExpression();
    result.setFhirPathType(FhirPathType.INTEGER);
    result.setFhirType(FHIRDefinedType.INTEGER);
    result.setFhirPath(ctx.getText());
    result.setLiteralValue(value);
    result.setPrimitive(true);
    result.setSingular(true);
    return result;
  }

  @Override
  public ParsedExpression visitBooleanLiteral(BooleanLiteralContext ctx) {
    boolean value = ctx.getText().equals("true");

    ParsedExpression result = new ParsedExpression();
    result.setFhirPathType(FhirPathType.BOOLEAN);
    result.setFhirType(FHIRDefinedType.BOOLEAN);
    result.setFhirPath(ctx.getText());
    result.setLiteralValue(new BooleanType(value));
    result.setPrimitive(true);
    result.setSingular(true);
    return result;
  }

  @Override
  public ParsedExpression visitNullLiteral(NullLiteralContext ctx) {
    // Create an empty dataset with and ID and value column.
    Dataset<Row> dataset = context.getSparkSession()
        .emptyDataset(Encoders.bean(IdAndBoolean.class)).toDF();
    Column idColumn = dataset.col(dataset.columns()[0]);
    Column valueColumn = dataset.col(dataset.columns()[1]);
    dataset = dataset.select(idColumn, valueColumn);

    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(ctx.getText());
    result.setDataset(dataset);
    result.setIdColumn(idColumn);
    result.setValueColumn(valueColumn);
    return result;
  }

  @Override
  public ParsedExpression visitQuantityLiteral(QuantityLiteralContext ctx) {
    throw new InvalidRequestException("Quantity literals are not supported");
  }

}
