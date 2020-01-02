/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.test;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.query.functions.FunctionInput;
import au.csiro.pathling.query.operators.BinaryOperatorInput;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import org.hl7.fhir.r4.model.Enumerations;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.StringType;

/**
 * @author Piotr Szul
 */
public class ParsedExpressionAssert {

  private final ParsedExpression parsedExpression;

  public ParsedExpressionAssert(ParsedExpression parsedExpression) {
    this.parsedExpression = parsedExpression;
  }

  public DatasetAssert selectResult() {
    return new DatasetAssert(parsedExpression.getDataset()
        .select(parsedExpression.getIdColumn(), parsedExpression.getValueColumn())
        .orderBy(parsedExpression.getIdColumn()));
  }

  public DatasetAssert selectPolymorphicResult() {
    return new DatasetAssert(parsedExpression.getDataset()
        .select(parsedExpression.getIdColumn(), parsedExpression.getResourceTypeColumn(),
            parsedExpression.getValueColumn())
        .orderBy(parsedExpression.getIdColumn()));
  }

  public DatasetAssert aggByIdResult() {
    return new DatasetAssert(
        parsedExpression.getAggregationDataset().groupBy(parsedExpression.getAggregationIdColumn())
            .agg(parsedExpression.getAggregationColumn())
            .orderBy(parsedExpression.getAggregationIdColumn()));
  }

  public DatasetAssert aggResult() {
    return new DatasetAssert(
        parsedExpression.getAggregationDataset().agg(parsedExpression.getAggregationColumn()));
  }

  public ParsedExpressionAssert isResultFor(FunctionInput input) {
    assertThat(parsedExpression.getFhirPath()).isEqualTo(input.getExpression());
    return this;
  }

  public ParsedExpressionAssert isResultFor(BinaryOperatorInput input) {
    assertThat(parsedExpression.getFhirPath()).isEqualTo(input.getExpression());
    return this;
  }

  public ParsedExpressionAssert isOfBooleanType() {
    return isOfType(FHIRDefinedType.BOOLEAN, FhirPathType.BOOLEAN).isPrimitive();
  }

  public ParsedExpressionAssert isOfType(FHIRDefinedType fhirType, FhirPathType fhirPathType) {
    assertThat(parsedExpression.getFhirPathType()).isEqualTo(fhirPathType);
    assertThat(parsedExpression.getFhirType()).isEqualTo(fhirType);
    return this;
  }

  public ParsedExpressionAssert hasSameTypeAs(ParsedExpression origin) {
    assertThat(parsedExpression.getFhirPathType()).isEqualTo(origin.getFhirPathType());
    assertThat(parsedExpression.getFhirType()).isEqualTo(origin.getFhirType());
    assertThat(parsedExpression.getDefinition()).isEqualTo(origin.getDefinition());
    assertThat(parsedExpression.getElementDefinition()).isEqualTo(origin.getElementDefinition());
    assertThat(parsedExpression.getResourceType()).isEqualTo(origin.getResourceType());
    return this;
  }

  public ParsedExpressionAssert isSelection() {
    assertThat(parsedExpression.getIdColumn()).isNotNull();
    assertThat(parsedExpression.getValueColumn()).isNotNull();
    assertThat(parsedExpression.getDataset()).isNotNull();
    return this;
  }

  public ParsedExpressionAssert isAggregation() {
    assertThat(parsedExpression.getIdColumn()).isNotNull();
    assertThat(parsedExpression.getAggregationColumn()).isNotNull();
    assertThat(parsedExpression.getAggregationDataset()).isNotNull();
    return this;
  }


  public ParsedExpressionAssert isResourceOfType(Enumerations.ResourceType resourceType,
      FHIRDefinedType fhirType) {
    assertThat(parsedExpression.isPrimitive()).isFalse();
    assertThat(parsedExpression.isResource()).isTrue();
    assertThat(parsedExpression.getResourceType()).isEqualTo(resourceType);
    assertThat(parsedExpression.getFhirType()).isEqualTo(fhirType);
    return this;
  }

  public ParsedExpressionAssert isPrimitive() {
    assertThat(parsedExpression.isPrimitive()).isTrue();
    assertThat(parsedExpression.isResource()).isFalse();
    assertThat(parsedExpression.getResourceType()).isNull();
    return this;
  }

  public ParsedExpressionAssert isSingular() {
    assertThat(parsedExpression.isSingular()).isTrue();
    return this;
  }

  public ParsedExpressionAssert isNotSingular() {
    assertThat(parsedExpression.isSingular()).isFalse();
    return this;
  }

  public ParsedExpressionAssert isPolymorphic() {
    assertThat(parsedExpression.isPolymorphic()).isTrue();
    assertThat(parsedExpression.getResourceTypeColumn()).isNotNull();
    return this;
  }

  public ParsedExpressionAssert isNotPolymorphic() {
    assertThat(parsedExpression.isPolymorphic()).isFalse();
    assertThat(parsedExpression.getResourceTypeColumn()).isNull();
    return this;
  }

  public ParsedExpressionAssert hasFhirPath(String expected) {
    assertThat(parsedExpression.getFhirPath()).isEqualTo(expected);
    return this;
  }

  public ParsedExpressionAssert hasOrigin(ParsedExpression expected) {
    assertThat(parsedExpression.getOrigin()).isEqualTo(expected);
    return this;
  }

  public ParsedExpressionAssert hasDefinition() {
    assertThat(parsedExpression.getDefinition()).isNotNull();
    assertThat(parsedExpression.getElementDefinition()).isNotNull();
    return this;
  }

  public ParsedExpressionAssert isStringLiteral(String value) {
    assertThat(parsedExpression.getLiteralValue()).isInstanceOf(StringType.class);
    assertThat(parsedExpression.getLiteralValue().toString()).isEqualTo(value);
    assertThat(parsedExpression.getJavaLiteralValue()).isEqualTo(value);
    return this;
  }
}
