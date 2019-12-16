package au.csiro.pathling.test;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.query.functions.FunctionInput;
import au.csiro.pathling.query.parsing.ParsedExpression;

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

  public DatasetAssert aggByIdResult() {
    return new DatasetAssert(parsedExpression.getAggregationDataset()
        .groupBy(parsedExpression.getAggregationIdColumn())
        .agg(parsedExpression.getAggregationColumn())
        .orderBy(parsedExpression.getAggregationIdColumn()));
  }

  public DatasetAssert aggResult() {
    return new DatasetAssert(parsedExpression.getAggregationDataset()
        .agg(parsedExpression.getAggregationColumn()));
  }

  public ParsedExpressionAssert isResultFor(FunctionInput input) {
    assertThat(parsedExpression.getFhirPath()).isEqualTo(input.getExpression());
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

  public ParsedExpressionAssert isAggreation() {
    assertThat(parsedExpression.getIdColumn()).isNotNull();
    assertThat(parsedExpression.getAggregationColumn()).isNotNull();
    assertThat(parsedExpression.getAggregationDataset()).isNotNull();
    return this;
  }


  public ParsedExpressionAssert isResource() {
    assertThat(parsedExpression.isPrimitive()).isFalse();
    assertThat(parsedExpression.isResource()).isTrue();
    assertThat(parsedExpression.getResourceType()).isNotNull();
    assertThat(parsedExpression.isSingular()).isTrue();
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
}
