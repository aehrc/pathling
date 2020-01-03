/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.operators;

import static au.csiro.pathling.TestUtilities.getFhirContext;
import static au.csiro.pathling.test.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.pathling.query.parsing.parser.ExpressionParserContext;
import au.csiro.pathling.test.DatasetBuilder;
import au.csiro.pathling.test.ResourceExpressionBuilder;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author John Grimes
 */
@Category(au.csiro.pathling.UnitTest.class)
public class PathTraversalOperatorTest {

  @Test
  public void simpleTraversal() {
    // This needs to match the expectations we have about the subject resource used as within the
    // expression parser context.
    ParsedExpression left = new ResourceExpressionBuilder(ResourceType.PATIENT,
        FHIRDefinedType.PATIENT)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withStructColumn("gender", DataTypes.StringType)
        .withStructColumn("active", DataTypes.BooleanType)
        .withRow("abc", RowFactory.create("female", true))
        .buildWithStructValue("123abcd");
    left.setSingular(true);
    left.setOrigin(left);

    ExpressionParserContext context = new ExpressionParserContext();
    context.setFhirContext(getFhirContext());

    PathTraversalInput input = new PathTraversalInput();
    input.setLeft(left);
    input.setRight("gender");
    input.setExpression("gender");
    input.setContext(context);

    PathTraversalOperator pathTraversalOperator = new PathTraversalOperator();
    ParsedExpression result = pathTraversalOperator.invoke(input);

    assertThat(result).hasFhirPath("gender");
    assertThat(result).isOfType(FHIRDefinedType.CODE, FhirPathType.STRING);
    assertThat(result).isPrimitive();
    assertThat(result).isSingular();
    assertThat(result).hasOrigin(left);
    assertThat(result).isSelection();
    assertThat(result).hasDefinition();

    Dataset<Row> expectedDataset = new DatasetBuilder()
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.StringType)
        .withRow("abc", "female")
        .build();
    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void rejectsPolymorphicInput() {
    ParsedExpression left = new ResourceExpressionBuilder(ResourceType.PATIENT,
        FHIRDefinedType.PATIENT)
        .build();
    left.setFhirPath("subject.resolve()");
    left.setSingular(true);
    left.setPolymorphic(true);

    PathTraversalInput input = new PathTraversalInput();
    input.setLeft(left);
    input.setRight("foo");

    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> new PathTraversalOperator().invoke(input))
        .withMessage("Attempt at path traversal on polymorphic input: subject.resolve()");
  }

  @Test
  public void throwsErrorOnNonExistentChild() {
    ExpressionParserContext context = new ExpressionParserContext();
    context.setFhirContext(getFhirContext());

    ParsedExpression left = new ResourceExpressionBuilder(ResourceType.ENCOUNTER,
        FHIRDefinedType.ENCOUNTER)
        .build();
    left.setFhirPath("Encounter");

    PathTraversalInput input = new PathTraversalInput();
    input.setLeft(left);
    input.setRight("reason");
    input.setContext(context);

    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> new PathTraversalOperator().invoke(input))
        .withMessage("Unknown child of " + left.getFhirPath() + ": reason");
  }
}
