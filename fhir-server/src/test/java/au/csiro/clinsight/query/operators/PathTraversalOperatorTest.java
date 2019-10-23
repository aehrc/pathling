/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.operators;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import au.csiro.clinsight.TestUtilities;
import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author John Grimes
 */
@Category(au.csiro.clinsight.UnitTest.class)
public class PathTraversalOperatorTest {

  private SparkSession spark;

  @Before
  public void setUp() {
    spark = SparkSession.builder()
        .appName("clinsight-test")
        .config("spark.master", "local")
        .config("spark.driver.host", "localhost")
        .getOrCreate();
  }

  @Test
  public void simpleTraversal() {
    Metadata metadata = new MetadataBuilder().build();
    StructField genderColumn = new StructField("gender", DataTypes.StringType, true, metadata);
    StructField activeColumn = new StructField("active", DataTypes.BooleanType, true, metadata);
    StructType resourceStruct = new StructType(new StructField[]{genderColumn, activeColumn});
    StructField id = new StructField("123abcd_id", DataTypes.StringType, false, metadata);
    StructField resource = new StructField("123abcd", resourceStruct, false, metadata);
    StructType rowStruct = new StructType(new StructField[]{id, resource});

    Row row = RowFactory.create("abc", RowFactory.create("female", true));
    Dataset<Row> dataset = spark.createDataFrame(Collections.singletonList(row), rowStruct);
    Column idColumn = dataset.col(dataset.columns()[0]);
    Column valueColumn = dataset.col(dataset.columns()[1]);

    ParsedExpression left = new ParsedExpression();
    left.setFhirPath("Patient");
    left.setResourceType(ResourceType.PATIENT);
    left.setResource(true);
    left.setOrigin(left);
    left.setDataset(dataset);
    left.setIdColumn(idColumn);
    left.setValueColumn(valueColumn);

    ExpressionParserContext context = new ExpressionParserContext();
    context.setFhirContext(TestUtilities.getFhirContext());

    PathTraversalInput input = new PathTraversalInput();
    input.setLeft(left);
    input.setRight("gender");
    input.setExpression("gender");
    input.setContext(context);

    PathTraversalOperator pathTraversalOperator = new PathTraversalOperator();
    ParsedExpression result = pathTraversalOperator.invoke(input);

    assertThat(result.getFhirPath()).isEqualTo("gender");
    assertThat(result.getFhirPathType()).isEqualTo(FhirPathType.STRING);
    assertThat(result.getFhirType()).isEqualTo(FHIRDefinedType.CODE);
    assertThat(result.isPrimitive()).isTrue();
    assertThat(result.isSingular()).isFalse();
    assertThat(result.getOrigin()).isEqualTo(left);
    assertThat(result.getIdColumn()).isNotNull();
    assertThat(result.getValueColumn()).isNotNull();
    assertThat(result.getDefinition()).isNotNull();

    Dataset<Row> resultDataset = result.getDataset()
        .select(result.getIdColumn(), result.getValueColumn());
    List<Row> resultRows = resultDataset.collectAsList();
    assertThat(resultRows.size()).isEqualTo(1);
    Row resultRow = resultRows.get(0);
    assertThat(resultRow.getString(0)).isEqualTo("abc");
    assertThat(resultRow.getString(1)).isEqualTo("female");
  }

  @Test
  public void rejectsPolymorphicInput() {
    ParsedExpression left = new ParsedExpression();
    left.setFhirPath("Encounter.subject.resolve()");
    left.setSingular(true);
    left.setPolymorphic(true);

    PathTraversalInput input = new PathTraversalInput();
    input.setLeft(left);
    input.setRight("foo");

    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> new PathTraversalOperator().invoke(input))
        .withMessage("Attempt at path traversal on polymorphic input: Encounter.subject.resolve()");
  }
}
