/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.operators;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import au.csiro.clinsight.TestUtilities;
import au.csiro.clinsight.fhir.TerminologyClient;
import au.csiro.clinsight.fhir.definitions.PathResolver;
import au.csiro.clinsight.fhir.definitions.ResourceDefinitions;
import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.junit.Before;
import org.junit.Test;

/**
 * @author John Grimes
 */
public class PathTraversalOperatorTest {

  private SparkSession spark;

  @Before
  public void setUp() throws IOException {
    spark = SparkSession.builder()
        .appName("clinsight-test")
        .config("spark.master", "local")
        .getOrCreate();

    TerminologyClient terminologyClient = mock(TerminologyClient.class);
    TestUtilities.mockDefinitionRetrieval(terminologyClient);
    ResourceDefinitions.ensureInitialized(terminologyClient);
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

    ParsedExpression left = new ParsedExpression();
    left.setFhirPath("%resource");
    left.setResourceDefinition("http://hl7.org/fhir/StructureDefinition/Patient");
    left.setOrigin(left);
    left.setDataset(dataset);
    left.setDatasetColumn("123abcd");
    left.setPathTraversal(PathResolver.resolvePath("Patient"));

    PathTraversalInput input = new PathTraversalInput();
    input.setLeft(left);
    input.setRight("gender");
    input.setExpression("gender");

    PathTraversalOperator pathTraversalOperator = new PathTraversalOperator();
    ParsedExpression result = pathTraversalOperator.invoke(input);

    assertThat(result.getFhirPath()).isEqualTo("gender");
    assertThat(result.getFhirPathType()).isEqualTo(FhirPathType.STRING);
    assertThat(result.isPrimitive()).isTrue();
    assertThat(result.isSingular()).isFalse();
    assertThat(result.getOrigin()).isEqualTo(left);
    assertThat(result.getDatasetColumn()).isNotBlank();

    Dataset<Row> resultDataset = result.getDataset();
    assertThat(resultDataset.columns().length).isEqualTo(2);
    assertThat(resultDataset.columns()[0]).isEqualTo(result.getDatasetColumn() + "_id");
    assertThat(resultDataset.columns()[1]).isEqualTo(result.getDatasetColumn());
    List<Row> resultRows = resultDataset.collectAsList();
    assertThat(resultRows.size()).isEqualTo(1);
    Row resultRow = resultRows.get(0);
    assertThat(resultRow.getString(0)).isEqualTo("abc");
    assertThat(resultRow.getString(1)).isEqualTo("female");
  }

  // TODO: Implement this test.
  // @Test
  // @SuppressWarnings("unchecked")
  // public void primitiveLeftExpression() {
  //   ParsedExpression left = new ParsedExpression();
  //   left.setFhirPath("%resource");
  //   left.setResourceDefinition("http://hl7.org/fhir/StructureDefinition/Patient");
  //   left.setOrigin(left);
  //   left.setDataset((Dataset<Row>) mock(Dataset.class));
  //   left.setDatasetColumn("123abcd");
  //   left.setPathTraversal(PathResolver.resolvePath("Patient"));
  //
  //   PathTraversalInput input = new PathTraversalInput();
  //   input.setLeft(left);
  //   input.setRight("gender");
  //   input.setExpression("gender");
  //
  //   PathTraversalOperator pathTraversalOperator = new PathTraversalOperator();
  //   ParsedExpression result = pathTraversalOperator.invoke(input);
  //
  // }
}
