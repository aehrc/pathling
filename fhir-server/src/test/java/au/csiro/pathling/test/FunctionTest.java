/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.test;

import au.csiro.pathling.TestUtilities;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.StringType;
import org.junit.Before;

public abstract class FunctionTest {

  protected SparkSession spark;

  @Before
  public void setUp() {
    spark = SparkSession.builder().appName("pathling-test").config("spark.master", "local")
        .config("spark.driver.host", "localhost").getOrCreate();
  }

  protected ParsedExpression createResourceParsedExpression(Dataset<Row> dataset,
      ResourceType resourceType) {
    Column idColumn = dataset.col(dataset.columns()[0]);
    Column valueColumn = dataset.col(dataset.columns()[1]);

    ParsedExpression input = new ParsedExpression();
    input.setFhirPath("%resource");
    input.setResource(true);
    input.setResourceType(resourceType);
    input.setOrigin(input);
    input.setDataset(dataset);
    input.setIdColumn(idColumn);
    input.setValueColumn(valueColumn);
    input.setSingular(true);
    return input;
  }

  protected ParsedExpression createLiteralExpression(String value) {
    // Build up the right expression for the function.
    ParsedExpression expression = new ParsedExpression();
    expression.setFhirPath("'female'");
    expression.setFhirPathType(FhirPathType.STRING);
    expression.setFhirType(FHIRDefinedType.STRING);
    expression.setLiteralValue(new StringType(value));
    expression.setSingular(true);
    expression.setPrimitive(true);
    return expression;
  }
  
  protected ParsedExpression createPrimitiveParsedExpression(Dataset<Row> dataset) {
    Column idColumn = dataset.col(dataset.columns()[0]);
    Column valueColumn = dataset.col(dataset.columns()[1]);

    assert (dataset.schema().fields()[0].dataType().equals(DataTypes.StringType));
    // Build up an input for the function.
    ParsedExpression input = new ParsedExpression();

    if (dataset.schema().fields()[0].dataType().equals(DataTypes.StringType)) {
      input.setFhirPathType(FhirPathType.STRING);
      input.setFhirType(FHIRDefinedType.STRING);
    } else {
      throw new IllegalArgumentException(
          dataset.schema().fields()[0].dataType() + "is not primitive or not supported");
    }

    input.setFhirPath("name.family");
    input.setPrimitive(true);
    input.setSingular(false);
    input.setOrigin(null);
    input.setPolymorphic(false);
    input.setResource(false);
    input.setDefinition(TestUtilities.getChildDefinition(HumanName.class, "family"), "family");
    input.setDataset(dataset);
    input.setIdColumn(idColumn);
    input.setValueColumn(valueColumn);
    return input;
  }
}
