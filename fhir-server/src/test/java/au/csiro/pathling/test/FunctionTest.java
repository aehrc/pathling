/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.test;

import au.csiro.pathling.TestUtilities;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.math.BigDecimal;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.IntegerType;
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

  protected ParsedExpression createLiteralBooleanExpression(boolean value) {
    // Build up the right expression for the function.
    ParsedExpression expression = new ParsedExpression();
    expression.setFhirPath(String.valueOf(value));
    expression.setFhirPathType(FhirPathType.BOOLEAN);
    expression.setFhirType(FHIRDefinedType.BOOLEAN);
    expression.setLiteralValue(new BooleanType(value));
    expression.setSingular(true);
    expression.setPrimitive(true);
    return expression;
  }

  protected ParsedExpression createLiteralStringExpression(String value) {
    // Build up the right expression for the function.
    ParsedExpression expression = new ParsedExpression();
    expression.setFhirPath("'" + value + "'");
    expression.setFhirPathType(FhirPathType.STRING);
    expression.setFhirType(FHIRDefinedType.STRING);
    expression.setLiteralValue(new StringType(value));
    expression.setSingular(true);
    expression.setPrimitive(true);
    return expression;
  }

  protected ParsedExpression createCodingLiteralExpression(String system, String version, String code) {
    // Build up the right expression for the function.
    
    Coding literalValue = new Coding(system, code, null);
    literalValue.setVersion(version);
    
    ParsedExpression expression = new ParsedExpression();
    expression.setFhirPath(Stream.of(system, version, code).filter(Objects::nonNull)
        .collect(Collectors.joining("|")));
    expression.setFhirPathType(FhirPathType.CODING);
    expression.setFhirType(FHIRDefinedType.CODING);
    expression.setLiteralValue(literalValue);
    expression.setSingular(true);
    expression.setPrimitive(false);
    return expression;
  }
    
  protected ParsedExpression createLiteralIntegerExpression(Integer value) {
    // Build up the right expression for the function.
    ParsedExpression expression = new ParsedExpression();
    expression.setFhirPath(value.toString());
    expression.setFhirPathType(FhirPathType.INTEGER);
    expression.setFhirType(FHIRDefinedType.INTEGER);
    expression.setLiteralValue(new IntegerType(value));
    expression.setSingular(true);
    expression.setPrimitive(true);
    return expression;
  }

  protected ParsedExpression createLiteralDecimalExpression(BigDecimal value) {
    // Build up the right expression for the function.
    ParsedExpression expression = new ParsedExpression();
    expression.setFhirPath(value.toString());
    expression.setFhirPathType(FhirPathType.DECIMAL);
    expression.setFhirType(FHIRDefinedType.DECIMAL);
    expression.setLiteralValue(new DecimalType(value));
    expression.setSingular(true);
    expression.setPrimitive(true);
    return expression;
  }

  protected ParsedExpression createLiteralDateTimeExpression(String value) {
    // Build up the right expression for the function.
    ParsedExpression expression = new ParsedExpression();
    expression.setFhirPath("@" + value);
    expression.setFhirPathType(FhirPathType.DATE_TIME);
    expression.setFhirType(FHIRDefinedType.DATETIME);
    expression.setLiteralValue(new StringType(value));
    expression.setSingular(true);
    expression.setPrimitive(true);
    return expression;
  }

  protected ParsedExpression createLiteralDateExpression(String value) {
    // Build up the right expression for the function.
    ParsedExpression expression = new ParsedExpression();
    expression.setFhirPath("@" + value);
    expression.setFhirPathType(FhirPathType.DATE);
    expression.setFhirType(FHIRDefinedType.DATE);
    expression.setLiteralValue(new StringType(value));
    expression.setSingular(true);
    expression.setPrimitive(true);
    return expression;
  }

  protected ParsedExpression createPrimitiveParsedExpression(Dataset<Row> dataset, String fhirPath,
      Class<? extends IBase> elementType, FhirPathType fhirPathType, FHIRDefinedType fhirType) {
    Column idColumn = dataset.col(dataset.columns()[0]);
    Column valueColumn = dataset.col(dataset.columns()[1]);

    // Build up an input for the function.
    ParsedExpression input = new ParsedExpression();

    // Infer the element name from the final component of the FHIRPath.
    String[] pathComponents = fhirPath.split("\\.");
    String elementName = pathComponents[pathComponents.length - 1];

    input.setFhirPath(fhirPath);
    input.setFhirPathType(fhirPathType);
    input.setFhirType(fhirType);
    input.setPrimitive(true);
    input.setSingular(false);
    input.setOrigin(null);
    input.setPolymorphic(false);
    input.setResource(false);
    input.setDefinition(TestUtilities.getChildDefinition(elementType, elementName), elementName);
    input.setDataset(dataset);
    input.setIdColumn(idColumn);
    input.setValueColumn(valueColumn);
    return input;
  }
}
