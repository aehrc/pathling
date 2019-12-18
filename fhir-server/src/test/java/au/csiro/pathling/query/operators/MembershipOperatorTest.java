/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.operators;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.test.FunctionTest;
import au.csiro.pathling.test.StringPrimitiveRowFixture;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;

import static au.csiro.pathling.test.StringPrimitiveRowFixture.*;


import org.apache.spark.sql.*;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static au.csiro.pathling.test.Assertions.assertThat;

/**
 * @author Piotr Szul
 */
@Category(au.csiro.pathling.UnitTest.class)
@RunWith(Parameterized.class)
public class MembershipOperatorTest extends FunctionTest {

	@Parameters(name = "{index}:{0}()")
	public static Object[] data() {
	    return new Object[] { "in", "contains" };
	}
	
	private final String operator;
	private final BinaryOperatorInput operatorInput = new BinaryOperatorInput();

  public MembershipOperatorTest(String operator) {
		this.operator = operator;
	}

  private String resolveOperator(String string) {
  	return string.replace("%op%", operator);
  }
   
  protected ParsedExpression testOperator(String expression, ParsedExpression collection, ParsedExpression element) {
    operatorInput.setExpression(resolveOperator(expression));    
    if ("in".equals(operator)) {
      operatorInput.setLeft(element);
      operatorInput.setRight(collection);    	
    } else if ("contains".equals(operator)) {
      operatorInput.setLeft(collection);
      operatorInput.setRight(element);    	
    } else {
    	throw new IllegalArgumentException("Membership operator '" + operator + "' cannot be tested");
    }
    ParsedExpression result = new MembershipOperator(operator).invoke(operatorInput);
    assertThat(result)
  	.isResultFor(operatorInput)
  	.isOfBooleanType()
  	.isSingular()
    .isSelection();
    return result;
  }
  
  @Test
  public void returnsCorrectResultWhenElementIsLiteral() {	
  	ParsedExpression collection = createPrimitiveParsedExpression(StringPrimitiveRowFixture.createCompleteDataset(spark));
  	ParsedExpression element = createLiteralExpression("Samuel");

  	ParsedExpression result  = testOperator("name.family.%op%('Samuel')", collection, element);    
    
    assertThat(result).selectResult().hasRows(
    		RowFactory.create(STRING_ROW_ID_1, false),
    		RowFactory.create(STRING_ROW_ID_2, true),
    		RowFactory.create(STRING_ROW_ID_3, null),
    		RowFactory.create(STRING_ROW_ID_4, false),
    		RowFactory.create(STRING_ROW_ID_5, null)
    );
    
    System.out.println(result.getIdColumn());
  }
  
  @Test
  public void returnsCorrectResultWhenElementIsExpression() {	
  	ParsedExpression collection = createPrimitiveParsedExpression(StringPrimitiveRowFixture.createCompleteDataset(spark));
  	ParsedExpression element = createPrimitiveParsedExpression(StringPrimitiveRowFixture.createDataset(spark, 
  			RowFactory.create(STRING_ROW_ID_1, "Eva"),
  			STRING_2_1_SAMUEL, 
  			STRING_4_1_ADAM
  	));
  	element.setSingular(true);
  	element.setFhirPath("name.family.first()");
    ParsedExpression result = testOperator("name.family.%op%(name.family.first())", collection, element);    
    
    assertThat(result).selectResult().hasRows(
    		RowFactory.create(STRING_ROW_ID_1, false),
    		RowFactory.create(STRING_ROW_ID_2, true),
    		RowFactory.create(STRING_ROW_ID_3, null),
    		RowFactory.create(STRING_ROW_ID_4, true),
    		RowFactory.create(STRING_ROW_ID_5, null)
    );
  }
  
  @Test
  public void resultIsEmptyWhenCollectionIsEmpty() {
  	ParsedExpression collection = createPrimitiveParsedExpression(StringPrimitiveRowFixture.createNullRowsDataset(spark));
  	ParsedExpression element = createLiteralExpression("Samuel");

  	ParsedExpression result = testOperator("name.family.%op%('Samuel')", collection, element);    
    
    assertThat(result).selectResult().hasRows(
    		STRING_3_1_NULL,
    		STRING_5_1_NULL
    );
  }
  
  @Test
  public void returnsFalseWhenElementIsEmpty() {	  	
  	ParsedExpression collection = createPrimitiveParsedExpression(StringPrimitiveRowFixture.createCompleteDataset(spark));
  	ParsedExpression element = createPrimitiveParsedExpression(StringPrimitiveRowFixture.createAllRowsNullDataset(spark)); 
  	element.setSingular(true);
  	element.setFhirPath("name.family.first()");
    ParsedExpression result = testOperator("name.family.%op%(name.family.first())", collection, element);    
    
    assertThat(result).selectResult().hasRows(
    		RowFactory.create(STRING_ROW_ID_1, false),
    		RowFactory.create(STRING_ROW_ID_2, false),
    		RowFactory.create(STRING_ROW_ID_3, null),
    		RowFactory.create(STRING_ROW_ID_4, false),
    		RowFactory.create(STRING_ROW_ID_5, null)
    );  
  }
  
  @Test
  public void throwExceptionWhenElementIsNotSingular() {	
  	ParsedExpression collection = createPrimitiveParsedExpression(StringPrimitiveRowFixture.createNullRowsDataset(spark));
  	ParsedExpression element = createPrimitiveParsedExpression(StringPrimitiveRowFixture.createNullRowsDataset(spark));
  	element.setFhirPath("name.given");
 
    assertThatExceptionOfType(InvalidRequestException.class)
    .isThrownBy(() -> testOperator("name.family.contains(name.given)", collection, element))
    .withMessage(resolveOperator("Element operand to %op% operator is not singular: ")  +  "name.given");
  }
  
}
