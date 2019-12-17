/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.operators;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.test.FunctionTest;
import au.csiro.pathling.test.StringPrimitiveRowFixture;
import static au.csiro.pathling.test.StringPrimitiveRowFixture.*;


import org.apache.spark.sql.*;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static au.csiro.pathling.test.Assertions.assertThat;

/**
 * @author Piotr Szul
 */
@Category(au.csiro.pathling.UnitTest.class)
public class ContatinsOperatorTest extends FunctionTest {
  @Test
  public void returnsCorrectResultWhenRightIsLiteral() {	
  	ParsedExpression left = createPrimitiveParsedExpression(StringPrimitiveRowFixture.createCompleteDataset(spark));
  	ParsedExpression right = createLiteralExpression("Samuel");
    BinaryOperatorInput operatorInput = new BinaryOperatorInput();
    operatorInput.setLeft(left);
    operatorInput.setRight(right);
    operatorInput.setExpression("name.given = 'Samuel'");    
    ParsedExpression result = new MembershipOperator("contains").invoke(operatorInput);  
    assertThat(result)
    	.isOfBooleanType()
    	.isSingular();
    
    assertThat(result).selectResult().hasRows(
    		RowFactory.create(STRING_ROW_ID_1, false),
    		RowFactory.create(STRING_ROW_ID_2, true),
    		RowFactory.create(STRING_ROW_ID_3, null),
    		RowFactory.create(STRING_ROW_ID_4, false),
    		RowFactory.create(STRING_ROW_ID_5, null)
    );
  }
  
  @Test
  public void returnsCorrectResultWhenRightIsExpression() {	
  	ParsedExpression left = createPrimitiveParsedExpression(StringPrimitiveRowFixture.createCompleteDataset(spark));
  	ParsedExpression right = createPrimitiveParsedExpression(StringPrimitiveRowFixture.createDataset(spark, 
  			RowFactory.create(STRING_ROW_ID_1, "Eva"),
  			STRING_2_1_SAMUEL, 
  			STRING_4_1_ADAM
  	));
  	right.setSingular(true);
    BinaryOperatorInput operatorInput = new BinaryOperatorInput();
    operatorInput.setLeft(left);
    operatorInput.setRight(right);
    operatorInput.setExpression("name.given = 'Samuel'");    
    ParsedExpression result = new MembershipOperator("contains").invoke(operatorInput);  
    assertThat(result)
    	.isOfBooleanType()
    	.isSingular();
    
    assertThat(result).selectResult().hasRows(
    		RowFactory.create(STRING_ROW_ID_1, false),
    		RowFactory.create(STRING_ROW_ID_2, true),
    		RowFactory.create(STRING_ROW_ID_3, null),
    		RowFactory.create(STRING_ROW_ID_4, true),
    		RowFactory.create(STRING_ROW_ID_5, null)
    );
  }
  
  @Test
  public void resultIsEmptyWhenLeftIsEmpty() {
  	ParsedExpression left = createPrimitiveParsedExpression(StringPrimitiveRowFixture.createNullRowsDataset(spark));
  	ParsedExpression right = createLiteralExpression("female");
    BinaryOperatorInput operatorInput = new BinaryOperatorInput();
    operatorInput.setLeft(left);
    operatorInput.setRight(right);
    operatorInput.setExpression("gender = 'female'");    
    ParsedExpression result = new MembershipOperator("contains").invoke(operatorInput);  
    assertThat(result)
    	.isOfBooleanType()
    	.isSingular();
    
    assertThat(result).selectResult().hasRows(
    		STRING_3_1_NULL,
    		STRING_5_1_NULL
    );
  }
  
  @Test
  public void returnsFalseWhenRightIsEmpty() {	  	
  }
  
  @Test
  public void throwExceptionWhenLeftIsNotSingular() {	
  }
  
}
