/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.operators;

import au.csiro.clinsight.query.parsing.ParsedExpression;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * An operator that is supported for use within FHIRPath expressions, and has two operands: left and
 * right.
 *
 * @author John Grimes
 */
public interface BinaryOperator {

  // Maps FHIRPath functions to the equivalent functions within Spark SQL.
  Map<String, BinaryOperator> operatorToObject = new HashMap<String, BinaryOperator>() {{
    put("and", new BooleanOperator("and"));
    put("or", new BooleanOperator("or"));
    put("xor", new BooleanOperator("xor"));
    put("implies", new BooleanOperator("implies"));
    put("+", new MathOperator("+"));
    put("-", new MathOperator("-"));
    put("*", new MathOperator("*"));
    put("/", new MathOperator("/"));
    put("mod", new MathOperator("mod"));
    put("<=", new ComparisonOperator("<="));
    put("<", new ComparisonOperator("<"));
    put(">", new ComparisonOperator(">"));
    put(">=", new ComparisonOperator(">="));
    put("=", new EqualityOperator("="));
    put("!=", new EqualityOperator("!="));
    put("in", new MembershipOperator("in"));
    put("contains", new MembershipOperator("contains"));
  }};

  static BinaryOperator getBinaryOperator(String operatorName) {
    return operatorToObject.get(operatorName);
  }

  @Nonnull
  ParsedExpression invoke(@Nonnull BinaryOperatorInput input);

}
