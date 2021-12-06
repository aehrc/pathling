package au.csiro.pathling.encoders;

import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext;
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Test;

public class ExpressionsTest {


  @Test
  public void testPutFid() {
    final PutFid putFidExpr = new PutFid(
        new Literal(UTF8String.fromString("xx"), DataTypes.StringType),
        new Literal(10, DataTypes.IntegerType));

    final CodegenContext ctx = new CodegenContext();
    final ExprCode code = putFidExpr.genCode(ctx);

    System.out.println(code.code());

    java.util.HashMap x = new java.util.HashMap();

  }
}
