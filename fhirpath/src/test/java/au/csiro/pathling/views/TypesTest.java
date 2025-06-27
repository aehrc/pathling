package au.csiro.pathling.views;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser$;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;
import java.util.Arrays;

// TODO: remove
public class TypesTest {
  
  
  @Test
  void testTypes() {


    // CHARACTER(n) / CHAR(n)
    // CHARACTER VARYING(n) / VARCHAR(n)
    // NUMERIC(p, s)
    // DECIMAL(p, s) / DEC(p, s)
    // SMALLINT
    // INTEGER / INT
    // BIGINT
    // FLOAT(p)
    // REAL
    // DOUBLE PRECISION
    // BOOLEAN
    // BINARY(n)
    // BINARY VARYING(n) / VARBINARY(n)
    // DATE
    // TIMESTAMP [(p)] [ WITHOUT TIME ZONE | WITH TIME ZONE ]
    // INTERVAL
    
    // create a list with strings respresenting all the above types
    final String[] types = {
        "CHARACTER(10)", "CHAR(10)", "CHARACTER VARYING(10)", "VARCHAR(10)",
        "NUMERIC(10, 2)", "DECIMAL(10, 2)",
        "DEC(10, 2)",  // DECIMAL synonym
        "SMALLINT", "INTEGER", "INT", "BIGINT",
        "FLOAT",
        "FLOAT(24)", "REAL", "DOUBLE PRECISION", "BOOLEAN", "BINARY(10)",
        "VARBINARY(10)", "DATE",
        "TIMESTAMP",
        "TIMESTAMP(3)",  // Example with precision
        "TIMESTAMP WITHOUT TIME ZONE",
        "TIMESTAMP WITH TIME ZONE", "INTERVAL"
    };

    Arrays.stream(types).forEach(type -> {
      try {
        // Parse the type using Spark's Catalyst SQL parser
        DataType dataType = CatalystSqlParser$.MODULE$.parseDataType(type);
        System.out.println(type + " -> " + dataType + " -> " + dataType.sql());
      } catch (Exception e) {
        // If parsing fails, print the error
        System.out.println(type + " -> " + "!!! ERROR: ");
      }
    });
  }
}
