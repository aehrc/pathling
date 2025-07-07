package au.csiro.pathling.views.ansi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.errors.InvalidUserInputError;
import java.util.stream.Stream;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class AnsiSqlTypeParserTest {

  static Stream<Arguments> validTypesProvider() {
    return Stream.of(
        // Character types
        Arguments.of("CHARACTER", DataTypes.StringType),
        Arguments.of("CHARACTER(10)", DataTypes.StringType),
        Arguments.of("CHAR", DataTypes.StringType),
        Arguments.of("CHAR(10)", DataTypes.StringType),
        Arguments.of("CHARACTER VARYING", DataTypes.StringType),
        Arguments.of("CHARACTER VARYING(10)", DataTypes.StringType),
        Arguments.of("VARCHAR", DataTypes.StringType),
        Arguments.of("VARCHAR(10)", DataTypes.StringType),

        // Numeric types - exact
        Arguments.of("NUMERIC", DataTypes.createDecimalType()),
        Arguments.of("NUMERIC(10)", DataTypes.createDecimalType(10, 0)),
        Arguments.of("NUMERIC(10,2)", DataTypes.createDecimalType(10, 2)),
        Arguments.of("DECIMAL", DataTypes.createDecimalType()),
        Arguments.of("DECIMAL(10)", DataTypes.createDecimalType(10, 0)),
        Arguments.of("DECIMAL(10,2)", DataTypes.createDecimalType(10, 2)),
        Arguments.of("DEC", DataTypes.createDecimalType()),
        Arguments.of("DEC(10)", DataTypes.createDecimalType(10, 0)),
        Arguments.of("DEC(10,2)", DataTypes.createDecimalType(10, 2)),

        // Numeric types - integer
        Arguments.of("SMALLINT", DataTypes.ShortType),
        Arguments.of("INTEGER", DataTypes.IntegerType),
        Arguments.of("INT", DataTypes.IntegerType),
        Arguments.of("BIGINT", DataTypes.LongType),

        // Numeric types - approximate
        Arguments.of("FLOAT", DataTypes.DoubleType),
        Arguments.of("FLOAT(25)", DataTypes.DoubleType),
        Arguments.of("FLOAT(24)", DataTypes.FloatType),
        Arguments.of("REAL", DataTypes.FloatType),
        Arguments.of("DOUBLE PRECISION", DataTypes.DoubleType),

        // Boolean type
        Arguments.of("BOOLEAN", DataTypes.BooleanType),

        // Binary types
        Arguments.of("BINARY", DataTypes.BinaryType),
        Arguments.of("BINARY(10)", DataTypes.BinaryType),
        Arguments.of("BINARY VARYING", DataTypes.BinaryType),
        Arguments.of("BINARY VARYING(10)", DataTypes.BinaryType),
        Arguments.of("VARBINARY", DataTypes.BinaryType),
        Arguments.of("VARBINARY(10)", DataTypes.BinaryType),

        // Temporal types
        Arguments.of("DATE", DataTypes.DateType),
        Arguments.of("TIMESTAMP", DataTypes.TimestampNTZType),
        Arguments.of("TIMESTAMP(3)", DataTypes.TimestampNTZType),
        Arguments.of("TIMESTAMP WITHOUT TIME ZONE", DataTypes.TimestampNTZType),
        Arguments.of("TIMESTAMP(4) WITHOUT TIME ZONE", DataTypes.TimestampNTZType),
        Arguments.of("TIMESTAMP WITH TIME ZONE", DataTypes.TimestampType),
        Arguments.of("TIMESTAMP(4) WITH TIME ZONE", DataTypes.TimestampType),

        Arguments.of("INTERVAL", DataTypes.StringType),

        // Simple complex types
        Arguments.of("ROW", DataTypes.createStructType(new StructField[0])),
        Arguments.of("ARRAY<INTEGER>", DataTypes.createArrayType(DataTypes.IntegerType)),
        Arguments.of("ARRAY<VARCHAR(10)>", DataTypes.createArrayType(DataTypes.StringType)),

        // Complex ROW types
        Arguments.of("ROW(Id INTEGER, namE VARCHAR)",
            DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("Id", DataTypes.IntegerType, true),
                DataTypes.createStructField("namE", DataTypes.StringType, true)
            })),

        // Nested complex types
        Arguments.of("ARRAY<ROW(id INTEGER, values ARRAY<DECIMAL(10,2)>)>",
            DataTypes.createArrayType(
                DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("id", DataTypes.IntegerType, true),
                    DataTypes.createStructField("values",
                        DataTypes.createArrayType(DataTypes.createDecimalType(10, 2)),
                        true)
                })
            ))
    );
  }

  @ParameterizedTest
  @MethodSource("validTypesProvider")
  void testParseValidTypes(String typeString, DataType expectedType) {
    final DataType result = AnsiSqlTypeParser.parseType(typeString);
    assertEquals(expectedType, result, "Type should match expected for: " + typeString);
  }

  @Test
  void testParseInvalidType() {
    final InvalidUserInputError ex = assertThrows(InvalidUserInputError.class,
        () -> AnsiSqlTypeParser.parseType("INVALID_TYPE"));
    final String expectedMsgPrefix = "Error parsing ANSI SQL type: mismatched input 'INVALID_TYPE'";
    assertEquals(expectedMsgPrefix,
        ex.getMessage().substring(0, expectedMsgPrefix.length()));
  }

  @Test
  void testCaseInsensitivity() {
    DataType upperResult = AnsiSqlTypeParser.parseType("INTEGER");
    DataType lowerResult = AnsiSqlTypeParser.parseType("integer");
    DataType mixedResult = AnsiSqlTypeParser.parseType("InTeGeR");

    assertEquals(DataTypes.IntegerType, upperResult);
    assertEquals(DataTypes.IntegerType, lowerResult);
    assertEquals(DataTypes.IntegerType, mixedResult);
  }

}
