package au.csiro.pathling.views.ansi;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import java.util.Optional;

class AnsiSqlTypeParserTest {

  @ParameterizedTest
  @CsvSource({
      "CHARACTER, org.apache.spark.sql.types.StringType",
      "CHARACTER(10), org.apache.spark.sql.types.StringType",
      "CHAR, org.apache.spark.sql.types.StringType",
      "CHAR(10), org.apache.spark.sql.types.StringType",
      "CHARACTER VARYING, org.apache.spark.sql.types.StringType",
      "CHARACTER VARYING(10), org.apache.spark.sql.types.StringType",
      "VARCHAR, org.apache.spark.sql.types.StringType",
      "VARCHAR(10), org.apache.spark.sql.types.StringType",
      "NUMERIC, org.apache.spark.sql.types.DecimalType",
      "NUMERIC(10), org.apache.spark.sql.types.DecimalType",
      "NUMERIC(10,2), org.apache.spark.sql.types.DecimalType",
      "DECIMAL, org.apache.spark.sql.types.DecimalType",
      "DECIMAL(10), org.apache.spark.sql.types.DecimalType",
      "DECIMAL(10,2), org.apache.spark.sql.types.DecimalType",
      "DEC, org.apache.spark.sql.types.DecimalType",
      "DEC(10), org.apache.spark.sql.types.DecimalType",
      "DEC(10,2), org.apache.spark.sql.types.DecimalType",
      "SMALLINT, org.apache.spark.sql.types.ShortType",
      "INTEGER, org.apache.spark.sql.types.IntegerType",
      "INT, org.apache.spark.sql.types.IntegerType",
      "BIGINT, org.apache.spark.sql.types.LongType",
      "FLOAT, org.apache.spark.sql.types.DoubleType",
      "FLOAT(10), org.apache.spark.sql.types.DoubleType",
      "FLOAT(24), org.apache.spark.sql.types.FloatType",
      "REAL, org.apache.spark.sql.types.FloatType",
      "DOUBLE PRECISION, org.apache.spark.sql.types.DoubleType",
      "BOOLEAN, org.apache.spark.sql.types.BooleanType",
      "BINARY, org.apache.spark.sql.types.BinaryType",
      "BINARY(10), org.apache.spark.sql.types.BinaryType",
      "BINARY VARYING, org.apache.spark.sql.types.BinaryType",
      "BINARY VARYING(10), org.apache.spark.sql.types.BinaryType",
      "VARBINARY, org.apache.spark.sql.types.BinaryType",
      "VARBINARY(10), org.apache.spark.sql.types.BinaryType",
      "DATE, org.apache.spark.sql.types.DateType",
      "TIMESTAMP, org.apache.spark.sql.types.TimestampType",
      "TIMESTAMP(3), org.apache.spark.sql.types.TimestampType",
      "TIMESTAMP WITHOUT TIME ZONE, org.apache.spark.sql.types.TimestampType",
      "TIMESTAMP WITH TIME ZONE, org.apache.spark.sql.types.TimestampType",
      "INTERVAL, org.apache.spark.sql.types.StringType",
      "ROW, org.apache.spark.sql.types.StructType",
      "ROW(id INTEGER, name VARCHAR), org.apache.spark.sql.types.StructType",
      "ARRAY<INTEGER>, org.apache.spark.sql.types.ArrayType",
      "ARRAY<VARCHAR(10)>, org.apache.spark.sql.types.ArrayType"
  })
  void testParseValidTypes(String typeString, String expectedType) {
    Optional<DataType> result = AnsiSqlTypeParserUtils.parse(typeString);
    assertTrue(result.isPresent(), "Parser should return a result for valid type: " + typeString);
    assertEquals(expectedType, result.get().getClass().getName(), 
        "Type should match expected for: " + typeString);
    
    // Additional checks for specific types
    if (typeString.contains("DECIMAL") || typeString.contains("NUMERIC") || typeString.contains("DEC")) {
      if (typeString.contains(",")) {
        // Check precision and scale for decimal types with both parameters
        DecimalType decimalType = (DecimalType) result.get();
        assertEquals(10, decimalType.precision(), "Precision should be 10 for: " + typeString);
        assertEquals(2, decimalType.scale(), "Scale should be 2 for: " + typeString);
      }
    }
  }

  @Test
  void testParseInvalidType() {
    Optional<DataType> result = AnsiSqlTypeParserUtils.parse("INVALID_TYPE");
    assertFalse(result.isPresent(), "Parser should return empty for invalid type");
  }

  @Test
  void testCaseInsensitivity() {
    Optional<DataType> upperResult = AnsiSqlTypeParserUtils.parse("INTEGER");
    Optional<DataType> lowerResult = AnsiSqlTypeParserUtils.parse("integer");
    Optional<DataType> mixedResult = AnsiSqlTypeParserUtils.parse("InTeGeR");
    
    assertTrue(upperResult.isPresent());
    assertTrue(lowerResult.isPresent());
    assertTrue(mixedResult.isPresent());
    
    assertEquals(upperResult.get(), lowerResult.get());
    assertEquals(upperResult.get(), mixedResult.get());
  }
  
  @Test
  void testComplexTypes() {
    // Test ROW type with fields
    Optional<DataType> rowResult = AnsiSqlTypeParserUtils.parse("ROW(id INTEGER, name VARCHAR)");
    assertTrue(rowResult.isPresent());
    assertTrue(rowResult.get() instanceof StructType);
    StructType rowType = (StructType) rowResult.get();
    assertEquals(2, rowType.fields().length);
    assertEquals("id", rowType.fields()[0].name());
    assertEquals(DataTypes.IntegerType, rowType.fields()[0].dataType());
    assertEquals("name", rowType.fields()[1].name());
    assertEquals(DataTypes.StringType, rowType.fields()[1].dataType());
    
    // Test ARRAY type with element type
    Optional<DataType> arrayResult = AnsiSqlTypeParserUtils.parse("ARRAY<INTEGER>");
    assertTrue(arrayResult.isPresent());
    assertTrue(arrayResult.get().getClass().getName().contains("ArrayType"));
    assertEquals(DataTypes.IntegerType, ((org.apache.spark.sql.types.ArrayType) arrayResult.get()).elementType());
    
    // Test nested complex types
    Optional<DataType> nestedResult = AnsiSqlTypeParserUtils.parse("ARRAY<ROW(id INTEGER, values ARRAY<DECIMAL(10,2)>)>");
    assertTrue(nestedResult.isPresent());
    assertTrue(nestedResult.get().getClass().getName().contains("ArrayType"));
  }
}
