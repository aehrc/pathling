package au.csiro.pathling.views.ansi;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import java.util.Optional;
import java.util.stream.Stream;

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
        Arguments.of("FLOAT(10)", DataTypes.DoubleType),
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
        Arguments.of("TIMESTAMP", DataTypes.TimestampType),
        Arguments.of("TIMESTAMP(3)", DataTypes.TimestampType),
        Arguments.of("TIMESTAMP WITHOUT TIME ZONE", DataTypes.TimestampType),
        Arguments.of("TIMESTAMP WITH TIME ZONE", DataTypes.TimestampType),
        Arguments.of("INTERVAL", DataTypes.StringType),
        
        // Simple complex types
        Arguments.of("ROW", DataTypes.createStructType(new StructField[0])),
        Arguments.of("ARRAY<INTEGER>", DataTypes.createArrayType(DataTypes.IntegerType)),
        Arguments.of("ARRAY<VARCHAR(10)>", DataTypes.createArrayType(DataTypes.StringType))
    );
  }

  @ParameterizedTest
  @MethodSource("validTypesProvider")
  void testParseValidTypes(String typeString, DataType expectedType) {
    Optional<DataType> result = AnsiSqlTypeParserUtils.parse(typeString);
    assertTrue(result.isPresent(), "Parser should return a result for valid type: " + typeString);
    assertEquals(expectedType, result.get(), "Type should match expected for: " + typeString);
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
  void testComplexRowType() {
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
  }
  
  @Test
  void testNestedComplexTypes() {
    // Test nested complex types
    Optional<DataType> nestedResult = AnsiSqlTypeParserUtils.parse("ARRAY<ROW(id INTEGER, values ARRAY<DECIMAL(10,2)>)>");
    assertTrue(nestedResult.isPresent());
    assertTrue(nestedResult.get() instanceof ArrayType);
    
    ArrayType arrayType = (ArrayType) nestedResult.get();
    assertTrue(arrayType.elementType() instanceof StructType);
    
    StructType rowType = (StructType) arrayType.elementType();
    assertEquals(2, rowType.fields().length);
    assertEquals("id", rowType.fields()[0].name());
    assertEquals(DataTypes.IntegerType, rowType.fields()[0].dataType());
    assertEquals("values", rowType.fields()[1].name());
    assertTrue(rowType.fields()[1].dataType() instanceof ArrayType);
    
    ArrayType valuesArrayType = (ArrayType) rowType.fields()[1].dataType();
    assertTrue(valuesArrayType.elementType() instanceof DecimalType);
    DecimalType decimalType = (DecimalType) valuesArrayType.elementType();
    assertEquals(10, decimalType.precision());
    assertEquals(2, decimalType.scale());
  }
}
