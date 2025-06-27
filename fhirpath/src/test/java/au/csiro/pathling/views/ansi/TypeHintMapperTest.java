package au.csiro.pathling.views.ansi;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.sql.types.*;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import au.csiro.pathling.fhirpath.FhirPathType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TypeHintMapperTest {

  @Test
  void testGetDataTypeForFhirTypeWithoutHint() {
    assertEquals(DataTypes.BooleanType, 
        TypeHintMapper.getDataTypeForFhirType(FHIRDefinedType.BOOLEAN, null));
    assertEquals(DataTypes.StringType, 
        TypeHintMapper.getDataTypeForFhirType(FHIRDefinedType.STRING, null));
    assertEquals(DataTypes.IntegerType, 
        TypeHintMapper.getDataTypeForFhirType(FHIRDefinedType.INTEGER, null));
    assertEquals(DataTypes.StringType, 
        TypeHintMapper.getDataTypeForFhirType(FHIRDefinedType.DECIMAL, null));
  }

  @Test
  void testGetDataTypeForFhirTypeWithHint() {
    // Type hint should override default mapping
    assertEquals(DataTypes.IntegerType, 
        TypeHintMapper.getDataTypeForFhirType(FHIRDefinedType.STRING, "INTEGER"));
    assertEquals(DataTypes.StringType, 
        TypeHintMapper.getDataTypeForFhirType(FHIRDefinedType.BOOLEAN, "VARCHAR"));
    
    // Test with invalid hint - should fall back to default
    assertEquals(DataTypes.BooleanType, 
        TypeHintMapper.getDataTypeForFhirType(FHIRDefinedType.BOOLEAN, "INVALID_TYPE"));
  }

  @Test
  void testGetDataTypeForFhirPathTypeWithoutHint() {
    assertEquals(DataTypes.BooleanType, 
        TypeHintMapper.getDataTypeForFhirPathType(FhirPathType.BOOLEAN, null));
    assertEquals(DataTypes.StringType, 
        TypeHintMapper.getDataTypeForFhirPathType(FhirPathType.STRING, null));
    assertEquals(DataTypes.IntegerType, 
        TypeHintMapper.getDataTypeForFhirPathType(FhirPathType.INTEGER, null));
    assertEquals(DataTypes.StringType, 
        TypeHintMapper.getDataTypeForFhirPathType(FhirPathType.DECIMAL, null));
  }

  @Test
  void testGetDataTypeForFhirPathTypeWithHint() {
    // Type hint should override default mapping
    assertEquals(DataTypes.IntegerType, 
        TypeHintMapper.getDataTypeForFhirPathType(FhirPathType.STRING, "INTEGER"));
    assertEquals(DataTypes.StringType, 
        TypeHintMapper.getDataTypeForFhirPathType(FhirPathType.BOOLEAN, "VARCHAR"));
    
    // Test with invalid hint - should fall back to default
    assertEquals(DataTypes.BooleanType, 
        TypeHintMapper.getDataTypeForFhirPathType(FhirPathType.BOOLEAN, "INVALID_TYPE"));
  }

  @Test
  void testGetDataTypeWithBothTypes() {
    // FHIR type should take precedence over FHIRPath type
    assertEquals(DataTypes.BooleanType, 
        TypeHintMapper.getDataType(FHIRDefinedType.BOOLEAN, FhirPathType.STRING, null));
    
    // Type hint should override both
    assertEquals(DataTypes.IntegerType, 
        TypeHintMapper.getDataType(FHIRDefinedType.BOOLEAN, FhirPathType.STRING, "INTEGER"));
  }

  @Test
  void testGetDataTypeWithNoTypeInfo() {
    // Should default to string with no type info
    assertEquals(DataTypes.StringType, 
        TypeHintMapper.getDataType(null, null, null));
    
    // Type hint should be used if provided
    assertEquals(DataTypes.IntegerType, 
        TypeHintMapper.getDataType(null, null, "INTEGER"));
  }

  @ParameterizedTest
  @CsvSource({
      "DECIMAL(10,2), org.apache.spark.sql.types.DecimalType",
      "VARCHAR(255), org.apache.spark.sql.types.StringType",
      "TIMESTAMP WITH TIME ZONE, org.apache.spark.sql.types.TimestampType"
  })
  void testComplexTypeHints(String typeHint, String expectedType) {
    DataType result = TypeHintMapper.getDataType(null, null, typeHint);
    assertEquals(expectedType, result.getClass().getName(), 
        "Type should match expected for hint: " + typeHint);
  }
}
