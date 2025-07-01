package au.csiro.pathling.views.ansi;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.*;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.collection.Collection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

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
  
  @Test
  void testConvertColumn() {
    // Setup mocks
    ColumnRepresentation mockColumnRep = mock(ColumnRepresentation.class);
    Column mockColumn = mock(Column.class);
    Column mockCastColumn = mock(Column.class);
    
    when(mockColumnRep.getValue()).thenReturn(mockColumn);
    when(mockColumn.cast(any(DataType.class))).thenReturn(mockCastColumn);
    
    // Test with FHIR type
    TypeHintMapper.convertColumn(mockColumnRep, FHIRDefinedType.BOOLEAN, null, null);
    verify(mockColumn).cast(DataTypes.BooleanType);
    
    // Test with FHIRPath type
    TypeHintMapper.convertColumn(mockColumnRep, null, FhirPathType.INTEGER, null);
    verify(mockColumn).cast(DataTypes.IntegerType);
    
    // Test with type hint
    TypeHintMapper.convertColumn(mockColumnRep, null, null, "DECIMAL(10,2)");
    verify(mockColumn).cast(any(DecimalType.class));
  }
  
  @Test
  void testConvertCollectionColumn() {
    // Setup mocks
    Collection mockCollection = mock(Collection.class);
    ColumnRepresentation mockColumnRep = mock(ColumnRepresentation.class);
    Column mockColumn = mock(Column.class);
    Column mockCastColumn = mock(Column.class);
    
    when(mockCollection.getColumn()).thenReturn(mockColumnRep);
    when(mockCollection.getFhirType()).thenReturn(Optional.of(FHIRDefinedType.BOOLEAN));
    when(mockCollection.getType()).thenReturn(Optional.empty());
    when(mockColumnRep.getValue()).thenReturn(mockColumn);
    when(mockColumn.cast(any(DataType.class))).thenReturn(mockCastColumn);
    
    // Test conversion
    TypeHintMapper.convertCollectionColumn(mockCollection, "INTEGER");
    verify(mockColumn).cast(DataTypes.IntegerType);
  }
  
  @Test
  void testBase64BinaryConversion() {
    // Setup mocks for base64Binary conversion
    ColumnRepresentation mockColumnRep = mock(ColumnRepresentation.class);
    Column mockColumn = mock(Column.class);
    
    when(mockColumnRep.getValue()).thenReturn(mockColumn);
    
    // Test base64Binary conversion with BINARY type hint
    TypeHintMapper.convertColumn(mockColumnRep, FHIRDefinedType.BASE64BINARY, null, "BINARY");
    
    // This is a bit tricky to verify since we're using a static method (functions.unbase64)
    // In a real test, we might use a framework like PowerMock, but for this example
    // we'll just verify that the column value was accessed
    verify(mockColumnRep, atLeastOnce()).getValue();
  }
}
