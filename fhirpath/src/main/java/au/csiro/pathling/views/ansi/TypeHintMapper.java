package au.csiro.pathling.views.ansi;

import org.apache.spark.sql.types.*;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import au.csiro.pathling.fhirpath.FhirPathType;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Maps FHIR and FHIRPath types to Spark SQL DataTypes, with support for ANSI SQL type hints.
 */
public class TypeHintMapper {

  private static final Map<FHIRDefinedType, DataType> FHIR_TYPE_MAP = new HashMap<>();
  private static final Map<FhirPathType, DataType> FHIRPATH_TYPE_MAP = new HashMap<>();

  static {
    // Initialize FHIR type mappings
    FHIR_TYPE_MAP.put(FHIRDefinedType.BASE64BINARY, DataTypes.BinaryType);
    FHIR_TYPE_MAP.put(FHIRDefinedType.BOOLEAN, DataTypes.BooleanType);
    FHIR_TYPE_MAP.put(FHIRDefinedType.CANONICAL, DataTypes.StringType);
    FHIR_TYPE_MAP.put(FHIRDefinedType.CODE, DataTypes.StringType);
    FHIR_TYPE_MAP.put(FHIRDefinedType.DATE, DataTypes.StringType);
    FHIR_TYPE_MAP.put(FHIRDefinedType.DATETIME, DataTypes.StringType);
    FHIR_TYPE_MAP.put(FHIRDefinedType.DECIMAL, DataTypes.StringType);
    FHIR_TYPE_MAP.put(FHIRDefinedType.ID, DataTypes.StringType);
    FHIR_TYPE_MAP.put(FHIRDefinedType.INSTANT, DataTypes.StringType);
    FHIR_TYPE_MAP.put(FHIRDefinedType.INTEGER, DataTypes.IntegerType);
    //FHIR_TYPE_MAP.put(FHIRDefinedType.INTEGER64, DataTypes.LongType);
    FHIR_TYPE_MAP.put(FHIRDefinedType.MARKDOWN, DataTypes.StringType);
    FHIR_TYPE_MAP.put(FHIRDefinedType.OID, DataTypes.StringType);
    FHIR_TYPE_MAP.put(FHIRDefinedType.POSITIVEINT, DataTypes.IntegerType);
    FHIR_TYPE_MAP.put(FHIRDefinedType.STRING, DataTypes.StringType);
    FHIR_TYPE_MAP.put(FHIRDefinedType.TIME, DataTypes.StringType);
    FHIR_TYPE_MAP.put(FHIRDefinedType.UNSIGNEDINT, DataTypes.IntegerType);
    FHIR_TYPE_MAP.put(FHIRDefinedType.URI, DataTypes.StringType);
    FHIR_TYPE_MAP.put(FHIRDefinedType.URL, DataTypes.StringType);
    FHIR_TYPE_MAP.put(FHIRDefinedType.UUID, DataTypes.StringType);
    
    // Initialize FHIRPath type mappings
    FHIRPATH_TYPE_MAP.put(FhirPathType.BOOLEAN, DataTypes.BooleanType);
    FHIRPATH_TYPE_MAP.put(FhirPathType.STRING, DataTypes.StringType);
    FHIRPATH_TYPE_MAP.put(FhirPathType.INTEGER, DataTypes.IntegerType);
    FHIRPATH_TYPE_MAP.put(FhirPathType.DECIMAL, DataTypes.StringType);
    FHIRPATH_TYPE_MAP.put(FhirPathType.DATE, DataTypes.StringType);
    FHIRPATH_TYPE_MAP.put(FhirPathType.TIME, DataTypes.StringType);
    FHIRPATH_TYPE_MAP.put(FhirPathType.DATETIME, DataTypes.StringType);
    // Quantity and Coding are complex types represented as structs
  }

  /**
   * Get the Spark SQL DataType for a given FHIR type, with optional ANSI SQL type hint.
   *
   * @param fhirType the FHIR type
   * @param typeHint optional ANSI SQL type hint
   * @return the corresponding Spark SQL DataType
   */
  @Nonnull
  public static DataType getDataTypeForFhirType(
      @Nullable final FHIRDefinedType fhirType,
      @Nullable final String typeHint) {
    
    // If there's a type hint, try to parse it first
    if (typeHint != null && !typeHint.isEmpty()) {
      Optional<DataType> hintedType = AnsiSqlTypeParserUtils.parse(typeHint);
      if (hintedType.isPresent()) {
        return hintedType.get();
      }
    }
    
    // Fall back to default mapping if type hint is invalid or not provided
    if (fhirType != null && FHIR_TYPE_MAP.containsKey(fhirType)) {
      return FHIR_TYPE_MAP.get(fhirType);
    }
    
    // Default to string if no mapping exists
    return DataTypes.StringType;
  }

  /**
   * Get the Spark SQL DataType for a given FHIRPath type, with optional ANSI SQL type hint.
   *
   * @param fhirPathType the FHIRPath type
   * @param typeHint optional ANSI SQL type hint
   * @return the corresponding Spark SQL DataType
   */
  @Nonnull
  public static DataType getDataTypeForFhirPathType(
      @Nullable final FhirPathType fhirPathType,
      @Nullable final String typeHint) {
    
    // If there's a type hint, try to parse it first
    if (typeHint != null && !typeHint.isEmpty()) {
      Optional<DataType> hintedType = AnsiSqlTypeParserUtils.parse(typeHint);
      if (hintedType.isPresent()) {
        return hintedType.get();
      }
    }
    
    // Fall back to default mapping if type hint is invalid or not provided
    if (fhirPathType != null && FHIRPATH_TYPE_MAP.containsKey(fhirPathType)) {
      return FHIRPATH_TYPE_MAP.get(fhirPathType);
    }
    
    // Default to string if no mapping exists
    return DataTypes.StringType;
  }

  /**
   * Get the Spark SQL DataType based on FHIR and FHIRPath types, with optional ANSI SQL type hint.
   * FHIR type takes precedence over FHIRPath type when both are provided.
   *
   * @param fhirType the FHIR type
   * @param fhirPathType the FHIRPath type
   * @param typeHint optional ANSI SQL type hint
   * @return the corresponding Spark SQL DataType
   */
  @Nonnull
  public static DataType getDataType(
      @Nullable final FHIRDefinedType fhirType,
      @Nullable final FhirPathType fhirPathType,
      @Nullable final String typeHint) {
    
    // FHIR type takes precedence over FHIRPath type
    if (fhirType != null) {
      return getDataTypeForFhirType(fhirType, typeHint);
    } else if (fhirPathType != null) {
      return getDataTypeForFhirPathType(fhirPathType, typeHint);
    }
    
    // If there's a type hint but no type information, try to parse the hint
    if (typeHint != null && !typeHint.isEmpty()) {
      Optional<DataType> hintedType = AnsiSqlTypeParserUtils.parse(typeHint);
      if (hintedType.isPresent()) {
        return hintedType.get();
      }
    }
    
    // Default to string if no type information is available
    return DataTypes.StringType;
  }
}
