package au.csiro.pathling.views.ansi;

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a collection value that can be converted to a Spark SQL column with optional type
 * hints. This class handles the conversion of FHIR and FHIRPath types to appropriate Spark SQL
 * types according to the ANSI SQL type hints specification.
 */
@Value(staticConstructor = "of")
public class CollectionValue {

  /**
   * The collection to be converted.
   */
  @Nonnull
  Collection collection;

  /**
   * Creates a function that performs element-wise cast of a column representation to the specified
   * data type.
   *
   * @param typeHint The target Spark SQL data type
   * @return A function that applies the cast to a column representation
   */
  @Nonnull
  private static Function<ColumnRepresentation, Column> makeElementCast(
      @Nonnull final DataType typeHint) {
    return cr -> cr.elementCast(typeHint).getValue();
  }


  /**
   * Creates a function casts  a column representation to the specified data type.
   */
  @Nonnull
  private static Function<ColumnRepresentation, Column> makeCast(@Nonnull final DataType typeHint) {
    return cr -> cr.pruneAnnotations().getValue().cast(typeHint);
    
    
  }


  // Maps from FHIR types to column conversion functions
  private static final Map<FHIRDefinedType, Function<ColumnRepresentation, Column>> FHIR_CONVERTER_MAP = new HashMap<>();

  // Maps from FHIRPath types to column conversion functions
  private static final Map<FhirPathType, Function<ColumnRepresentation, Column>> FHIRPATH_CONVERTER_MAP = new HashMap<>();

  static {
    // Initialize FHIR type converters
    // Special case for Base64Binary to handle decoding
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.BASE64BINARY, cr -> functions.unbase64(cr.getValue()));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.BOOLEAN, makeElementCast(DataTypes.BooleanType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.CANONICAL, makeElementCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.CODE, makeElementCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.DATE, makeElementCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.DATETIME, makeElementCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.DECIMAL, makeElementCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.ID, makeElementCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.INSTANT, makeElementCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.INTEGER, makeElementCast(DataTypes.IntegerType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.MARKDOWN, makeElementCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.OID, makeElementCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.POSITIVEINT, makeElementCast(DataTypes.IntegerType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.STRING, makeElementCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.TIME, makeElementCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.UNSIGNEDINT, makeElementCast(DataTypes.IntegerType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.URI, makeElementCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.URL, makeElementCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.UUID, makeElementCast(DataTypes.StringType));

    // Initialize FHIRPath type converters
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.BOOLEAN, makeElementCast(DataTypes.BooleanType));
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.STRING, makeElementCast(DataTypes.StringType));
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.INTEGER, makeElementCast(DataTypes.IntegerType));
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.DECIMAL, makeElementCast(DataTypes.StringType));
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.DATE, makeElementCast(DataTypes.StringType));
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.TIME, makeElementCast(DataTypes.StringType));
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.DATETIME, makeElementCast(DataTypes.StringType));
    // Quantity and Coding are complex types that remain as structs
  }

  /**
   * Gets the default converter function for a collection based on its FHIR or FHIRPath type. This
   * implements the default type mapping as specified in the ANSI SQL type hints specification.
   *
   * @param collection The collection to get a converter for
   * @return A function that converts the collection's column representation to a Spark SQL column
   */
  @Nonnull
  public static Function<ColumnRepresentation, Column> getDefaultConverter(
      @Nonnull final Collection collection) {

    // First check for FHIR type, then FHIRPath type, falling back to identity function
    return collection.getFhirType()
        .map(fhirType -> FHIR_CONVERTER_MAP.getOrDefault(fhirType,
            cr -> cr.pruneAnnotations().getValue()))
        .orElseGet(
            () -> collection.getType()
                .map(type -> FHIRPATH_CONVERTER_MAP.getOrDefault(type,
                    ColumnRepresentation::getValue))
                .orElse(ColumnRepresentation::getValue)
        );
  }

  /**
   * Gets the Spark SQL column for this collection, applying the specified data type hint if
   * provided. If no type hint is provided, uses the default converter based on the collection's
   * type.
   *
   * @param maybeDataType Optional data type hint to apply
   * @return A Spark SQL column with the appropriate type conversion applied
   */
  @Nonnull
  public Column get(@Nonnull final Optional<DataType> maybeDataType) {
    return maybeDataType.map(CollectionValue::makeCast)
        .orElseGet(() -> getDefaultConverter(collection))
        .apply(collection.getColumn());
  }
}
