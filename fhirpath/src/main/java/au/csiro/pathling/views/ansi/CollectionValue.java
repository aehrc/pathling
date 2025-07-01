package au.csiro.pathling.views.ansi;

import au.csiro.pathling.fhirpath.collection.Collection;
import lombok.Value;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

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
   * Creates a function that casts a column representation to the specified data type.
   *
   * @param typeHint The target Spark SQL data type
   * @return A function that applies the cast to a column representation
   */
  @Nonnull
  private static Function<ColumnRepresentation, Column> makeCast(@Nonnull final DataType typeHint) {
    return columnRepresentation -> columnRepresentation.cast(typeHint).getValue();
  }

  // Maps from FHIR types to column conversion functions
  private static final Map<FHIRDefinedType, Function<ColumnRepresentation, Column>> FHIR_CONVERTER_MAP = new HashMap<>();

  // Maps from FHIRPath types to column conversion functions
  private static final Map<FhirPathType, Function<ColumnRepresentation, Column>> FHIRPATH_CONVERTER_MAP = new HashMap<>();

  static {
    // Initialize FHIR type converters
    // Special case for Base64Binary to handle decoding
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.BASE64BINARY, cr -> functions.unbase64(cr.getValue()));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.BOOLEAN, makeCast(DataTypes.BooleanType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.CANONICAL, makeCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.CODE, makeCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.DATE, makeCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.DATETIME, makeCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.DECIMAL, makeCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.ID, makeCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.INSTANT, makeCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.INTEGER, makeCast(DataTypes.IntegerType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.MARKDOWN, makeCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.OID, makeCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.POSITIVEINT, makeCast(DataTypes.IntegerType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.STRING, makeCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.TIME, makeCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.UNSIGNEDINT, makeCast(DataTypes.IntegerType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.URI, makeCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.URL, makeCast(DataTypes.StringType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.UUID, makeCast(DataTypes.StringType));

    // Initialize FHIRPath type converters
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.BOOLEAN, makeCast(DataTypes.BooleanType));
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.STRING, makeCast(DataTypes.StringType));
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.INTEGER, makeCast(DataTypes.IntegerType));
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.DECIMAL, makeCast(DataTypes.StringType));
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.DATE, makeCast(DataTypes.StringType));
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.TIME, makeCast(DataTypes.StringType));
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.DATETIME, makeCast(DataTypes.StringType));
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
        .map(fhirType -> FHIR_CONVERTER_MAP.getOrDefault(fhirType, ColumnRepresentation::getValue))
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
