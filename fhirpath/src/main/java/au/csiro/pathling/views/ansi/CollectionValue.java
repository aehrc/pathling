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

@Value(staticConstructor = "of")
public class CollectionValue {


  @Nonnull
  Collection collection;

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
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.BASE64BINARY,
        cr -> functions.unbase64(cr.getValue()));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.BOOLEAN,
        cr -> cr.getValue().cast(DataTypes.BooleanType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.INTEGER,
        cr -> cr.getValue().cast(DataTypes.IntegerType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.POSITIVEINT,
        cr -> cr.getValue().cast(DataTypes.IntegerType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.UNSIGNEDINT,
        cr -> cr.getValue().cast(DataTypes.IntegerType));
    FHIR_CONVERTER_MAP.put(FHIRDefinedType.DECIMAL,
        cr -> cr.getValue().cast(DataTypes.StringType));

    // Initialize FHIRPath type converters
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.BOOLEAN,
        cr -> cr.getValue().cast(DataTypes.BooleanType));
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.STRING,
        cr -> cr.getValue().cast(DataTypes.StringType));
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.INTEGER,
        cr -> cr.getValue().cast(DataTypes.IntegerType));
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.DECIMAL,
        cr -> cr.getValue().cast(DataTypes.StringType));
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.DATE,
        cr -> cr.getValue().cast(DataTypes.StringType));
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.TIME,
        cr -> cr.getValue().cast(DataTypes.StringType));
    FHIRPATH_CONVERTER_MAP.put(FhirPathType.DATETIME,
        cr -> cr.getValue().cast(DataTypes.StringType));
  }
  
  @Nonnull
  public static Function<ColumnRepresentation, Column> getDefaultConverter(
      @Nonnull final Collection collection) {

    // the default converter for FHIR defined types that do not have explicit mapping is to get value (recursively strip annoations)
    return collection.getFhirType()
        .map(fhirType -> FHIR_CONVERTER_MAP.getOrDefault(fhirType, ColumnRepresentation::getValue))
        .orElseGet(
            () -> collection.getType()
                .map(type -> FHIRPATH_CONVERTER_MAP.getOrDefault(type,
                    ColumnRepresentation::getValue))
                .orElse(ColumnRepresentation::getValue)
        );
  }

  @Nonnull
  public Column get(@Nonnull final Optional<DataType> maybeDataType) {
    return maybeDataType.map(CollectionValue::makeCast)
        .orElseGet(() -> getDefaultConverter(collection))
        .apply(collection.getColumn());
  }
  
}
