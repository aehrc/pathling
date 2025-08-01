package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.CodingCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DateCollection;
import au.csiro.pathling.fhirpath.collection.DateTimeCollection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.collection.TimeCollection;
import au.csiro.pathling.fhirpath.encoding.CodingSchema;
import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents one of the types defined within the FHIRPath specification.
 *
 * @author John Grimes
 */
@Getter
public enum FhirPathType {

  /**
   * Boolean FHIRPath type.
   */
  BOOLEAN("Boolean", DataTypes.BooleanType, BooleanCollection.class, FHIRDefinedType.BOOLEAN),

  /**
   * String FHIRPath type.
   */
  STRING("String", DataTypes.StringType, StringCollection.class, FHIRDefinedType.STRING),

  /**
   * Integer FHIRPath type.
   */
  INTEGER("Integer", DataTypes.IntegerType, IntegerCollection.class, FHIRDefinedType.INTEGER),

  /**
   * Decimal FHIRPath type.
   */
  DECIMAL("Decimal", DecimalCollection.getDecimalType(), DecimalCollection.class,
      FHIRDefinedType.DECIMAL),

  /**
   * Date FHIRPath type.
   */
  DATE("Date", DataTypes.StringType, DateCollection.class, FHIRDefinedType.DATE),

  /**
   * DateTime FHIRPath type.
   */
  DATETIME("DateTime", DataTypes.StringType, DateTimeCollection.class, FHIRDefinedType.DATETIME),

  /**
   * Time FHIRPath type.
   */
  TIME("Time", DataTypes.StringType, TimeCollection.class, FHIRDefinedType.TIME),

  /**
   * Coding FHIRPath type.
   */
  CODING("Coding", CodingSchema.codingStructType(), CodingCollection.class,
      FHIRDefinedType.CODING);

  @Nonnull
  private final String typeSpecifier;

  @Nonnull
  private final DataType sqlDataType;

  @Nonnull
  private final Class<? extends Collection> collectionClass;

  @Nonnull
  private final FHIRDefinedType defaultFhirType;

  // Maps FHIR types to FhirPathType
  @Nonnull
  private static final Map<FHIRDefinedType, FhirPathType> FHIR_TYPE_TO_FHIR_PATH_TYPE =
      new ImmutableMap.Builder<FHIRDefinedType, FhirPathType>()
          .put(FHIRDefinedType.BOOLEAN, BOOLEAN)
          .put(FHIRDefinedType.STRING, STRING)
          .put(FHIRDefinedType.URI, STRING)
          .put(FHIRDefinedType.URL, STRING)
          .put(FHIRDefinedType.CANONICAL, STRING)
          .put(FHIRDefinedType.CODE, STRING)
          .put(FHIRDefinedType.OID, STRING)
          .put(FHIRDefinedType.ID, STRING)
          .put(FHIRDefinedType.UUID, STRING)
          .put(FHIRDefinedType.MARKDOWN, STRING)
          .put(FHIRDefinedType.BASE64BINARY, STRING)
          .put(FHIRDefinedType.INTEGER, INTEGER)
          .put(FHIRDefinedType.UNSIGNEDINT, INTEGER)
          .put(FHIRDefinedType.POSITIVEINT, INTEGER)
          .put(FHIRDefinedType.DECIMAL, DECIMAL)
          .put(FHIRDefinedType.DATE, DATE)
          .put(FHIRDefinedType.DATETIME, DATETIME)
          .put(FHIRDefinedType.INSTANT, DATETIME)
          .put(FHIRDefinedType.TIME, TIME)
          .put(FHIRDefinedType.CODING, CODING)
          .build();

  FhirPathType(@Nonnull final String typeSpecifier, @Nonnull final DataType sqlDataType,
      @Nonnull final Class<? extends Collection> collectionClass,
      @Nonnull final FHIRDefinedType defaultFhirType) {
    this.typeSpecifier = typeSpecifier;
    this.sqlDataType = sqlDataType;
    this.collectionClass = collectionClass;
    this.defaultFhirType = defaultFhirType;
  }

  /**
   * @param typeSpecifier a type specifier
   * @return true if the type specifier is a valid FHIRPath type
   */
  public static boolean isValidFhirPathType(@Nonnull final String typeSpecifier) {
    for (final FhirPathType fhirPathType : FhirPathType.values()) {
      if (fhirPathType.getTypeSpecifier().equals(typeSpecifier)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param fhirType a {@link FHIRDefinedType}
   * @return the corresponding {@link FhirPathType} according to the rules of automatic conversion
   * within the FHIR spec
   */
  @Nonnull
  public static Optional<FhirPathType> forFhirType(@Nonnull final FHIRDefinedType fhirType) {
    return Optional.ofNullable(FHIR_TYPE_TO_FHIR_PATH_TYPE.get(fhirType));
  }

  /**
   * @return a list of FHIR types that correspond to this FhirPathType
   */
  @Nonnull
  public List<FHIRDefinedType> getFhirTypes() {
    // This method is currently returning an empty list
    return FHIR_TYPE_TO_FHIR_PATH_TYPE.entrySet().stream()
        .filter(entry -> entry.getValue() == this)
        .map(Map.Entry::getKey)
        .toList();
  }
}
