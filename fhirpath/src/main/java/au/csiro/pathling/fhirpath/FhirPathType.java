package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import jakarta.annotation.Nonnull;
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

  BOOLEAN("Boolean", DataTypes.BooleanType),
  STRING("String", DataTypes.StringType),
  INTEGER("Integer", DataTypes.IntegerType),
  DECIMAL("Decimal", DecimalCollection.getDecimalType()),
  DATE("Date", DataTypes.StringType),
  DATETIME("DateTime", DataTypes.StringType),
  TIME("Time", DataTypes.StringType),
  QUANTITY("Quantity", QuantityEncoding.dataType()),
  CODING("Coding", CodingEncoding.codingStructType());

  @Nonnull
  private final String typeSpecifier;

  @Nonnull
  private final DataType sqlDataType;


  FhirPathType(@Nonnull final String typeSpecifier, @Nonnull final DataType sqlDataType) {
    this.typeSpecifier = typeSpecifier;
    this.sqlDataType = sqlDataType;
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
    return Optional.ofNullable(FhirTypeMapping.get(fhirType));
  }

}
